#include "agent_session.h"
#include "forwarder.h"
#include "xfr_channel.h"
#include <algorithm>

using Clock = std::chrono::steady_clock;

AgentSession::RelayEndpoint AgentSession::RelayEndpoint::from_params(const njson &p, RelayProtocol protocol)
{
    RelayEndpoint endpoint;
    endpoint.address = config::required_string(p, "data_address", "relay endpoint");
    endpoint.port = config::message_data_port(p);
    endpoint.uuid = config::require_unsigned(p, "uuid", true);
    endpoint.ticket = config::require_unsigned(p, "ticket", true);
    endpoint.flow_id = config::optional_unsigned(p, "flow_id", true);
    if (protocol == RelayProtocol::Udp)
    {
        endpoint.session_id = config::require_unsigned(p, "session_id", true);
    }
    return endpoint;
}

AgentSession::AgentSession(Forwarder &owner, std::uint64_t request, ServiceKey service, std::string destination,
                           Clock::time_point deadline)
    : owner(owner), request(request), service(std::move(service)), destination(std::move(destination)),
      deadline(deadline),
      data(this->service.protocol == RelayProtocol::Udp ? Data(std::in_place_type<DatagramData>, owner.executor())
                                                        : Data(std::in_place_type<StreamData>, owner.executor())),
      changed(owner.executor(), deadline)
{
}

bool AgentSession::cancel(std::string cause)
{
    if (!reason.empty())
    {
        return false;
    }

    reason = cause.empty() ? "relay cancelled" : std::move(cause);
    close_io();
    return true;
}

void AgentSession::fail(std::string cause)
{
    if (!cancel(cause.empty() ? "relay failed" : std::move(cause)))
    {
        return;
    }

    const auto uuid = endpoint ? endpoint->uuid : 0;
    PROXY_ERROR_PRINT("Relay failed %s %s service=%s request_id=%llu uuid=%llu reason=%s",
                      relay_protocol_name(service.protocol).data(), producer() ? "producer" : "consumer",
                      service.service.c_str(), static_cast<unsigned long long>(request),
                      static_cast<unsigned long long>(uuid), reason.c_str());
}

void AgentSession::close_io()
{
    changed.cancel();
    std::visit(
        [](auto &sockets) {
            sockets.resolver.cancel();
            asio::error_code ignored;
            if constexpr (std::is_same_v<std::decay_t<decltype(sockets)>, StreamData>)
            {
                sockets.local.close(ignored);
                if (sockets.tls)
                {
                    sockets.tls->lowest_layer().close(ignored);
                }
                else
                {
                    sockets.transfer.close(ignored);
                }
            }
            else
            {
                sockets.transfer.close(ignored);
                if (sockets.target)
                    sockets.target->close(ignored);
            }
        },
        data);
}

void AgentSession::handle(const CtrlMessage &message)
{
    if (node_closed)
    {
        return;
    }

    const auto &p = config::message_params(message);
    if (p.contains("epoch") && p.at("epoch") != selection.epoch)
    {
        return;
    }

    if (endpoint.has_value() && p.contains("flow_id") && (!endpoint->flow_id || p.at("flow_id") != *endpoint->flow_id))
    {
        return;
    }

    const auto command = message.type();
    if (!reason.empty())
    {
        // Local cancellation still accepts the Node's terminal acknowledgement.
        node_closed = command == CtrlCommand::RelayError || command == CtrlCommand::RelayClosed;
        return;
    }

    if (command == CtrlCommand::RelayOpened && !endpoint)
    {
        endpoint = RelayEndpoint::from_params(p, service.protocol);
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayReady && !tunnel_ready())
    {
        phase = Phase::TunnelReady;
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayError || command == CtrlCommand::RelayClosed)
    {
        auto cause = config::optional_string(p, "reason", "relay closed");
        const bool completed = command == CtrlCommand::RelayClosed && tunnel_ready() &&
                               service.protocol != RelayProtocol::Udp && cause == "stream complete";
        if (p.contains("stage"))
        {
            cause = config::optional_string(p, "stage", "control") + ": " + cause;
        }

        // Commit the terminal state only after its metadata has been validated.
        node_closed = true;
        if (completed)
        {
            return;
        }

        if (command == CtrlCommand::RelayError)
        {
            fail(std::move(cause));
        }
        else
        {
            cancel(std::move(cause));
        }
    }
}

asio::awaitable<void> AgentSession::run()
{
    // relay_finished() removes the owner's references before this coroutine returns.
    auto keep_alive = shared_from_this();
    bool submitted = false;
    std::string stage = "select entry";
    try
    {
        if (!producer())
        {
            if (!reason.empty())
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            // Selection accesses control_io state; attach and transfer stay on transfer_io.
            selection = co_await owner.select_relay(service, destination, deadline);
            if (!reason.empty())
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            if (Clock::now() >= deadline)
            {
                throw std::runtime_error("relay establishment timed out");
            }

            stage = "relay.open";
            njson values{{"request_id", request},
                         {"service", service.service},
                         {"protocol", relay_protocol_name(service.protocol)}};
            if (selection.path.size() > 1)
            {
                values["path"] = selection.path;
                values["epoch"] = selection.epoch;
            }

            if (!owner.send_control(selection.server, CtrlMessage(CtrlCommand::RelayOpen, std::move(values))))
            {
                throw std::runtime_error("primary control unavailable");
            }

            submitted = true;
            const auto [error] = co_await changed.async_wait(use_nothrow_awaitable);
            if (!error || Clock::now() >= deadline)
            {
                throw std::runtime_error("relay establishment timed out");
            }
        }

        stage = "attach";
        co_await attach();
        if (service.protocol == RelayProtocol::Tls)
        {
            stage = "end-to-end TLS";
            co_await establish_tls();
        }
        if (!reason.empty())
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        if (Clock::now() >= deadline)
        {
            throw std::runtime_error("relay establishment timed out");
        }
        phase = Phase::Ready;
        PROXY_INFO_PRINT("Relay [+] %s %s service=%s uuid=%llu", relay_protocol_name(service.protocol).data(),
                         producer() ? "producer" : "consumer", service.service.c_str(),
                         static_cast<unsigned long long>(endpoint->uuid));
        stage = "transfer";
        if (service.protocol == RelayProtocol::Udp)
        {
            auto &sockets = std::get<DatagramData>(data);
            if (producer())
            {
                co_await relay_udp_connected(*sockets.target, sockets.transfer, sockets.session_header);
            }
            else
            {
                co_await owner.relay_datagrams(*this);
            }
        }
        else
        {
            auto &sockets = std::get<StreamData>(data);
            if (sockets.tls)
            {
                co_await relay_tls(sockets.local, *sockets.tls);
                stage = "TLS shutdown";
                co_await finish_tls();
            }
            else
            {
                co_await relay_tcp(sockets.local, sockets.transfer);
            }
        }
    }
    catch (const std::exception &error)
    {
        auto cause = std::string(error.what());
        if (!ready() && Clock::now() >= deadline)
        {
            cause = "relay establishment deadline exceeded: " + cause;
        }
        fail(stage + ": " + cause);
    }

    const auto uuid = endpoint ? endpoint->uuid : 0;
    if (!reason.empty() && !node_closed && (submitted || producer()))
    {
        njson values{{"request_id", request}, {"reason", reason}};
        if (uuid)
        {
            values["uuid"] = uuid;
        }
        owner.send_control(selection.server, CtrlMessage(producer() && !tunnel_ready() ? CtrlCommand::RelayReject
                                                                                       : CtrlCommand::RelayCancel,
                                                         std::move(values)));
    }
    if (ready())
    {
        PROXY_INFO_PRINT("Relay [x] %s %s service=%s uuid=%llu reason=%s", relay_protocol_name(service.protocol).data(),
                         producer() ? "producer" : "consumer", service.service.c_str(),
                         static_cast<unsigned long long>(uuid), reason.empty() ? "stream complete" : reason.c_str());
    }

    if (reason.empty())
        close_io(); // cancel()/fail() already closed I/O on aborted sessions.
    owner.relay_finished(*this);
}

asio::awaitable<void> AgentSession::attach()
{
    const auto &options = owner.session_options();
    if (!reason.empty())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }

    const auto &host = endpoint->address;
    const auto port = endpoint->port;
    const auto uuid = endpoint->uuid;
    const auto ticket = endpoint->ticket;
    RelayAttach attachment{producer() ? RelayAttach::Producer : RelayAttach::Consumer, uuid, ticket, {}};
    BytesBuf frame;

    if (service.protocol == RelayProtocol::Udp)
    {
        frame = WireMessage::pack(RelayAttach::to_msg(attachment));
        auto &sockets = std::get<DatagramData>(data);
        sockets.session_header = DatagramHeader::encode(endpoint->session_id);
        sockets.maximum_payload =
            endpoint->flow_id.has_value() ? LnkFrameHeader::maximum_payload : DatagramHeader::maximum_user_payload;
        if (target)
        {
            auto endpoints = co_await sockets.resolver.async_resolve(
                target->target_host, std::to_string(target->target_port),
                asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
            if (!reason.empty())
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            const auto address = endpoints.begin()->endpoint();
            sockets.target.emplace(owner.executor());
            sockets.target->open(address.protocol());
            sockets.target->connect(address);
        }
        auto endpoints = co_await sockets.resolver.async_resolve(
            host, std::to_string(port), asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        if (!reason.empty())
        {
            throw asio::system_error(asio::error::operation_aborted);
        }

        const auto address = endpoints.begin()->endpoint();
        sockets.transfer.open(address.protocol());
        sockets.transfer.connect(address);
    }
    else
    {
        auto &sockets = std::get<StreamData>(data);
        if (target && service.protocol != RelayProtocol::Tls)
        {
            co_await connect_target();
        }
        auto endpoints = co_await sockets.resolver.async_resolve(
            host, std::to_string(port), asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        co_await asio::async_connect(sockets.transfer, endpoints,
                                     asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        attachment.connection_id.emplace();
        co_await asio::async_read(sockets.transfer, asio::buffer(*attachment.connection_id),
                                  asio::cancel_at(deadline, asio::use_awaitable));
        if (!owner.send_control(selection.server, RelayAttach::to_msg(attachment)))
        {
            throw std::runtime_error("primary control unavailable for attachment");
        }
    }

    while (!tunnel_ready() && reason.empty())
    {
        if (Clock::now() >= deadline)
        {
            throw std::runtime_error("relay.ready timed out");
        }
        auto wakeup = deadline;
        if (service.protocol == RelayProtocol::Udp)
        {
            auto &sockets = std::get<DatagramData>(data);
            const auto sent = co_await sockets.transfer.async_send(asio::buffer(frame),
                                                                   asio::cancel_at(deadline, asio::use_awaitable));
            if (sent != frame.size())
            {
                throw std::runtime_error("truncated relay.attach");
            }
            wakeup = std::min(deadline, Clock::now() + std::chrono::milliseconds(500));
            if (tunnel_ready() || !reason.empty())
            {
                break;
            }
        }
        changed.expires_at(wakeup);
        co_await changed.async_wait(use_nothrow_awaitable);
    }

    if (!reason.empty())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}

Clock::duration AgentSession::remaining(Clock::duration timeout) const
{
    if (!reason.empty())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    const auto budget = deadline - Clock::now();
    if (budget <= Clock::duration::zero())
    {
        throw std::runtime_error("relay establishment timed out");
    }
    return std::min(budget, timeout);
}

asio::awaitable<void> AgentSession::connect_target()
{
    auto &sockets = std::get<StreamData>(data);
    const auto timeout = owner.session_options().connect_timeout;
    const auto endpoints =
        co_await sockets.resolver.async_resolve(target->target_host, std::to_string(target->target_port),
                                                asio::cancel_after(remaining(timeout), asio::use_awaitable));
    co_await asio::async_connect(sockets.local, endpoints, asio::cancel_after(remaining(timeout), asio::use_awaitable));
}

asio::awaitable<void> AgentSession::establish_tls()
{
    auto &sockets = std::get<StreamData>(data);
    const auto &options = owner.session_options();
    sockets.tls.emplace(std::move(sockets.transfer), options.ssl_context);
    auto &tls = *sockets.tls;
    tls.next_layer().set_option(asio::ip::tcp::no_delay(true));
    co_await tls.async_handshake(producer() ? asio::ssl::stream_base::server : asio::ssl::stream_base::client,
                                 asio::cancel_after(remaining(options.handshake_timeout), asio::use_awaitable));
    std::array<std::uint8_t, 4> header;
    std::array<std::uint8_t, 1> confirmation{1};
    if (producer())
    {
        co_await asio::async_read(tls, asio::buffer(header), asio::cancel_at(deadline, asio::use_awaitable));
        const auto size = decode_tls_data_size(header);
        if (size != service.service.size())
        {
            throw std::runtime_error("TLS service mismatch");
        }
        std::string requested(size, '\0');
        co_await asio::async_read(tls, asio::buffer(requested), asio::cancel_at(deadline, asio::use_awaitable));
        if (requested != service.service)
        {
            throw std::runtime_error("TLS service mismatch");
        }
        co_await connect_target();
        co_await asio::async_write(tls, asio::buffer(confirmation), asio::cancel_at(deadline, asio::use_awaitable));
    }
    else
    {
        header = encode_tls_data_size(service.service.size());
        const std::array<asio::const_buffer, 2> request{asio::buffer(header), asio::buffer(service.service)};
        co_await asio::async_write(tls, request, asio::cancel_at(deadline, asio::use_awaitable));
        co_await asio::async_read(tls, asio::buffer(confirmation), asio::cancel_at(deadline, asio::use_awaitable));
        if (confirmation[0] != 1)
        {
            throw std::runtime_error("TLS service rejected");
        }
    }
}

asio::awaitable<void> AgentSession::finish_tls()
{
    auto &tls = *std::get<StreamData>(data).tls;
    const auto finish_deadline = Clock::now() + owner.session_options().disconnect_timeout;
    co_await tls.async_shutdown(asio::cancel_at(finish_deadline, asio::use_awaitable));
    tls.next_layer().shutdown(asio::ip::tcp::socket::shutdown_send);
    std::array<std::uint8_t, 1> tail;
    const auto [error, size] = co_await tls.next_layer().async_read_some(
        asio::buffer(tail), asio::cancel_at(finish_deadline, use_nothrow_awaitable));
    if (error != asio::error::eof || size != 0 || SSL_pending(tls.native_handle()) != 0)
    {
        throw std::runtime_error("TLS carrier did not end cleanly");
    }
}
