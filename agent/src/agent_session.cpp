#include "agent_session.h"
#include "forwarder.h"
#include "xfr_channel.h"
#include <algorithm>

using Clock = std::chrono::steady_clock;

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

    const auto uuid = endpoint.empty() ? 0 : config::require_unsigned(endpoint, "uuid", true);
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

    if (!endpoint.empty() && p.contains("flow_id") &&
        (!endpoint.contains("flow_id") || p.at("flow_id") != endpoint.at("flow_id")))
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

    if (command == CtrlCommand::RelayOpened && endpoint.empty())
    {
        endpoint = p;
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayReady && !ready)
    {
        ready = true;
        PROXY_INFO_PRINT("Relay [+] %s %s service=%s uuid=%llu", relay_protocol_name(service.protocol).data(),
                         producer() ? "producer" : "consumer", service.service.c_str(),
                         static_cast<unsigned long long>(config::require_unsigned(endpoint, "uuid", true)));
        owner.relay_ready(*this);
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayError || command == CtrlCommand::RelayClosed)
    {
        auto cause = config::optional_string(p, "reason", "relay closed");
        const bool completed = command == CtrlCommand::RelayClosed && ready && service.protocol != RelayProtocol::Udp &&
                               cause == "stream complete";
        if (p.contains("stage"))
        {
            cause = config::optional_string(p, "stage", "control") + ": " + cause;
        }

        // Commit the terminal state only after its metadata has been validated.
        node_closed = true;
        if (completed)
        {
            changed.cancel();
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
                co_await owner.receive_datagrams(*this);
            }

            cancel("UDP forwarding ended");
        }
        else
        {
            auto &sockets = std::get<StreamData>(data);
            if (sockets.tls)
            {
                co_await relay_tls(sockets.local, *sockets.tls);
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
        if (!ready && Clock::now() >= deadline)
        {
            cause = "relay establishment deadline exceeded: " + cause;
        }
        fail(stage + ": " + cause);
    }

    const auto uuid = endpoint.empty() ? 0 : config::require_unsigned(endpoint, "uuid", true);
    if (!reason.empty() && !node_closed && (submitted || producer()))
    {
        njson values{{"request_id", request}, {"reason", reason}};
        if (uuid)
        {
            values["uuid"] = uuid;
        }
        owner.send_control(
            selection.server,
            CtrlMessage(producer() && !ready ? CtrlCommand::RelayReject : CtrlCommand::RelayCancel, std::move(values)));
    }
    if (ready)
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

    const auto host = config::required_string(endpoint, "data_address", "relay endpoint");
    const auto port = config::message_data_port(endpoint);
    const auto uuid = config::require_unsigned(endpoint, "uuid", true);
    const auto ticket = config::require_unsigned(endpoint, "ticket", true);
    auto attachment = RelayAttach::to_msg({producer() ? RelayAttach::Producer : RelayAttach::Consumer, uuid, ticket});
    if (service.protocol == RelayProtocol::Tls)
    {
        if (endpoint.value("tls_framing", std::string{}) != tls_data_framing)
        {
            throw std::invalid_argument("Unsupported TLS data framing");
        }
        (*attachment.params)["tls_framing"] = std::string(tls_data_framing);
    }
    const auto frame = WireMessage::pack(std::move(attachment));
    const auto remaining = [&](Clock::duration timeout) {
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
    };

    if (service.protocol == RelayProtocol::Udp)
    {
        auto &sockets = std::get<DatagramData>(data);
        sockets.session_header = DatagramHeader::encode(config::require_unsigned(endpoint, "session_id", true));
        sockets.maximum_payload =
            endpoint.contains("flow_id") ? LnkFrameHeader::maximum_payload : DatagramHeader::maximum_user_payload;
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
        if (target)
        {
            auto endpoints = co_await sockets.resolver.async_resolve(
                target->target_host, std::to_string(target->target_port),
                asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
            co_await asio::async_connect(sockets.local, endpoints,
                                         asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        }
        auto endpoints = co_await sockets.resolver.async_resolve(
            host, std::to_string(port), asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        co_await asio::async_connect(sockets.transfer, endpoints,
                                     asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        if (service.protocol == RelayProtocol::Tls)
        {
            sockets.tls.emplace(std::move(sockets.transfer), options.ssl_context);
            configure_tls_client(*sockets.tls, host, options.server_name);
            co_await sockets.tls->async_handshake(
                asio::ssl::stream_base::client,
                asio::cancel_after(remaining(options.handshake_timeout), asio::use_awaitable));
            co_await asio::async_write(*sockets.tls, asio::buffer(frame),
                                       asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        }
        else
        {
            co_await asio::async_write(sockets.transfer, asio::buffer(frame),
                                       asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
        }
    }

    while (!ready && reason.empty())
    {
        if (Clock::now() >= deadline)
        {
            throw std::runtime_error("relay.ready timed out");
        }
        auto wakeup = deadline;
        if (service.protocol == RelayProtocol::Udp)
        {
            auto &sockets = std::get<DatagramData>(data);
            const auto sent = co_await sockets.transfer.async_send(
                asio::buffer(frame), asio::cancel_after(remaining(options.connect_timeout), asio::use_awaitable));
            if (sent != frame.size())
            {
                throw std::runtime_error("truncated relay.attach");
            }
            wakeup = std::min(deadline, Clock::now() + std::chrono::milliseconds(500));
            if (ready || !reason.empty())
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
