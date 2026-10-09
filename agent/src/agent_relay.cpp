#include "agent_relay.h"
#include "forwarder.h"
#include <array>

using Clock = std::chrono::steady_clock;

AgentRelay::AgentRelay(Forwarder &owner, std::uint64_t request, ServiceKey service, std::string destination,
                       Clock::time_point deadline)
    : owner(owner), request(request), service(std::move(service)), destination(std::move(destination)), deadline(deadline),
      data(this->service.protocol == RelayProtocol::Udp
               ? Data(std::in_place_type<DatagramData>, owner.transfer_work_.get_executor())
               : Data(std::in_place_type<StreamData>, owner.transfer_work_.get_executor())),
      changed(owner.transfer_work_.get_executor())
{
}

void AgentRelay::cancel(std::string cause)
{
    if (cancelled)
    {
        return;
    }
    cancelled = true;
    reason = std::move(cause);
    cancellation.emit(asio::cancellation_type::all);
    changed.cancel();
    std::visit([](auto &sockets) {
        sockets.resolver.cancel();
        asio::error_code ignored;
        sockets.transfer.close(ignored);
        if constexpr (std::is_same_v<std::decay_t<decltype(sockets)>, StreamData>)
        {
            sockets.local.close(ignored);
            if (sockets.tls)
            {
                sockets.tls->lowest_layer().close(ignored);
            }
        }
        else if (sockets.target)
        {
            sockets.target->close(ignored);
        }
    }, data);
}

void AgentRelay::handle(const CtrlMessage &message)
{
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
    if (command == CtrlCommand::RelayOpened && endpoint.empty())
    {
        endpoint = p;
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayReady && !endpoint.empty() && !cancelled)
    {
        ready = true;
        if (forward)
        {
            owner.datagram_forwards_.at(*forward).retry_delay = std::chrono::milliseconds(500);
        }
        changed.cancel();
    }
    else if (command == CtrlCommand::RelayError || command == CtrlCommand::RelayClosed)
    {
        if (command == CtrlCommand::RelayClosed && ready && service.protocol != RelayProtocol::Udp &&
            config::optional_string(p, "reason", "") == "stream complete")
        {
            completed = true;
            changed.cancel();
            return;
        }
        // The Node has already ended this instance; cleanup must not send another cancel.
        completed = true;
        failed = command == CtrlCommand::RelayError;
        auto cause = config::optional_string(p, "reason", "relay closed");
        if (p.contains("stage"))
        {
            cause = config::optional_string(p, "stage", "control") + ": " + cause;
        }
        cancel(std::move(cause));
    }
}

// transfer executor; only selection/release cross to RelayAgent's control executor.
asio::awaitable<void> AgentRelay::run()
{
    auto keep_alive = shared_from_this();
    std::string stage = "select entry";
    try
    {
        if (cancelled || owner.state_ != Forwarder::State::Running)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        if (!producer())
        {
            selection = co_await asio::co_spawn(owner.agent_.control_executor_,
                owner.agent_.select_relay(service, destination, deadline),
                asio::use_awaitable);
            if (cancelled || owner.state_ != Forwarder::State::Running)
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            stage = "relay.open";
            njson values{{"request_id", request}, {"service", service.service}, {"protocol", relay_protocol_name(service.protocol)}};
            if (selection.path.size() > 1)
            {
                const auto budget = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
                if (budget <= 0)
                {
                    throw std::runtime_error("relay establishment timed out");
                }
                values["path"] = selection.path;
                values["epoch"] = selection.epoch;
                values["budget_ms"] = budget;
            }
            if (!owner.send_control(selection.server, CtrlMessage(CtrlCommand::RelayOpen, std::move(values))))
            {
                throw std::runtime_error("entry control unavailable");
            }
            submitted = true;
            while (endpoint.empty() && !cancelled)
            {
                changed.expires_at(deadline);
                const auto [error] = co_await changed.async_wait(use_nothrow_awaitable);
                if (!error || Clock::now() >= deadline)
                {
                    throw std::runtime_error("relay establishment timed out");
                }
            }
        }
        stage = "attach";
        co_await attach();
        stage = "transfer";
        if (service.protocol == RelayProtocol::Udp)
        {
            auto &sockets = std::get<DatagramData>(data);
            const auto header = DatagramHeader::encode(config::require_unsigned(endpoint, "session_id", true));
            if (producer())
            {
                co_await relay_udp_connected(*sockets.target, sockets.transfer, header);
            }
            else
            {
                co_await receive_datagrams();
            }
        }
        else
        {
            auto &sockets = std::get<StreamData>(data);
            if (endpoint.contains("flow_id"))
            {
                if (sockets.tls)
                {
                    co_await relay_halfclose(sockets.local, *sockets.tls);
                }
                else
                {
                    co_await relay_halfclose(sockets.local, sockets.transfer);
                }
                // Both local directions ended; retain the entry lease while Nodes drain FIN.
                while (!completed && !cancelled)
                {
                    changed.expires_at(Clock::time_point::max());
                    co_await changed.async_wait(use_nothrow_awaitable);
                }
            }
            else if (sockets.tls)
            {
                co_await relay_tls(sockets.local, *sockets.tls);
                completed = true;
            }
            else
            {
                co_await relay_tcp(sockets.local, sockets.transfer);
                completed = true;
            }
        }
    }
    catch (const std::exception &)
    {
        if (reason.empty())
        {
            reason = stage + ": " + (!ready && Clock::now() >= deadline ?
                "relay establishment timed out" : exception_description(std::current_exception()));
        }
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await finish();
}

asio::awaitable<void> AgentRelay::attach()
{
    if (cancelled || endpoint.empty())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    const auto &server = selection.server;
    const auto port = config::message_data_port(endpoint);
    const auto uuid = config::require_unsigned(endpoint, "uuid", true);
    const auto ticket = config::require_unsigned(endpoint, "ticket", true);
    const auto frame = WireMessage::pack(RelayAttach::to_msg(
        {producer() ? RelayAttach::Producer : RelayAttach::Consumer, uuid, ticket}));
    const auto remaining = [&] { return std::min(deadline - Clock::now(), owner.connect_timeout_); };
    if (service.protocol == RelayProtocol::Udp)
    {
        auto &sockets = std::get<DatagramData>(data);
        if (target)
        {
            auto endpoints = co_await sockets.resolver.async_resolve(target->target_host,
                std::to_string(target->target_port), asio::cancel_after(remaining(), asio::use_awaitable));
            const auto address = endpoints.begin()->endpoint();
            sockets.target.emplace(owner.transfer_work_.get_executor());
            sockets.target->open(address.protocol());
            sockets.target->connect(address);
        }
        auto endpoints = co_await sockets.resolver.async_resolve(server.host, std::to_string(port),
            asio::cancel_after(remaining(), asio::use_awaitable));
        const auto address = endpoints.begin()->endpoint();
        sockets.transfer.open(address.protocol());
        sockets.transfer.connect(address);
        while (!ready && !cancelled)
        {
            const auto sent = co_await sockets.transfer.async_send(asio::buffer(frame), asio::cancel_after(remaining(), asio::use_awaitable));
            if (sent != frame.size())
            {
                throw std::runtime_error("truncated relay.attach");
            }
            if (ready || cancelled)
            {
                break;
            }
            changed.expires_at(std::min(deadline, Clock::now() + std::chrono::milliseconds(500)));
            co_await changed.async_wait(use_nothrow_awaitable);
            if (!ready && Clock::now() >= deadline)
            {
                throw std::runtime_error("relay.ready timed out");
            }
        }
    }
    else
    {
        auto &sockets = std::get<StreamData>(data);
        if (target)
        {
            auto endpoints = co_await sockets.resolver.async_resolve(target->target_host,
                std::to_string(target->target_port), asio::cancel_after(remaining(), asio::use_awaitable));
            co_await asio::async_connect(sockets.local, endpoints, asio::cancel_after(remaining(), asio::use_awaitable));
        }
        auto endpoints = co_await sockets.resolver.async_resolve(server.host, std::to_string(port),
            asio::cancel_after(remaining(), asio::use_awaitable));
        co_await asio::async_connect(sockets.transfer, endpoints, asio::cancel_after(remaining(), asio::use_awaitable));
        if (service.protocol == RelayProtocol::Tls)
        {
            sockets.tls.emplace(std::move(sockets.transfer), owner.ssl_context_);
            configure_tls_client(*sockets.tls, server.host, owner.server_name_);
            co_await sockets.tls->async_handshake(asio::ssl::stream_base::client,
                asio::cancel_after(std::min(deadline - Clock::now(), owner.handshake_timeout_), asio::use_awaitable));
            co_await asio::async_write(*sockets.tls, asio::buffer(frame), asio::cancel_after(remaining(), asio::use_awaitable));
        }
        else
        {
            co_await asio::async_write(sockets.transfer, asio::buffer(frame), asio::cancel_after(remaining(), asio::use_awaitable));
        }
        while (!ready && !cancelled)
        {
            changed.expires_at(deadline);
            const auto [error] = co_await changed.async_wait(use_nothrow_awaitable);
            if (!error || Clock::now() >= deadline)
            {
                throw std::runtime_error("relay.ready timed out");
            }
        }
    }
    if (cancelled)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    PROXY_INFO_PRINT("Relay [+] %s %s service=%s uuid=%llu", relay_protocol_name(service.protocol).data(),
        producer() ? "producer" : "consumer", service.service.c_str(), static_cast<unsigned long long>(uuid));
}

asio::awaitable<void> AgentRelay::finish()
{
    const auto uuid = endpoint.empty() ? 0 : config::require_unsigned(endpoint, "uuid", true);
    if (!completed && (submitted || producer()))
    {
        njson values{{"request_id", request}, {"reason", reason}};
        if (uuid)
        {
            values["uuid"] = uuid;
        }
        owner.send_control(selection.server, CtrlMessage(
            producer() && !ready ? CtrlCommand::RelayReject : CtrlCommand::RelayCancel, std::move(values)));
    }
    if (!reason.empty() && (!cancelled || failed) && owner.state_ == Forwarder::State::Running)
    {
        PROXY_ERROR_PRINT("Relay failed %s %s service=%s request_id=%llu uuid=%llu reason=%s",
            relay_protocol_name(service.protocol).data(), producer() ? "producer" : "consumer",
            service.service.c_str(), static_cast<unsigned long long>(request), static_cast<unsigned long long>(uuid), reason.c_str());
    }
    if (ready)
    {
        PROXY_INFO_PRINT("Relay [x] %s %s service=%s uuid=%llu reason=%s",
            relay_protocol_name(service.protocol).data(), producer() ? "producer" : "consumer", service.service.c_str(),
            static_cast<unsigned long long>(uuid), reason.empty() ? "stream complete" : reason.c_str());
    }
    cancel(reason);
    owner.relays_.erase(request);
    if (selection.lease)
    {
        co_await asio::co_spawn(owner.agent_.control_executor_, [agent = &owner.agent_, lease = selection.lease]() -> asio::awaitable<void> {
            agent->release_entry(lease);
            co_return;
        }, asio::use_awaitable);
    }
    if (forward)
    {
        auto &listener = owner.datagram_forwards_.at(*forward);
        if (listener.relay.get() == this)
        {
            listener.relay.reset();
            owner.schedule_datagram_retry(*forward);
        }
    }
}

asio::awaitable<void> AgentRelay::send_datagram(std::span<const std::uint8_t> payload)
{
    if (!ready || cancelled || (endpoint.contains("flow_id") && payload.size() > LnkFrameHeader::maximum_payload))
    {
        co_return;
    }
    auto &sockets = std::get<DatagramData>(data);
    const auto header = DatagramHeader::encode(config::require_unsigned(endpoint, "session_id", true));
    const std::array buffers{asio::buffer(header), asio::buffer(payload)};
    const auto [error, size] = co_await sockets.transfer.async_send(buffers, use_nothrow_awaitable);
    if (error || size != header.size() + payload.size())
    {
        failed = true;
        cancel(error ? error.message() : "truncated relay datagram");
    }
}

asio::awaitable<void> AgentRelay::receive_datagrams()
{
    auto &sockets = std::get<DatagramData>(data);
    auto &listener = owner.datagram_forwards_.at(*forward);
    const auto header = DatagramHeader::encode(config::require_unsigned(endpoint, "session_id", true));
    const auto maximum_payload = endpoint.contains("flow_id") ? LnkFrameHeader::maximum_payload : DatagramHeader::maximum_user_payload;
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> buffer;
    for (;;)
    {
        const auto [error, size] = co_await sockets.transfer.async_receive(asio::buffer(buffer), use_nothrow_awaitable);
        if (error == asio::error::message_size)
        {
            continue;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        if (size < header.size() || size > header.size() + maximum_payload ||
            !std::equal(header.begin(), header.end(), buffer.begin()) || !listener.local_peer)
        {
            continue;
        }
        const auto peer = *listener.local_peer;
        const auto payload_size = size - header.size();
        const auto sent = co_await listener.listener_socket.async_send_to(asio::buffer(buffer.data() + header.size(), payload_size),
                                                                        peer, asio::use_awaitable);
        if (sent != payload_size)
        {
            throw std::runtime_error("truncated local datagram");
        }
    }
}
