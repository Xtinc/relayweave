#include "forwarder.h"
#include "agent_session.h"
#include <algorithm>
#include <array>
#include <cassert>

using tcp = asio::ip::tcp;
using udp = asio::ip::udp;
using Clock = std::chrono::steady_clock;

Forwarder::StreamForward::StreamForward(asio::any_io_executor executor, AgentForwardConfig config)
    : acceptor(std::move(executor)), config(std::move(config))
{
}

Forwarder::DatagramForward::DatagramForward(asio::any_io_executor executor, AgentForwardConfig config)
    : listener_socket(std::move(executor)), config(std::move(config))
{
}

Forwarder::Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                     const AgentConfig &config)
    : agent_(agent), transfer_work_(asio::make_work_guard(std::move(executor))),
      session_options_{ssl_context, config.server_name, config.channel.handshake_timeout, config.connect_timeout},
      relay_open_timeout_(config.relay_open_timeout), udp_idle_timeout_(config.udp_idle_timeout),
      udp_idle_timer_(transfer_work_.get_executor()),
      stopped_event_(transfer_work_.get_executor())
{
    if (config.udp_idle_timeout <= Clock::duration::zero())
    {
        throw std::invalid_argument("UDP idle timeout must be positive");
    }
    for (const auto &service : config.services)
    {
        services_.emplace(service.name, service);
    }

    const auto datagram_count = std::count_if(config.forwards.begin(), config.forwards.end(),
        [](const auto &forward) { return forward.protocol == RelayProtocol::Udp; });
    stream_forwards_.reserve(config.forwards.size() - datagram_count);
    datagram_forwards_.reserve(datagram_count);
    for (const auto &forward : config.forwards)
    {
        if (forward.protocol != RelayProtocol::Udp)
        {
            stream_forwards_.emplace_back(transfer_work_.get_executor(), forward);
        }
        else
        {
            datagram_forwards_.emplace_back(transfer_work_.get_executor(), forward);
        }
    }
}

void Forwarder::start()
{
    for (auto &forward : stream_forwards_)
    {
        const tcp::endpoint endpoint(asio::ip::make_address(forward.config.listen_address), forward.config.listen_port);
        forward.acceptor.open(endpoint.protocol());
        forward.acceptor.set_option(asio::socket_base::reuse_address(true));
        forward.acceptor.bind(endpoint);
        forward.acceptor.listen();
    }

    for (auto &forward : datagram_forwards_)
    {
        const udp::endpoint endpoint(asio::ip::make_address(forward.config.listen_address), forward.config.listen_port);
        forward.listener_socket.open(endpoint.protocol());
        forward.listener_socket.set_option(asio::socket_base::reuse_address(true));
        forward.listener_socket.bind(endpoint);
    }

    state_ = State::Running;
    for (auto &forward : stream_forwards_)
    {
        PROXY_INFO_PRINT("Forward listening %s %s:%u -> service=%s",
                         relay_protocol_name(forward.config.protocol).data(), forward.config.listen_address.c_str(),
                         static_cast<unsigned int>(forward.config.listen_port), forward.config.service.c_str());
        spawn_task(accept_stream(forward), "Local stream accept loop");
    }

    for (DatagramForwardId id = 0; id < datagram_forwards_.size(); ++id)
    {
        const auto &forward = datagram_forwards_[id];
        PROXY_INFO_PRINT("Forward listening udp %s:%u -> service=%s", forward.config.listen_address.c_str(),
                         static_cast<unsigned int>(forward.config.listen_port), forward.config.service.c_str());
        spawn_task(receive_local_datagrams(id), "Local UDP receive loop");
    }
    if (!datagram_forwards_.empty())
    {
        spawn_task(expire_datagrams(), "UDP idle expiry");
    }
}

void Forwarder::set_service(ServiceKey service, std::string node)
{
    const auto existing = service_nodes_.find(service);
    if (existing != service_nodes_.end() && existing->second != node && service.protocol == RelayProtocol::Udp)
    {
        clear_service(service, "service location changed");
    }
    service_nodes_.insert_or_assign(std::move(service), std::move(node));
}

void Forwarder::clear_service(const ServiceKey &service, std::string reason)
{
    service_nodes_.erase(service);
    for (const auto &[request, relay] : relays_)
    {
        if (!relay->producer() && relay->service == service &&
            (!relay->ready || service.protocol == RelayProtocol::Udp))
        {
            relay->cancel(reason);
        }
    }
}

void Forwarder::control_disconnected()
{
    service_nodes_.clear();
    for (const auto &[request, relay] : relays_)
    {
        relay->cancel("primary control disconnected");
    }
}

void Forwarder::retain_nodes(const std::unordered_set<std::string> &nodes)
{
    for (const auto &[request, relay] : relays_)
    {
        if (!relay->selection.server.node.empty() && !nodes.contains(relay->selection.server.node))
        {
            relay->cancel("business node unavailable");
        }
    }
}

void Forwarder::invalidate_relays(std::string reason)
{
    for (const auto &[request, relay] : relays_)
    {
        if (!relay->producer() && !relay->ready)
        {
            relay->cancel(reason);
        }
    }
}

asio::awaitable<void> Forwarder::async_stop()
{
    state_ = State::Stopping;
    udp_idle_timer_.cancel();
    for (auto &forward : stream_forwards_)
    {
        asio::error_code ignored;
        forward.acceptor.close(ignored);
    }
    for (auto &forward : datagram_forwards_)
    {
        asio::error_code ignored;
        forward.listener_socket.close(ignored);
    }
    for (const auto &[request, relay] : relays_)
    {
        relay->cancel("agent stopping");
    }
    finish_if_stopped();

    if (!co_await stopped_event_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}

asio::awaitable<void> Forwarder::accept_stream(StreamForward &forward)
{
    const ServiceKey service{forward.config.service, forward.config.protocol};
    for (;;)
    {
        auto [error, socket] = co_await forward.acceptor.async_accept(use_nothrow_awaitable);
        if (state_ != State::Running)
        {
            co_return;
        }
        if (error)
        {
            PROXY_ERROR_PRINT("Forward accept failed %s service=%s listen=%s:%u reason=%s",
                              relay_protocol_name(forward.config.protocol).data(), forward.config.service.c_str(),
                              forward.config.listen_address.c_str(),
                              static_cast<unsigned int>(forward.config.listen_port), error.message().c_str());
            continue;
        }

        const auto destination = service_nodes_.find(service);
        if (destination == service_nodes_.end())
        {
            PROXY_DEBUG_PRINT("Relay unavailable service=%s/%s", service.service.c_str(),
                              relay_protocol_name(service.protocol).data());
            continue;
        }

        auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), service, destination->second,
                                                    Clock::now() + relay_open_timeout_);
        std::get<AgentSession::StreamData>(relay->data).local = std::move(socket);
        start_relay(std::move(relay));
    }
}

asio::awaitable<void> Forwarder::receive_local_datagrams(DatagramForwardId forward_id)
{
    auto &forward = datagram_forwards_[forward_id];
    std::array<std::uint8_t, DatagramHeader::maximum_user_payload + 1> payload;
    for (;;)
    {
        udp::endpoint source;
        const auto [error, size] =
            co_await forward.listener_socket.async_receive_from(asio::buffer(payload), source, use_nothrow_awaitable);
        if (state_ != State::Running)
        {
            co_return;
        }
        if (error)
        {
            if (error != asio::error::message_size)
            {
                PROXY_ERROR_PRINT("Forward receive failed udp service=%s listen=%s:%u reason=%s",
                                  forward.config.service.c_str(), forward.config.listen_address.c_str(),
                                  static_cast<unsigned int>(forward.config.listen_port), error.message().c_str());
            }
            continue;
        }

        if (size > DatagramHeader::maximum_user_payload)
        {
            continue;
        }

        const auto client = forward.clients.find(source);
        if (client == forward.clients.end())
        {
            const ServiceKey service{forward.config.service, RelayProtocol::Udp};
            const auto destination = service_nodes_.find(service);
            if (destination == service_nodes_.end() || relays_.size() >= relay_capacity)
            {
                continue;
            }
            const auto opening = std::count_if(relays_.begin(), relays_.end(), [](const auto &entry) {
                return entry.second->forward && !entry.second->ready;
            });
            if (static_cast<std::size_t>(opening) >= udp_opening_capacity)
            {
                continue;
            }
            try
            {
                auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), service, destination->second,
                                                            Clock::now() + relay_open_timeout_);
                relay->forward = forward_id;
                auto &sockets = std::get<AgentSession::DatagramData>(relay->data);
                sockets.local_peer = source;
                sockets.first_packet.emplace(payload.begin(), payload.begin() + size);
                start_relay(std::move(relay));
            }
            catch (const std::exception &error)
            {
                PROXY_ERROR_PRINT("UDP association failed service=%s reason=%s", forward.config.service.c_str(),
                                  error.what());
            }
            continue;
        }

        auto relay = relays_.at(client->second);
        auto &sockets = std::get<AgentSession::DatagramData>(relay->data);
        if (!relay->reason.empty() || !relay->ready || sockets.first_packet || size > sockets.maximum_payload)
        {
            continue;
        }
        try
        {
            co_await send_datagram(*relay, std::span(payload.data(), size));
        }
        catch (const std::exception &error)
        {
            relay->fail("send relay datagram: " + std::string(error.what()));
        }
    }
}

asio::awaitable<void> Forwarder::expire_datagrams()
{
    const auto interval = std::min(udp_idle_timeout_, Clock::duration(std::chrono::seconds(1)));
    while (state_ == State::Running)
    {
        udp_idle_timer_.expires_after(interval);
        const auto [error] = co_await udp_idle_timer_.async_wait(use_nothrow_awaitable);
        if (state_ != State::Running)
        {
            co_return;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        const auto now = Clock::now();
        for (const auto &[request, relay] : relays_)
        {
            if (relay->forward && relay->ready && relay->reason.empty())
            {
                const auto &sockets = std::get<AgentSession::DatagramData>(relay->data);
                if (!sockets.first_packet && now - sockets.last_activity >= udp_idle_timeout_)
                {
                    relay->cancel("UDP idle timeout");
                }
            }
        }
    }
}

void Forwarder::start_relay(std::shared_ptr<AgentSession> relay)
{
    if (relays_.size() >= relay_capacity)
    {
        if (relay->producer())
        {
            send_control(relay->selection.server,
                         CtrlMessage(CtrlCommand::RelayReject,
                                     njson{{"uuid", relay->endpoint->uuid}, {"reason", "relay capacity reached"}}));
        }
        return;
    }

    relays_.emplace(relay->request, relay);
    try
    {
        if (relay->forward)
        {
            const auto &peer = std::get<AgentSession::DatagramData>(relay->data).local_peer;
            datagram_forwards_[*relay->forward].clients.emplace(peer, relay->request);
        }
        spawn_task(relay->run(), "Agent session");
    }
    catch (...)
    {
        relay_finished(*relay);
        throw;
    }
}

void Forwarder::relay_message(ServerRoute route, CtrlMessage message)
{
    if (!message.params || !message.params->is_object())
    {
        PROXY_ERROR_PRINT("Relay message rejected command=%s: parameters must be an object", message.command.c_str());
        return;
    }
    const auto &p = *message.params;
    const auto command = message.type();
    const auto identity = p.find("uuid");
    const auto uuid = identity != p.end() && identity->is_number_unsigned() ? identity->get<std::uint64_t>() : 0;
    if (state_ != State::Running)
    {
        if (uuid)
        {
            send_control(route, CtrlMessage(CtrlCommand::RelayCancel, njson{{"uuid", uuid}}));
        }
        return;
    }

    if (command == CtrlCommand::RelayOffer)
    {
        try
        {
            const auto protocol = config::message_protocol(p);
            const auto service = config::message_service(p);
            const auto target = services_.find(service);
            if (!uuid || target == services_.end() || target->second.protocol != protocol)
            {
                send_control(route, CtrlMessage(CtrlCommand::RelayReject,
                                                njson{{"uuid", uuid}, {"reason", "service unavailable"}}));
                return;
            }
            for (const auto &[request, active] : relays_)
            {
                if (active->producer() && active->selection.server.node == route.node && active->endpoint->uuid == uuid)
                {
                    return;
                }
            }
            auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), ServiceKey{service, protocol},
                                                        std::string{}, Clock::now() + relay_open_timeout_);
            relay->target = target->second;
            relay->selection.server = route;
            relay->selection.epoch = config::optional_unsigned(p, "epoch", true).value_or(0);
            relay->endpoint = AgentSession::RelayEndpoint::from_params(p, protocol);
            start_relay(std::move(relay));
        }
        catch (const std::exception &error)
        {
            send_control(route, CtrlMessage(CtrlCommand::RelayReject,
                                            njson{{"uuid", uuid},
                                                  {"reason", "invalid relay.offer: " + std::string(error.what())}}));
        }
        return;
    }

    const auto request = p.find("request_id");
    for (const auto &[id, relay] : relays_)
    {
        if (relay->selection.server.node != route.node)
        {
            continue;
        }
        const bool by_request = (command == CtrlCommand::RelayOpened || command == CtrlCommand::RelayError) &&
                                request != p.end() && request->is_number_unsigned() &&
                                *request == id && !relay->producer();
        const bool by_uuid =
            command != CtrlCommand::RelayOpened && uuid && relay->endpoint.has_value() && relay->endpoint->uuid == uuid;
        if (by_request || by_uuid)
        {
            // A single-node relay may have both roles in this Agent and share one UUID.
            try
            {
                if (config::message_protocol(p) != relay->service.protocol)
                {
                    continue;
                }
                relay->handle(message);
            }
            catch (const std::exception &error)
            {
                relay->fail("invalid " + message.command + ": " + error.what());
            }
            if (command == CtrlCommand::RelayOpened)
            {
                return;
            }
        }
    }

    if (command == CtrlCommand::RelayOpened && uuid)
    {
        send_control(route, CtrlMessage(CtrlCommand::RelayCancel, njson{{"uuid", uuid}}));
    }
}

void Forwarder::spawn_task(asio::awaitable<void> task, std::string_view context)
{
    ++active_tasks_;
    try
    {
        asio::co_spawn(transfer_work_.get_executor(), std::move(task), [this, context](std::exception_ptr failure) {
            if (failure)
            {
                PROXY_ERROR_PRINT("%.*s stopped reason=%s", static_cast<int>(context.size()), context.data(),
                                  exception_description(failure).c_str());
            }
            assert(active_tasks_ != 0);
            --active_tasks_;
            finish_if_stopped();
        });
    }
    catch (...)
    {
        --active_tasks_;
        throw;
    }
}

void Forwarder::finish_if_stopped()
{
    if (state_ != State::Stopping || active_tasks_ != 0)
    {
        return;
    }

    state_ = State::Stopped;
    transfer_work_.reset();
    stopped_event_.notify_all();
}

bool Forwarder::send_control(const ServerRoute &route, CtrlMessage message)
{
    auto channel = route.channel.lock();
    if (!channel)
    {
        return false;
    }

    (*message.params)["node_id"] = route.node;
    channel->send(std::move(message));
    return true;
}

std::uint64_t Forwarder::allocate_request_id()
{
    const auto request = next_request_id_++;
    if (next_request_id_ == 0)
    {
        next_request_id_ = 1;
    }
    return request;
}

asio::awaitable<RelaySelection> Forwarder::select_relay(ServiceKey service, std::string destination,
                                                        Clock::time_point deadline)
{
    return agent_.select_relay(std::move(service), std::move(destination), deadline);
}

void Forwarder::relay_finished(AgentSession &relay)
{
    if (relay.forward)
    {
        auto &clients = datagram_forwards_[*relay.forward].clients;
        auto &sockets = std::get<AgentSession::DatagramData>(relay.data);
        sockets.first_packet.reset();
        const auto client = clients.find(sockets.local_peer);
        if (client != clients.end() && client->second == relay.request)
        {
            clients.erase(client);
        }
    }
    relays_.erase(relay.request);
}

asio::awaitable<void> Forwarder::send_datagram(AgentSession &relay, std::span<const std::uint8_t> payload)
{
    auto &sockets = std::get<AgentSession::DatagramData>(relay.data);
    const std::array<asio::const_buffer, 2> buffers{asio::buffer(sockets.session_header),
                                                  asio::buffer(payload.data(), payload.size())};
    const auto sent = co_await sockets.transfer.async_send(buffers, asio::use_awaitable);
    if (sent != sockets.session_header.size() + payload.size())
    {
        throw std::runtime_error("truncated relay datagram");
    }
    sockets.last_activity = Clock::now();
}

asio::awaitable<void> Forwarder::relay_datagrams(AgentSession &relay)
{
    auto &sockets = std::get<AgentSession::DatagramData>(relay.data);
    sockets.last_activity = Clock::now();
    if (sockets.first_packet->size() <= sockets.maximum_payload)
    {
        co_await send_datagram(relay, *sockets.first_packet);
    }
    sockets.first_packet.reset();

    auto &listener = datagram_forwards_[*relay.forward];
    const auto &header = sockets.session_header;
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> buffer;
    while (relay.reason.empty())
    {
        const auto [error, size] = co_await sockets.transfer.async_receive(asio::buffer(buffer), use_nothrow_awaitable);
        if (!relay.reason.empty())
        {
            co_return;
        }
        if (error == asio::error::message_size)
        {
            continue;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        if (size < header.size() || size > header.size() + sockets.maximum_payload ||
            !std::equal(header.begin(), header.end(), buffer.begin()))
        {
            continue;
        }
        sockets.last_activity = Clock::now();
        const auto peer = sockets.local_peer;
        const auto payload_size = size - header.size();
        const auto sent = co_await listener.listener_socket.async_send_to(
            asio::buffer(buffer.data() + header.size(), payload_size), peer, asio::use_awaitable);
        if (sent != payload_size)
        {
            throw std::runtime_error("truncated local datagram");
        }
    }
}
