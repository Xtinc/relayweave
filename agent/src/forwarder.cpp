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
    : listener_socket(executor), retry_timer(std::move(executor)), config(std::move(config))
{
}

Forwarder::Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                     std::optional<std::string> server_name, std::chrono::steady_clock::duration handshake_timeout,
                     std::chrono::steady_clock::duration connect_timeout,
                     std::chrono::steady_clock::duration relay_open_timeout, std::vector<AgentServiceConfig> services,
                     std::vector<AgentForwardConfig> forwards)
    : agent_(agent), transfer_work_(asio::make_work_guard(std::move(executor))),
      session_options_{ssl_context, std::move(server_name), handshake_timeout, connect_timeout},
      relay_open_timeout_(relay_open_timeout), stopped_event_(transfer_work_.get_executor())
{
    for (auto &service : services)
    {
        auto name = service.name;
        services_.emplace(std::move(name), std::move(service));
    }

    const auto datagram_count = std::count_if(
        forwards.begin(), forwards.end(), [](const auto &forward) { return forward.protocol == RelayProtocol::Udp; });
    stream_forwards_.reserve(forwards.size() - datagram_count);
    datagram_forwards_.reserve(datagram_count);
    for (auto &forward : forwards)
    {
        if (forward.protocol != RelayProtocol::Udp)
        {
            stream_forwards_.emplace_back(transfer_work_.get_executor(), std::move(forward));
        }
        else
        {
            datagram_forwards_.emplace_back(transfer_work_.get_executor(), std::move(forward));
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
}

void Forwarder::set_service(ServiceKey service, std::string node)
{
    service_nodes_.insert_or_assign(service, std::move(node));
    if (service.protocol != RelayProtocol::Udp)
    {
        return;
    }

    for (DatagramForwardId id = 0; id < datagram_forwards_.size(); ++id)
    {
        if (datagram_forwards_[id].config.service == service.service)
        {
            open_datagram_forward(id);
        }
    }
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

    if (service.protocol == RelayProtocol::Udp)
    {
        for (auto &forward : datagram_forwards_)
        {
            if (forward.config.service == service.service)
            {
                // Preserve the wakeup even if the owned retry has not started waiting yet.
                forward.retry_timer.expires_at(Clock::now());
            }
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
    for (auto &forward : datagram_forwards_)
    {
        forward.retry_timer.expires_at(Clock::now());
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
    for (auto &forward : stream_forwards_)
    {
        asio::error_code ignored;
        forward.acceptor.close(ignored);
    }
    for (auto &forward : datagram_forwards_)
    {
        asio::error_code ignored;
        forward.listener_socket.close(ignored);
        forward.retry_timer.expires_at(Clock::now());
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

        if (size > DatagramHeader::maximum_user_payload ||
            (forward.local_peer && forward.local_peer->address() != source.address()))
        {
            continue;
        }

        forward.local_peer = source;
        auto relay = forward.relay;
        if (!relay || !relay->ready || !relay->reason.empty())
        {
            continue;
        }

        auto &sockets = std::get<AgentSession::DatagramData>(relay->data);
        if (size > sockets.maximum_payload)
        {
            continue;
        }

        const std::array buffers{asio::buffer(sockets.session_header), asio::buffer(payload.data(), size)};
        const auto [send_error, sent] = co_await sockets.transfer.async_send(buffers, use_nothrow_awaitable);
        if (state_ != State::Running)
        {
            co_return;
        }
        if (send_error || sent != sockets.session_header.size() + size)
        {
            relay->fail("send relay datagram: " + (send_error ? send_error.message() : "truncated datagram"));
        }
    }
}

void Forwarder::open_datagram_forward(DatagramForwardId forward_id)
{
    auto &forward = datagram_forwards_[forward_id];
    if (state_ != State::Running || forward.relay || forward.retry_scheduled)
    {
        return;
    }

    const ServiceKey service{forward.config.service, RelayProtocol::Udp};
    const auto destination = service_nodes_.find(service);
    if (destination == service_nodes_.end())
    {
        return;
    }

    auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), service, destination->second,
                                                Clock::now() + relay_open_timeout_);
    relay->forward = forward_id;
    start_relay(std::move(relay));
}

void Forwarder::schedule_datagram_retry(DatagramForwardId forward_id)
{
    auto &forward = datagram_forwards_[forward_id];
    if (state_ != State::Running || !service_nodes_.contains(ServiceKey{forward.config.service, RelayProtocol::Udp}))
    {
        return;
    }

    forward.retry_scheduled = true;
    forward.retry_timer.expires_after(forward.retry_delay);
    forward.retry_delay = std::min(forward.retry_delay * 2, Clock::duration(std::chrono::seconds(10)));
    try
    {
        spawn_task(retry_datagram_forward(forward_id), "UDP forward retry");
    }
    catch (...)
    {
        forward.retry_scheduled = false;
        throw;
    }
}

asio::awaitable<void> Forwarder::retry_datagram_forward(DatagramForwardId forward_id)
{
    auto &forward = datagram_forwards_[forward_id];
    const auto [error] = co_await forward.retry_timer.async_wait(use_nothrow_awaitable);
    forward.retry_scheduled = false;
    if (error && error != asio::error::operation_aborted)
    {
        throw std::runtime_error("udp service=" + forward.config.service + " retry: " + error.message());
    }

    open_datagram_forward(forward_id);
}

void Forwarder::start_relay(std::shared_ptr<AgentSession> relay)
{
    if (relays_.size() >= 1000)
    {
        if (relay->forward)
        {
            schedule_datagram_retry(*relay->forward);
        }

        if (relay->producer())
        {
            send_control(relay->selection.server,
                         CtrlMessage(CtrlCommand::RelayReject,
                                     njson{{"uuid", relay->endpoint->uuid}, {"reason", "relay capacity reached"}}));
        }
        return;
    }

    relays_.emplace(relay->request, relay);
    if (relay->forward)
    {
        datagram_forwards_[*relay->forward].relay = relay;
    }

    try
    {
        spawn_task(relay->run(), "Agent session");
    }
    catch (...)
    {
        relays_.erase(relay->request);
        if (relay->forward)
        {
            datagram_forwards_[*relay->forward].relay.reset();
        }
        throw;
    }
}

void Forwarder::relay_message(ServerRoute route, CtrlMessage message)
{
    const auto &p = config::message_params(message);
    const auto command = message.type();
    const auto protocol = config::message_protocol(p);
    const auto uuid = config::optional_unsigned(p, "uuid", true);
    if (state_ != State::Running)
    {
        if (uuid)
        {
            send_control(route, CtrlMessage(CtrlCommand::RelayCancel, njson{{"uuid", *uuid}}));
        }
        return;
    }

    if (command == CtrlCommand::RelayOffer)
    {
        const auto service = config::message_service(p);
        const auto target = services_.find(service);
        if (!uuid || target == services_.end() || target->second.protocol != protocol)
        {
            send_control(route, CtrlMessage(CtrlCommand::RelayReject,
                                            njson{{"uuid", uuid.value_or(0)}, {"reason", "service unavailable"}}));
            return;
        }
        for (const auto &[request, active] : relays_)
        {
            if (active->producer() && active->selection.server.node == route.node && active->endpoint->uuid == *uuid)
            {
                return;
            }
        }
        auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), ServiceKey{service, protocol},
                                                    std::string{}, Clock::now() + relay_open_timeout_);
        relay->target = target->second;
        relay->selection.server = std::move(route);
        relay->selection.epoch = config::optional_unsigned(p, "epoch", true).value_or(0);
        relay->endpoint = AgentSession::RelayEndpoint::from_params(p, protocol);
        start_relay(std::move(relay));
        return;
    }

    const auto request = config::optional_unsigned(p, "request_id");
    for (const auto &[id, relay] : relays_)
    {
        if (relay->selection.server.node != route.node || relay->service.protocol != protocol)
        {
            continue;
        }
        const bool by_request = (command == CtrlCommand::RelayOpened || command == CtrlCommand::RelayError) &&
                                request && *request == id && !relay->producer();
        const bool by_uuid = command != CtrlCommand::RelayOpened && uuid && relay->endpoint.has_value() &&
                             relay->endpoint->uuid == *uuid;
        if (by_request || by_uuid)
        {
            // A single-node relay may have both roles in this Agent and share one UUID.
            relay->handle(message);
            if (command == CtrlCommand::RelayOpened)
            {
                return;
            }
        }
    }

    if (command == CtrlCommand::RelayOpened && uuid)
    {
        send_control(route, CtrlMessage(CtrlCommand::RelayCancel, njson{{"uuid", *uuid}}));
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

void Forwarder::relay_ready(const AgentSession &relay)
{
    if (relay.forward)
    {
        datagram_forwards_[*relay.forward].retry_delay = std::chrono::milliseconds(500);
    }
}

void Forwarder::relay_finished(const AgentSession &relay)
{
    relays_.erase(relay.request);
    if (relay.forward)
    {
        auto &listener = datagram_forwards_[*relay.forward];
        listener.relay.reset();
        schedule_datagram_retry(*relay.forward);
    }
}

asio::awaitable<void> Forwarder::receive_datagrams(AgentSession &relay)
{
    auto &sockets = std::get<AgentSession::DatagramData>(relay.data);
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
            !std::equal(header.begin(), header.end(), buffer.begin()) || !listener.local_peer)
        {
            continue;
        }
        const auto peer = *listener.local_peer;
        const auto payload_size = size - header.size();
        const auto sent = co_await listener.listener_socket.async_send_to(
            asio::buffer(buffer.data() + header.size(), payload_size), peer, asio::use_awaitable);
        if (sent != payload_size)
        {
            throw std::runtime_error("truncated local datagram");
        }
    }
}
