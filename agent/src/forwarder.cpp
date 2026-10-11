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
    : agent_(agent), transfer_work_(asio::make_work_guard(std::move(executor))), ssl_context_(ssl_context),
      server_name_(std::move(server_name)), handshake_timeout_(handshake_timeout), connect_timeout_(connect_timeout),
      relay_open_timeout_(relay_open_timeout), stopped_event_(transfer_work_.get_executor())
{
    for (auto &service : services)
    {
        auto name = service.name;
        services_.emplace(std::move(name), std::move(service));
    }

    stream_forwards_.reserve(forwards.size());
    DatagramForwardId datagram_forward_id = 0;
    for (auto &forward : forwards)
    {
        if (forward.protocol != RelayProtocol::Udp)
        {
            stream_forwards_.emplace_back(transfer_work_.get_executor(), std::move(forward));
        }
        else
        {
            const auto id = datagram_forward_id++;
            datagram_forwards_.try_emplace(id, transfer_work_.get_executor(), std::move(forward));
        }
    }
}

void Forwarder::start()
{
    if (state_ != State::Created)
    {
        throw std::logic_error("Forwarder can only be started once");
    }

    try
    {
        for (auto &forward : stream_forwards_)
        {
            const tcp::endpoint endpoint(asio::ip::make_address(forward.config.listen_address),
                                         forward.config.listen_port);
            forward.acceptor.open(endpoint.protocol());
            forward.acceptor.set_option(asio::socket_base::reuse_address(true));
            forward.acceptor.bind(endpoint);
            forward.acceptor.listen();
        }

        for (auto &[id, forward] : datagram_forwards_)
        {
            const udp::endpoint endpoint(asio::ip::make_address(forward.config.listen_address),
                                         forward.config.listen_port);
            forward.listener_socket.open(endpoint.protocol());
            forward.listener_socket.set_option(asio::socket_base::reuse_address(true));
            forward.listener_socket.bind(endpoint);
        }
    }
    catch (...)
    {
        for (auto &forward : stream_forwards_)
        {
            asio::error_code ignored;
            forward.acceptor.close(ignored);
        }

        for (auto &[id, forward] : datagram_forwards_)
        {
            asio::error_code ignored;
            forward.listener_socket.close(ignored);
        }
        throw;
    }

    state_ = State::Running;
    for (auto &forward : stream_forwards_)
    {
        PROXY_INFO_PRINT("Forward listening %s %s:%u -> service=%s",
                         relay_protocol_name(forward.config.protocol).data(), forward.config.listen_address.c_str(),
                         static_cast<unsigned int>(forward.config.listen_port), forward.config.service.c_str());
        spawn_task(accept_stream(forward), "Local stream accept loop");
    }

    for (auto &[id, forward] : datagram_forwards_)
    {
        PROXY_INFO_PRINT("Forward listening udp %s:%u -> service=%s", forward.config.listen_address.c_str(),
                         static_cast<unsigned int>(forward.config.listen_port), forward.config.service.c_str());
        spawn_task(receive_local_datagrams(id), "Local UDP receive loop");
    }
}

void Forwarder::set_service(ServiceKey service, std::string node)
{
    if (state_ != State::Running)
    {
        return;
    }

    service_nodes_.insert_or_assign(service, std::move(node));
    for (auto &[id, forward] : datagram_forwards_)
    {
        if (forward.config.service == service.service && service.protocol == RelayProtocol::Udp)
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
        for (auto &[id, forward] : datagram_forwards_)
        {
            if (forward.config.service == service.service)
            {
                // Preserve the wakeup even if the owned retry has not started waiting yet.
                forward.retry_timer.expires_at(Clock::now());
            }
        }
    }
}

void Forwarder::clear_server(const std::string &server_id)
{
    for (const auto &[request, relay] : relays_)
    {
        if (relay->selection.server.id == server_id)
        {
            relay->cancel("control disconnected");
        }
    }
}

void Forwarder::invalidate_relays(std::string reason, const std::optional<ServiceKey> &service)
{
    for (const auto &[request, relay] : relays_)
    {
        if (!relay->producer() && !relay->ready && (!service || relay->service == *service))
        {
            relay->cancel(reason);
        }
    }
}

asio::awaitable<void> Forwarder::async_stop()
{
    if (state_ == State::Stopped)
    {
        co_return;
    }

    if (state_ != State::Stopping)
    {
        state_ = State::Stopping;
        for (auto &forward : stream_forwards_)
        {
            asio::error_code ignored;
            forward.acceptor.close(ignored);
        }
        for (auto &[id, forward] : datagram_forwards_)
        {
            asio::error_code ignored;
            forward.listener_socket.close(ignored);
            forward.retry_timer.cancel();
        }
        for (const auto &[request, relay] : relays_)
        {
            relay->cancel("agent stopping");
        }
        finish_if_stopped();
    }

    if (!co_await stopped_event_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}

asio::awaitable<void> Forwarder::accept_stream(StreamForward &forward)
{
    for (;;)
    {
        auto [error, socket] = co_await forward.acceptor.async_accept(use_nothrow_awaitable);
        if (error)
        {
            if (!forward.acceptor.is_open() || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Forward accept failed %s service=%s listen=%s:%u reason=%s",
                              relay_protocol_name(forward.config.protocol).data(), forward.config.service.c_str(),
                              forward.config.listen_address.c_str(),
                              static_cast<unsigned int>(forward.config.listen_port), error.message().c_str());
            continue;
        }

        const ServiceKey service{forward.config.service, forward.config.protocol};
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
    auto &forward = datagram_forwards_.at(forward_id);
    std::array<std::uint8_t, DatagramHeader::maximum_user_payload + 1> payload;
    for (;;)
    {
        udp::endpoint source;
        const auto [error, size] =
            co_await forward.listener_socket.async_receive_from(asio::buffer(payload), source, use_nothrow_awaitable);
        if (error)
        {
            if (!forward.listener_socket.is_open() || error == asio::error::operation_aborted)
            {
                co_return;
            }
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
        if (send_error || sent != sockets.session_header.size() + size)
        {
            relay->fail("send relay datagram: " + (send_error ? send_error.message() : "truncated datagram"));
        }
    }
}

void Forwarder::open_datagram_forward(DatagramForwardId forward_id)
{
    auto &forward = datagram_forwards_.at(forward_id);
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
    auto &forward = datagram_forwards_.at(forward_id);
    if (state_ != State::Running || forward.retry_scheduled ||
        !service_nodes_.contains(ServiceKey{forward.config.service, RelayProtocol::Udp}))
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
    auto &forward = datagram_forwards_.at(forward_id);
    if (state_ != State::Running || !service_nodes_.contains(ServiceKey{forward.config.service, RelayProtocol::Udp}))
    {
        forward.retry_scheduled = false;
        co_return;
    }

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
    if (state_ != State::Running || relays_.size() >= 1000)
    {
        if (relay->forward)
        {
            schedule_datagram_retry(*relay->forward);
        }

        if (relay->producer())
        {
            send_control(relay->selection.server,
                         CtrlMessage(CtrlCommand::RelayReject, njson{{"uuid", relay->endpoint.at("uuid")},
                                                                     {"reason", "relay capacity reached"}}));
        }
        return;
    }

    relays_.emplace(relay->request, relay);
    if (relay->forward)
    {
        datagram_forwards_.at(*relay->forward).relay = relay;
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
            datagram_forwards_.at(*relay->forward).relay.reset();
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
            if (active->producer() && active->selection.server.id == route.id && active->endpoint.at("uuid") == *uuid)
            {
                return;
            }
        }
        auto relay = std::make_shared<AgentSession>(*this, allocate_request_id(), ServiceKey{service, protocol},
                                                    std::string{}, Clock::now() + relay_open_timeout_);
        relay->target = target->second;
        relay->selection.server = std::move(route);
        relay->selection.epoch = config::optional_unsigned(p, "epoch", true).value_or(0);
        relay->endpoint = p;
        start_relay(std::move(relay));
        return;
    }

    const auto request = config::optional_unsigned(p, "request_id");
    bool matched = false;
    for (const auto &[id, relay] : relays_)
    {
        if (relay->selection.server.id != route.id || relay->service.protocol != protocol)
        {
            continue;
        }
        const bool by_request = (command == CtrlCommand::RelayOpened || command == CtrlCommand::RelayError) &&
                                request && *request == id && !relay->producer();
        const bool by_uuid = command != CtrlCommand::RelayOpened && uuid && !relay->endpoint.empty() &&
                             relay->endpoint.at("uuid") == *uuid;
        if (by_request || by_uuid)
        {
            // A single-node relay may have both roles in this Agent and share one UUID.
            relay->handle(message);
            matched = true;
        }
    }

    if (!matched && command == CtrlCommand::RelayOpened && uuid)
    {
        send_control(route, CtrlMessage(CtrlCommand::RelayCancel, njson{{"uuid", *uuid}}));
    }
}

void Forwarder::spawn_task(asio::awaitable<void> task, std::string_view context)
{
    ++active_tasks_;
    try
    {
        asio::co_spawn(transfer_work_.get_executor(), std::move(task),
                       [self = shared_from_this(), context](std::exception_ptr failure) {
                           if (failure && self->state_ == State::Running)
                           {
                               PROXY_ERROR_PRINT("%.*s stopped reason=%s", static_cast<int>(context.size()),
                                                 context.data(), exception_description(failure).c_str());
                           }
                           assert(self->active_tasks_ != 0);
                           --self->active_tasks_;
                           self->finish_if_stopped();
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

    channel->send(std::move(message));
    return true;
}

std::uint64_t Forwarder::allocate_request_id()
{
    for (;;)
    {
        const auto request = next_request_id_++;
        if (next_request_id_ == 0)
        {
            next_request_id_ = 1;
        }
        if (!relays_.contains(request))
        {
            return request;
        }
    }
}
