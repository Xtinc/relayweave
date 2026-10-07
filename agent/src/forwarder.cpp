#include "forwarder.h"
#include <algorithm>
#include <array>

using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

template <class Socket> static void close_socket(Socket &socket) noexcept
{
    asio::error_code ignored;
    socket.lowest_layer().close(ignored);
}

static asio::awaitable<void> connect_tcp(tcp::resolver &resolver, tcp::socket &socket, const std::string &host,
                                         std::uint16_t port, std::chrono::steady_clock::duration timeout)
{
    auto endpoints =
        co_await resolver.async_resolve(host, std::to_string(port), asio::cancel_after(timeout, asio::use_awaitable));
    co_await asio::async_connect(socket, endpoints, asio::cancel_after(timeout, asio::use_awaitable));
}

static asio::awaitable<void> connect_udp(udp::resolver &resolver, udp::socket &socket, const std::string &host,
                                         std::uint16_t port, std::chrono::steady_clock::duration timeout)
{
    auto endpoints =
        co_await resolver.async_resolve(host, std::to_string(port), asio::cancel_after(timeout, asio::use_awaitable));
    const auto endpoint = endpoints.begin()->endpoint();
    socket.open(endpoint.protocol());
    socket.connect(endpoint);
}

Forwarder::StreamForward::StreamForward(asio::any_io_executor executor, AgentForwardConfig config)
    : acceptor(std::move(executor)), config(std::move(config))
{
}

Forwarder::DatagramForward::DatagramForward(asio::any_io_executor executor, AgentForwardConfig config)
    : listener_socket(executor), retry_timer(std::move(executor)), config(std::move(config))
{
}

struct Forwarder::StreamRelay
{
    StreamRelay(asio::any_io_executor executor, ServerRoute server, RelayProtocol protocol, std::string service)
        : service(std::move(service)), server(std::move(server)), resolver(executor), local_socket(executor),
          transfer_socket(executor), open_timer(executor), protocol(protocol)
    {
    }

    StreamRelay(asio::any_io_executor executor, ServerRoute server, RelayProtocol protocol, std::string service,
                tcp::socket socket)
        : service(std::move(service)), server(std::move(server)), resolver(executor), local_socket(std::move(socket)),
          transfer_socket(executor), open_timer(executor), protocol(protocol)
    {
    }

    void cancel() noexcept
    {
        resolver.cancel();
        try
        {
            open_timer.cancel();
        }
        catch (...)
        {
        }

        close_socket(local_socket);
        if (tls_stream)
        {
            close_socket(*tls_stream);
        }
        else
        {
            close_socket(transfer_socket);
        }
    }

    std::string service;
    ServerRoute server;
    std::uint64_t uuid = 0;
    tcp::resolver resolver;
    tcp::socket local_socket;
    tcp::socket transfer_socket;
    std::optional<TLSStream> tls_stream;
    asio::steady_timer open_timer;
    RelayProtocol protocol;
};

struct Forwarder::DatagramRelay
{
    enum class State
    {
        WaitingReady,
        Ready,
        Cancelled,
    };

    DatagramRelay(asio::any_io_executor executor, ServerRoute server, std::string service, std::uint64_t uuid,
                  std::uint64_t session_id, std::uint64_t ticket,
                  std::optional<DatagramForwardId> forward_id = std::nullopt)
        : service(std::move(service)), server(std::move(server)), uuid(uuid), forward_id(forward_id),
          session_header(DatagramHeader::encode(session_id)),
          attach_frame(WireMessage::pack(
              RelayAttach::to_msg({forward_id ? RelayAttach::Consumer : RelayAttach::Producer, uuid, ticket}))),
          resolver(executor), transfer_socket(executor), ready_waiter(executor)
    {
        if (is_producer())
        {
            target_socket.emplace(executor);
        }
    }

    bool is_producer() const noexcept
    {
        return !forward_id;
    }

    void cancel() noexcept
    {
        state = State::Cancelled;
        resolver.cancel();
        try
        {
            ready_waiter.cancel();
        }
        catch (...)
        {
        }

        if (target_socket)
        {
            close_socket(*target_socket);
        }
        close_socket(transfer_socket);
    }

    std::string service;
    ServerRoute server;
    std::uint64_t uuid;
    std::optional<DatagramForwardId> forward_id;
    DatagramHeader::Buffer session_header;
    BytesBuf attach_frame;
    udp::resolver resolver;
    std::optional<udp::socket> target_socket;
    udp::socket transfer_socket;
    asio::steady_timer ready_waiter;
    bool attach_sent = false;
    State state = State::WaitingReady;
};

Forwarder::Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                     std::optional<std::string> server_name, std::chrono::steady_clock::duration handshake_timeout,
                     std::chrono::steady_clock::duration connect_timeout,
                     std::chrono::steady_clock::duration stream_open_timeout, std::vector<AgentServiceConfig> services,
                     std::vector<AgentForwardConfig> forwards)
    : agent_(agent), transfer_work_(asio::make_work_guard(std::move(executor))), ssl_context_(ssl_context),
      server_name_(std::move(server_name)), handshake_timeout_(handshake_timeout), connect_timeout_(connect_timeout),
      stream_open_timeout_(stream_open_timeout), stopped_waiter_(transfer_work_.get_executor())
{
    stopped_waiter_.expires_at(std::chrono::steady_clock::time_point::max());
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
            const auto inserted = datagram_forwards_.insert(
                id, std::nullopt, DatagramForward(transfer_work_.get_executor(), std::move(forward)));
            if (!inserted.second)
            {
                throw std::logic_error("Datagram forward ID is already registered");
            }
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

        datagram_forwards_.for_each([](DatagramForwardId, DatagramForward &forward) {
            const udp::endpoint endpoint(asio::ip::make_address(forward.config.listen_address),
                                         forward.config.listen_port);
            forward.listener_socket.open(endpoint.protocol());
            forward.listener_socket.set_option(asio::socket_base::reuse_address(true));
            forward.listener_socket.bind(endpoint);
        });
    }
    catch (...)
    {
        for (auto &forward : stream_forwards_)
        {
            asio::error_code ignored;
            forward.acceptor.close(ignored);
        }

        datagram_forwards_.for_each([](DatagramForwardId, DatagramForward &forward) {
            asio::error_code ignored;
            forward.listener_socket.close(ignored);
        });
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

    datagram_forwards_.for_each([this](DatagramForwardId id, DatagramForward &forward) {
        PROXY_INFO_PRINT("Forward listening udp %s:%u -> service=%s", forward.config.listen_address.c_str(),
                         static_cast<unsigned int>(forward.config.listen_port), forward.config.service.c_str());
        spawn_task(receive_local_datagrams(id), "Local UDP receive loop");
    });
}

void Forwarder::set_route(std::string service, RelayProtocol protocol, ServerRoute route)
{
    if (state_ != State::Running)
    {
        return;
    }

    RouteKey key{service, protocol};
    RouteTarget target{std::move(route.host), std::move(route.channel)};
    auto server_id = std::move(route.id);
    routes_.erase_primary(key);
    if (!routes_.insert(std::move(key), std::move(server_id), std::move(target)).second)
    {
        throw std::logic_error("Forwarder route indices are inconsistent");
    }

    datagram_forwards_.for_each([&](DatagramForwardId id, DatagramForward &forward) {
        if (forward.config.service == service && forward.config.protocol == protocol)
        {
            forward.retry_delay = std::chrono::milliseconds(500);
            open_datagram_forward(id);
        }
    });
}

void Forwarder::clear_route(const std::string &service, RelayProtocol protocol)
{
    if (state_ != State::Running)
    {
        return;
    }

    routes_.erase_primary(RouteKey{service, protocol});
    if (protocol != RelayProtocol::Udp)
    {
        return;
    }

    datagram_forwards_.for_each([&](DatagramForwardId id, DatagramForward &forward) {
        if (forward.config.service != service)
        {
            return;
        }

        datagram_forwards_.set_secondary(id, std::nullopt);
        forward.retry_timer.cancel();
        forward.retry_scheduled = false;
        forward.server.reset();

        if (forward.relay)
        {
            forward.relay->cancel();
        }
        forward.relay.reset();
    });
}

void Forwarder::clear_server(const std::string &server_id)
{
    if (state_ != State::Running)
    {
        return;
    }

    routes_.erase_secondary(server_id);
    std::erase_if(pending_stream_opens_, [&](const auto &entry) {
        if (entry.second->server.id != server_id)
        {
            return false;
        }

        entry.second->cancel();
        return true;
    });

    std::erase_if(active_stream_relays_, [&](const auto &entry) {
        const auto relay = entry.second.lock();
        if (!relay)
        {
            return true;
        }
        if (relay->server.id != server_id)
        {
            return false;
        }

        relay->cancel();
        return true;
    });

    std::erase_if(active_datagram_relays_, [&](const auto &entry) {
        const auto relay = entry.second.lock();
        if (!relay)
        {
            return true;
        }
        if (relay->server.id != server_id)
        {
            return false;
        }

        relay->cancel();
        return true;
    });

    datagram_forwards_.for_each([&](DatagramForwardId id, DatagramForward &forward) {
        if (forward.server && forward.server->id == server_id)
        {
            datagram_forwards_.set_secondary(id, std::nullopt);
            forward.retry_timer.cancel();
            forward.retry_scheduled = false;

            if (forward.relay)
            {
                forward.relay->cancel();
            }
            forward.relay.reset();

            forward.server.reset();
        }
    });
}

asio::awaitable<void> Forwarder::async_stop()
{
    if (state_ == State::Stopped)
    {
        co_return;
    }

    if (state_ == State::Stopping)
    {
        co_await stopped_waiter_.async_wait(use_nothrow_awaitable);
        co_return;
    }

    state_ = State::Stopping;
    routes_.clear();
    for (auto &forward : stream_forwards_)
    {
        asio::error_code ignored;
        forward.acceptor.close(ignored);
    }

    datagram_forwards_.for_each([](DatagramForwardId, DatagramForward &forward) {
        asio::error_code ignored;
        forward.listener_socket.close(ignored);
        forward.retry_timer.cancel();
    });

    close_all_relays();
    finish_if_stopped();
    if (state_ != State::Stopped)
    {
        co_await stopped_waiter_.async_wait(use_nothrow_awaitable);
    }
}

void Forwarder::stream_relay_offer(ServerRoute route, RelayProtocol protocol, std::string service, std::uint64_t uuid,
                                   std::uint64_t ticket, std::uint16_t transfer_port)
{
    if (state_ != State::Running)
    {
        return;
    }

    const auto target = services_.find(service);
    if (route.channel.expired() || target == services_.end() || target->second.protocol != protocol ||
        protocol == RelayProtocol::Udp)
    {
        send_control(route, CtrlMessage{CtrlCommand::RelayReject,
                                        njson{{"uuid", uuid}, {"reason", "stream service unavailable"}}});
        return;
    }
    auto relay =
        std::make_shared<StreamRelay>(transfer_work_.get_executor(), std::move(route), protocol, std::move(service));
    relay->uuid = uuid;
    start_stream_relay(std::move(relay), RelayAttach{RelayAttach::Producer, uuid, ticket}, transfer_port,
                       &target->second);
}

void Forwarder::datagram_relay_offer(ServerRoute route, std::string service, std::uint64_t uuid,
                                     std::uint64_t session_id, std::uint64_t ticket, std::uint16_t transfer_port)
{
    if (state_ != State::Running)
    {
        return;
    }

    const auto target = services_.find(service);
    if (route.channel.expired() || target == services_.end() || target->second.protocol != RelayProtocol::Udp)
    {
        send_control(
            route, CtrlMessage{CtrlCommand::RelayReject, njson{{"uuid", uuid}, {"reason", "UDP service unavailable"}}});
        return;
    }
    auto relay = std::make_shared<DatagramRelay>(transfer_work_.get_executor(), std::move(route), std::move(service),
                                                 uuid, session_id, ticket);
    start_datagram_relay(std::move(relay), transfer_port, &target->second);
}

void Forwarder::stream_relay_opened(ServerRoute route, RelayProtocol protocol, std::uint64_t request_id,
                                    std::uint64_t uuid, std::uint64_t ticket, std::uint16_t transfer_port)
{
    if (state_ != State::Running)
    {
        return;
    }

    const auto pending = pending_stream_opens_.find(request_id);
    if (pending == pending_stream_opens_.end())
    {
        send_control(route, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", uuid}}});
        return;
    }

    if (pending->second->protocol != protocol || pending->second->server.id != route.id)
    {
        auto relay = std::move(pending->second);
        pending_stream_opens_.erase(pending);
        relay->cancel();
        send_control(route, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", uuid}}});
        return;
    }

    auto relay = std::move(pending->second);
    pending_stream_opens_.erase(pending);
    relay->open_timer.cancel();
    relay->uuid = uuid;
    start_stream_relay(std::move(relay), RelayAttach{RelayAttach::Consumer, uuid, ticket}, transfer_port, nullptr);
}

void Forwarder::datagram_relay_opened(ServerRoute route, std::uint64_t request_id, std::uint64_t uuid,
                                      std::uint64_t session_id, std::uint64_t ticket, std::uint16_t transfer_port)
{
    if (state_ != State::Running)
    {
        return;
    }

    const auto pending = datagram_forwards_.find_secondary_entry(request_id);
    if (!pending)
    {
        send_control(route, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", uuid}}});
        return;
    }

    const auto forward_id = *pending.primary;
    auto &forward = *pending.value;
    if (!forward.server || forward.server->id != route.id)
    {
        send_control(route, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", uuid}}});
        return;
    }

    if (!datagram_forwards_.set_secondary(forward_id, std::nullopt))
    {
        throw std::logic_error("Pending datagram forward is not registered");
    }
    auto relay = std::make_shared<DatagramRelay>(transfer_work_.get_executor(), std::move(route),
                                                 forward.config.service, uuid, session_id, ticket, forward_id);
    forward.relay = relay;
    start_datagram_relay(std::move(relay), transfer_port, nullptr);
}

void Forwarder::relay_open_failed(ServerRoute route, std::uint64_t request_id, RelayProtocol protocol,
                                  std::optional<std::uint64_t> uuid, std::string reason)
{
    if (state_ != State::Running)
    {
        return;
    }

    if (protocol != RelayProtocol::Udp)
    {
        const auto pending = pending_stream_opens_.find(request_id);
        PROXY_ERROR_PRINT("Relay open failed %s service=%s request_id=%llu uuid=%llu reason=%s",
                          relay_protocol_name(protocol).data(),
                          pending == pending_stream_opens_.end() ? "-" : pending->second->service.c_str(),
                          static_cast<unsigned long long>(request_id),
                          static_cast<unsigned long long>(uuid.value_or(0)), reason.c_str());
        if (pending == pending_stream_opens_.end())
        {
            if (uuid)
            {
                const auto [first, last] = active_stream_relays_.equal_range(RelayKey{route.id, *uuid});
                for (auto active = first; active != last; ++active)
                {
                    if (auto relay = active->second.lock())
                    {
                        relay->cancel();
                    }
                }
            }
            return;
        }

        auto relay = std::move(pending->second);
        pending_stream_opens_.erase(pending);
        relay->cancel();
        return;
    }

    const auto pending = datagram_forwards_.find_secondary_entry(request_id);
    PROXY_ERROR_PRINT("Relay open failed udp service=%s request_id=%llu uuid=%llu reason=%s",
                      pending ? pending.value->config.service.c_str() : "-",
                      static_cast<unsigned long long>(request_id), static_cast<unsigned long long>(uuid.value_or(0)),
                      reason.c_str());
    if (!pending)
    {
        if (uuid)
        {
            const auto [first, last] = active_datagram_relays_.equal_range(RelayKey{route.id, *uuid});
            for (auto active = first; active != last; ++active)
            {
                if (auto relay = active->second.lock())
                {
                    relay->cancel();
                }
            }
        }
        return;
    }

    const auto forward_id = *pending.primary;
    if (!datagram_forwards_.set_secondary(forward_id, std::nullopt))
    {
        throw std::logic_error("Pending datagram forward is not registered");
    }
    schedule_datagram_retry(forward_id);
}

void Forwarder::relay_ready(ServerRoute route, std::uint64_t uuid, RelayProtocol protocol)
{
    if (state_ != State::Running)
    {
        return;
    }

    if (protocol != RelayProtocol::Udp)
    {
        return;
    }

    const auto [first, last] = active_datagram_relays_.equal_range(RelayKey{route.id, uuid});
    if (first == last)
    {
        return;
    }

    for (auto iterator = first; iterator != last; ++iterator)
    {
        const auto relay = iterator->second.lock();
        if (!relay)
        {
            continue;
        }

        if (relay->state != DatagramRelay::State::WaitingReady)
        {
            continue;
        }

        relay->state = DatagramRelay::State::Ready;
        relay->ready_waiter.cancel();
        if (relay->forward_id)
        {
            if (auto *forward = datagram_forwards_.find_primary(*relay->forward_id))
            {
                forward->retry_delay = std::chrono::milliseconds(500);
            }
        }
    }
}

void Forwarder::relay_closed(ServerRoute route, std::uint64_t uuid, RelayProtocol protocol, std::string reason)
{
    if (state_ != State::Running)
    {
        return;
    }

    if (protocol != RelayProtocol::Udp)
    {
        return;
    }

    const auto [first, last] = active_datagram_relays_.equal_range(RelayKey{route.id, uuid});
    if (first == last)
    {
        return;
    }

    for (auto iterator = first; iterator != last; ++iterator)
    {
        const auto relay = iterator->second.lock();
        if (!relay)
        {
            continue;
        }

        PROXY_DEBUG_PRINT("Relay closing udp service=%s uuid=%llu reason=%s", relay->service.c_str(),
                          static_cast<unsigned long long>(uuid), reason.c_str());
        relay->cancel();
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

        auto route = find_route(forward.config.service, forward.config.protocol);
        if (!route)
        {
            close_socket(socket);
            PROXY_ERROR_PRINT("Relay rejected %s service=%s reason=no server route",
                              relay_protocol_name(forward.config.protocol).data(), forward.config.service.c_str());
            continue;
        }

        // Routing state belongs to control_io; this accept loop runs on transfer_io.
        co_await asio::co_spawn(
            agent_.control_executor_,
            agent_.calculate_service_paths(ServiceKey{forward.config.service, forward.config.protocol}, route->id),
            asio::use_awaitable);
        if (state_ != State::Running)
        {
            co_return;
        }

        const auto request_id = allocate_request_id();
        auto relay = std::make_shared<StreamRelay>(transfer_work_.get_executor(), std::move(*route),
                                                   forward.config.protocol, forward.config.service, std::move(socket));
        if (!send_control(
                relay->server,
                CtrlMessage{CtrlCommand::RelayOpen, njson{{"request_id", request_id},
                                                          {"service", forward.config.service},
                                                          {"protocol", relay_protocol_name(forward.config.protocol)}}}))
        {
            relay->cancel();
            continue;
        }

        PROXY_DEBUG_PRINT("Relay opening service=%s/%s request_id=%llu", relay->service.c_str(),
                          relay_protocol_name(relay->protocol).data(), static_cast<unsigned long long>(request_id));
        pending_stream_opens_.emplace(request_id, relay);
        relay->open_timer.expires_after(stream_open_timeout_);
        relay->open_timer.async_wait([self = shared_from_this(), request_id,
                                      protocol = forward.config.protocol](const asio::error_code &error) {
            if (!error)
            {
                const auto pending = self->pending_stream_opens_.find(request_id);
                if (pending == self->pending_stream_opens_.end())
                {
                    return;
                }

                auto route = pending->second->server;
                self->send_control(route, CtrlMessage{CtrlCommand::RelayCancel, njson{{"request_id", request_id}}});
                self->relay_open_failed(std::move(route), request_id, protocol, std::nullopt, "relay open timed out");
            }
        });
    }
}

asio::awaitable<void> Forwarder::receive_local_datagrams(DatagramForwardId forward_id)
{
    std::array<std::uint8_t, DatagramHeader::maximum_user_payload + 1> payload{};
    for (;;)
    {
        if (!datagram_forwards_.find_primary(forward_id))
        {
            co_return;
        }

        udp::endpoint source;
        auto [error, size] =
            co_await datagram_forwards_.find_primary(forward_id)
                ->listener_socket.async_receive_from(asio::buffer(payload), source, use_nothrow_awaitable);
        if (error)
        {
            const auto forward = datagram_forwards_.find_primary(forward_id);
            if (!forward || !forward->listener_socket.is_open() || error == asio::error::operation_aborted)
            {
                co_return;
            }

            if (error == asio::error::message_size)
            {
                continue;
            }

            PROXY_ERROR_PRINT("Forward receive failed udp service=%s reason=%s", forward->config.service.c_str(),
                              error.message().c_str());
            continue;
        }

        if (size > DatagramHeader::maximum_user_payload)
        {
            continue;
        }

        auto relay = [&]() -> std::shared_ptr<DatagramRelay> {
            auto *forward = datagram_forwards_.find_primary(forward_id);
            if (!forward)
            {
                return {};
            }

            if (!forward->local_peer)
            {
                forward->local_peer = source;
            }
            else if (forward->local_peer->address() != source.address())
            {
                return {};
            }
            else if (*forward->local_peer != source)
            {
                forward->local_peer = source;
            }
            return forward->relay;
        }();

        if (!relay || relay->state != DatagramRelay::State::Ready || !relay->transfer_socket.is_open())
        {
            continue;
        }

        const std::array buffers{asio::buffer(relay->session_header), asio::buffer(payload.data(), size)};
        auto [send_error, sent] = co_await relay->transfer_socket.async_send(buffers, use_nothrow_awaitable);
        if (send_error || sent != relay->session_header.size() + size)
        {
            if (send_error != asio::error::operation_aborted && state_ == State::Running)
                PROXY_ERROR_PRINT("Relay send failed udp service=%s uuid=%llu reason=%s",
                                  relay->service.c_str(), static_cast<unsigned long long>(relay->uuid),
                                  send_error ? send_error.message().c_str() : "truncated datagram");
            relay->cancel();
        }
    }
}

void Forwarder::open_datagram_forward(DatagramForwardId forward_id)
{
    if (state_ != State::Running)
    {
        return;
    }

    auto *forward = datagram_forwards_.find_primary(forward_id);
    if (!forward || datagram_forwards_.secondary_key(forward_id) || forward->relay || forward->retry_scheduled)
    {
        return;
    }

    auto route = find_route(forward->config.service, RelayProtocol::Udp);
    if (!route)
    {
        return;
    }

    const auto request_id = allocate_request_id();
    if (!send_control(*route, CtrlMessage{CtrlCommand::RelayOpen, njson{{"request_id", request_id},
                                                                        {"service", forward->config.service},
                                                                        {"protocol", "udp"}}}))
    {
        schedule_datagram_retry(forward_id);
        return;
    }

    forward->server = std::move(*route);
    if (!datagram_forwards_.set_secondary(forward_id, request_id))
    {
        forward->server.reset();
        throw std::logic_error("Datagram forward pending index is inconsistent");
    }
}

void Forwarder::schedule_datagram_retry(DatagramForwardId forward_id)
{
    if (state_ != State::Running)
    {
        return;
    }

    auto *forward = datagram_forwards_.find_primary(forward_id);
    if (!forward || !find_route(forward->config.service, RelayProtocol::Udp) || forward->retry_scheduled)
    {
        return;
    }

    forward->retry_scheduled = true;
    forward->retry_timer.expires_after(forward->retry_delay);
    forward->retry_delay =
        std::min(forward->retry_delay * 2, std::chrono::steady_clock::duration(std::chrono::seconds(10)));
    forward->retry_timer.async_wait([self = shared_from_this(), forward_id](const asio::error_code &error) {
        if (auto *forward = self->datagram_forwards_.find_primary(forward_id))
        {
            forward->retry_scheduled = false;
            if (!error)
            {
                self->open_datagram_forward(forward_id);
            }
        }
    });
}

void Forwarder::start_stream_relay(std::shared_ptr<StreamRelay> relay, RelayAttach attach, std::uint16_t transfer_port,
                                   const AgentServiceConfig *target)
{
    if (state_ != State::Running)
    {
        relay->cancel();
        return;
    }

    std::erase_if(active_stream_relays_, [](const auto &entry) { return entry.second.expired(); });
    active_stream_relays_.emplace(RelayKey{relay->server.id, attach.uuid}, relay);
    spawn_task(run_stream_relay(std::move(relay), attach, transfer_port, target), "Stream relay task");
}

asio::awaitable<void> Forwarder::run_stream_relay(std::shared_ptr<StreamRelay> relay, RelayAttach attach,
                                                  std::uint16_t transfer_port, const AgentServiceConfig *target)
{
    std::string failure;
    bool attached = false;
    const char *stage = target ? "connect local target" : "connect transfer endpoint";
    try
    {
        if (target)
        {
            PROXY_DEBUG_PRINT("Relay target connecting service=%s/%s uuid=%llu -> %s:%u", relay->service.c_str(),
                              relay_protocol_name(relay->protocol).data(), static_cast<unsigned long long>(attach.uuid),
                              target->target_host.c_str(), static_cast<unsigned int>(target->target_port));
            co_await connect_tcp(relay->resolver, relay->local_socket, target->target_host, target->target_port,
                                 connect_timeout_);
        }

        stage = "connect transfer endpoint";
        PROXY_DEBUG_PRINT("Relay transfer connecting service=%s/%s uuid=%llu", relay->service.c_str(),
                          relay_protocol_name(relay->protocol).data(), static_cast<unsigned long long>(attach.uuid));
        co_await connect_tcp(relay->resolver, relay->transfer_socket, relay->server.host, transfer_port,
                             connect_timeout_);
        const auto frame = WireMessage::pack(RelayAttach::to_msg(attach));
        if (relay->protocol == RelayProtocol::Tls)
        {
            stage = "configure transfer TLS";
            relay->tls_stream.emplace(std::move(relay->transfer_socket), ssl_context_);
            configure_tls_client(*relay->tls_stream, relay->server.host, server_name_);
            stage = "handshake transfer TLS";
            co_await relay->tls_stream->async_handshake(asio::ssl::stream_base::client,
                                                        asio::cancel_after(handshake_timeout_, asio::use_awaitable));
            stage = "send relay.attach";
            co_await asio::async_write(*relay->tls_stream, asio::buffer(frame),
                                       asio::cancel_after(connect_timeout_, asio::use_awaitable));
        }
        else
        {
            stage = "send relay.attach";
            co_await asio::async_write(relay->transfer_socket, asio::buffer(frame),
                                       asio::cancel_after(connect_timeout_, asio::use_awaitable));
        }

        attached = true;
        PROXY_INFO_PRINT("Relay [+] %s %s service=%s uuid=%llu", relay_protocol_name(relay->protocol).data(),
                         attach.role == RelayAttach::Producer ? "producer" : "consumer", relay->service.c_str(),
                         static_cast<unsigned long long>(attach.uuid));
        stage = "forward stream";
        if (relay->protocol == RelayProtocol::Tls)
        {
            co_await relay_tls(relay->local_socket, *relay->tls_stream);
        }
        else
        {
            co_await relay_tcp(relay->local_socket, relay->transfer_socket);
        }
    }
    catch (const std::exception &exception)
    {
        failure = exception.what();
    }
    catch (...)
    {
        failure = "unknown stream relay error";
    }

    relay->cancel();
    const auto [first, last] = active_stream_relays_.equal_range(RelayKey{relay->server.id, attach.uuid});
    const auto active = std::find_if(first, last, [&](const auto &entry) { return entry.second.lock() == relay; });
    if (active != last)
    {
        active_stream_relays_.erase(active);
    }

    if (attached)
    {
        PROXY_INFO_PRINT("Relay [x] %s %s service=%s uuid=%llu reason=%s",
                         relay_protocol_name(relay->protocol).data(),
                         attach.role == RelayAttach::Producer ? "producer" : "consumer", relay->service.c_str(),
                         static_cast<unsigned long long>(attach.uuid),
                         state_ != State::Running ? "agent stopping"
                         : failure.empty()        ? "I/O ended"
                                                  : "error");
    }

    if (!failure.empty() && state_ == State::Running && !relay->server.channel.expired())
    {
        PROXY_ERROR_PRINT("Relay failed %s %s service=%s uuid=%llu stage=%s transfer=%s:%u reason=%s",
                          relay_protocol_name(relay->protocol).data(),
                          attach.role == RelayAttach::Producer ? "producer" : "consumer", relay->service.c_str(),
                          static_cast<unsigned long long>(attach.uuid), stage, relay->server.host.c_str(),
                          static_cast<unsigned int>(transfer_port), failure.c_str());
        if (!attached && attach.role == RelayAttach::Producer)
        {
            send_control(relay->server,
                         CtrlMessage{CtrlCommand::RelayReject, njson{{"uuid", attach.uuid}, {"reason", failure}}});
        }
        else if (!attached)
        {
            send_control(relay->server, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", attach.uuid}}});
        }
    }
}

void Forwarder::start_datagram_relay(std::shared_ptr<DatagramRelay> relay, std::uint16_t transfer_port,
                                     const AgentServiceConfig *target)
{
    if (state_ != State::Running)
    {
        relay->cancel();
        return;
    }

    std::erase_if(active_datagram_relays_, [](const auto &entry) { return entry.second.expired(); });
    active_datagram_relays_.emplace(RelayKey{relay->server.id, relay->uuid}, relay);
    if (relay->is_producer())
    {
        spawn_task(run_datagram_producer(std::move(relay), transfer_port, *target), "UDP producer task");
    }
    else
    {
        spawn_task(run_datagram_consumer(std::move(relay), transfer_port), "UDP consumer task");
    }
}

asio::awaitable<void> Forwarder::prepare_datagram_relay(const std::shared_ptr<DatagramRelay> &relay,
                                                        std::uint16_t transfer_port)
{
    if (relay->state == DatagramRelay::State::Cancelled)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }

    PROXY_DEBUG_PRINT("Relay transfer connecting udp service=%s uuid=%llu", relay->service.c_str(),
                      static_cast<unsigned long long>(relay->uuid));
    co_await connect_udp(relay->resolver, relay->transfer_socket, relay->server.host, transfer_port, connect_timeout_);
    if (relay->state == DatagramRelay::State::Cancelled)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }

    PROXY_DEBUG_PRINT("Relay attaching udp service=%s uuid=%llu", relay->service.c_str(),
                      static_cast<unsigned long long>(relay->uuid));
    while (relay->state != DatagramRelay::State::Ready)
    {
        const auto sent = co_await relay->transfer_socket.async_send(
            asio::buffer(relay->attach_frame), asio::cancel_after(connect_timeout_, asio::use_awaitable));
        if (sent != relay->attach_frame.size())
        {
            throw std::runtime_error("UDP relay attach was truncated");
        }
        relay->attach_sent = true;
        if (relay->state == DatagramRelay::State::Ready)
        {
            break;
        }
        if (relay->state == DatagramRelay::State::Cancelled)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }

        relay->ready_waiter.expires_after(std::chrono::milliseconds(500));
        const auto [wait_error] = co_await relay->ready_waiter.async_wait(use_nothrow_awaitable);
        if (relay->state == DatagramRelay::State::Ready)
        {
            break;
        }
        if (relay->state == DatagramRelay::State::Cancelled || wait_error == asio::error::operation_aborted)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        if (wait_error)
        {
            throw asio::system_error(wait_error);
        }
    }
}

asio::awaitable<void> Forwarder::run_datagram_producer(std::shared_ptr<DatagramRelay> relay,
                                                       std::uint16_t transfer_port, const AgentServiceConfig &target)
{
    std::string failure;
    const char *stage = "connect target";
    try
    {
        PROXY_DEBUG_PRINT("Relay target connecting udp service=%s uuid=%llu -> %s:%u", relay->service.c_str(),
                          static_cast<unsigned long long>(relay->uuid), target.target_host.c_str(),
                          static_cast<unsigned int>(target.target_port));
        co_await connect_udp(relay->resolver, *relay->target_socket, target.target_host, target.target_port,
                             connect_timeout_);
        stage = "setup transfer";
        co_await prepare_datagram_relay(relay, transfer_port);
        PROXY_INFO_PRINT("Relay [+] udp producer service=%s uuid=%llu", relay->service.c_str(),
                         static_cast<unsigned long long>(relay->uuid));
        stage = "forward datagrams";
        co_await relay_udp_connected(*relay->target_socket, relay->transfer_socket, relay->session_header);
    }
    catch (const std::exception &exception)
    {
        failure = std::string(stage) + ": " + exception.what();
    }
    catch (...)
    {
        failure = "unknown UDP producer error";
    }

    if (std::string_view(stage) == "forward datagrams")
    {
        PROXY_INFO_PRINT("Relay [x] udp producer service=%s uuid=%llu reason=%s", relay->service.c_str(),
                         static_cast<unsigned long long>(relay->uuid),
                         state_ != State::Running                          ? "agent stopping"
                         : relay->state == DatagramRelay::State::Cancelled ? "cancelled"
                         : failure.empty()                                 ? "I/O ended"
                                                                           : "error");
    }
    finish_datagram_relay(relay, std::move(failure));
}

asio::awaitable<void> Forwarder::run_datagram_consumer(std::shared_ptr<DatagramRelay> relay,
                                                       std::uint16_t transfer_port)
{
    std::string failure;
    const char *stage = "setup transfer";
    try
    {
        co_await prepare_datagram_relay(relay, transfer_port);
        PROXY_INFO_PRINT("Relay [+] udp consumer service=%s uuid=%llu", relay->service.c_str(),
                         static_cast<unsigned long long>(relay->uuid));
        stage = "forward datagrams";
        std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> datagram{};
        for (;;)
        {
            auto [read_error, size] =
                co_await relay->transfer_socket.async_receive(asio::buffer(datagram), use_nothrow_awaitable);
            if (read_error)
            {
                if (read_error == asio::error::message_size)
                {
                    continue;
                }
                if (read_error != asio::error::operation_aborted)
                    failure = "receive transfer: " + read_error.message();
                break;
            }

            if (size < DatagramHeader::length || size > DatagramHeader::maximum_wire_payload)
            {
                continue;
            }

            if (!std::equal(relay->session_header.begin(), relay->session_header.end(), datagram.begin()) ||
                !relay->forward_id)
            {
                continue;
            }

            const auto forward_id = *relay->forward_id;
            const auto local_peer = [&]() -> std::optional<udp::endpoint> {
                const auto forward = datagram_forwards_.find_primary(forward_id);
                return forward ? forward->local_peer : std::nullopt;
            }();
            if (!local_peer || !datagram_forwards_.find_primary(forward_id))
            {
                continue;
            }

            const auto payload_size = size - DatagramHeader::length;
            auto [write_error, written] = co_await datagram_forwards_.find_primary(forward_id)
                                              ->listener_socket.async_send_to(
                                                  asio::buffer(datagram.data() + DatagramHeader::length, payload_size),
                                                  *local_peer, use_nothrow_awaitable);
            if (write_error || written != payload_size)
            {
                if (write_error != asio::error::operation_aborted)
                    failure = write_error ? "send local: " + write_error.message() : "send local: truncated datagram";
                break;
            }
        }
    }
    catch (const std::exception &exception)
    {
        failure = std::string(stage) + ": " + exception.what();
    }
    catch (...)
    {
        failure = "unknown UDP consumer error";
    }
    if (std::string_view(stage) == "forward datagrams")
    {
        PROXY_INFO_PRINT("Relay [x] udp consumer service=%s uuid=%llu reason=%s", relay->service.c_str(),
                         static_cast<unsigned long long>(relay->uuid),
                         state_ != State::Running                          ? "agent stopping"
                         : relay->state == DatagramRelay::State::Cancelled ? "cancelled"
                         : failure.empty()                                 ? "I/O ended"
                                                                           : "error");
    }
    finish_datagram_relay(relay, std::move(failure));
}

void Forwarder::finish_datagram_relay(const std::shared_ptr<DatagramRelay> &relay, std::string failure)
{
    const bool cancelled = relay->state == DatagramRelay::State::Cancelled || state_ != State::Running;
    relay->cancel();
    const auto [first, last] = active_datagram_relays_.equal_range(RelayKey{relay->server.id, relay->uuid});
    const auto active = std::find_if(first, last, [&](const auto &entry) { return entry.second.lock() == relay; });
    if (active != last)
    {
        active_datagram_relays_.erase(active);
    }

    if (relay->forward_id)
    {
        const auto forward_id = *relay->forward_id;
        if (auto *forward = datagram_forwards_.find_primary(forward_id); forward && forward->relay == relay)
        {
            forward->relay.reset();
            schedule_datagram_retry(forward_id);
        }
    }

    if (relay->attach_sent && !relay->server.channel.expired())
    {
        send_control(relay->server, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", relay->uuid}}});
    }

    if (failure.empty() || cancelled || relay->server.channel.expired())
    {
        return;
    }

    PROXY_ERROR_PRINT("Relay failed udp %s service=%s uuid=%llu reason=%s",
                      relay->is_producer() ? "producer" : "consumer", relay->service.c_str(),
                      static_cast<unsigned long long>(relay->uuid), failure.c_str());
    if (!relay->attach_sent)
    {
        if (relay->is_producer())
        {
            send_control(relay->server,
                         CtrlMessage{CtrlCommand::RelayReject, njson{{"uuid", relay->uuid}, {"reason", failure}}});
        }
        else
        {
            send_control(relay->server, CtrlMessage{CtrlCommand::RelayCancel, njson{{"uuid", relay->uuid}}});
        }
    }
}

void Forwarder::close_all_relays()
{
    for (auto &[_, relay] : pending_stream_opens_)
    {
        relay->cancel();
    }

    pending_stream_opens_.clear();

    for (const auto &[_, relay] : active_stream_relays_)
    {
        if (auto active = relay.lock())
        {
            active->cancel();
        }
    }
    active_stream_relays_.clear();

    for (const auto &[_, relay] : active_datagram_relays_)
    {
        if (auto active = relay.lock())
        {
            active->cancel();
        }
    }
    active_datagram_relays_.clear();

    datagram_forwards_.for_each([this](DatagramForwardId id, DatagramForward &forward) {
        datagram_forwards_.set_secondary(id, std::nullopt);
        forward.relay.reset();
        forward.local_peer.reset();
    });
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
                           if (self->active_tasks_ == 0)
                           {
                               PROXY_ERROR_PRINT("Forwarder task accounting underflow.");
                               return;
                           }

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
    stopped_waiter_.cancel();
}

std::optional<ServerRoute> Forwarder::find_route(const std::string &service, RelayProtocol protocol)
{
    const RouteKey key{service, protocol};
    const auto route = routes_.find_primary(key);
    const auto server_id = routes_.secondary_key(key);
    if (!route || !server_id || route->channel.expired())
    {
        return std::nullopt;
    }

    return ServerRoute{*server_id, route->host, route->channel};
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
        const auto request_id = next_request_id_++;
        if (next_request_id_ == 0)
        {
            next_request_id_ = 1;
        }

        if (request_id != 0 && !pending_stream_opens_.contains(request_id) &&
            !datagram_forwards_.find_secondary(request_id))
        {
            return request_id;
        }
    }
}
