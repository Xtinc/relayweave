#include "datagram_mgr.h"
#include "frame_io.h"
#include <algorithm>

std::optional<DatagramMgr::udp::endpoint> &DatagramMgr::LocalPair::endpoint(Side side)
{
    return side == Side::Producer ? producer_endpoint : consumer_endpoint;
}

const DatagramHeader::Buffer &DatagramMgr::LocalPair::session_header(Side side) const
{
    return side == Side::Producer ? producer_session_header : consumer_session_header;
}

bool DatagramMgr::LocalPair::allow(Side side, std::size_t payload_size)
{
    return side == Side::Producer ? rx_limiter.try_consume(payload_size) : tx_limiter.try_consume(payload_size);
}

DatagramMgr::DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                         udp::endpoint listen_endpoint, std::size_t capacity, TrafficLimitConfig config)
    : executor_(std::move(executor)), id_allocator_(std::move(id_allocator)),
      listen_endpoint_(std::move(listen_endpoint)), socket_(executor_), sends_(executor_, 16), capacity_(capacity),
      config_(std::move(config))
{
}

void DatagramMgr::start()
{
    if (started_ || stopped_)
    {
        throw std::logic_error("UDP manager can only be started once");
    }

    socket_.open(listen_endpoint_.protocol());
    try
    {
        socket_.set_option(asio::socket_base::reuse_address(true));
        socket_.bind(listen_endpoint_);
    }
    catch (...)
    {
        asio::error_code ignored;
        socket_.close(ignored);
        throw;
    }

    started_ = true;
    asio::co_spawn(
        executor_, [self = shared_from_this()]() -> asio::awaitable<void> { co_await self->receive_datagram(); },
        asio::detached);
    // One UDP socket write chain serves both single-node and multi-node relays.
    asio::co_spawn(
        executor_, [self = shared_from_this()]() -> asio::awaitable<void> { co_await self->send_datagrams(); },
        asio::detached);
}

// transfer_udp_io: queued packets own their bytes until this sole writer completes.
asio::awaitable<void> DatagramMgr::send_datagrams()
{
    try
    {
        for (;;)
        {
            auto request = co_await sends_.async_receive(asio::use_awaitable);
            if (request->cancelled)
            {
                continue;
            }
            if (stopped_)
            {
                request->done.try_send(asio::error::operation_aborted, std::size_t{});
                continue;
            }
            const auto [error, size] = co_await socket_.async_send_to(
                asio::buffer(request->data), request->destination, use_nothrow_awaitable);
            request->done.try_send(error, size);
        }
    }
    catch (const asio::system_error &error)
    {
        if (!stopped_)
        {
            PROXY_ERROR_PRINT("UDP writer failed reason=%s", error.what());
        }
    }
}

asio::awaitable<std::tuple<asio::error_code, std::size_t>> DatagramMgr::send_datagram(BytesBuf data, udp::endpoint destination)
{
    auto request = std::make_shared<DatagramSend>(executor_, std::move(data), std::move(destination));
    // UDP queue pressure drops a packet without terminating its relay.
    if (!sends_.try_send(asio::error_code{}, request))
    {
        co_return std::tuple{asio::error_code{}, std::size_t{}};
    }
    ScopeGuard cancel([request] { request->cancelled = true; });
    co_return co_await request->done.async_receive(use_nothrow_awaitable);
}

njson DatagramMgr::install_endpoint(int role)
{
    if (role != RelayAttach::Consumer && role != RelayAttach::Producer)
    {
        throw std::invalid_argument("invalid relay endpoint role");
    }
    if (stopped_ || local_pairs_.size() + path_endpoints_.size() >= capacity_)
    {
        throw std::runtime_error("relay endpoint capacity reached or datagram manager stopped");
    }
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    const auto session = allocate_session_id();
    auto endpoint = std::make_shared<PathEndpoint>(executor_, role);
    endpoint->ticket = generate_random_id();
    endpoint->session_id = session;
    path_endpoints_.emplace(uuid, endpoint);
    bindings_.emplace(session, Binding{uuid, role == RelayAttach::Producer ? Side::Producer : Side::Consumer});
    rollback.dismiss();
    return {{"uuid", uuid}, {"ticket", endpoint->ticket}, {"session_id", session},
            {"data_port", listen_endpoint_.port()}};
}

asio::awaitable<bool> DatagramMgr::wait_endpoint(std::uint64_t uuid)
{
    const auto it = path_endpoints_.find(uuid);
    if (it == path_endpoints_.end())
    {
        co_return false;
    }
    auto endpoint = it->second;
    co_return co_await endpoint->attached.wait() && endpoint->source && !endpoint->closed;
}

void DatagramMgr::bind_endpoint(std::uint64_t uuid, LnkChannel &channel, std::uint64_t epoch,
                                std::uint64_t flow_id, SRVTrafficPtr traffic, std::string accessor)
{
    path_endpoints_.at(uuid)->bind(channel, epoch, flow_id, config_, std::move(traffic), std::move(accessor));
}

asio::awaitable<void> DatagramMgr::read_endpoint(std::shared_ptr<PathEndpoint> endpoint)
{
    for (;;)
    {
        auto payload = co_await endpoint->received.async_receive(asio::use_awaitable);
        // UDP rate limits drop individual datagrams instead of delaying the shared listener.
        auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->rx_limiter : endpoint->tx_limiter;
        if (!limiter.try_consume(payload.size()))
        {
            continue;
        }
        const auto size = payload.size();
        co_await endpoint->send(LnkFrType::Data, std::move(payload));
        endpoint->count(true, size);
    }
}

asio::awaitable<void> DatagramMgr::write_endpoint(std::shared_ptr<PathEndpoint> endpoint)
{
    const auto header = DatagramHeader::encode(endpoint->session_id);
    for (;;)
    {
        auto frame = co_await endpoint->receive();
        auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->tx_limiter : endpoint->rx_limiter;
        if (!limiter.try_consume(frame.payload.size()))
        {
            continue;
        }
        BytesBuf datagram(header.begin(), header.end());
        datagram.insert(datagram.end(), frame.payload.begin(), frame.payload.end());
        const auto [error, sent] = co_await send_datagram(std::move(datagram), *endpoint->source);
        if (error)
        {
            throw asio::system_error(error);
        }
        if (!sent)
        {
            continue;
        }
        if (sent != header.size() + frame.payload.size())
        {
            throw std::runtime_error("truncated relay datagram");
        }
        endpoint->count(false, frame.payload.size());
    }
}

void DatagramMgr::activate_endpoint(std::uint64_t uuid)
{
    auto endpoint = path_endpoints_.at(uuid);
    if (endpoint->closed || !endpoint->source || !endpoint->channel || endpoint->active)
    {
        throw std::runtime_error("relay bridge unavailable");
    }
    endpoint->active = true;
}

asio::awaitable<void> DatagramMgr::run_endpoint(std::uint64_t uuid)
{
    auto endpoint = path_endpoints_.at(uuid);
    if (endpoint->closed || !endpoint->source || !endpoint->channel || !endpoint->active)
    {
        throw std::runtime_error("relay bridge unavailable");
    }
    if (endpoint->traffic)
    {
        endpoint->traffic->add_accessor(endpoint->accessor);
    }
    ScopeGuard accessor([endpoint] {
        if (endpoint->traffic)
        {
            endpoint->traffic->remove_accessor(endpoint->accessor);
        }
    });
    // Both directions are owned by this task and cancelled/drained together.
    co_await await_transfers(read_endpoint(endpoint), write_endpoint(endpoint));
}

void DatagramMgr::close_endpoint(std::uint64_t uuid)
{
    const auto it = path_endpoints_.find(uuid);
    if (it == path_endpoints_.end())
    {
        return;
    }
    auto endpoint = it->second;
    path_endpoints_.erase(it);
    endpoint->closed = true;
    bindings_.erase(endpoint->session_id);
    endpoint->received.cancel();
    endpoint->received.close();
    endpoint->attached.notify_all();
    id_allocator_->release(uuid);
}

njson DatagramMgr::install_pair()
{
    if (stopped_ || local_pairs_.size() + path_endpoints_.size() >= capacity_)
        throw std::runtime_error(stopped_ ? "server stopping" : "relay capacity reached");
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    auto pair = std::make_shared<LocalPair>(executor_);
    do
        pair->consumer_ticket = generate_random_id();
    while (pair->consumer_ticket == pair->producer_ticket);
    pair->producer_session_id = allocate_session_id();
    bindings_.emplace(pair->producer_session_id, Binding{uuid, Side::Producer});
    ScopeGuard binding_rollback([this, pair] { bindings_.erase(pair->producer_session_id); });
    pair->consumer_session_id = allocate_session_id();
    pair->producer_session_header = DatagramHeader::encode(pair->producer_session_id);
    pair->consumer_session_header = DatagramHeader::encode(pair->consumer_session_id);
    local_pairs_.emplace(uuid, pair);
    bindings_.emplace(pair->consumer_session_id, Binding{uuid, Side::Consumer});
    binding_rollback.dismiss();
    rollback.dismiss();
    return {{"producer", {{"uuid", uuid}, {"ticket", pair->producer_ticket},
                          {"session_id", pair->producer_session_id}, {"data_port", listen_endpoint_.port()}}},
            {"consumer", {{"uuid", uuid}, {"ticket", pair->consumer_ticket},
                          {"session_id", pair->consumer_session_id}, {"data_port", listen_endpoint_.port()}}}};
}

asio::awaitable<bool> DatagramMgr::wait_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    co_return co_await pair->attached.wait() && !pair->closed && pair->producer_endpoint && pair->consumer_endpoint;
}

void DatagramMgr::bind_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor)
{
    auto pair = local_pairs_.at(uuid);
    if (pair->closed || !pair->producer_endpoint || !pair->consumer_endpoint || pair->traffic || !traffic)
        throw std::runtime_error("local pair cannot bind service");
    pair->traffic = std::move(traffic);
    pair->accessor = std::move(accessor);
    pair->rx_limiter = TokenBucket(config_.rx_bytes_per_second, config_.rx_burst_bytes);
    pair->tx_limiter = TokenBucket(config_.tx_bytes_per_second, config_.tx_burst_bytes);
}

void DatagramMgr::activate_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    if (pair->closed || !pair->traffic || pair->active)
        throw std::runtime_error("local pair unavailable");
    pair->active = true;
    pair->traffic->add_accessor(pair->accessor);
}

asio::awaitable<void> DatagramMgr::run_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    if (pair->closed || !pair->active)
        throw std::runtime_error("local pair unavailable");
    // The shared listener performs I/O; this owned wait follows the resource lifetime.
    if (!co_await pair->finished.wait())
        throw asio::system_error(asio::error::operation_aborted);
}

void DatagramMgr::close_pair(std::uint64_t uuid)
{
    const auto it = local_pairs_.find(uuid);
    if (it == local_pairs_.end())
        return;
    auto pair = it->second;
    local_pairs_.erase(it);
    pair->closed = true;
    bindings_.erase(pair->producer_session_id);
    bindings_.erase(pair->consumer_session_id);
    pair->attached.notify_all();
    pair->finished.notify_all();
    if (pair->active)
        pair->traffic->remove_accessor(pair->accessor);
    id_allocator_->release(uuid);
}

void DatagramMgr::stop()
{
    if (stopped_)
    {
        return;
    }

    stopped_ = true;
    sends_.close();
    while (!path_endpoints_.empty())
    {
        close_endpoint(path_endpoints_.begin()->first);
    }
    asio::error_code ignored;
    socket_.close(ignored);
    bindings_.clear();
    while (!local_pairs_.empty())
        close_pair(local_pairs_.begin()->first);
}

asio::awaitable<void> DatagramMgr::receive_datagram()
{
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> buffer{};
    for (;;)
    {
        udp::endpoint source;
        auto [error, size] = co_await socket_.async_receive_from(asio::buffer(buffer), source, use_nothrow_awaitable);
        if (error)
        {
            if (stopped_ || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Transfer receive failed udp reason=%s", error.message().c_str());
            continue;
        }

        if (size > DatagramHeader::maximum_wire_payload)
        {
            continue;
        }

        auto route = route_datagram(std::span(buffer.data(), size), source);
        if (route)
        {
            co_await forward_datagram(std::span<const std::uint8_t>(buffer.data(), size), std::move(*route));
        }
    }
}

std::optional<DatagramMgr::RoutedDatagram> DatagramMgr::route_datagram(std::span<std::uint8_t> datagram,
                                                                       const udp::endpoint &source)
{
    if (stopped_ || datagram.size() < DatagramHeader::length)
    {
        return std::nullopt;
    }

    const auto session_id = DatagramHeader::decode(datagram.first(DatagramHeader::length));
    if (!session_id)
    {
        return std::nullopt;
    }

    const auto binding = bindings_.find(*session_id);
    if (binding == bindings_.end())
    {
        std::optional<RelayAttach> attach;
        try
        {
            attach = RelayAttach::from_msg(decode_ctrl_datagram(datagram));
        }
        catch (...)
        {
            return std::nullopt;
        }

        const auto path = path_endpoints_.find(attach->uuid);
        if (path != path_endpoints_.end())
        {
            auto &endpoint = *path->second;
            if (!endpoint.closed && !endpoint.source && attach->role == endpoint.role &&
                attach->ticket == endpoint.ticket)
            {
                endpoint.source = source;
                endpoint.attached.notify_all();
            }
            return std::nullopt;
        }
        const auto owner = local_pairs_.find(attach->uuid);
        if (owner == local_pairs_.end() || owner->second->active || owner->second->closed)
            return std::nullopt;
        const auto &pair = owner->second;
        const auto side = attach->role == RelayAttach::Producer ? Side::Producer : Side::Consumer;
        auto &source_endpoint = pair->endpoint(side);
        const auto expected = side == Side::Producer ? pair->producer_ticket : pair->consumer_ticket;
        if (source_endpoint || attach->ticket != expected)
            return std::nullopt;
        source_endpoint = source;
        if (pair->producer_endpoint && pair->consumer_endpoint)
            pair->attached.notify_all();
        return std::nullopt;
    }

    if (const auto path = path_endpoints_.find(binding->second.relay_uuid); path != path_endpoints_.end())
    {
        const auto &endpoint = path->second;
        const auto payload = datagram.subspan(DatagramHeader::length);
        if (endpoint->active && !endpoint->closed && endpoint->source == source &&
            payload.size() <= LnkFrameHeader::maximum_payload)
        {
            // A full local UDP queue drops this datagram, like the UDP rate limiter.
            endpoint->received.try_send(asio::error_code{}, BytesBuf(payload.begin(), payload.end()));
        }
        return std::nullopt;
    }
    const auto owner = local_pairs_.find(binding->second.relay_uuid);
    if (owner == local_pairs_.end())
    {
        return std::nullopt;
    }

    const auto &relay = owner->second;
    const auto side = binding->second.side;
    auto &source_endpoint = relay->endpoint(side);
    if (!source_endpoint)
    {
        return std::nullopt;
    }

    if (*source_endpoint != source || !relay->active || relay->closed)
    {
        return std::nullopt;
    }

    const auto payload_size = datagram.size() - DatagramHeader::length;
    if (!relay->allow(side, payload_size))
    {
        return std::nullopt;
    }

    const auto other_side = side == Side::Producer ? Side::Consumer : Side::Producer;
    const auto &destination = relay->endpoint(other_side);
    const auto &destination_header = relay->session_header(other_side);
    std::copy(destination_header.begin(), destination_header.end(), datagram.begin());
    return RoutedDatagram{*destination, relay->traffic, side, payload_size};
}

asio::awaitable<void> DatagramMgr::forward_datagram(std::span<const std::uint8_t> datagram, RoutedDatagram route)
{
    const auto [error, sent] =
        co_await send_datagram(BytesBuf(datagram.begin(), datagram.end()), route.destination);
    if (!error && !sent)
    {
        co_return;
    }
    if (!error && sent == datagram.size())
    {
        auto &counter = route.source_side == Side::Producer ? route.traffic->rx : route.traffic->tx;
        counter.add(route.payload_size);
    }

    if ((error || sent != datagram.size()) && !stopped_)
    {
        PROXY_ERROR_PRINT("Transfer send failed udp -> %s:%u reason=%s", route.destination.address().to_string().c_str(),
                          static_cast<unsigned int>(route.destination.port()),
                          error ? error.message().c_str() : "partial datagram send");
    }
}

std::uint64_t DatagramMgr::allocate_session_id() const
{
    std::uint64_t session_id;
    do
    {
        session_id = generate_random_id();
    } while (bindings_.contains(session_id));
    return session_id;
}
