#include "datagram_mgr.h"
#include "flow_protocol.h"
#include "frame_io.h"
#include <algorithm>

void DatagramMgr::LocalPair::close()
{
    LocalRelayPair::close();
    finished.notify_all();
}

asio::awaitable<void> DatagramMgr::RemotePair::bind_flow(FlowProtocol &channel, std::uint64_t epoch,
                                                         std::uint64_t flow_id, asio::any_io_executor transfer)
{
    if (flow)
    {
        throw std::runtime_error("relay endpoint cannot bind flow");
    }
    flow = co_await asio::co_spawn(
        channel.executor(),
        [protocol = channel.shared_from_this(), epoch, flow_id, reverse = role == RelayAttach::Producer,
         transfer]() -> asio::awaitable<std::shared_ptr<lnk::FlowDatagram>> {
            co_return protocol->bind_datagram_flow(epoch, flow_id, reverse, transfer);
        },
        asio::use_awaitable);
}

void DatagramMgr::RemotePair::close()
{
    if (flow)
    {
        flow->close("relay endpoint closed");
    }
    RemoteRelayPair::close();
}

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
      listen_endpoint_(std::move(listen_endpoint)), socket_(executor_), capacity_(capacity), config_(std::move(config))
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
    asio::co_spawn(executor_, [self = shared_from_this()] { return self->receive_datagram(); }, asio::detached);
}

AttachInfo DatagramMgr::install_remote_pair(int role)
{
    if (role != RelayAttach::Consumer && role != RelayAttach::Producer)
    {
        throw std::invalid_argument("invalid relay endpoint role");
    }
    if (stopped_ || local_pairs_.size() + remote_pairs_.size() >= capacity_)
    {
        throw std::runtime_error("relay endpoint capacity reached or datagram manager stopped");
    }
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    const auto session = allocate_session_id();
    auto endpoint = std::make_shared<RemotePair>(executor_, role);
    endpoint->session_id = session;
    remote_pairs_.emplace(uuid, endpoint);
    bindings_.emplace(session, Binding{uuid, role == RelayAttach::Producer ? Side::Producer : Side::Consumer});
    rollback.dismiss();
    return {uuid, endpoint->ticket, listen_endpoint_.port(), session};
}

asio::awaitable<void> DatagramMgr::prepare_remote_pair(std::uint64_t uuid, FlowProtocol &channel, std::uint64_t epoch,
                                                       std::uint64_t flow_id, SRVTrafficPtr traffic,
                                                       std::string accessor)
{
    const auto it = remote_pairs_.find(uuid);
    if (it == remote_pairs_.end())
    {
        throw std::runtime_error("relay endpoint closed before attach");
    }
    auto endpoint = it->second;
    co_await endpoint->prepare(channel, epoch, flow_id, executor_, config_, std::move(traffic), std::move(accessor));
}

asio::awaitable<void> DatagramMgr::run_remote_pair(std::uint64_t uuid)
{
    auto endpoint = remote_pairs_.at(uuid);
    if (!endpoint->active)
    {
        throw std::runtime_error("relay bridge unavailable");
    }
    const auto accessor = endpoint->track_accessor();
    const auto header = DatagramHeader::encode(endpoint->session_id);
    auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->tx_limiter : endpoint->rx_limiter;
    for (;;)
    {
        auto data = co_await endpoint->flow->receive();
        if (!limiter.try_consume(data.size()))
        {
            continue;
        }
        const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(data.data(), data.size())};
        const auto sent = co_await socket_.async_send_to(buffers, *endpoint->source, asio::use_awaitable);
        if (sent != header.size() + data.size())
        {
            throw std::runtime_error("truncated relay datagram");
        }
        endpoint->count(false, data.size());
    }
}

void DatagramMgr::activate_remote_pair(std::uint64_t uuid)
{
    auto endpoint = remote_pairs_.at(uuid);
    endpoint->activate(endpoint->source.has_value() && endpoint->flow);
}

void DatagramMgr::close_remote_pair(std::uint64_t uuid)
{
    const auto it = remote_pairs_.find(uuid);
    if (it == remote_pairs_.end())
    {
        return;
    }
    auto endpoint = it->second;
    remote_pairs_.erase(it);
    bindings_.erase(endpoint->session_id);
    endpoint->close();
    id_allocator_->release(uuid);
}

AttachPair DatagramMgr::install_local_pair()
{
    if (stopped_ || local_pairs_.size() + remote_pairs_.size() >= capacity_)
        throw std::runtime_error(stopped_ ? "server stopping" : "relay capacity reached");
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    auto pair = std::make_shared<LocalPair>(executor_);
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
    return {{uuid, pair->producer_ticket, listen_endpoint_.port(), pair->producer_session_id},
            {uuid, pair->consumer_ticket, listen_endpoint_.port(), pair->consumer_session_id}};
}

asio::awaitable<void> DatagramMgr::prepare_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor)
{
    auto pair = local_pairs_.at(uuid);
    co_await pair->prepare(config_, std::move(traffic), std::move(accessor));
}

asio::awaitable<void> DatagramMgr::run_local_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    if (!pair->active)
        throw std::runtime_error("local pair unavailable");
    if (!co_await pair->finished.wait())
        throw asio::system_error(asio::error::operation_aborted);
}

void DatagramMgr::close_local_pair(std::uint64_t uuid)
{
    const auto it = local_pairs_.find(uuid);
    if (it == local_pairs_.end())
        return;
    auto pair = it->second;
    local_pairs_.erase(it);
    bindings_.erase(pair->producer_session_id);
    bindings_.erase(pair->consumer_session_id);
    pair->close();
    id_allocator_->release(uuid);
}

void DatagramMgr::stop()
{
    stopped_ = true;
    while (!remote_pairs_.empty())
    {
        close_remote_pair(remote_pairs_.begin()->first);
    }
    asio::error_code ignored;
    socket_.close(ignored);
    while (!local_pairs_.empty())
        close_local_pair(local_pairs_.begin()->first);
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
            const auto [send_error, sent] = co_await socket_.async_send_to(asio::buffer(buffer.data(), size),
                                                                           route->destination, use_nothrow_awaitable);
            if (!send_error && sent == size)
            {
                auto &counter = route->source_side == Side::Producer ? route->traffic->rx : route->traffic->tx;
                counter.add(size - DatagramHeader::length);
            }
            else if (!stopped_)
            {
                PROXY_ERROR_PRINT("Transfer send failed udp -> %s:%u reason=%s",
                                  route->destination.address().to_string().c_str(),
                                  static_cast<unsigned int>(route->destination.port()),
                                  send_error ? send_error.message().c_str() : "partial datagram send");
            }
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
        RelayAttach attach;
        try
        {
            attach = RelayAttach::from_msg(decode_ctrl_datagram(datagram));
        }
        catch (...)
        {
            return std::nullopt;
        }

        const auto path = remote_pairs_.find(attach.uuid);
        if (path != remote_pairs_.end())
        {
            auto &endpoint = *path->second;
            if (!endpoint.source && attach.role == endpoint.role && attach.ticket == endpoint.ticket)
            {
                endpoint.source = source;
                endpoint.attached.notify_all();
            }
            return std::nullopt;
        }
        const auto owner = local_pairs_.find(attach.uuid);
        if (owner == local_pairs_.end())
            return std::nullopt;
        const auto &pair = owner->second;
        const auto side = attach.role == RelayAttach::Producer ? Side::Producer : Side::Consumer;
        auto &source_endpoint = pair->endpoint(side);
        const auto expected = side == Side::Producer ? pair->producer_ticket : pair->consumer_ticket;
        if (source_endpoint || attach.ticket != expected)
            return std::nullopt;
        source_endpoint = source;
        if (pair->producer_endpoint && pair->consumer_endpoint)
            pair->attached.notify_all();
        return std::nullopt;
    }

    if (const auto path = remote_pairs_.find(binding->second.relay_uuid); path != remote_pairs_.end())
    {
        const auto &endpoint = path->second;
        const auto payload = datagram.subspan(DatagramHeader::length);
        if (endpoint->active && endpoint->source == source && payload.size() <= LnkFrameHeader::maximum_payload)
        {
            auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->rx_limiter : endpoint->tx_limiter;
            if (limiter.try_consume(payload.size()) && endpoint->flow->try_send(payload))
            {
                endpoint->count(true, payload.size());
            }
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
    if (relay->endpoint(side) != source || !relay->active)
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
    return RoutedDatagram{*destination, relay->traffic, side};
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
