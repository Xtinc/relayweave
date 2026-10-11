#include "relay_endpoint.h"

namespace
{
void check_send_status(FlowSendStatus status)
{
    if (status != FlowSendStatus::Queued)
    {
        throw std::runtime_error(status == FlowSendStatus::CapacityExceeded ? "flow send capacity exceeded"
                                 : status == FlowSendStatus::Closed         ? "flow closed"
                                                                            : "invalid flow frame");
    }
}
} // namespace

void RelayEndpoint::bind(LnkChannel &flow_channel, std::uint64_t flow_epoch, std::uint64_t id,
                         const TrafficLimitConfig &limits, SRVTrafficPtr service_traffic, std::string client)
{
    if (closed || channel || !flow_epoch || !id)
    {
        throw std::runtime_error("relay endpoint cannot bind flow");
    }
    channel = &flow_channel;
    epoch = flow_epoch;
    flow_id = id;
    rx_limiter = TokenBucket(limits.rx_bytes_per_second, limits.rx_burst_bytes);
    tx_limiter = TokenBucket(limits.tx_bytes_per_second, limits.tx_burst_bytes);
    traffic = std::move(service_traffic);
    accessor = std::move(client);
}

// Transfer executor: move the owned frame to cluster_data_io for enqueueing.
asio::awaitable<void> RelayEndpoint::send(LnkFrType kind, BytesBuf payload, std::string reason)
{
    FlowFrame frame{epoch, flow_id, role == RelayAttach::Producer, kind, std::move(payload), std::move(reason)};
    check_send_status(co_await channel->async_send_flow(std::move(frame)));
}

asio::awaitable<void> RelayEndpoint::send_data(std::span<const std::uint8_t> payload)
{
    check_send_status(co_await channel->async_send_flow_data(epoch, flow_id, role == RelayAttach::Producer, payload));
}

asio::awaitable<FlowFrame> RelayEndpoint::receive()
{
    auto frame = co_await channel->receive_flow(epoch, flow_id);
    if (frame.reverse != (role == RelayAttach::Consumer))
    {
        throw std::runtime_error("wrong flow direction at relay endpoint");
    }
    co_return frame;
}

void RelayEndpoint::count(bool from_agent, std::size_t size)
{
    if (traffic)
    {
        const bool rx = from_agent == (role == RelayAttach::Producer);
        (rx ? traffic->rx : traffic->tx).add(size);
    }
}
