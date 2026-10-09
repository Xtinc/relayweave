#include "relay_endpoint.h"

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

// Transfer executor: only the copied frame crosses to cluster_data_io.
asio::awaitable<void> RelayEndpoint::send(LnkFrType kind, BytesBuf payload, std::string reason)
{
    FlowFrame frame{epoch, flow_id, role == RelayAttach::Producer, kind, std::move(payload), std::move(reason)};
    const auto status = co_await asio::co_spawn(channel->executor(),
        [flow_channel = channel, frame = std::move(frame)]() mutable -> asio::awaitable<FlowSendStatus> {
            co_return flow_channel->send_flow(std::move(frame));
        }, asio::use_awaitable);
    if (status != FlowSendStatus::Queued)
    {
        throw std::runtime_error(status == FlowSendStatus::CapacityExceeded ? "flow send capacity exceeded" :
                                 status == FlowSendStatus::Closed ? "flow closed" : "invalid flow frame");
    }
}

asio::awaitable<FlowFrame> RelayEndpoint::receive()
{
    auto frame = co_await asio::co_spawn(channel->executor(), channel->receive_flow(epoch, flow_id), asio::use_awaitable);
    if (frame.reverse != (role == RelayAttach::Consumer))
    {
        throw std::runtime_error("wrong flow direction at relay endpoint");
    }
    co_return frame;
}

asio::awaitable<void> RelayEndpoint::limit(bool from_agent, std::size_t size)
{
    const bool rx = from_agent == (role == RelayAttach::Producer);
    const auto now = TokenBucket::Clock::now();
    const auto until = (rx ? rx_limiter : tx_limiter).reserve(size, now);
    if (until > now)
    {
        asio::steady_timer timer(co_await asio::this_coro::executor, until);
        co_await timer.async_wait(asio::use_awaitable);
    }
}

void RelayEndpoint::count(bool from_agent, std::size_t size)
{
    if (traffic)
    {
        const bool rx = from_agent == (role == RelayAttach::Producer);
        (rx ? traffic->rx : traffic->tx).add(size);
    }
}
