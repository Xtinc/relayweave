#ifndef RELAYWEAVE_RELAY_ENDPOINT_H
#define RELAYWEAVE_RELAY_ENDPOINT_H

#include "app_common.h"
#include "async_event.h"
#include "lnk_channel.h"

// Local attach and bridge state. All access requires the owning transfer executor.
struct RelayEndpoint
{
    RelayEndpoint(asio::any_io_executor executor, int role)
        : role(role), attached(executor)
    {
    }
    void bind(LnkChannel &channel, std::uint64_t epoch, std::uint64_t flow_id,
              const TrafficLimitConfig &limits, SRVTrafficPtr traffic, std::string accessor);
    asio::awaitable<void> send(LnkFrType kind, BytesBuf payload = {}, std::string reason = {});
    asio::awaitable<void> send_data(std::span<const std::uint8_t> payload);
    asio::awaitable<FlowFrame> receive();
    void count(bool from_agent, std::size_t size);

    std::uint64_t ticket = 0;
    int role;
    AsyncEvent attached;
    bool closed = false;
    bool active = false;
    LnkChannel *channel = nullptr;
    std::uint64_t epoch = 0;
    std::uint64_t flow_id = 0;
    TokenBucket rx_limiter;
    TokenBucket tx_limiter;
    SRVTrafficPtr traffic;
    std::string accessor;
};

#endif
