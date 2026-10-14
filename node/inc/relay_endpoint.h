#ifndef RELAYWEAVE_RELAY_ENDPOINT_H
#define RELAYWEAVE_RELAY_ENDPOINT_H

#include "app_common.h"
#include "async_event.h"

// Local attach and bridge state. All access requires the owning transfer executor.
struct RelayEndpoint
{
    RelayEndpoint(asio::any_io_executor executor, int role) : role(role), attached(executor)
    {
    }
    void bind(const TrafficLimitConfig &limits, SRVTrafficPtr traffic, std::string accessor);
    void count(bool from_agent, std::size_t size);

    std::uint64_t ticket = 0;
    int role;
    AsyncEvent attached;
    bool closed = false;
    bool active = false;
    TokenBucket rx_limiter;
    TokenBucket tx_limiter;
    SRVTrafficPtr traffic;
    std::string accessor;
};

#endif
