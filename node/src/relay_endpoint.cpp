#include "relay_endpoint.h"

void RelayEndpoint::bind(const TrafficLimitConfig &limits, SRVTrafficPtr service_traffic, std::string client)
{
    if (closed)
    {
        throw std::runtime_error("relay endpoint is closed");
    }
    rx_limiter = TokenBucket(limits.rx_bytes_per_second, limits.rx_burst_bytes);
    tx_limiter = TokenBucket(limits.tx_bytes_per_second, limits.tx_burst_bytes);
    traffic = std::move(service_traffic);
    accessor = std::move(client);
}

void RelayEndpoint::count(bool from_agent, std::size_t size)
{
    if (traffic)
    {
        const bool rx = from_agent == (role == RelayAttach::Producer);
        (rx ? traffic->rx : traffic->tx).add(size);
    }
}
