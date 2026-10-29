#include "node_common.h"
#include <algorithm>
#include <limits>
#include <openssl/rand.h>
#include <stdexcept>

njson AttachInfo::to_params(RelayProtocol protocol) const
{
    njson params{{"uuid", uuid}, {"ticket", ticket}, {"data_port", data_port}};
    if (protocol == RelayProtocol::Udp)
    {
        params["session_id"] = session_id;
    }
    else if (protocol == RelayProtocol::Tls)
    {
        params["tls_framing"] = std::string(tls_data_framing);
    }
    return params;
}

ServiceTraffic::ServiceTraffic() : last_sample_(Clock::now())
{
}

void ServiceTraffic::sample(TimePoint now) noexcept
{
    const auto elapsed = now - last_sample_;
    last_sample_ = now;
    const auto rx_rate = rate(static_cast<double>(rx.take_interval()), elapsed);
    const auto tx_rate = rate(static_cast<double>(tx.take_interval()), elapsed);
    if (!sampled_)
    {
        rx_ema_ = rx_rate;
        tx_ema_ = tx_rate;
        sampled_ = true;
        return;
    }
    rx_ema_ = (rx_ema_ + rx_rate) * 0.5;
    tx_ema_ = (tx_ema_ + tx_rate) * 0.5;
}

std::uint64_t ServiceTraffic::rx_bytes_per_second() const noexcept
{
    return rounded_ema(rx_ema_);
}

std::uint64_t ServiceTraffic::tx_bytes_per_second() const noexcept
{
    return rounded_ema(tx_ema_);
}

void ServiceTraffic::add_accessor(const std::string &client)
{
    const std::lock_guard lock(accessors_mutex_);
    ++accessors_[client];
}

void ServiceTraffic::remove_accessor(const std::string &client) noexcept
{
    const std::lock_guard lock(accessors_mutex_);
    const auto accessor = accessors_.find(client);
    if (accessor == accessors_.end())
        return;
    if (accessor->second > 1)
        --accessor->second;
    else
        accessors_.erase(accessor);
}

ServiceTraffic::AccessorMap ServiceTraffic::accessors() const
{
    const std::lock_guard lock(accessors_mutex_);
    return accessors_;
}

double ServiceTraffic::rate(double bytes, Clock::duration elapsed) noexcept
{
    const auto seconds = std::chrono::duration<double>(elapsed).count();
    if (seconds <= 0.0)
    {
        return 0.0;
    }
    return std::min(bytes / seconds, static_cast<double>(std::numeric_limits<std::uint64_t>::max()));
}

std::uint64_t ServiceTraffic::rounded_ema(double value) noexcept
{
    const auto maximum = static_cast<double>(std::numeric_limits<std::uint64_t>::max());
    if (value >= maximum)
    {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return value <= 0.0 ? 0 : static_cast<std::uint64_t>(value + 0.5);
}

void RelayPair::bind_traffic(const TrafficLimitConfig &limits, SRVTrafficPtr service_traffic, std::string client)
{
    rx_limiter = TokenBucket(limits.rx_bytes_per_second, limits.rx_burst_bytes);
    tx_limiter = TokenBucket(limits.tx_bytes_per_second, limits.tx_burst_bytes);
    traffic = std::move(service_traffic);
    accessor = std::move(client);
}

void RelayPair::close()
{
    closed = true;
    attached.notify_all();
}

LocalRelayPair::LocalRelayPair(asio::any_io_executor executor)
    : RelayPair(executor), producer_ticket(generate_random_id())
{
    do
    {
        consumer_ticket = generate_random_id();
    } while (consumer_ticket == producer_ticket);
}

asio::awaitable<void> LocalRelayPair::prepare(const TrafficLimitConfig &limits, SRVTrafficPtr service_traffic,
                                              std::string client)
{
    if (!co_await attached.wait() || closed)
    {
        throw std::runtime_error("relay setup timed out");
    }
    if (traffic || !service_traffic)
    {
        throw std::runtime_error("local pair cannot bind service");
    }
    bind_traffic(limits, std::move(service_traffic), std::move(client));
    active = true;
    traffic->add_accessor(accessor);
}

void LocalRelayPair::close()
{
    RelayPair::close();
    if (active)
    {
        traffic->remove_accessor(accessor);
        active = false;
    }
}

RemoteRelayPair::RemoteRelayPair(asio::any_io_executor executor, int agent_role)
    : RelayPair(executor), role(agent_role), ticket(generate_random_id())
{
}

asio::awaitable<void> RemoteRelayPair::prepare(FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                                               asio::any_io_executor transfer, const TrafficLimitConfig &limits,
                                               SRVTrafficPtr service_traffic, std::string client)
{
    if (!co_await attached.wait() || closed)
    {
        throw std::runtime_error("relay endpoint closed before attach");
    }
    co_await bind_flow(channel, epoch, flow_id, transfer);
    if (closed)
    {
        close();
        throw std::runtime_error("relay endpoint closed during binding");
    }
    bind_traffic(limits, std::move(service_traffic), std::move(client));
}

void RemoteRelayPair::activate(bool ready)
{
    if (!ready || active)
    {
        throw std::runtime_error("relay bridge unavailable");
    }
    active = true;
}

void RemoteRelayPair::count(bool from_agent, std::size_t size)
{
    if (traffic)
    {
        const bool rx = from_agent == (role == RelayAttach::Producer);
        (rx ? traffic->rx : traffic->tx).add(size);
    }
}

std::uint64_t RelayIdAllocator::allocate()
{
    const std::lock_guard lock(mutex_);
    for (;;)
    {
        const auto uuid = next_uuid_++;
        if (next_uuid_ == 0)
        {
            next_uuid_ = 1;
        }
        if (active_ids_.insert(uuid).second)
        {
            return uuid;
        }
    }
}

void RelayIdAllocator::release(std::uint64_t uuid) noexcept
{
    const std::lock_guard lock(mutex_);
    active_ids_.erase(uuid);
}

std::uint64_t generate_random_id()
{
    std::uint64_t value;
    do
    {
        if (RAND_bytes(reinterpret_cast<unsigned char *>(&value), static_cast<int>(sizeof(value))) != 1)
        {
            throw std::runtime_error("OpenSSL failed to generate relay ID");
        }
    } while (value == 0);
    return value;
}
