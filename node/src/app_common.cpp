#include "app_common.h"
#include <algorithm>
#include <limits>
#include <openssl/rand.h>
#include <stdexcept>

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
