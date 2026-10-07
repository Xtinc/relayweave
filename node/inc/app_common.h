#ifndef PROXY_APP_COMMON_HEADER
#define PROXY_APP_COMMON_HEADER

#include "xfr_channel.h"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>

struct TrafficLimitConfig
{
    std::size_t rx_bytes_per_second = 0;
    std::size_t rx_burst_bytes = 0;
    std::size_t tx_bytes_per_second = 0;
    std::size_t tx_burst_bytes = 0;
};

class ServiceTraffic
{
  public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using AccessorMap = std::map<std::string, std::size_t>;

    ServiceTraffic();

    void sample(TimePoint now) noexcept;
    std::uint64_t rx_bytes_per_second() const noexcept;
    std::uint64_t tx_bytes_per_second() const noexcept;
    void add_accessor(const std::string &client);
    void remove_accessor(const std::string &client) noexcept;
    AccessorMap accessors() const;

    TrafficCounter rx;
    TrafficCounter tx;

  private:
    static double rate(double bytes, Clock::duration elapsed) noexcept;
    static std::uint64_t rounded_ema(double value) noexcept;

    TimePoint last_sample_;
    bool sampled_{};
    double rx_ema_{};
    double tx_ema_{};
    mutable std::mutex accessors_mutex_;
    AccessorMap accessors_;
};

using SRVTrafficPtr = std::shared_ptr<ServiceTraffic>;

class RelayIdAllocator
{
  public:
    std::uint64_t allocate();
    void release(std::uint64_t uuid) noexcept;

  private:
    std::mutex mutex_;
    std::unordered_set<std::uint64_t> active_ids_;
    std::uint64_t next_uuid_ = 1;
};

std::uint64_t generate_random_id();

#endif
