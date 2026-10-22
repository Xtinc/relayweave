#ifndef PROXY_APP_COMMON_HEADER
#define PROXY_APP_COMMON_HEADER

#include "async_event.h"
#include "xfr_channel.h"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>

/** @brief Set service byte rates and burst sizes; a zero rate disables that direction's limit. */
struct TrafficLimitConfig
{
    std::size_t rx_bytes_per_second = 0;
    std::size_t rx_burst_bytes = 0;
    std::size_t tx_bytes_per_second = 0;
    std::size_t tx_burst_bytes = 0;
};

/**
 * @brief Describe one Agent's data attachment without owning its runtime resources.
 * @details Managers return this value to control_io. session_id is nonzero for UDP and zero for streams.
 */
struct AttachInfo
{
    /**
     * @brief Encode attachment fields for an Agent control notification.
     * @param protocol Agent-facing transport determining UDP session and TLS framing fields.
     * @return Parameters containing uuid, ticket, and data_port, plus transport-specific fields.
     * @pre The descriptor belongs to protocol; UDP requires a nonzero session_id.
     */
    njson to_params(RelayProtocol protocol) const;

    std::uint64_t uuid = 0;
    std::uint64_t ticket = 0;
    std::uint16_t data_port = 0;
    std::uint64_t session_id = 0;
};

/** @brief Describe both Agent attachments of a local pair, sharing a UUID and using distinct tickets. */
struct AttachPair
{
    AttachInfo producer;
    AttachInfo consumer;
};

/**
 * @brief Share service byte counters, sampled rates, and active accessor counts.
 * @details Transfer executors update atomic counters and mutex-protected accessors. Sampling
 * and sampled-rate reads require control_io. RX denotes Producer-to-Consumer service traffic.
 */
class ServiceTraffic
{
  public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using AccessorMap = std::map<std::string, std::size_t>;

    /** @brief Initialize the sampling clock with empty counters and accessor counts. */
    ServiceTraffic();

    /**
     * @brief Consume interval counters and update rates with EMA weight 0.5 on control_io.
     * @param now Steady-clock timestamp shared by the current service sampling round.
     */
    void sample(TimePoint now) noexcept;
    /**
     * @brief Return the rounded sampled Producer-to-Consumer rate on control_io.
     * @return Rounded EMA of Producer-to-Consumer payload bytes per second.
     */
    std::uint64_t rx_bytes_per_second() const noexcept;
    /**
     * @brief Return the rounded sampled Consumer-to-Producer rate on control_io.
     * @return Rounded EMA of Consumer-to-Producer payload bytes per second.
     */
    std::uint64_t tx_bytes_per_second() const noexcept;
    /**
     * @brief Increment an accessor's active relay count under the internal mutex.
     * @param client Consumer peer label whose active relay count is being updated.
     */
    void add_accessor(const std::string &client);
    /**
     * @brief Decrement an accessor count and erase its final reference under the internal mutex.
     * @param client Consumer peer label whose active relay count is being updated.
     */
    void remove_accessor(const std::string &client) noexcept;
    /**
     * @brief Return a mutex-protected copy of active accessor counts.
     * @return Independent snapshot mapping each Consumer peer label to its active relay count.
     */
    AccessorMap accessors() const;

    /** @brief Count Producer-to-Consumer bytes atomically across transfer executors. */
    TrafficCounter rx;
    /** @brief Count Consumer-to-Producer bytes atomically across transfer executors. */
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

/**
 * @brief Hold common Agent-to-Flow state for stream and datagram remote endpoints.
 * @details All access requires the owning transfer executor. The manager removes lookup
 * entries before waking tasks that still retain the endpoint during closure.
 */
struct RelayEndpoint
{
    /**
     * @brief Initialize the Agent role and its attachment event on the transfer executor.
     * @param executor Executor that owns the object's timers, I/O resources, and mutable state.
     * @param role Local Agent role: RelayAttach::Producer or RelayAttach::Consumer.
     */
    RelayEndpoint(asio::any_io_executor executor, int role) : role(role), attached(executor)
    {
    }
    /**
     * @brief Bind traffic limits and service accounting; throw if the endpoint has closed.
     * @param limits Per-direction byte rates and burst sizes applied at this endpoint.
     * @param traffic Shared accounting record for the locally hosted service, or empty when no local record
     * exists.
     * @param accessor Consumer peer label used to count active service accessors.
     * @throws std::runtime_error If the endpoint has already closed.
     */
    void bind(const TrafficLimitConfig &limits, SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Attribute payload bytes to service RX or TX using the local Agent role.
     * @param from_agent True for Agent-to-Flow traffic; false for Flow-to-Agent traffic.
     * @param size Number of successfully transferred user payload bytes.
     */
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

/**
 * @brief Allocate nonzero relay IDs from a Node-wide active namespace.
 * @details TCP/TLS and UDP transfer executors share this allocator through its internal mutex.
 */
class RelayIdAllocator
{
  public:
    /**
     * @brief Reserve a currently unused nonzero ID, skipping active IDs after wraparound.
     * @return Nonzero ID reserved in the shared Node-wide active namespace until release().
     */
    std::uint64_t allocate();
    /**
     * @brief Release a reservation; releasing an unknown ID is harmless.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    void release(std::uint64_t uuid) noexcept;

  private:
    std::mutex mutex_;
    std::unordered_set<std::uint64_t> active_ids_;
    std::uint64_t next_uuid_ = 1;
};

/**
 * @brief Generate a nonzero cryptographic ID; throw if OpenSSL random generation fails.
 * @return Cryptographically generated nonzero 64-bit value; uniqueness is checked by the caller where
 * required.
 * @throws std::runtime_error If OpenSSL cannot produce random bytes.
 */
std::uint64_t generate_random_id();

#endif
