#ifndef RELAYWEAVE_NODE_COMMON_H
#define RELAYWEAVE_NODE_COMMON_H

#include "async_event.h"
#include "tls_channel.h"
#include "xfr_channel.h"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

class FlowProtocol;

/**
 * @brief Identify an Agent reply address, locally or through its primary Node, on control_io.
 * @details Local sessions are borrowed weakly; remote lifetimes follow cluster notifications.
 */
struct AgentPeer
{
    /**
     * @brief Borrow a local Agent control session without extending its lifetime.
     * @param session Original Agent control session, stored weakly.
     */
    AgentPeer(const ControlSessionPtr &session) : local(session)
    {
    }
    /**
     * @brief Describe the original Agent session carried by an authenticated cluster message.
     * @param node Authenticated primary Node identity.
     * @param session_id Original Agent session ID allocated by that primary.
     * @param accessor Original Agent peer address for accounting and status.
     */
    AgentPeer(std::string node, std::uint64_t session_id, std::string accessor)
        : node(std::move(node)), session_id(session_id), accessor(std::move(accessor))
    {
    }

    /**
     * @brief Compare reply identities independently of business request IDs and UUIDs.
     * @param other Reply address to compare.
     * @return True for the same local weak owner or remote primary/session pair.
     */
    bool operator==(const AgentPeer &other) const noexcept
    {
        if (node != other.node || session_id != other.session_id)
        {
            return false;
        }
        return !node.empty() || (!local.owner_before(other.local) && !other.local.owner_before(local));
    }
    /**
     * @brief Inspect the locally known reply address.
     * @return True for a live local session or a remote address awaiting cluster lifetime notifications.
     */
    explicit operator bool() const noexcept
    {
        return !node.empty() || !local.expired();
    }
    /**
     * @brief Read the original Agent address for traffic accounting and status reports.
     * @return Borrowed peer address, or empty after a local session expires.
     */
    std::string_view peer() const
    {
        if (!node.empty())
        {
            return accessor;
        }
        const auto session = local.lock();
        return session ? session->peer() : std::string_view{};
    }

    std::weak_ptr<ControlSession> local;
    std::string node;
    std::uint64_t session_id = 0;
    std::string accessor;
};

/** @brief Carry parsed relay construction parameters on control_io; not retained alongside controller state. */
struct RelayRequest
{
    std::string service;
    RelayProtocol protocol = RelayProtocol::Tcp;
    std::uint64_t request_id = 0;
    std::vector<std::string> path;
    std::uint64_t epoch = 0;
    std::uint64_t flow_id = 0;
    std::string peer;
    std::string accessor;
};

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
 * @brief Hold attachment, traffic and lifetime state shared by local and remote relay pairs.
 * @details All access requires the owning transfer executor. Managers retain concrete resource-owning pairs
 * and remove their lookup entries before close(); awaiting tasks retain the pair until they drain.
 */
struct RelayPair
{
    /**
     * @brief Initialize the attachment event on the owning transfer executor.
     * @param executor Single-threaded transfer executor owning the pair.
     */
    explicit RelayPair(asio::any_io_executor executor) : attached(executor)
    {
    }
    /** @brief Destroy through a common pair pointer after all borrowing tasks have drained. */
    virtual ~RelayPair() = default;
    /** @brief Close concrete resources and wake waiters after the manager removes its lookup entry. */
    virtual void close();

    AsyncEvent attached;
    bool closed = false;
    bool active = false;
    TokenBucket rx_limiter;
    TokenBucket tx_limiter;
    SRVTrafficPtr traffic;
    std::string accessor;

  protected:
    /** @brief Bind limits and accounting after preparation validates readiness on the transfer executor. */
    void bind_traffic(const TrafficLimitConfig &limits, SRVTrafficPtr traffic, std::string accessor);
};

/** @brief Share two-role credentials and local service accounting without owning transport resources. */
struct LocalRelayPair : RelayPair
{
    /**
     * @brief Generate distinct nonzero tickets and initialize the attachment event.
     * @param executor Single-threaded transfer executor owning the pair.
     */
    explicit LocalRelayPair(asio::any_io_executor executor);
    /**
     * @brief Await both attachments, then bind accounting and activate on the transfer executor.
     * @param limits Per-direction byte rates and burst sizes.
     * @param traffic Nonempty local service accounting record.
     * @param accessor Consumer peer label used for accounting.
     * @return Completes after activation; attachment closure or cancellation throws.
     * @details The caller must retain this pair until the coroutine exits.
     */
    asio::awaitable<void> prepare(const TrafficLimitConfig &limits, SRVTrafficPtr traffic, std::string accessor);
    /** @brief Wake attachment waiters and remove an active pair's accessor after lookup removal. */
    void close() override;

    std::uint64_t producer_ticket;
    std::uint64_t consumer_ticket;
};

/**
 * @brief Share one Agent's credentials, activation and directional accounting on its transfer executor.
 * @details Concrete pairs own their Flow port; the common preparation has no concrete Flow type dependency.
 */
struct RemoteRelayPair : RelayPair
{
    /**
     * @brief Generate the attachment ticket and retain the local Agent role.
     * @param executor Single-threaded transfer executor owning the endpoint.
     * @param role Local Agent role: RelayAttach::Producer or RelayAttach::Consumer.
     */
    RemoteRelayPair(asio::any_io_executor executor, int role);
    /**
     * @brief Await attachment, bind the concrete Flow, and install accounting on the transfer executor.
     * @param channel Protocol service owning the committed Flow, borrowed through preparation.
     * @param epoch Cluster authorization epoch of the Flow.
     * @param flow_id Nonzero committed Flow ID.
     * @param transfer Executor owning this pair and the returned Flow port.
     * @param limits Per-direction byte rates and burst sizes, borrowed through preparation.
     * @param traffic Optional local service accounting record.
     * @param accessor Consumer peer label used for accounting.
     * @return Completes after binding; attachment, binding and cancellation failures propagate directly.
     * @details The caller retains this pair until completion. A Flow returned after closure is closed too.
     */
    asio::awaitable<void> prepare(FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                                  asio::any_io_executor transfer, const TrafficLimitConfig &limits,
                                  SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Enable an attached, bound endpoint before relay.ready on its transfer executor.
     * @param ready True if the authenticated Agent transport and its Flow port are both present.
     */
    void activate(bool ready);
    /**
     * @brief Count an optional service accessor for the scope of the forwarding task.
     * @return Guard removing the accessor before the retained endpoint leaves the task scope.
     */
    auto track_accessor()
    {
        if (traffic)
        {
            traffic->add_accessor(accessor);
        }
        return ScopeGuard([this] {
            if (traffic)
            {
                traffic->remove_accessor(accessor);
            }
        });
    }
    /**
     * @brief Attribute bytes to service RX or TX using the local Agent role.
     * @param from_agent True for Agent-to-Flow traffic, false for Flow-to-Agent traffic.
     * @param size Successfully transferred user payload bytes.
     */
    void count(bool from_agent, std::size_t size);

    int role;
    std::uint64_t ticket;

  protected:
    /** @brief Bind and retain the concrete Flow port; called only by prepare() on the transfer executor. */
    virtual asio::awaitable<void> bind_flow(FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                                            asio::any_io_executor transfer) = 0;
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
