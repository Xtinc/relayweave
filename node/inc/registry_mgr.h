#ifndef PROXY_REGISTRY_MGR_H
#define PROXY_REGISTRY_MGR_H

#include "node_common.h"
#include "dualindex_map.h"
#include "tls_channel.h"
#include <map>
#include <vector>

/**
 * @brief Index control sessions and locally registered services on control_io.
 * @details Session tasks retain their connections until remove(); the registry stores weak
 * references and owns service traffic records. All entry points require control_io.
 */
class RegistryMgr
{
  public:
    using SessionId = std::uint64_t;
    /** @brief Describe registration success or the admission rule that rejected it. */
    enum class Result
    {
        Registered,
        NameConflict,
        SessionLimitReached,
        GlobalLimitReached,
        RegistryStopped,
    };

    /** @brief Return a live service owner, transport, and shared traffic record for lookup. */
    struct Service
    {
        ControlSessionPtr session;
        RelayProtocol protocol;
        SRVTrafficPtr traffic;
    };

    /**
     * @brief Translate a registration result into a stable diagnostic string.
     * @param result Registration outcome to translate to a diagnostic string.
     * @return Diagnostic text describing the supplied registration result.
     */
    static std::string_view result2string(Result result);

    /**
     * @brief Set independent limits for sessions, all services, and services per session.
     * @param max_sessions Maximum simultaneously registered Agent control sessions.
     * @param max_services Maximum registered service names across this Node.
     * @param max_services_per_session Maximum services owned by one control session.
     */
    RegistryMgr(std::size_t max_sessions, std::size_t max_services, std::size_t max_services_per_session);

    /**
     * @brief Return whether admission is blocked by shutdown or the session limit.
     * @return True if the registry is stopped or has reached its session limit.
     */
    bool full() noexcept;
    /**
     * @brief Return the number of registered session IDs.
     * @return Number of registered session IDs, including sessions whose owner has not yet removed them.
     */
    std::size_t session_count() noexcept;
    /**
     * @brief Test whether a session ID is registered.
     * @param id Registered control-session ID.
     * @return True if the session ID is present in the registry.
     */
    bool contains(SessionId id) noexcept;
    /**
     * @brief Borrow a live session under a unique nonzero ID; throw on invalid or full admission.
     * @param id Registered control-session ID.
     * @param session Agent control session whose identity is checked before applying the message.
     */
    void add(SessionId id, const ControlSessionPtr &session);
    /**
     * @brief Remove a session and all its services; an unknown ID is harmless.
     * @param id Registered control-session ID.
     */
    void remove(SessionId id) noexcept;
    /**
     * @brief Lock the borrowed session, returning an empty pointer if absent or expired.
     * @param id Registered control-session ID.
     * @return A strong reference to the live session, or an empty pointer if missing or expired.
     */
    ControlSessionPtr find_session(SessionId id) const;

    /**
     * @brief Register a service idempotently for the same owner and protocol, subject to limits.
     * @param id Registered control-session ID.
     * @param service Service name unique within this Node.
     * @param protocol Agent-facing service transport: TCP, TLS, or UDP.
     * @return Registered for a new or identical registration; otherwise the conflict, capacity, or shutdown
     * result.
     */
    Result register_service(SessionId id, const std::string &service, RelayProtocol protocol);
    /**
     * @brief Return service metadata only while its owning session remains live.
     * @param service Service name unique within this Node.
     * @return Live owner, transport, and traffic record, or std::nullopt if the service or its owner is
     * unavailable.
     */
    std::optional<Service> find_service(const std::string &service);
    /**
     * @brief Return registered service names in sorted order.
     * @return Registered service names sorted lexicographically.
     */
    std::vector<std::string> service_names();
    /** @brief Sample every service's interval counters using one common timestamp. */
    void sample_traffic();
    /**
     * @brief Sum sampled Producer-to-Consumer rates for Node bandwidth availability.
     * @return Sum of sampled Producer-to-Consumer service rates, in bytes per second.
     */
    double bytes_per_second();
    /**
     * @brief Build per-service totals, sampled rates, and active accessor counts.
     * @return JSON array of service identities, byte totals, sampled rates, and active accessor counts.
     */
    njson traffic_report();

    /** @brief Reject further admission, request session disconnects, and clear registry entries. */
    void stop();

  private:
    /** @brief Own transport metadata and traffic counters indexed by service name and session ID. */
    struct ServiceEntry
    {
        RelayProtocol protocol;
        SRVTrafficPtr traffic;
    };

    std::size_t max_sessions_;
    std::size_t max_services_;
    std::size_t max_services_per_session_;
    std::map<SessionId, std::weak_ptr<ControlSession>> sessions_;
    DualIndexMap<std::string, SessionId, ServiceEntry> services_;
    bool stopped_ = false;
};

#endif
