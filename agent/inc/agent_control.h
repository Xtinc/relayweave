#ifndef RELAYWEAVE_AGENT_CONTROL_H
#define RELAYWEAVE_AGENT_CONTROL_H

#include "agent_common.h"
#include "async_event.h"
#include <map>
#include <unordered_map>

struct AgentConfig;
class AgentRouting;
class NodeConnectionMgr;
class Forwarder;

/**
 * @brief Own service discovery, request correlation, entrance waits, and business control dispatch.
 * @details All methods and mutable state require control_io, except construction/start before workers.
 * Borrows configuration, NodeConnectionMgr, AgentRouting, and Forwarder from RelayAgent. The owner
 * drains control producers before Forwarder and joins workers before destroying either module.
 * Queued transfer notifications borrow Forwarder. Each selection owns its map entry
 * until its coroutine exits; references remain valid across other insertions and erasures.
 */
class AgentControl
{
  public:
    /**
     * @brief Construct control state and derive discovery records from configured forwards.
     * @param control_executor Single-threaded control execution domain.
     * @param transfer_executor Forwarder execution domain for posted notifications.
     * @param config Borrowed immutable configuration, valid through shutdown.
     * @param connections Borrowed entrance pool, valid through all connection notifications and selections.
     * @param routing Borrowed route state, valid through shutdown.
     * @param forwarder Borrowed transfer owner, drained by RelayAgent after all control producers exit.
     */
    AgentControl(asio::any_io_executor control_executor, asio::any_io_executor transfer_executor,
                 const AgentConfig &config, NodeConnectionMgr &connections, AgentRouting &routing,
                 Forwarder &forwarder);
    /** @brief Start optional discovery maintenance once after the owner starts the primary pool. */
    void start();
    /** @brief Invalidate selections and cancel discovery maintenance on control_io. */
    void stop();
    /**
     * @brief Drain discovery on control_io after the owner requests control and routing shutdown.
     * @pre stop() has been called.
     * @return Completes when discovery no longer accesses the borrowed pool or route state.
     */
    asio::awaitable<void> close();
    /**
     * @brief Select a path and await its identified entrance entirely on control_io.
     * @param service Discovered service name and transport.
     * @param destination Expected destination Node identity.
     * @param deadline Absolute steady-clock deadline covering the whole establishment.
     * @return Entrance metadata, path, epoch, and shared connection handle after identification.
     * @throws std::exception On invalidation, missing location, capacity, or deadline expiry.
     */
    asio::awaitable<RelaySelection> select_relay(ServiceKey service, std::string destination,
                                                 std::chrono::steady_clock::time_point deadline);
    /**
     * @brief Register primary services or wake matching entrance selections after identification.
     * @param connection Borrowed connection, valid until this notification returns on control_io.
     */
    void connection_ready(const NodeConnection &connection);
    /**
     * @brief Invalidate affected business sessions and, for primary, discovery and route state.
     * @param connection Borrowed connection, valid until this notification returns on control_io.
     */
    void connection_closed(const NodeConnection &connection);
    /**
     * @brief Release pending handles and preserve endpoint metadata before pool erasure.
     * @param connection Borrowed connection, valid until this notification returns on control_io.
     */
    void connection_finished(const NodeConnection &connection);
    /**
     * @brief Match discovery/control responses or post business messages to Forwarder.
     * @param connection Authenticated source connection, borrowed during this call on control_io.
     * @param message Owned control message whose source and pending identity are checked.
     */
    void handle_control_message(const NodeConnection &connection, CtrlMessage message);

  private:
    /** @brief Poll unresolved services and topology, maintain the pool, and refresh probes. */
    asio::awaitable<void> discovery_loop();
    /** @brief Query unresolved services through an identified primary on control_io. */
    void query_services(bool refresh = false);
    /** @brief Request a fresh topology snapshot unless one is already pending. */
    void query_topology();
    /** @brief Derive required probe targets from primary and discovered service locations. */
    void update_probe_targets();
    /** @brief Apply a matching service location and publish its availability. */
    void locate_service(const njson &params);
    /** @brief Clear a configured service location and invalidate affected selections and forwards. */
    void forget_service(const ServiceKey &service, std::string reason);
    /** @brief Set a terminal reason and wake matching pending entrance selections. */
    void invalidate_entries(std::string reason, const std::optional<ServiceKey> &service = std::nullopt);
    /** @brief Allocate a nonzero identifier for discovery, topology, and entrance requests. */
    std::uint64_t allocate_request_id();
    /** @brief Own one selection wait and its original establishment deadline. */
    struct EntryWait
    {
        /** @brief Construct the selection notification timer on control_io. */
        EntryWait(asio::any_io_executor executor, ServiceKey service, std::string node,
                  std::chrono::steady_clock::time_point deadline);
        asio::steady_timer changed;
        ServiceKey service;
        std::string node;
        std::chrono::steady_clock::time_point deadline;
        /** @brief Copy a located endpoint without retaining its JSON response. */
        struct Location
        {
            std::string address;
            std::uint16_t port;
        };
        std::optional<Location> location;
        std::shared_ptr<NodeConnection> connection;
        std::string reason;
    };
    std::map<std::uint64_t, EntryWait> entry_waits_;

    asio::any_io_executor control_executor_;
    asio::any_io_executor transfer_executor_;
    const AgentConfig &config_;
    AgentRouting &routing_;
    Forwarder &forwarder_;
    NodeConnectionMgr &connections_;
    asio::steady_timer discovery_timer_;
    /** @brief Preserve discovery correlation and the destination endpoint independently of connections. */
    struct ServiceLocation
    {
        std::uint64_t request = 0;
        std::string node;
        std::string address;
        std::uint16_t port = 0;
    };
    std::unordered_map<ServiceKey, ServiceLocation> service_locations_;
    std::uint64_t next_request_id_ = 1;
    AsyncEvent discovery_done_;
    bool running_ = false;
    bool discovery_active_ = false;
};

#endif
