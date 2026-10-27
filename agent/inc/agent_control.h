#ifndef RELAYWEAVE_AGENT_CONTROL_H
#define RELAYWEAVE_AGENT_CONTROL_H

#include "agent_common.h"
#include "async_event.h"
#include <unordered_map>

struct AgentConfig;
class AgentRouting;
class NodeConnectionMgr;
class NodeConnection;
class Forwarder;

/**
 * @brief Own service discovery, path selection and primary business control dispatch.
 * @details All methods and mutable state require control_io, except construction/start before workers.
 * Borrows configuration, NodeConnectionMgr, AgentRouting, and Forwarder from RelayAgent. The owner
 * drains control producers before Forwarder and joins workers before destroying either module.
 * Queued transfer notifications borrow Forwarder until transfer shutdown drains them.
 */
class AgentControl
{
  public:
    /**
     * @brief Construct control state and derive discovery records from configured forwards.
     * @param control_executor Single-threaded control execution domain.
     * @param transfer_executor Forwarder execution domain for posted notifications.
     * @param config Borrowed immutable configuration, valid through shutdown.
     * @param connections Borrowed primary connection owner, valid through all connection notifications and selections.
     * @param routing Borrowed route state, valid through shutdown.
     * @param forwarder Borrowed transfer owner, drained by RelayAgent after all control producers exit.
     */
    AgentControl(asio::any_io_executor control_executor, asio::any_io_executor transfer_executor,
                 const AgentConfig &config, NodeConnectionMgr &connections, AgentRouting &routing,
                 Forwarder &forwarder);
    /** @brief Start optional discovery maintenance once after the owner starts the primary connection. */
    void start();
    /** @brief Cancel discovery maintenance on control_io. */
    void stop();
    /**
     * @brief Drain discovery on control_io after the owner requests control and routing shutdown.
     * @pre stop() has been called.
     * @return Completes when discovery no longer accesses the borrowed connection or route state.
     */
    asio::awaitable<void> close();
    /**
     * @brief Select a path and its logical entrance using the identified primary on control_io.
     * @param service Discovered service name and transport.
     * @param destination Expected destination Node identity.
     * @param deadline Absolute steady-clock deadline covering the whole establishment.
     * @return Primary control route, logical entrance, path, and epoch.
     * @throws std::exception On missing service, unavailable primary, or deadline expiry.
     */
    asio::awaitable<RelaySelection> select_relay(ServiceKey service, std::string destination,
                                                 std::chrono::steady_clock::time_point deadline);
    /**
     * @brief Register services and start discovery after primary identification.
     * @param connection Borrowed connection, valid until this notification returns on control_io.
     */
    void connection_ready(const NodeConnection &connection);
    /**
     * @brief Clear discovery/route state and notify Forwarder of primary disconnection.
     */
    void connection_closed();
    /**
     * @brief Match discovery/control responses or post business messages to Forwarder.
     * @param connection Authenticated source connection, borrowed during this call on control_io.
     * @param message Owned control message whose source and pending identity are checked.
     */
    void handle_control_message(const NodeConnection &connection, CtrlMessage message);

  private:
    /** @brief Poll unresolved services and topology and refresh probes. */
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
    /** @brief Allocate a nonzero identifier for discovery and topology requests. */
    std::uint64_t allocate_request_id();
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
    };
    std::unordered_map<ServiceKey, ServiceLocation> service_locations_;
    std::uint64_t next_request_id_ = 1;
    AsyncEvent discovery_done_;
    bool running_ = false;
    bool discovery_active_ = false;
};

#endif
