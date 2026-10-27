#ifndef RELAYWEAVE_NODE_CONTROL_H
#define RELAYWEAVE_NODE_CONTROL_H

#include "cluster_mgr.h"
#include "node_session.h"
#include "registry_mgr.h"
#include <atomic>
#include <chrono>
#include <limits>

class FlowProtocol;
class Topology;

/**
 * @brief Own executor queue probes and control-side service and forwarding samples.
 * @details RegistryMgr and FlowProtocol are borrowed. Three timers run on their respective
 * control/TCP/UDP executors; queue delays are atomic snapshots, while rates and uptime
 * require control_io. Timer handlers retain this object until cancellation completes.
 */
class NodeMetrics : public std::enable_shared_from_this<NodeMetrics>
{
  public:
    /** @brief Report queue delays in microseconds; UINT32_MAX means no valid sample. */
    struct QueueDelays
    {
        std::uint32_t control_queue_delay_us;
        std::uint32_t transfer_tcp_queue_delay_us;
        std::uint32_t transfer_udp_queue_delay_us;
    };

    /**
     * @brief Construct probes for three executors and borrow the service registry.
     * @param control Executor for control-side metrics and service sampling.
     * @param tcp Executor for the TCP/TLS queue-delay probe.
     * @param udp Executor for the UDP queue-delay probe.
     * @param registry Service registry borrowed for control-side rate sampling; must outlive the metrics
     * provider.
     * @param bandwidth_mbps Configured Node capacity in megabits per second, used to normalize availability.
     * @pre bandwidth_mbps is positive and registry outlives this metrics provider.
     */
    NodeMetrics(asio::any_io_executor control, asio::any_io_executor tcp, asio::any_io_executor udp,
                RegistryMgr &registry, std::uint64_t bandwidth_mbps);
    /**
     * @brief Record uptime and borrow the protocol during bootstrap before cluster queries arrive.
     * @param protocol Node-owned protocol borrowed for forwarded-byte sampling until control sampling stops.
     * @param started_at Steady-clock startup timestamp used for uptime and bandwidth sample intervals.
     * @pre Called before activate() and before cluster queries can observe uptime.
     */
    void start(FlowProtocol &protocol, std::chrono::steady_clock::time_point started_at);
    /** @brief Schedule probes during startup after protocol binding and Node activation. */
    void activate();
    /** @brief Disable probe rescheduling and cancel the control timer on control_io. */
    void stop_control();
    /** @brief Cancel the TCP probe on transfer_tcp_io after stop_control(). */
    void stop_tcp();
    /** @brief Cancel the UDP probe on transfer_udp_io after stop_control(). */
    void stop_udp();
    /**
     * @brief Return independent atomic delay snapshots from any execution domain.
     * @return Independent atomic delay samples in microseconds; UINT32_MAX marks an unavailable sample.
     */
    QueueDelays queue_delays() const noexcept;
    /**
     * @brief Return the last sampled bandwidth availability in [0, 1] on control_io.
     * @return Last sampled fraction of configured bandwidth remaining, clamped to [0, 1].
     * @pre Called on control_io.
     */
    double available() const noexcept;
    /**
     * @brief Return nonnegative elapsed time since start() on control_io.
     * @return Elapsed steady-clock time since start(), clamped to a nonnegative millisecond duration.
     * @pre Called on control_io after start().
     */
    std::chrono::milliseconds uptime() const noexcept;

  private:
    using atomic_u32 = std::atomic<std::uint32_t>;

    void schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us);

    RegistryMgr &registry_;
    std::uint64_t bandwidth_mbps_;
    FlowProtocol *protocol_ = nullptr;
    asio::steady_timer control_probe_timer_;
    asio::steady_timer transfer_tcp_probe_timer_;
    asio::steady_timer transfer_udp_probe_timer_;
    atomic_u32 control_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    atomic_u32 transfer_tcp_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    atomic_u32 transfer_udp_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    std::chrono::steady_clock::time_point started_at_{};
    std::chrono::steady_clock::time_point bandwidth_at_{};
    double available_ = 1.0;
    std::atomic_bool running_{false};
};

/**
 * @brief Handle public Agent queries and cluster replies on control_io.
 * @details Borrows the registry, cluster, topology, metrics, and business metadata container.
 * Owns no relay instances or background tasks and never reads data-executor state.
 */
class NodeControl
{
    using SessionId = RegistryMgr::SessionId;

  public:
    /**
     * @brief Borrow Node-owned dependencies and retain the local advertised control endpoint.
     * @param registry Node-owned session and service registry borrowed on control_io.
     * @param cluster Node-owned cluster control manager borrowed for message routing.
     * @param topology Node-owned topology and current membership state borrowed on control_io.
     * @param metrics Node-owned metrics provider borrowed for control-side snapshots.
     * @param relays Node-owned business container borrowed for read-only status reporting.
     * @param node_id Unique nonempty cluster Node identity.
     * @param advertise_address Reachable Node address published for Agent control connections.
     * @param control_port Port of the Node's public Agent control listener.
     */
    NodeControl(RegistryMgr &registry, ClusterMgr &cluster, const Topology &topology, const NodeMetrics &metrics,
                const NodeSessionList &relays, std::string node_id, std::string advertise_address,
                std::uint16_t control_port);
    /**
     * @brief Handle a public client command; return false if the command belongs elsewhere.
     * @pre RelayNode has validated the params object; command-specific fields are checked here.
     * @param id Registered Agent session ID used to correlate replies routed through the cluster.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param command Control command already parsed by the caller.
     * @param params Client parameter object validated by RelayNode; command-specific fields are checked here.
     * @return True if this component handled the command; false if it belongs to another handler.
     */
    bool handle_client(SessionId id, const ControlSessionPtr &session, CtrlCommand command, const njson &params);
    /**
     * @brief Handle a cluster query or reply after ClusterMgr validates params and authenticates source.
     * @param command Control command already parsed by the caller.
     * @param message Cluster query or reply with parameter object and authenticated source validated by
     * ClusterMgr.
     */
    void handle_cluster(CtrlCommand command, CtrlMessage message);

  private:
    void locate_node(SessionId id, const ControlSessionPtr &session, const njson &params);
    void handle_node_lookup(const njson &params);
    void handle_node_location(CtrlCommand command, CtrlMessage message);
    void broadcast_client_query(SessionId id, CtrlMessage message);
    void attach_cluster_reply_route(CtrlMessage &reply, const njson &query);
    ControlSessionPtr prepare_client_reply(CtrlMessage &reply, std::string_view location);
    void register_service(SessionId id, const ControlSessionPtr &session, const njson &params);
    void locate_service(SessionId id, const ControlSessionPtr &session, const njson &params);
    void report_cluster_status(SessionId id, const ControlSessionPtr &session, const njson &params);
    void handle_cluster_lookup(const njson &params);
    void handle_cluster_location(CtrlMessage message);
    void handle_cluster_status_query(const njson &params);
    void handle_cluster_status_report(CtrlMessage message);
    void report_topology(SessionId id, const ControlSessionPtr &session, const njson &params);
    void handle_topology_query(const njson &params);
    void handle_topology_snapshot(CtrlMessage message);
    std::optional<CtrlMessage> service_location(const njson &params);
    CtrlMessage server_status_message(std::uint64_t request_id);
    void list_services(const ControlSessionPtr &session, const njson &params);
    void report_load(const ControlSessionPtr &session, const njson &params);
    void report_traffic(const ControlSessionPtr &session, const njson &params);

    RegistryMgr &registry_;
    ClusterMgr &cluster_;
    const Topology &topology_;
    const NodeMetrics &metrics_;
    const NodeSessionList &relay_sessions_;
    std::string node_id_;
    std::string advertise_address_;
    std::uint16_t control_port_;
};

#endif
