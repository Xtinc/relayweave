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

// Owns the three executor probes and control-side traffic sampling.
class NodeMetrics : public std::enable_shared_from_this<NodeMetrics>
{
  public:
    struct QueueDelays
    {
        std::uint32_t control_queue_delay_us;
        std::uint32_t transfer_tcp_queue_delay_us;
        std::uint32_t transfer_udp_queue_delay_us;
    };

    NodeMetrics(asio::any_io_executor control, asio::any_io_executor tcp, asio::any_io_executor udp,
                RegistryMgr &registry, std::uint64_t bandwidth_mbps);
    // Bootstrap records uptime before cluster queries can arrive; activate after Node is running.
    void start(FlowProtocol &protocol, std::chrono::steady_clock::time_point started_at);
    void activate();
    // Each stop entry point requires its corresponding executor.
    void stop_control();
    void stop_tcp();
    void stop_udp();
    // available/uptime require control_io; queue delays are atomic snapshots.
    QueueDelays queue_delays() const noexcept;
    double available() const noexcept;
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

// Public client/cluster control protocol. All entry points require control_io.
class NodeControl
{
    using SessionId = RegistryMgr::SessionId;

  public:
    NodeControl(RegistryMgr &registry, ClusterMgr &cluster, const Topology &topology, const NodeMetrics &metrics,
                const NodeSessionList &relays, std::string node_id, std::string advertise_address,
                std::uint16_t control_port);
    // RelayNode has already validated the client params object before dispatch.
    bool handle_client(SessionId id, const ControlSessionPtr &session, CtrlCommand command, const njson &params);
    // ClusterMgr has validated params and authenticated source before dispatch.
    void handle_cluster(CtrlMessage message);

  private:
    void locate_node(SessionId id, const ControlSessionPtr &session, const njson &params);
    void handle_node_lookup(const njson &params);
    void handle_node_location(CtrlMessage message);
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
