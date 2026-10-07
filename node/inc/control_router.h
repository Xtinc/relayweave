#ifndef RELAYWEAVE_CONTROL_ROUTER_H
#define RELAYWEAVE_CONTROL_ROUTER_H

#include "cluster_mgr.h"
#include "datagram_mgr.h"
#include "pipeline_mgr.h"
#include "registry_mgr.h"
#include <atomic>
#include <vector>

class Topology;

struct ControlRouterConfig
{
    std::string node_id;
    std::string advertise_address;
    std::uint16_t control_port = 0;
    std::size_t max_connections = 0;
    std::size_t max_services = 0;
    std::size_t max_services_per_session = 0;
};

class ControlRouter
{
  public:
    using SessionId = RegistryMgr::SessionId;
    using ClusterMessageChannel = asio::experimental::channel<void(asio::error_code, CtrlMessage)>;

    ControlRouter(asio::any_io_executor transfer_tcp_executor, asio::any_io_executor transfer_udp_executor,
                  ControlRouterConfig config, std::shared_ptr<ClusterMgr> cluster_mgr,
                  std::shared_ptr<TcpPipeline> tcp_pipeline, std::shared_ptr<TlsPipeline> tls_pipeline,
                  std::shared_ptr<DatagramMgr> datagram_mgr, Topology &topology,
                  ClusterMessageChannel &cluster_messages, const std::atomic<std::uint32_t> &control_queue_delay_us,
                  const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us,
                  const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us);

    void start();
    void stop();
    bool full() noexcept;
    std::size_t session_count() noexcept;
    SessionId add_session(const ControlSessionPtr &session);
    void remove_session(SessionId id, const ControlSessionPtr &session);
    void handle(SessionId id, const ControlSessionPtr &session, CtrlMessage message);
    void handle_cluster(CtrlMessage message);
    void sample_traffic();

  private:
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
    void open_relay(const ControlSessionPtr &consumer, const njson &params);
    void reject_relay(const ControlSessionPtr &session, const njson &params);
    void cancel_relay(const ControlSessionPtr &session, const njson &params);
    SessionId allocate_session_id();

    asio::any_io_executor transfer_tcp_executor_;
    asio::any_io_executor transfer_udp_executor_;
    ControlRouterConfig config_;
    std::shared_ptr<ClusterMgr> cluster_mgr_;
    std::shared_ptr<TcpPipeline> tcp_pipeline_;
    std::shared_ptr<TlsPipeline> tls_pipeline_;
    std::shared_ptr<DatagramMgr> datagram_mgr_;
    Topology &topology_;
    ClusterMessageChannel &cluster_messages_;
    const std::atomic<std::uint32_t> &control_queue_delay_us_;
    const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us_;
    const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us_;
    RegistryMgr registry_;
    std::chrono::steady_clock::time_point started_at_{};
    SessionId next_session_id_ = 1;
};

#endif
