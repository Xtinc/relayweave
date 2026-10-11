#ifndef RELAYWEAVE_RELAY_NODE_H
#define RELAYWEAVE_RELAY_NODE_H

#include "cluster_mgr.h"
#include "datagram_mgr.h"
#include "nodelink_mgr.h"
#include "pipeline_mgr.h"
#include "registry_mgr.h"
#include "topology.h"
#include <atomic>
#include <limits>

class ControlRouterMulti;
class NodeSession;

struct ControlNodeConfig
{
    std::string address;
    std::string advertise_address;
    std::uint16_t port = 0;
    std::size_t max_connections = 1024;
    std::size_t max_services = 256;
    std::size_t max_services_per_session = 16;
};

struct StreamNodeConfig
{
    std::string address;
    std::uint16_t port = 0;
    std::size_t max_setup_connections = 1024;
    std::size_t max_relays = 1024;
    std::chrono::steady_clock::duration setup_timeout = std::chrono::seconds(10);
    TrafficLimitConfig traffic;
};

struct DatagramNodeConfig
{
    std::string address;
    std::uint16_t port = 0;
    std::size_t max_relays = 1024;
    std::chrono::steady_clock::duration setup_timeout = std::chrono::seconds(10);
    TrafficLimitConfig traffic;
};

struct NodeConfig
{
    static constexpr std::size_t maximum_services = 256;

    ControlNodeConfig control;
    ClusterConfig cluster;
    StreamNodeConfig tcp;
    StreamNodeConfig tls;
    DatagramNodeConfig datagram;
    std::filesystem::path ca_file;
    std::filesystem::path server_ca_file;
    std::filesystem::path certificate_chain;
    std::filesystem::path private_key;
    TLSChannelConfig channel;
};

ClusterConfig parse_cluster_config(const njson &root);
NodeConfig load_node_config(const std::filesystem::path &path);

class RelayNode : public std::enable_shared_from_this<RelayNode>
{
    using tcp = asio::ip::tcp;
    using SessionId = RegistryMgr::SessionId;

  public:
    RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
              asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config);
    ~RelayNode();

    void start();
    void stop();

  private:
    friend class ClusterMgr;
    friend class ControlRouterSingle;
    friend class ControlRouterMulti;
    friend class NodeSession;

    asio::awaitable<void> control_accept_loop();
    asio::awaitable<void> run_control_session(SessionId id, ControlSessionPtr session);
    void schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us);
    void schedule_traffic_sample();
    void handle_cluster_message(CtrlMessage message);
    void handle_control_message(SessionId id, const ControlSessionPtr &session, CtrlMessage message);
    void handle_control_cluster_message(CtrlMessage message);
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
    SessionId allocate_session_id();

    void handle_relay(const ControlSessionPtr &session, const CtrlMessage &message);
    void handle_relay_peer(CtrlMessage message);
    void start_relay(std::shared_ptr<NodeSession> relay);
    void cancel_relays(std::string reason, const ControlSessionPtr &session = {});
    void invalidate_relays(std::string reason = {});
    asio::awaitable<void> stop_relays();

    asio::io_context::executor_type control_executor_;
    asio::io_context::executor_type transfer_tcp_executor_;
    asio::io_context::executor_type transfer_udp_executor_;
    asio::io_context::executor_type cluster_data_executor_;
    asio::steady_timer control_probe_timer_;
    asio::steady_timer transfer_tcp_probe_timer_;
    asio::steady_timer transfer_udp_probe_timer_;
    asio::steady_timer traffic_sample_timer_;
    asio::steady_timer control_sessions_done_;
    std::atomic<std::uint32_t> control_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    std::atomic<std::uint32_t> transfer_tcp_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    std::atomic<std::uint32_t> transfer_udp_queue_delay_us_{std::numeric_limits<std::uint32_t>::max()};
    tcp::acceptor control_acceptor_;
    asio::ssl::context &ssl_context_;
    NodeConfig config_;
    asio::ssl::context cluster_context_{asio::ssl::context::tls};
    std::shared_ptr<ClusterMgr> cluster_mgr_;
    std::shared_ptr<RelayIdAllocator> relay_id_allocator_;
    std::shared_ptr<TcpPipeline> tcp_pipeline_;
    std::shared_ptr<TlsPipeline> tls_pipeline_;
    std::shared_ptr<DatagramMgr> datagram_mgr_;
    std::unique_ptr<Topology> topology_;
    std::unique_ptr<NodeLinkMgr> nodelink_mgr_;
    RegistryMgr registry_;
    std::chrono::steady_clock::time_point started_at_{};
    SessionId next_session_id_ = 1;
    std::vector<std::shared_ptr<NodeSession>> relay_sessions_;
    asio::steady_timer relays_done_;
    std::once_flag stop_once_;
    std::exception_ptr stop_error_;
    std::size_t active_control_sessions_ = 0;

    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };
    std::atomic<State> state_{State::Created};
};

#endif
