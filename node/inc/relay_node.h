#ifndef RELAYWEAVE_RELAY_NODE_H
#define RELAYWEAVE_RELAY_NODE_H

#include "cluster_mgr.h"
#include "datagram_mgr.h"
#include "node_session.h"
#include "nodelink_mgr.h"
#include "pipeline_mgr.h"
#include "registry_mgr.h"
#include "topology.h"
#include <atomic>
#include <mutex>

class NodeControl;
class NodeMetrics;

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
    std::uint64_t bandwidth_mbps = 0;

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

    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };

  public:
    RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
              asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config);
    ~RelayNode();

    // Bootstrap before run(), or call on control_io. After failure, stop() drains published tasks.
    void start();
    // Returns after draining; false reports a handled shutdown error.
    bool stop();

  private:
    friend class ClusterMgr;
    friend class ControlRouterSingle;
    friend class ControlRouterMulti;
    friend class NodeSession;

    asio::awaitable<void> control_accept_loop();
    asio::awaitable<void> run_control_session(SessionId id, ControlSessionPtr session);
    void handle_cluster_message(CtrlMessage message);
    void handle_control_message(SessionId id, const ControlSessionPtr &session, const CtrlMessage &message);
    SessionId allocate_session_id();

    void handle_relay(const ControlSessionPtr &session, const CtrlMessage &message);
    void handle_relay_peer(const CtrlMessage &message);
    void start_relay(std::shared_ptr<NodeSession> relay);
    void invalidate_relays(std::string reason = {});
    // Runs on control_io; stop() waits before stopping the transfer executors.
    asio::awaitable<bool> stop_control();
    asio::awaitable<void> stop_relays();

    // Execution domains outlive their I/O resources and modules.
    asio::io_context::executor_type control_executor_;
    asio::io_context::executor_type transfer_tcp_executor_;
    asio::io_context::executor_type transfer_udp_executor_;
    asio::io_context::executor_type cluster_data_executor_;

    // Configuration and TLS dependencies.
    NodeConfig config_;
    asio::ssl::context &ssl_context_;
    asio::ssl::context cluster_context_{asio::ssl::context::tls};

    // Control listener and shutdown waiters.
    tcp::acceptor control_acceptor_;
    asio::steady_timer control_sessions_done_;
    asio::steady_timer relays_done_;

    // Runtime bookkeeping remains alive while modules are destroyed.
    SessionId next_session_id_ = 1;
    std::size_t active_control_sessions_ = 0;
    std::mutex stop_mutex_;
    bool stop_ok_ = true;
    std::atomic<State> state_{State::Created};

    // Owners precede their borrowers; destruction runs in reverse.
    RegistryMgr registry_;
    std::shared_ptr<RelayIdAllocator> relay_id_allocator_;
    std::shared_ptr<TcpPipeline> tcp_pipeline_;
    std::shared_ptr<TlsPipeline> tls_pipeline_;
    std::shared_ptr<DatagramMgr> datagram_mgr_;
    std::shared_ptr<ClusterMgr> cluster_mgr_;
    std::shared_ptr<NodeMetrics> metrics_;
    std::unique_ptr<Topology> topology_;
    std::unique_ptr<NodeLinkMgr> nodelink_mgr_;
    NodeSessionList relay_sessions_;
    std::unique_ptr<NodeControl> control_;
};

#endif
