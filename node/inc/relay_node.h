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

class NodeControl;
class NodeMetrics;

/** @brief Configure Agent control admission, service registration limits, and the advertised address. */
struct ControlNodeConfig
{
    std::string address;
    std::string advertise_address;
    std::uint16_t port = 0;
    std::size_t max_connections = 1024;
    std::size_t max_services = 256;
    std::size_t max_services_per_session = 16;
};

/** @brief Configure one TCP or TLS listener, setup deadlines, relay capacity, and service traffic limits. */
struct StreamNodeConfig
{
    std::string address;
    std::uint16_t port = 0;
    std::size_t max_setup_connections = 1024;
    std::size_t max_relays = 1024;
    std::chrono::steady_clock::duration setup_timeout = std::chrono::seconds(10);
    TrafficLimitConfig traffic;
};

/** @brief Configure the shared Agent UDP listener, relay capacity, setup deadline, and traffic limits. */
struct DatagramNodeConfig
{
    std::string address;
    std::uint16_t port = 0;
    std::size_t max_relays = 1024;
    std::chrono::steady_clock::duration setup_timeout = std::chrono::seconds(10);
    TrafficLimitConfig traffic;
};

/** @brief Aggregate Node listeners, cluster membership, bandwidth, and TLS credentials. */
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

/**
 * @brief Parse and validate the cluster section of a Node configuration.
 * @param root Root JSON object containing cluster and public listener sections for conflict validation.
 * @return Validated cluster role, identity, master endpoint, and local NodeLink ports.
 * @throws std::invalid_argument If fields, role, reserved identity, or local listener ports are invalid.
 */
ClusterConfig parse_cluster_config(const njson &root);
/**
 * @brief Load and validate Node configuration, resolving credential paths relative to the file.
 * @param path Node JSON configuration file; relative credential paths are resolved against its directory.
 * @return Validated configuration with resolved credential paths and initialized logging settings.
 * @throws std::exception If JSON loading, field validation, or credential path validation fails.
 */
NodeConfig load_node_config(const std::filesystem::path &path);

/**
 * @brief Own Node modules and coordinate control sessions, relay admission, and shutdown.
 * @details Control state belongs to control_io; TCP/TLS, UDP, and NodeLink data each use a
 * distinct single-threaded executor. Members are ordered so borrowers are destroyed before
 * their dependencies. The io_context instances and external TLS context must outlive the Node.
 */
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
    /**
     * @brief Construct modules with four distinct execution domains without starting listeners.
     * @param control_io Single-threaded io_context owning control sessions, topology, and business
     * coordination.
     * @param transfer_tcp_io Distinct single-threaded io_context owning Agent TCP/TLS sockets and copy tasks.
     * @param transfer_udp_io Distinct single-threaded io_context owning the shared Agent UDP listener and
     * bindings.
     * @param cluster_data_io Distinct single-threaded io_context owning FlowProtocol and NodeLink data state.
     * @param ssl_context TLS server context borrowed for Agent TLS handshakes.
     * @param config Validated Node listener, cluster, capacity, bandwidth, and credential configuration.
     * @pre All four io_context instances are distinct and each is driven by a single thread.
     * @throws std::invalid_argument If execution domains overlap or cluster credentials are inconsistent.
     */
    RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
              asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config);
    /** @brief Destroy modules after stop() has drained tasks and worker threads have exited. */
    ~RelayNode();

    /**
     * @brief Start listeners and publish owned tasks exactly once.
     * @pre Called before worker run() calls, or on control_io, without overlapping stop().
     * @throws std::exception On setup failure; keep executors running and call stop() to drain tasks.
     */
    void start();
    /**
     * @brief Stop admission, cancel relays, and synchronously drain dependencies.
     * @pre Called sequentially by the owner outside server executors, with workers still running.
     * @return False for a handled shutdown error; completed repeated calls reuse the result.
     * @details Submission or drain exceptions leave shutdown retryable in the Stopping state.
     */
    bool stop();

  private:
    friend class ClusterMgr;
    friend class ControlRouterSingle;
    friend class ControlRouterMulti;
    friend class NodeSession;

    asio::awaitable<void> control_accept_loop();
    asio::awaitable<void> run_control_session(SessionId id, ControlSessionPtr session);
    void handle_cluster_message(CtrlCommand command, CtrlMessage message);
    void handle_control_message(SessionId id, const ControlSessionPtr &session, const CtrlMessage &message);
    SessionId allocate_session_id();

    void handle_relay(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message);
    /** @brief Send a business notification directly or back through the Agent's primary Node. */
    bool send_relay(const AgentPeer &route, CtrlMessage message);
    /** @brief Route incoming business messages without creating remote control sessions. */
    void handle_cluster_relay(CtrlCommand command, CtrlMessage message);
    void handle_relay_peer(CtrlCommand command, const CtrlMessage &message);
    void start_relay(std::shared_ptr<NodeSession> relay);
    void invalidate_relays(std::string reason = {});
    /** @brief Drain control-owned tasks on control_io before transfer executors are stopped. */
    asio::awaitable<bool> stop_control();
    asio::awaitable<void> stop_relays();

    asio::io_context::executor_type control_executor_;
    asio::io_context::executor_type transfer_tcp_executor_;
    asio::io_context::executor_type transfer_udp_executor_;
    asio::io_context::executor_type cluster_data_executor_;

    NodeConfig config_;
    asio::ssl::context &ssl_context_;
    asio::ssl::context cluster_context_{asio::ssl::context::tls};

    tcp::acceptor control_acceptor_;
    asio::steady_timer control_sessions_done_;
    asio::steady_timer relays_done_;

    std::size_t active_control_sessions_ = 0;
    bool stop_ok_ = true;
    std::atomic<State> state_{State::Created};

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
