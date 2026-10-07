#ifndef RELAYWEAVE_RELAY_NODE_H
#define RELAYWEAVE_RELAY_NODE_H

#include "cluster_mgr.h"
#include "datagram_mgr.h"
#include "pipeline_mgr.h"
#include "topology.h"
#include <atomic>
#include <limits>

class ControlRouter;

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
    std::chrono::steady_clock::duration service_wait_timeout = std::chrono::minutes(5);
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

NodeConfig load_node_config(const std::filesystem::path &path);

class RelayNode : public std::enable_shared_from_this<RelayNode>
{
    using tcp = asio::ip::tcp;
    using SessionId = std::uint64_t;
    using cluster_message_channel = asio::experimental::channel<void(asio::error_code, CtrlMessage)>;

  public:
    RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
              asio::ssl::context &ssl_context, NodeConfig config);
    ~RelayNode();

    void start();
    void stop();
    void send_cluster(std::string target, CtrlMessage message);
    void broadcast_cluster(CtrlMessage message);
    asio::awaitable<CtrlMessage> async_receive_cluster();

  private:
    friend class ClusterMgr;

    asio::awaitable<void> control_accept_loop();
    asio::awaitable<void> run_control_session(SessionId id, ControlSessionPtr session);
    void schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us);
    void schedule_traffic_sample();
    void handle_cluster_message(CtrlMessage message);

    asio::io_context::executor_type control_executor_;
    asio::io_context::executor_type transfer_tcp_executor_;
    asio::io_context::executor_type transfer_udp_executor_;
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
    cluster_message_channel cluster_messages_;
    std::shared_ptr<ClusterMgr> cluster_mgr_;
    std::shared_ptr<RelayIdAllocator> relay_id_allocator_;
    std::shared_ptr<TcpPipeline> tcp_pipeline_;
    std::shared_ptr<TlsPipeline> tls_pipeline_;
    std::shared_ptr<DatagramMgr> datagram_mgr_;
    std::unique_ptr<Topology> topology_;
    std::unique_ptr<ControlRouter> control_router_;
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
