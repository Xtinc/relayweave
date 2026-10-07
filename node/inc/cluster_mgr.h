#ifndef PROXY_CLUSTER_MGR_H
#define PROXY_CLUSTER_MGR_H

#include "tls_channel.h"

class RelayNode;
class ClusterRoom;

inline constexpr std::string_view cluster_broadcast_target = "all";

struct ClusterConfig
{
    enum class Role
    {
        Master,
        Slave
    };

    Role role = Role::Master;
    std::string node_id;
    std::string address;
    std::uint16_t control_port = 0;
    std::uint16_t tcp_port = 0;
    std::uint16_t udp_port = 0;
    std::string advertise_address;
};

class ClusterMgr : public std::enable_shared_from_this<ClusterMgr>
{
    using tcp = asio::ip::tcp;
    using done_channel = asio::experimental::channel<void(asio::error_code, std::exception_ptr)>;
    using message_channel = asio::experimental::channel<void(asio::error_code, CtrlMessage)>;

  public:
    ClusterMgr(RelayNode &server, asio::any_io_executor executor, asio::ssl::context &context, ClusterConfig config,
               TLSChannelConfig channel, std::size_t max_connections);

    void start();
    asio::awaitable<void> async_stop();
    void send(std::string target, CtrlMessage message);
    void broadcast(CtrlMessage message);

  private:
    class LocalParticipant;
    class Connector;

    asio::awaitable<void> accept_loop();
    asio::awaitable<void> slave_loop();
    asio::awaitable<void> accept_sessions(ClusterRoom &room);
    asio::awaitable<void> deliver_messages(ClusterRoom &room);
    void receive(CtrlMessage message);

    RelayNode &server_;
    asio::any_io_executor executor_;
    asio::ssl::context &context_;
    ClusterConfig config_;
    TLSChannelConfig channel_config_;
    std::size_t max_connections_;
    tcp::acceptor acceptor_;
    done_channel loop_done_;
    message_channel outbound_channel_;
    Connector *connector_ = nullptr;
    std::uint64_t epoch_ = 0;
    bool running_ = false;
};

#endif // CLUSTER_MGR_H
