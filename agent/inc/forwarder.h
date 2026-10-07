#ifndef PROXY_FORWARDER_H
#define PROXY_FORWARDER_H

#include "dualindex_map.h"
#include "relay_agent.h"
#include "xfr_channel.h"
#include <map>
#include <unordered_map>

struct ServerRoute
{
    std::string id;
    std::string host;
    std::weak_ptr<TLSChannel> channel;
};

class Forwarder : public std::enable_shared_from_this<Forwarder>
{
  public:
    Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
              std::optional<std::string> server_name,
              std::chrono::steady_clock::duration handshake_timeout,
              std::chrono::steady_clock::duration connect_timeout,
              std::chrono::steady_clock::duration stream_open_timeout, std::vector<AgentServiceConfig> services,
              std::vector<AgentForwardConfig> forwards);

    void start();
    void set_route(std::string service, RelayProtocol protocol, ServerRoute route);
    void clear_route(const std::string &service, RelayProtocol protocol);
    void clear_server(const std::string &server_id);
    // The Agent awaits this on its transfer executor and stays alive until it completes.
    asio::awaitable<void> async_stop();

    void stream_relay_offer(ServerRoute route, RelayProtocol protocol, std::string service, std::uint64_t uuid,
                            std::uint64_t ticket, std::uint16_t transfer_port);
    void datagram_relay_offer(ServerRoute route, std::string service, std::uint64_t uuid, std::uint64_t session_id,
                              std::uint64_t ticket, std::uint16_t transfer_port);
    void stream_relay_opened(ServerRoute route, RelayProtocol protocol, std::uint64_t request_id, std::uint64_t uuid,
                             std::uint64_t ticket, std::uint16_t transfer_port);
    void datagram_relay_opened(ServerRoute route, std::uint64_t request_id, std::uint64_t uuid,
                               std::uint64_t session_id, std::uint64_t ticket, std::uint16_t transfer_port);
    void relay_open_failed(ServerRoute route, std::uint64_t request_id, RelayProtocol protocol,
                           std::optional<std::uint64_t> uuid, std::string reason);
    void relay_ready(ServerRoute route, std::uint64_t uuid, RelayProtocol protocol);
    void relay_closed(ServerRoute route, std::uint64_t uuid, RelayProtocol protocol, std::string reason);

  private:
    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };

    struct StreamRelay;
    struct DatagramRelay;

    using RouteKey = ServiceKey;
    using DatagramForwardId = std::size_t;

    struct RouteTarget
    {
        std::string host;
        std::weak_ptr<TLSChannel> channel;
    };

    struct RelayKey
    {
        std::string server;
        std::uint64_t uuid;
        auto operator<=>(const RelayKey &) const = default;
    };

    struct StreamForward
    {
        StreamForward(asio::any_io_executor executor, AgentForwardConfig config);

        asio::ip::tcp::acceptor acceptor;
        AgentForwardConfig config;
    };

    struct DatagramForward
    {
        DatagramForward(asio::any_io_executor executor, AgentForwardConfig config);

        asio::ip::udp::socket listener_socket;
        asio::steady_timer retry_timer;
        AgentForwardConfig config;
        std::optional<asio::ip::udp::endpoint> local_peer;
        std::optional<ServerRoute> server;
        std::shared_ptr<DatagramRelay> relay;
        std::chrono::steady_clock::duration retry_delay = std::chrono::milliseconds(500);
        bool retry_scheduled = false;
    };

    asio::awaitable<void> accept_stream(StreamForward &forward);
    asio::awaitable<void> receive_local_datagrams(DatagramForwardId forward_id);
    void open_datagram_forward(DatagramForwardId forward_id);
    void schedule_datagram_retry(DatagramForwardId forward_id);
    void start_stream_relay(std::shared_ptr<StreamRelay> relay, RelayAttach attach, std::uint16_t transfer_port,
                            const AgentServiceConfig *target);
    asio::awaitable<void> run_stream_relay(std::shared_ptr<StreamRelay> relay, RelayAttach attach,
                                           std::uint16_t transfer_port, const AgentServiceConfig *target);
    void start_datagram_relay(std::shared_ptr<DatagramRelay> relay, std::uint16_t transfer_port,
                              const AgentServiceConfig *target);
    asio::awaitable<void> run_datagram_producer(std::shared_ptr<DatagramRelay> relay, std::uint16_t transfer_port,
                                                const AgentServiceConfig &target);
    asio::awaitable<void> run_datagram_consumer(std::shared_ptr<DatagramRelay> relay, std::uint16_t transfer_port);
    asio::awaitable<void> prepare_datagram_relay(const std::shared_ptr<DatagramRelay> &relay,
                                                 std::uint16_t transfer_port);
    void finish_datagram_relay(const std::shared_ptr<DatagramRelay> &relay, std::string failure);
    void close_all_relays();
    void spawn_task(asio::awaitable<void> task, std::string_view context);
    void finish_if_stopped();
    std::optional<ServerRoute> find_route(const std::string &service, RelayProtocol protocol);
    static bool send_control(const ServerRoute &route, CtrlMessage message);
    std::uint64_t allocate_request_id();

    // The owning Agent waits for Forwarder shutdown before destruction.
    RelayAgent &agent_;
    asio::executor_work_guard<asio::any_io_executor> transfer_work_;
    asio::ssl::context &ssl_context_;
    std::optional<std::string> server_name_;
    std::chrono::steady_clock::duration handshake_timeout_;
    std::chrono::steady_clock::duration connect_timeout_;
    std::chrono::steady_clock::duration stream_open_timeout_;
    std::unordered_map<std::string, AgentServiceConfig> services_;
    std::vector<StreamForward> stream_forwards_;
    DualIndexMap<DatagramForwardId, std::uint64_t, DatagramForward, SecondaryKeyMode::Unique> datagram_forwards_;
    DualIndexMap<RouteKey, std::string, RouteTarget> routes_;
    std::unordered_map<std::uint64_t, std::shared_ptr<StreamRelay>> pending_stream_opens_;
    std::multimap<RelayKey, std::weak_ptr<StreamRelay>> active_stream_relays_;
    std::multimap<RelayKey, std::weak_ptr<DatagramRelay>> active_datagram_relays_;
    std::uint64_t next_request_id_ = 1;
    asio::steady_timer stopped_waiter_;
    std::size_t active_tasks_ = 0;
    State state_ = State::Created;
};

#endif
