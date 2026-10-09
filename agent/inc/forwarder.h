#ifndef PROXY_FORWARDER_H
#define PROXY_FORWARDER_H

#include "relay_agent.h"
#include "xfr_channel.h"
#include <asio/cancellation_signal.hpp>
#include <map>
#include <unordered_map>

class AgentRelay;

class Forwarder : public std::enable_shared_from_this<Forwarder>
{
  public:
    Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
              std::optional<std::string> server_name,
              std::chrono::steady_clock::duration handshake_timeout,
              std::chrono::steady_clock::duration connect_timeout,
              std::chrono::steady_clock::duration relay_open_timeout, std::vector<AgentServiceConfig> services,
              std::vector<AgentForwardConfig> forwards);
    void start();
    void set_service(ServiceKey service, std::string node);
    void clear_service(const ServiceKey &service);
    void clear_server(const std::string &server_id);
    void relay_message(ServerRoute route, CtrlMessage message);
    void invalidate_relays(std::string reason, const std::optional<ServiceKey> &service = std::nullopt);
    // The owning Agent awaits this on the transfer executor before destruction.
    asio::awaitable<void> async_stop();

  private:
    friend class AgentRelay;
    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };
    using DatagramForwardId = std::size_t;
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
        std::shared_ptr<AgentRelay> relay;
        std::chrono::steady_clock::duration retry_delay = std::chrono::milliseconds(500);
        bool retry_scheduled = false;
    };
    asio::awaitable<void> accept_stream(StreamForward &forward);
    asio::awaitable<void> receive_local_datagrams(DatagramForwardId forward_id);
    void open_datagram_forward(DatagramForwardId forward_id);
    void schedule_datagram_retry(DatagramForwardId forward_id);
    void start_relay(std::shared_ptr<AgentRelay> relay);
    void spawn_task(asio::awaitable<void> task, std::string_view context, asio::cancellation_slot slot = {});
    void finish_if_stopped();
    static bool send_control(const ServerRoute &route, CtrlMessage message);
    std::uint64_t allocate_request_id();

    RelayAgent &agent_;
    asio::executor_work_guard<asio::any_io_executor> transfer_work_;
    asio::ssl::context &ssl_context_;
    std::optional<std::string> server_name_;
    std::chrono::steady_clock::duration handshake_timeout_;
    std::chrono::steady_clock::duration connect_timeout_;
    std::chrono::steady_clock::duration relay_open_timeout_;
    std::unordered_map<std::string, AgentServiceConfig> services_;
    std::vector<StreamForward> stream_forwards_;
    std::map<DatagramForwardId, DatagramForward> datagram_forwards_;
    // Service destination identity only; control connections belong to RelayAgent.
    std::unordered_map<ServiceKey, std::string> service_nodes_;
    std::map<std::uint64_t, std::shared_ptr<AgentRelay>> relays_;
    std::uint64_t next_request_id_ = 1;
    asio::steady_timer stopped_waiter_;
    std::size_t active_tasks_ = 0;
    State state_ = State::Created;
};

#endif
