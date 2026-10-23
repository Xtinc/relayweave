#ifndef RELAYWEAVE_FORWARDER_H
#define RELAYWEAVE_FORWARDER_H

#include "async_event.h"
#include "relay_agent.h"
#include "xfr_channel.h"
#include <map>
#include <unordered_map>

class AgentSession;

/**
 * @brief Own local forwards, business sessions, UDP source tracking, and retry tasks on transfer_io.
 * @details Borrows RelayAgent and the TLS context through shutdown. Owns all listeners and session
 * associations; AgentSession uses lifecycle notifications instead of modifying these containers.
 * async_stop() drains listeners, retries, and business tasks before releasing the transfer work guard.
 */
class Forwarder
{
  public:
    /**
     * @brief Construct listeners and session settings without starting I/O.
     * @param agent Borrowed composition root used for cross-domain entrance selection.
     * @param executor Single-threaded transfer executor owning all forward and session state.
     * @param ssl_context Borrowed TLS context, valid until business tasks drain.
     * @param server_name Optional TLS server name override.
     * @param handshake_timeout Maximum duration of one TLS handshake.
     * @param connect_timeout Maximum duration of one resolve/connect operation.
     * @param relay_open_timeout Total per-session establishment budget.
     * @param services Local targets, owned by value.
     * @param forwards Local listener descriptions, owned by value.
     */
    Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
              std::optional<std::string> server_name, std::chrono::steady_clock::duration handshake_timeout,
              std::chrono::steady_clock::duration connect_timeout,
              std::chrono::steady_clock::duration relay_open_timeout, std::vector<AgentServiceConfig> services,
              std::vector<AgentForwardConfig> forwards);
    /** @brief Immutable connection settings borrowed by sessions for their owner's lifetime. */
    struct SessionOptions
    {
        asio::ssl::context &ssl_context;
        std::optional<std::string> server_name;
        std::chrono::steady_clock::duration handshake_timeout;
        std::chrono::steady_clock::duration connect_timeout;
    };
    /** @brief Read the business execution domain. @return Executor owning every session socket. */
    asio::any_io_executor executor() const
    {
        return transfer_work_.get_executor();
    }
    /** @brief Read shared session configuration. @return Immutable options, valid until owner destruction. */
    const SessionOptions &session_options() const
    {
        return session_options_;
    }
    /**
     * @brief Obtain an identified business entrance through RelayAgent's control executor.
     * @param service Discovered service name and protocol.
     * @param destination Expected destination Node identity.
     * @param deadline Original absolute steady-clock establishment deadline.
     * @return Selection result on transfer_io after control-side lookup/connection completes.
     */
    asio::awaitable<RelaySelection> select_relay(ServiceKey service, std::string destination,
                                                 std::chrono::steady_clock::time_point deadline);
    /**
     * @brief Reset the associated UDP retry delay on transfer_io.
     * @param relay Session that has received ready, borrowed during this notification.
     */
    void relay_ready(const AgentSession &relay);
    /**
     * @brief Unregister a drained session, release its UDP association, and consider retry on transfer_io.
     * @param relay Session retained by its run task through this notification.
     */
    void relay_finished(const AgentSession &relay);
    /**
     * @brief Return authenticated Node datagrams through the owned local UDP listener on transfer_io.
     * @param relay Live consumer session, retained by its run task until this call finishes.
     * @return Completes on session cancellation or throws the original receive/send error.
     */
    asio::awaitable<void> receive_datagrams(AgentSession &relay);
    /**
     * @brief Submit output through a weak control route; TLSChannel serializes its own executor.
     * @param route Entrance metadata with a weak TLS channel.
     * @param message Owned protocol message.
     * @return False if the channel has expired; true means queued, not acknowledged.
     */
    static bool send_control(const ServerRoute &route, CtrlMessage message);
    /**
     * @brief Bind listeners and start owned tasks once, before the transfer worker runs.
     * @throws std::exception On setup failure; call async_stop() to drain partial startup.
     */
    void start();
    /**
     * @brief Publish service availability and trigger eligible UDP forwards on transfer_io.
     * @param service Service name and transport.
     * @param node Destination Node identity, independent of entrance connections.
     */
    void set_service(ServiceKey service, std::string node);
    /**
     * @brief Clear availability and cancel affected opening/UDP sessions on transfer_io.
     * @param service Service name and transport to invalidate.
     * @param reason First cancellation cause retained by affected sessions.
     */
    void clear_service(const ServiceKey &service, std::string reason = "service unavailable");
    /**
     * @brief Cancel sessions using one disconnected control entrance on transfer_io.
     * @param server_id Immutable host:port connection identifier.
     */
    void clear_server(const std::string &server_id);
    /**
     * @brief Match a business message or create its producer session on transfer_io.
     * @param route Authenticated control source and data hostname.
     * @param message Owned message, matched by source, protocol, request and/or UUID.
     */
    void relay_message(ServerRoute route, CtrlMessage message);
    /**
     * @brief Cancel all consumer sessions still establishing on transfer_io.
     * @param reason First cancellation cause.
     */
    void invalidate_relays(std::string reason);
    /**
     * @brief Cancel listeners, retries and sessions, and await every owned task on transfer_io.
     * @return Completes after work guard release.
     */
    asio::awaitable<void> async_stop();

  private:
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
        std::shared_ptr<AgentSession> relay;
        std::chrono::steady_clock::duration retry_delay = std::chrono::milliseconds(500);
        bool retry_scheduled = false;
    };

    asio::awaitable<void> accept_stream(StreamForward &forward);
    asio::awaitable<void> receive_local_datagrams(DatagramForwardId forward_id);
    void open_datagram_forward(DatagramForwardId forward_id);
    void schedule_datagram_retry(DatagramForwardId forward_id);
    asio::awaitable<void> retry_datagram_forward(DatagramForwardId forward_id);
    void start_relay(std::shared_ptr<AgentSession> relay);
    void spawn_task(asio::awaitable<void> task, std::string_view context);
    void finish_if_stopped();
    std::uint64_t allocate_request_id();

    RelayAgent &agent_;
    asio::executor_work_guard<asio::any_io_executor> transfer_work_;
    SessionOptions session_options_;
    std::chrono::steady_clock::duration relay_open_timeout_;
    std::unordered_map<std::string, AgentServiceConfig> services_;
    std::vector<StreamForward> stream_forwards_;
    std::vector<DatagramForward> datagram_forwards_;
    std::unordered_map<ServiceKey, std::string> service_nodes_;
    std::map<std::uint64_t, std::shared_ptr<AgentSession>> relays_;
    std::uint64_t next_request_id_ = 1;
    AsyncEvent stopped_event_;
    std::size_t active_tasks_ = 0;
    State state_ = State::Created;
};

#endif
