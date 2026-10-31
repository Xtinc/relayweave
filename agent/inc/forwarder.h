#ifndef RELAYWEAVE_FORWARDER_H
#define RELAYWEAVE_FORWARDER_H

#include "async_event.h"
#include "relay_agent.h"
#include "xfr_channel.h"
#include <map>
#include <unordered_map>
#include <unordered_set>

class AgentSession;

/**
 * @brief Own local forwards, business sessions, UDP associations, and idle expiry on transfer_io.
 * @details Borrows RelayAgent and the TLS context through shutdown. Owns all listeners and session
 * associations; AgentSession uses lifecycle notifications instead of modifying these containers.
 * async_stop() drains listeners, idle expiry, and business tasks before releasing the transfer work guard.
 */
class Forwarder
{
  public:
    /**
     * @brief Construct listeners and session settings without starting I/O.
     * @param agent Borrowed composition root used for cross-domain entrance selection.
     * @param executor Single-threaded transfer executor owning all forward and session state.
     * @param ssl_context Borrowed TLS context, valid until business tasks drain.
     * @param config Settings and listener descriptions copied during construction.
     */
    Forwarder(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
              const AgentConfig &config);
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
     * @brief Unregister a drained session and release its UDP association on transfer_io.
     * @param relay Session retained by its run task through this notification.
     */
    void relay_finished(AgentSession &relay);
    /**
     * @brief Send the cached first packet, then return Node datagrams through the local UDP listener.
     * @param relay Attached, ready consumer with an owned forward and a cached first packet.
     * @return Completes on session cancellation or throws the original receive/send error.
     */
    asio::awaitable<void> relay_datagrams(AgentSession &relay);
    /**
     * @brief Submit output through a weak control route; TLSChannel serializes its own executor.
     * @param route Entrance metadata with a weak TLS channel.
     * @param message Owned protocol message.
     * @pre Message parameters are an object and the route identifies a selected Node.
     * @return False if the channel has expired; true means queued, not acknowledged.
     */
    static bool send_control(const ServerRoute &route, CtrlMessage message);
    /**
     * @brief Bind listeners and start owned tasks once, before the transfer worker runs.
     * @throws std::exception On setup failure; call async_stop() to drain partial startup.
     */
    void start();
    /**
     * @brief Publish service availability; a changed location cancels its old UDP associations.
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
     * @brief Clear availability and cancel business sessions on transfer_io.
     */
    void control_disconnected();
    /**
     * @brief Cancel sessions whose endpoint left the cluster, on transfer_io.
     * @param nodes Member IDs from the latest accepted topology snapshot.
     */
    void retain_nodes(const std::unordered_set<std::string> &nodes);
    /**
     * @brief Match a business message or create its producer session on transfer_io.
     * @param route Primary control source and logical business Node.
     * @param message Owned message, matched by source, protocol, request and/or UUID.
     */
    void relay_message(ServerRoute route, CtrlMessage message);
    /**
     * @brief Cancel all consumer sessions still establishing on transfer_io.
     * @param reason First cancellation cause.
     */
    void invalidate_relays(std::string reason);
    /**
     * @brief Cancel listeners, idle expiry and sessions, and await every owned task on transfer_io.
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
        AgentForwardConfig config;
        std::map<asio::ip::udp::endpoint, std::uint64_t> clients;
    };

    asio::awaitable<void> accept_stream(StreamForward &forward);
    asio::awaitable<void> receive_local_datagrams(DatagramForwardId forward_id);
    asio::awaitable<void> send_datagram(AgentSession &relay, std::span<const std::uint8_t> payload);
    asio::awaitable<void> expire_datagrams();
    void start_relay(std::shared_ptr<AgentSession> relay);
    void spawn_task(asio::awaitable<void> task, std::string_view context);
    void finish_if_stopped();
    std::uint64_t allocate_request_id();

    static constexpr std::size_t relay_capacity = 1000;
    static constexpr std::size_t udp_opening_capacity = 100;
    RelayAgent &agent_;
    asio::executor_work_guard<asio::any_io_executor> transfer_work_;
    SessionOptions session_options_;
    std::chrono::steady_clock::duration relay_open_timeout_;
    std::chrono::steady_clock::duration udp_idle_timeout_;
    asio::steady_timer udp_idle_timer_;
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
