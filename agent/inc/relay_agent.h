#ifndef RELAYWEAVE_RELAY_AGENT_H
#define RELAYWEAVE_RELAY_AGENT_H

#include "agent_control.h"
#include "agent_routing.h"
#include "node_connection_mgr.h"
#include "async_event.h"

struct AgentServiceConfig
{
    std::string name;
    std::string target_host;
    std::uint16_t target_port = 0;
    RelayProtocol protocol = RelayProtocol::Tcp;
};

struct AgentForwardConfig
{
    std::string service;
    std::string listen_address;
    std::uint16_t listen_port = 0;
    RelayProtocol protocol = RelayProtocol::Tcp;
};

struct AgentConfig
{
    std::string host;
    std::uint16_t port = 0;
    std::filesystem::path ca_file;
    std::filesystem::path certificate_chain;
    std::filesystem::path private_key;
    std::optional<std::string> server_name;
    std::chrono::steady_clock::duration connect_timeout = std::chrono::seconds(5);
    std::chrono::steady_clock::duration reconnect_initial_delay = std::chrono::milliseconds(500);
    std::chrono::steady_clock::duration reconnect_max_delay = std::chrono::seconds(10);
    std::chrono::steady_clock::duration relay_open_timeout = std::chrono::seconds(10);
    std::vector<AgentServiceConfig> services;
    std::vector<AgentForwardConfig> forwards;
    TLSChannelConfig channel;
    std::size_t routing_max_nodes = 4;
};

AgentConfig load_agent_config(const std::filesystem::path &path);

class Forwarder;

/**
 * @brief Own Agent modules and coordinate startup, executor crossing, and shutdown.
 * @details Configuration outlives its borrowers. Control modules run on control_io; Forwarder and
 * business sockets run on transfer_io. Both executors and the external TLS context must outlive
 * async_stop() and all worker threads. Control borrows the earlier-declared pool, routing, and
 * Forwarder; task completions retain the Agent, and shutdown drains producers before borrowers die.
 */
class RelayAgent : public std::enable_shared_from_this<RelayAgent>
{
  public:
    /**
     * @brief Construct modules without starting control connections or local listeners.
     * @param control_io Single-threaded control execution domain.
     * @param transfer_io Distinct single-threaded business execution domain.
     * @param ssl_context Borrowed client TLS context, valid through shutdown.
     * @param config Validated configuration, owned by value.
     * @throws std::invalid_argument If execution domains overlap.
     */
    RelayAgent(asio::io_context &control_io, asio::io_context &transfer_io, asio::ssl::context &ssl_context,
               AgentConfig config);
    /**
     * @brief Start local listeners and control tasks once, before workers run.
     * @throws std::exception On setup failure; keep executors running and await async_stop().
     */
    void start();
    /**
     * @brief Cancel and drain control tasks, then business tasks; callable from either executor.
     * @return Completes after all owned tasks drain; concurrent callers await the same completion.
     * @throws std::exception If draining fails; the failure is preserved and state remains Stopping.
     */
    asio::awaitable<void> async_stop();
    /**
     * @brief Select an entrance on control_io and return to the caller's executor.
     * @param service Service name and transport identifying the discovered service.
     * @param destination Expected destination Node identity from Forwarder.
     * @param deadline Absolute steady-clock establishment deadline; never renewed.
     * @return Path, epoch, entrance metadata, and shared connection handle after identification.
     * @throws std::exception If selection fails, expires, or is invalidated by shutdown.
     */
    asio::awaitable<RelaySelection> select_relay(ServiceKey service, std::string destination,
                                                std::chrono::steady_clock::time_point deadline);

  private:
    friend class NodeConnectionMgr;
    friend class NodeConnection;
    /** @brief Dispatch an identified entrance to the control component on control_io. */
    void connection_ready(const NodeConnection &connection);
    /** @brief Dispatch an entrance disconnect while all peer modules remain alive. */
    void connection_closed(const NodeConnection &connection);
    /** @brief Release matching entrance waits before the manager erases a completed connection. */
    void connection_finished(const NodeConnection &connection);
    /** @brief Deliver authenticated connection messages to the control component on control_io. */
    void handle_control_message(const NodeConnection &connection, CtrlMessage message);

    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };
    /** @brief Serialize shutdown and await module drains on control_io. */
    asio::awaitable<void> stop_on_control_executor();
    /** @brief Drain dependencies in an independently owned shutdown task on control_io. */
    asio::awaitable<void> drain_stop();

    asio::any_io_executor control_executor_;
    asio::any_io_executor transfer_executor_;
    AgentConfig config_;
    AgentRouting routing_;
    NodeConnectionMgr connections_;
    std::shared_ptr<Forwarder> forwarder_;
    AgentControl control_;
    AsyncEvent stopped_event_;
    std::exception_ptr stop_failure_;
    State state_ = State::Created;
};

#endif
