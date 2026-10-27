#ifndef RELAYWEAVE_NODE_CONNECTION_H
#define RELAYWEAVE_NODE_CONNECTION_H

#include "agent_common.h"
#include "async_event.h"

struct AgentConfig;
class NodeConnectionMgr;
class RelayAgent;

/**
 * @brief Own one Node control connection's DNS, socket, TLS identity, retry, and receive lifecycle.
 * @details Mutable state and operations require control_io. The manager owns this object until run()
 * completes. Operations are owned in-place during run(), while business sessions borrow the active
 * TLSChannel through a weak pointer.
 */
class NodeConnection
{
  public:
    /**
     * @brief Describe an endpoint without starting I/O.
     * @param host DNS name or IP address used for connection and TLS verification.
     * @param port Node control TCP port.
     */
    NodeConnection(std::string host, std::uint16_t port);
    /** @brief Inspect identified connection readiness on control_io.
     * @return True only while running with a verified server.identified identity.
     */
    bool ready() const noexcept;
    /** @brief Read the identified primary Node on control_io.
     * @return Borrowed Node ID, possibly empty before primary identification.
     */
    const std::string &node_id() const noexcept;
    /** @brief Copy the primary control route on control_io.
     * @return Identified Node and weak primary control channel.
     */
    ServerRoute route() const;
    /** @brief Enqueue control output on control_io.
     * @param message Owned message to send.
     * @pre ready() is true on the same executor.
     */
    void send(CtrlMessage message) const;

  private:
    friend class NodeConnectionMgr;
    /** @brief Own cancellable I/O for the duration of the connection task. */
    struct Operations
    {
        /** @brief Construct operations on the connection executor. */
        explicit Operations(asio::any_io_executor executor);
        /** @brief Cancel DNS, connect, and retry operations. */
        void cancel();
        asio::ip::tcp::resolver resolver;
        asio::ip::tcp::socket socket;
        asio::steady_timer retry;
    };
    /** @brief Run identified receive/retry cycles on control_io, then drain the TLS channel. */
    asio::awaitable<void> run(asio::ssl::context &ssl_context, const AgentConfig &config, RelayAgent &agent);
    /** @brief Idempotently cancel all connection stages on control_io. */
    void stop();
    std::string node_id_;
    std::string host_;
    std::uint16_t port_;
    std::string id_;
    std::weak_ptr<TLSChannel> channel_;
    std::optional<Operations> operations_;
    bool running_ = true;
    bool connected_ = false;
};

/**
 * @brief Own the Agent's sole primary control connection and drain it on control_io.
 * @details Borrows its Agent, configuration and TLS context until close() completes.
 */
class NodeConnectionMgr
{
  public:
    /**
     * @brief Configure the primary connection owner without starting I/O.
     * @param agent Owning Agent, borrowed through shutdown.
     * @param executor Single-threaded control executor.
     * @param ssl_context Client TLS context, borrowed through shutdown.
     * @param config Immutable Agent configuration, borrowed through shutdown.
     */
    NodeConnectionMgr(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                      const AgentConfig &config);
    /** @brief Start the primary connection once, before workers or on control_io. */
    void start();
    /**
     * @brief Borrow the primary connection on control_io.
     * @return Current connection, or null before start and after its task drains.
     */
    const NodeConnection *primary() const;
    /** @brief Cancel DNS, retry and the primary TLS channel on control_io. */
    void stop();
    /**
     * @brief Await primary task completion after stop() on control_io.
     * @return Completes after the connection has drained and been released.
     */
    asio::awaitable<void> close();

  private:
    /** @brief Own and drain the primary connection without completion callbacks. */
    asio::awaitable<void> run();
    RelayAgent &agent_;
    asio::any_io_executor executor_;
    asio::ssl::context &ssl_context_;
    const AgentConfig &config_;
    std::unique_ptr<NodeConnection> primary_;
    AsyncEvent stopped_event_;
    bool stopping_ = false;
};

#endif
