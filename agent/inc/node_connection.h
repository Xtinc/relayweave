#ifndef RELAYWEAVE_NODE_CONNECTION_H
#define RELAYWEAVE_NODE_CONNECTION_H

#include "agent_common.h"
#include "async_event.h"

struct AgentConfig;
class RelayAgent;

/**
 * @brief Own the Agent's sole primary control connection and its complete retry/drain lifecycle.
 * @details RelayAgent owns this object directly. Mutable state and operations require control_io;
 * start() may also run before workers. The borrowed Agent, configuration and TLS context outlive
 * close(). Business sessions borrow the active TLSChannel through a weak pointer.
 */
class NodeConnection
{
  public:
    /**
     * @brief Configure the primary connection without starting I/O.
     * @param agent Owning Agent, borrowed through shutdown.
     * @param executor Single-threaded control executor.
     * @param ssl_context Client TLS context, borrowed through shutdown.
     * @param config Immutable Agent configuration, borrowed through shutdown.
     */
    NodeConnection(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                   const AgentConfig &config);
    /** @brief Start the primary connection once, before workers or on control_io. */
    void start();
    /** @brief Cancel DNS, connect, retry and the primary TLS channel on control_io. */
    void stop();
    /**
     * @brief Await task completion after stop() on control_io.
     * @return Completes after connection I/O and the TLS channel have drained.
     */
    asio::awaitable<void> close();
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
     * @pre ready() is true on the same executor; run() owns the active channel.
     */
    void send(CtrlMessage message) const;

  private:
    /** @brief Own cancellable I/O from start() until the connection task drains. */
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
    /** @brief Run identified receive/retry cycles, report terminal failure, and notify drainage on control_io. */
    asio::awaitable<void> run();
    RelayAgent &agent_;
    asio::any_io_executor executor_;
    asio::ssl::context &ssl_context_;
    const AgentConfig &config_;
    std::string node_id_;
    std::string id_;
    std::weak_ptr<TLSChannel> channel_;
    std::optional<Operations> operations_;
    AsyncEvent stopped_event_;
    bool running_ = true;
    bool connected_ = false;
};

#endif
