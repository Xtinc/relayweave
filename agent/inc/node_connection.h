#ifndef RELAYWEAVE_NODE_CONNECTION_H
#define RELAYWEAVE_NODE_CONNECTION_H

#include "agent_common.h"

struct AgentConfig;
class NodeConnectionMgr;
class RelayAgent;

/**
 * @brief Own one Node control connection's DNS, socket, TLS identity, retry, and receive lifecycle.
 * @details Mutable state and operations require control_io. The manager owns this object until run()
 * completes; other executors may retain shared handles without reading mutable state. Operations are
 * owned in-place during run(), while the active TLSChannel is borrowed through a weak pointer.
 */
class NodeConnection
{
  public:
    /**
     * @brief Describe an endpoint without starting I/O.
     * @param node_id Expected Node identity, or empty until primary identification.
     * @param host DNS name or IP address used for connection and TLS verification.
     * @param port Node control TCP port.
     */
    NodeConnection(std::string node_id, std::string host, std::uint16_t port);
    /** @brief Inspect identified connection readiness on control_io.
     * @return True only while running with a verified server.identified identity.
     */
    bool ready() const noexcept;
    /** @brief Read the immutable endpoint identity.
     * @return Borrowed host:port identifier, valid for the connection lifetime.
     */
    const std::string &id() const noexcept;
    /** @brief Read the expected or identified Node on control_io.
     * @return Borrowed Node ID, possibly empty before primary identification.
     */
    const std::string &node_id() const noexcept;
    /** @brief Read the immutable control and data hostname.
     * @return Borrowed hostname valid for the connection lifetime.
     */
    const std::string &host() const noexcept;
    /** @brief Read the immutable control port.
     * @return Configured control TCP port.
     */
    std::uint16_t port() const noexcept;
    /** @brief Copy entrance metadata on control_io.
     * @return Endpoint identity, hostname, and weak control channel.
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
    asio::awaitable<void> run(asio::ssl::context &ssl_context, const AgentConfig &config, NodeConnectionMgr &owner,
                              RelayAgent &agent);
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
    std::optional<std::chrono::steady_clock::time_point> idle_since_;
};

#endif
