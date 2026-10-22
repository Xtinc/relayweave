#ifndef RELAYWEAVE_NODE_CONNECTION_MGR_H
#define RELAYWEAVE_NODE_CONNECTION_MGR_H

#include "node_connection.h"
#include "async_event.h"
#include "dualindex_map.h"

struct AgentConfig;
class RelayAgent;

/**
 * @brief Own indexed control connections and their background tasks on control_io.
 * @details Borrows its owning RelayAgent, configuration, and TLS context until close() completes.
 * The pool retains stopping connections until run() returns, then notifies the Agent before erasure.
 * Background completions retain RelayAgent without increasing connection use_count().
 */
class NodeConnectionMgr
{
  public:
    /**
     * @brief Construct an empty pool.
     * @param agent Owning composition root, borrowed through all task completions and notifications.
     * @param executor Single-threaded control executor.
     * @param ssl_context Borrowed TLS context, valid through close().
     * @param config Borrowed immutable Agent configuration, valid through close().
     */
    NodeConnectionMgr(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                      const AgentConfig &config);
    /**
     * @brief Start the primary connection once on control_io, or before its worker runs.
     */
    void start();
    /**
     * @brief Reuse a Node or endpoint connection, or start one new connection task on control_io.
     * @param node_id Expected Node identity, empty for the initial primary endpoint.
     * @param host Node control hostname or IP address.
     * @param port Node control TCP port.
     * @return Shared handle, possibly connecting or stopping; readiness requires separate checking.
     * @throws std::exception On identity conflict, shutdown, or task submission failure.
     */
    std::shared_ptr<NodeConnection> ensure_connection(std::string node_id, std::string host, std::uint16_t port);
    /**
     * @brief Find a known Node and reset its idle interval on control_io.
     * @param node Expected Node identity.
     * @return Shared handle including stopping connections, or empty if absent.
     */
    std::shared_ptr<NodeConnection> find_node(const std::string &node);
    /** @brief Inspect primary on control_io. @return Borrowed connection, or null before startup/after drain. */
    const NodeConnection *primary() const;
    /**
     * @brief Compare an entrance with the configured primary endpoint.
     * @param connection Connection whose endpoint is compared.
     * @return True if this is the primary connection.
     */
    bool is_primary(const NodeConnection &connection) const;
    /**
     * @brief Stop secondary connections held only by the pool for sixty continuous seconds.
     * @param now Current steady-clock time supplied by control maintenance.
     */
    void collect_idle_connections(std::chrono::steady_clock::time_point now);
    /** @brief Refuse new tasks and cancel all pooled connections on control_io. */
    void stop();
    /**
     * @brief Stop and await all connection completions on control_io; supports concurrent callers.
     * @return Completes after the pool and both indices are empty.
     */
    asio::awaitable<void> close();

  private:
    friend class NodeConnection;
    /** @brief Commit the identified Node index before notifying control. */
    void connection_ready(NodeConnection &connection);
    /** @brief Notify pending selections, erase the drained connection, and signal pool completion. */
    void connection_finished(NodeConnection &connection, std::exception_ptr failure);
    static constexpr auto CONNECTION_IDLE_TIMEOUT = std::chrono::seconds(60);
    RelayAgent &agent_;
    asio::any_io_executor executor_;
    asio::ssl::context &ssl_context_;
    const AgentConfig &config_;
    std::string primary_connection_id_;
    DualIndexMap<std::string, std::string, std::shared_ptr<NodeConnection>, SecondaryKeyMode::Unique> connections_;
    AsyncEvent stopped_event_;
    bool stopping_ = false;
};

#endif
