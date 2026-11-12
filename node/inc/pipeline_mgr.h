#ifndef RELAYWEAVE_STREAM_PIPELINE_H
#define RELAYWEAVE_STREAM_PIPELINE_H

#include "node_common.h"
#include "tls_channel.h"
#include "xfr_channel.h"
#include <atomic>
#include <map>
#include <stdexcept>

/**
 * @brief Own one Agent stream listener and its local pairs and remote Flow bridges.
 * @details Pair operations require transfer_tcp_io. RelayNode calls start() during serialized startup;
 * wait_for_setup_tasks() blocks the owner outside this executor. SessionTransfer owns and drains copy
 * tasks before close; setup tasks retain the pipeline until timeout or attachment completes.
 * Closing removes lookup entries before waking tasks that still retain a pair.
 */
class StreamPipeline : public RelayTransport, public std::enable_shared_from_this<StreamPipeline>
{
    using tcp = asio::ip::tcp;
    using ConnectionId = RelayAttach::ConnectionId;

    struct LocalPair;
    struct RemotePair;

  public:
    /**
     * @brief Configure listener, setup admission, relay capacity, deadline, and traffic limits.
     * @param executor Single-threaded transfer_tcp_io executor owning listener, pair state, and stream I/O.
     * @param id_allocator Node-wide allocator shared by TCP, TLS, and UDP relay resources.
     * @param listen_address Local address on which Agent stream connections are accepted.
     * @param data_port Local Agent data listener port advertised in attachment descriptors.
     * @param max_setup_connections Maximum accepted TCP sockets awaiting control authorization.
     * @param capacity Combined limit for local pairs and remote endpoints managed by this listener.
     * @param setup_timeout Maximum duration of greeting and control authorization for an accepted socket.
     * @param config Per-direction service byte rates and burst sizes.
     */
    StreamPipeline(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                   std::string listen_address, std::uint16_t data_port, std::size_t max_setup_connections,
                   std::size_t capacity, std::chrono::steady_clock::duration setup_timeout, TrafficLimitConfig config);

    asio::any_io_executor executor() const override
    {
        return executor_;
    }
    /** @brief Open the listener and launch its receive or accept task exactly once during bootstrap. */
    void start();
    /**
     * @brief Reserve a relay ID and return attachment credentials for one multi-node Agent role.
     * @param role Local Agent role: RelayAttach::Producer or RelayAttach::Consumer.
     * @return Agent attachment information with uuid, ticket, data_port, and a zero session_id.
     * @throws std::invalid_argument If role is not Producer or Consumer.
     * @throws std::runtime_error If the manager is stopped or its combined relay capacity is exhausted.
     */
    AttachInfo install_remote_pair(int role) override;
    /**
     * @brief Await Agent attachment and bind its Flow in one transfer-executor operation.
     * @param uuid Node-local relay ID returned by install_remote_pair().
     * @param channel Protocol service owning the committed Flow, borrowed through preparation.
     * @param epoch Cluster authorization epoch of the Flow.
     * @param flow_id Nonzero committed Flow ID.
     * @param traffic Optional local service accounting record.
     * @param accessor Consumer peer label used for accounting.
     * @return Completes after attachment and Flow binding; failures and cancellation propagate as exceptions.
     * @details Runs on transfer_tcp_io and retains the endpoint through attachment and Flow binding.
     */
    asio::awaitable<void> prepare_remote_pair(std::uint64_t uuid, Authorizations &authorizations, FlowProtocol &channel,
                                              std::uint64_t epoch, std::uint64_t flow_id, SRVTrafficPtr traffic,
                                              std::string accessor) override;
    /**
     * @brief Require an attached, bound remote endpoint and enable forwarding before relay.ready.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    void activate_remote_pair(std::uint64_t uuid) override;
    /**
     * @brief Await remote forwarding until completion or cancellation; the controller drains this task.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @pre The endpoint has been activated on transfer_tcp_io.
     * @details Normal EOF finishes only its own direction; I/O failures or cancellation drain the opposite
     * direction before propagation.
     */
    asio::awaitable<void> run_remote_pair(std::uint64_t uuid) override;
    /**
     * @brief Idempotently erase the remote endpoint, close its Flow port, and release the relay ID.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @details Pending attachment waits are woken after lookup removal; tasks retaining the resource
     * can finish safely. Keep the manager alive until those tasks have drained.
     */
    void close_remote_pair(std::uint64_t uuid) override;
    /**
     * @brief Reserve one relay ID and return separate Producer and Consumer attachment credentials.
     * @return Producer and Consumer attachment descriptors sharing one uuid and using distinct tickets.
     * @throws std::runtime_error If the manager is stopped or its combined relay capacity is exhausted.
     */
    AttachPair install_local_pair() override;
    /**
     * @brief Await both Agent attachments and prepare service accounting on the transfer executor.
     * @param uuid Node-local relay ID returned by install_local_pair().
     * @param traffic Nonempty local service accounting record.
     * @param accessor Consumer peer label used for accounting.
     * @return Completes after preparation; throws if attachment closes or its wait is cancelled.
     * @details Runs on transfer_tcp_io and retains the pair through attachment and preparation.
     */
    asio::awaitable<void> prepare_local_pair(std::uint64_t uuid, Authorizations &authorizations, SRVTrafficPtr traffic,
                                             std::string accessor) override;
    /** @brief Activate the prepared local pair before relay.ready. */
    void activate_local_pair(std::uint64_t uuid) override;
    /**
     * @brief Await local transfer lifetime; the controller must drain this task before closing the pair.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    asio::awaitable<void> run_local_pair(std::uint64_t uuid) override;
    /**
     * @brief Idempotently erase the local pair, wake waiters, and release its relay ID.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @details Pending attachment waits are woken after lookup removal; tasks retaining the resource
     * can finish safely. Keep the manager alive until those tasks have drained.
     */
    void close_local_pair(std::uint64_t uuid) override;
    /** @brief Reject admission and close the listener and all pairs on the owning transfer executor. */
    void stop();
    /** @brief Block outside transfer_tcp_io until accepted setup tasks finish; workers must remain live. */
    void wait_for_setup_tasks() noexcept;

  private:
    /** @brief Consume one validated authorization per Agent role inside the preparing task. */
    asio::awaitable<void> attach_streams(Authorizations &authorizations, int count);
    /** @brief Await greeting completion and claim the socket; authorizations are already deduplicated. */
    asio::awaitable<void> bind(RelayAttach request);
    asio::awaitable<void> accept_loop();
    struct PendingConnection;
    asio::awaitable<void> run_transfer_session(ConnectionId id, std::shared_ptr<PendingConnection> pending,
                                               asio::ip::address_v4 source);
    // run_remote_pair retains the endpoint and drains both directions before releasing it.
    asio::awaitable<void> read_remote_pair(RemotePair &endpoint);
    asio::awaitable<void> write_remote_pair(RemotePair &endpoint);

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    std::string listen_address_;
    std::uint16_t data_port_;
    std::size_t max_setup_connections_;
    std::size_t capacity_;
    std::chrono::steady_clock::duration setup_timeout_;
    TrafficLimitConfig config_;
    tcp::acceptor acceptor_;
    PeerBlacklist blacklist_{"agent-data"};
    std::unordered_map<std::uint64_t, std::shared_ptr<LocalPair>> local_pairs_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RemotePair>> remote_pairs_;
    /** @brief Unclaimed sockets on the data executor; its size controls setup admission. */
    std::map<ConnectionId, std::shared_ptr<PendingConnection>> unbound_;
    /** @brief Live setup tasks, including claimed sockets awaiting cleanup; used for cross-domain drainage. */
    std::atomic_size_t setup_tasks_{0};
    bool started_ = false;
    bool stopped_ = false;
};

#endif
