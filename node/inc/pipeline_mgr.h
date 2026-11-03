#ifndef RELAYWEAVE_STREAM_PIPELINE_H
#define RELAYWEAVE_STREAM_PIPELINE_H

#include "flow_protocol.h"
#include "node_common.h"
#include "xfr_channel.h"
#include <atomic>
#include <map>
#include <stdexcept>

/**
 * @brief Own one Agent stream listener and its local pairs and remote Flow bridges.
 * @details Pair operations require transfer_tcp_io. RelayNode calls start() during serialized startup;
 * wait_for_setup_tasks() blocks the owner outside this executor. Controllers own and drain copy
 * tasks before close; setup tasks retain the pipeline until timeout or attachment completes.
 * Closing removes lookup entries before waking tasks that still retain a pair.
 */
class StreamPipeline : public std::enable_shared_from_this<StreamPipeline>
{
    using tcp = asio::ip::tcp;
    using ConnectionId = RelayAttach::ConnectionId;

    /** @brief Own one attached Agent stream and its bound multi-node Flow endpoint. */
    struct RemotePair : RemoteRelayPair
    {
        using RemoteRelayPair::RemoteRelayPair;
        /** @copydoc RelayPair::close */
        void close() override
        {
            if (flow)
            {
                flow->close("relay endpoint closed");
            }
            if (stream)
            {
                asio::error_code ignored;
                stream->lowest_layer().close(ignored);
            }
            RemoteRelayPair::close();
        }

        std::shared_ptr<lnk::FlowStream> flow;
        std::optional<tcp::socket> stream;
        ConnectionId connection_id{};

      private:
        /** @brief Bind and retain a FlowStream on the owning transfer executor. */
        asio::awaitable<void> bind_flow(FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                                        asio::any_io_executor transfer) override
        {
            if (flow)
            {
                throw std::runtime_error("relay endpoint cannot bind flow");
            }
            flow = co_await asio::co_spawn(
                channel.executor(),
                [protocol = channel.shared_from_this(), epoch, flow_id, reverse = role == RelayAttach::Producer,
                 transfer]() -> asio::awaitable<std::shared_ptr<lnk::FlowStream>> {
                    co_return protocol->bind_stream_flow(epoch, flow_id, reverse, transfer);
                },
                asio::use_awaitable);
        }
    };

    /** @brief Own both Agent streams, attachment credentials, and single-node service accounting. */
    struct LocalPair : LocalRelayPair
    {
        using LocalRelayPair::LocalRelayPair;
        /** @copydoc LocalRelayPair::close */
        void close() override
        {
            asio::error_code ignored;
            if (producer_stream)
            {
                producer_stream->lowest_layer().close(ignored);
            }
            if (consumer_stream)
            {
                consumer_stream->lowest_layer().close(ignored);
            }
            LocalRelayPair::close();
        }

        std::optional<tcp::socket> producer_stream;
        std::optional<tcp::socket> consumer_stream;
        ConnectionId producer_id{};
        ConnectionId consumer_id{};
    };

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

    /**
     * @brief Bind an authorized control request on transfer_tcp_io; the caller drains this task.
     * @param request Validated role, pair ticket, and one-use pending connection identifier.
     * @return Completes after the greeting is sent and the socket is atomically attached; throws on failure.
     */
    asio::awaitable<void> bind(RelayAttach request);
    /** @brief Open the listener and launch its receive or accept task exactly once during bootstrap. */
    void start();
    /**
     * @brief Reserve a relay ID and return attachment credentials for one multi-node Agent role.
     * @param role Local Agent role: RelayAttach::Producer or RelayAttach::Consumer.
     * @return Agent attachment information with uuid, ticket, data_port, and a zero session_id.
     * @throws std::invalid_argument If role is not Producer or Consumer.
     * @throws std::runtime_error If the manager is stopped or its combined relay capacity is exhausted.
     */
    AttachInfo install_remote_pair(int role);
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
    asio::awaitable<void> prepare_remote_pair(std::uint64_t uuid, FlowProtocol &channel, std::uint64_t epoch,
                                              std::uint64_t flow_id, SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Require an attached, bound remote endpoint and enable forwarding before relay.ready.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    void activate_remote_pair(std::uint64_t uuid);
    /**
     * @brief Await remote forwarding until completion or cancellation; the controller drains this task.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @pre The endpoint has been activated on transfer_tcp_io.
     * @details Normal EOF finishes only its own direction; I/O failures or cancellation drain the opposite
     * direction before propagation.
     */
    asio::awaitable<void> run_remote_pair(std::uint64_t uuid);
    /**
     * @brief Idempotently erase the remote endpoint, close its Flow port, and release the relay ID.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @details Pending attachment waits are woken after lookup removal; tasks retaining the resource
     * can finish safely. Keep the manager alive until those tasks have drained.
     */
    void close_remote_pair(std::uint64_t uuid);
    /**
     * @brief Reserve one relay ID and return separate Producer and Consumer attachment credentials.
     * @return Producer and Consumer attachment descriptors sharing one uuid and using distinct tickets.
     * @throws std::runtime_error If the manager is stopped or its combined relay capacity is exhausted.
     */
    AttachPair install_local_pair();
    /**
     * @brief Await both Agent attachments, bind service accounting, and activate on the transfer executor.
     * @param uuid Node-local relay ID returned by install_local_pair().
     * @param traffic Nonempty local service accounting record.
     * @param accessor Consumer peer label used for accounting.
     * @return Completes after activation; throws if attachment closes or its wait is cancelled.
     * @details Runs on transfer_tcp_io and retains the pair through attachment and preparation.
     */
    asio::awaitable<void> prepare_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Await local transfer lifetime; the controller must drain this task before closing the pair.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    asio::awaitable<void> run_local_pair(std::uint64_t uuid);
    /**
     * @brief Idempotently erase the local pair, wake waiters, and release its relay ID.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @details Pending attachment waits are woken after lookup removal; tasks retaining the resource
     * can finish safely. Keep the manager alive until those tasks have drained.
     */
    void close_local_pair(std::uint64_t uuid);
    /** @brief Reject admission and close the listener and all pairs on the owning transfer executor. */
    void stop();
    /** @brief Block outside transfer_tcp_io until accepted setup tasks finish; workers must remain live. */
    void wait_for_setup_tasks() noexcept;

  private:
    asio::awaitable<void> accept_loop();
    /** @brief Own an unbound socket through greeting, authorization, timeout, and shutdown. */
    struct PendingConnection
    {
        PendingConnection(tcp::socket socket, std::chrono::steady_clock::time_point deadline)
            : socket(std::move(socket)), deadline(deadline), announced(this->socket.get_executor())
        {
        }
        tcp::socket socket;
        std::chrono::steady_clock::time_point deadline;
        AsyncEvent announced;
    };
    asio::awaitable<void> run_transfer_session(ConnectionId id, std::shared_ptr<PendingConnection> pending);
    asio::awaitable<void> read_remote_pair(std::shared_ptr<RemotePair> endpoint);
    asio::awaitable<void> write_remote_pair(std::shared_ptr<RemotePair> endpoint);

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    std::string listen_address_;
    std::uint16_t data_port_;
    std::size_t max_setup_connections_;
    std::size_t capacity_;
    std::chrono::steady_clock::duration setup_timeout_;
    TrafficLimitConfig config_;
    tcp::acceptor acceptor_;
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
