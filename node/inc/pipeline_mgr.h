#ifndef RELAYWEAVE_STREAM_PIPELINE_H
#define RELAYWEAVE_STREAM_PIPELINE_H

#include "app_common.h"
#include "flow_protocol.h"
#include "tls_channel.h"
#include "xfr_channel.h"
#include <atomic>

/** @brief Prepare plain TCP sockets and provide bidirectional service copying on transfer_tcp_io. */
struct TcpTransport
{
    using Stream = asio::ip::tcp::socket;

    /**
     * @brief Accept the common transport constructor's TLS context without using it.
     * @note The unnamed TLS context argument exists only to share the transport construction interface.
     */
    explicit TcpTransport(asio::ssl::context &) noexcept;

    static constexpr std::string_view name = "TCP";

    /**
     * @brief Return the accepted socket unchanged; plain TCP needs no transport handshake.
     * @param socket Accepted socket whose ownership is transferred into transport preparation.
     * @param deadline Absolute setup deadline; plain TCP returns immediately, while TLS must finish its
     * handshake before it.
     * @return An owned stream ready for RelayAttach parsing.
     */
    asio::awaitable<Stream> prepare(Stream socket, std::chrono::steady_clock::time_point deadline) const;
    /**
     * @brief Copy both directions with service limits, accounting, and independent TCP half-closes.
     * @param producer Attached Producer data stream; borrowed until both copy directions finish.
     * @param consumer Attached Consumer data stream; borrowed until both copy directions finish.
     * @param rx_limiter Limiter for Producer-to-Consumer payload bytes.
     * @param tx_limiter Limiter for Consumer-to-Producer payload bytes.
     * @param traffic Service counters updated only after successful payload writes.
     */
    static asio::awaitable<void> relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                      TokenBucket &tx_limiter, ServiceTraffic &traffic);
};

/** @brief Borrow the server TLS context for Agent handshakes and framed stream copying. */
struct TlsTransport
{
    using Stream = TLSStream;

    /**
     * @brief Borrow a TLS context that must outlive the pipeline and all accepted streams.
     * @param context TLS context borrowed for the lifetime of the connection resources.
     */
    explicit TlsTransport(asio::ssl::context &context) noexcept;

    static constexpr std::string_view name = "TLS";

    /**
     * @brief Complete a server TLS handshake within the remaining absolute setup deadline.
     * @param socket Accepted socket whose ownership is transferred into transport preparation.
     * @param deadline Absolute setup deadline; plain TCP returns immediately, while TLS must finish its
     * handshake before it.
     * @return An owned stream ready for RelayAttach parsing.
     */
    asio::awaitable<Stream> prepare(asio::ip::tcp::socket socket,
                                    std::chrono::steady_clock::time_point deadline) const;
    /**
     * @brief Copy framed TLS data with service limits, accounting, and independent FIN directions.
     * @param producer Attached Producer data stream; borrowed until both copy directions finish.
     * @param consumer Attached Consumer data stream; borrowed until both copy directions finish.
     * @param rx_limiter Limiter for Producer-to-Consumer payload bytes.
     * @param tx_limiter Limiter for Consumer-to-Producer payload bytes.
     * @param traffic Service counters updated only after successful payload writes.
     */
    static asio::awaitable<void> relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                      TokenBucket &tx_limiter, ServiceTraffic &traffic);

  private:
    asio::ssl::context &context_;
};

/**
 * @brief Own one Agent stream listener and its local pairs and remote Flow bridges.
 * @tparam Transport TcpTransport or TlsTransport, selecting preparation and copy semantics.
 * @details Pair operations require transfer_tcp_io. RelayNode calls start() during serialized startup;
 * wait_for_pending() blocks the owner outside this executor. Controllers own and drain copy
 * tasks before close; setup tasks retain the pipeline until timeout or attachment completes.
 * Closing removes lookup entries before waking tasks that still retain a pair.
 */
template <typename Transport>
class StreamPipeline : public std::enable_shared_from_this<StreamPipeline<Transport>>
{
    using tcp = asio::ip::tcp;
    using Stream = typename Transport::Stream;

    /** @brief Own one attached Agent stream and its bound multi-node Flow endpoint. */
    struct RemotePair : RelayEndpoint
    {
        using RelayEndpoint::RelayEndpoint;
        std::shared_ptr<lnk::FlowStream> flow;
        std::optional<Stream> stream;
    };

    /** @brief Own both Agent streams, attachment credentials, and single-node service accounting. */
    struct LocalPair
    {
        /**
         * @brief Initialize the shared attachment event on the transfer executor.
         * @param executor Executor that owns the object's timers, I/O resources, and mutable state.
         */
        explicit LocalPair(asio::any_io_executor executor) : attached(executor)
        {
        }
        AsyncEvent attached;
        std::uint64_t producer_ticket = generate_random_id();
        std::uint64_t consumer_ticket = 0;
        std::optional<Stream> producer_stream;
        std::optional<Stream> consumer_stream;
        TokenBucket rx_limiter;
        TokenBucket tx_limiter;
        SRVTrafficPtr traffic;
        std::string accessor;
        bool active = false;
        bool closed = false;
    };

  public:
    /**
     * @brief Configure listener, setup admission, relay capacity, deadline, and traffic limits.
     * @param executor Single-threaded transfer_tcp_io executor owning listener, pair state, and stream I/O.
     * @param ssl_context TLS server context borrowed for Agent TLS handshakes.
     * @param id_allocator Node-wide allocator shared by TCP, TLS, and UDP relay resources.
     * @param listen_address Local address on which Agent stream connections are accepted.
     * @param data_port Local Agent data listener port advertised in attachment descriptors.
     * @param max_setup_connections Maximum accepted stream sockets still performing handshake or attachment.
     * @param capacity Combined limit for local pairs and remote endpoints managed by this listener.
     * @param setup_timeout Maximum duration of transport handshake and RelayAttach parsing for an accepted
     * socket.
     * @param config Per-direction service byte rates and burst sizes.
     */
    StreamPipeline(asio::any_io_executor executor, asio::ssl::context &ssl_context,
                   std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address, std::uint16_t data_port,
                   std::size_t max_setup_connections, std::size_t capacity,
                   std::chrono::steady_clock::duration setup_timeout, TrafficLimitConfig config);

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
     * @brief Await Agent attachment; return false if closure wakes the waiter before attachment.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @return True after the Agent attaches; false if the endpoint is missing or closes before attachment.
     */
    asio::awaitable<bool> wait_remote_pair(std::uint64_t uuid);
    /**
     * @brief Bind service accounting and a committed Flow, crossing to the protocol executor once.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @param channel Protocol service owning the committed Flow; must outlive endpoint binding.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param flow_id Nonzero ID of the committed Flow within the supplied epoch.
     * @param traffic Optional local service accounting record; remote-service ingress may omit it.
     * @param accessor Consumer peer label used to count active service accessors.
     * @pre Called on transfer_tcp_io for an installed, open, unbound endpoint and a committed Flow.
     * @throws std::runtime_error If the endpoint cannot bind or closes during the cross-executor operation.
     */
    asio::awaitable<void> bind_remote_pair(std::uint64_t uuid, FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                       SRVTrafficPtr traffic, std::string accessor);
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
     * @brief Await both Agent roles; return false when closure wakes the waiter.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @return True after both roles attach; false if the pair closes or the attachment event is cancelled.
     */
    asio::awaitable<bool> wait_local_pair(std::uint64_t uuid);
    /**
     * @brief Bind a fully attached local pair to service accounting and configured traffic limits.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @param traffic Shared service accounting record retained while the relay transfers data.
     * @param accessor Consumer peer label used to count active service accessors.
     * @pre Both Agent streams are attached and traffic is nonempty.
     * @throws std::runtime_error If the pair cannot bind to the service.
     */
    void bind_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Enable a bound local pair before notifying both Agent roles that the relay is ready.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     */
    void activate_local_pair(std::uint64_t uuid);
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
    void wait_for_pending() noexcept;

  private:
    asio::awaitable<void> accept_loop();
    asio::awaitable<void> run_transfer_session(tcp::socket socket);
    bool attach_remote_pair(int role, std::uint64_t uuid, std::uint64_t ticket, Stream &stream);
    void attach(int role, std::uint64_t uuid, std::uint64_t ticket, Stream stream);
    asio::awaitable<void> read_remote_pair(std::shared_ptr<RemotePair> endpoint);
    asio::awaitable<void> write_remote_pair(std::shared_ptr<RemotePair> endpoint);

    asio::any_io_executor executor_;
    Transport transport_;
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
    std::atomic_size_t pending_sockets_{0};
    bool started_ = false;
    bool stopped_ = false;
};

using TcpPipeline = StreamPipeline<TcpTransport>;
using TlsPipeline = StreamPipeline<TlsTransport>;

extern template class StreamPipeline<TcpTransport>;
extern template class StreamPipeline<TlsTransport>;

#endif
