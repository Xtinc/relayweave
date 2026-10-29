#ifndef PROXY_DATAGRAM_MANAGER_HEADER
#define PROXY_DATAGRAM_MANAGER_HEADER

#include "flow_protocol.h"
#include "node_common.h"
#include "xfr_channel.h"

/**
 * @brief Own the shared Agent UDP listener, source bindings, and relay resources.
 * @details Pair operations require transfer_udp_io; RelayNode calls start() during serialized startup.
 * Local pairs forward through the shared receive loop; remote pairs bridge a FlowDatagram.
 * Controllers drain pair tasks before close. Closing erases bindings before waking waiters;
 * invalid, oversized, rate-limited, or congested datagrams are dropped as whole packets.
 */
class DatagramMgr : public std::enable_shared_from_this<DatagramMgr>
{
    using udp = asio::ip::udp;

    enum class Side
    {
        Producer,
        Consumer,
    };

    /** @brief Own both Agent UDP identities, authenticated sources, limits, and lifetime events. */
    struct LocalPair : LocalRelayPair
    {
        /**
         * @brief Initialize attachment and lifetime events on the UDP transfer executor.
         * @param executor Executor that owns the object's timers, I/O resources, and mutable state.
         */
        explicit LocalPair(asio::any_io_executor executor) : LocalRelayPair(executor), finished(executor)
        {
        }
        /** @copydoc LocalRelayPair::close */
        void close() override;
        /**
         * @brief Borrow the authenticated source endpoint for a selected Agent role.
         * @param side Producer or Consumer role whose source binding or directional limit is selected.
         * @return Mutable reference to the selected role's optional authenticated UDP source endpoint.
         */
        std::optional<udp::endpoint> &endpoint(Side side);
        /**
         * @brief Borrow the encoded UDP session header for a selected destination role.
         * @param side Producer or Consumer role whose source binding or directional limit is selected.
         * @return Reference to the selected destination role's pre-encoded UDP session header.
         */
        const DatagramHeader::Buffer &session_header(Side side) const;
        /**
         * @brief Consume available service tokens for a source role without waiting.
         * @param side Producer or Consumer role whose source binding or directional limit is selected.
         * @param payload_size User payload length in bytes, excluding the session header.
         * @return True if enough directional tokens were consumed; false if this datagram should be dropped.
         */
        bool allow(Side side, std::size_t payload_size);

        AsyncEvent finished;
        std::uint64_t producer_session_id = 0;
        std::uint64_t consumer_session_id = 0;
        DatagramHeader::Buffer producer_session_header;
        DatagramHeader::Buffer consumer_session_header;
        std::optional<udp::endpoint> producer_endpoint;
        std::optional<udp::endpoint> consumer_endpoint;
    };

    /** @brief Own a bound Flow datagram endpoint and one authenticated Agent UDP source. */
    struct RemotePair : RemoteRelayPair
    {
        using RemoteRelayPair::RemoteRelayPair;
        /** @copydoc RelayPair::close */
        void close() override;

        std::shared_ptr<lnk::FlowDatagram> flow;
        std::uint64_t session_id = 0;
        std::optional<udp::endpoint> source;

      private:
        /** @brief Bind and retain a FlowDatagram on the owning transfer executor. */
        asio::awaitable<void> bind_flow(FlowProtocol &channel, std::uint64_t epoch, std::uint64_t flow_id,
                                        asio::any_io_executor transfer) override;
    };

    /** @brief Map a UDP session ID to a relay resource and its Agent role. */
    struct Binding
    {
        std::uint64_t relay_uuid;
        Side side;
    };

    /** @brief Describe a validated local destination and accounting for the current receive buffer. */
    struct RoutedDatagram
    {
        udp::endpoint destination;
        SRVTrafficPtr traffic;
        Side source_side;
    };

  public:
    /**
     * @brief Configure the listener, shared relay ID allocator, capacity, and service traffic limits.
     * @param executor Single-threaded transfer_udp_io executor owning socket, bindings, and pair state.
     * @param id_allocator Node-wide allocator shared by TCP, TLS, and UDP relay resources.
     * @param listen_endpoint Local endpoint of the shared Agent UDP socket.
     * @param capacity Combined limit for local pairs and remote endpoints managed by this listener.
     * @param config Per-direction service byte rates and burst sizes; excess datagrams are dropped.
     */
    DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                udp::endpoint listen_endpoint, std::size_t capacity, TrafficLimitConfig config);

    /** @brief Open the listener and launch its receive or accept task exactly once during bootstrap. */
    void start();
    /**
     * @brief Reserve a relay ID and return attachment credentials for one multi-node Agent role.
     * @param role Local Agent role: RelayAttach::Producer or RelayAttach::Consumer.
     * @return Agent attachment information with uuid, ticket, data_port, and a nonzero UDP session_id.
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
     * @details Runs on transfer_udp_io and retains the endpoint through attachment and Flow binding.
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
     * @pre The endpoint has been activated on transfer_udp_io.
     * @details The shared listener forwards Agent input; this task awaits the Flow-to-Agent return path.
     * Cancellation and I/O failures propagate.
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
     * @details Runs on transfer_udp_io and retains the pair through attachment and preparation.
     */
    asio::awaitable<void> prepare_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor);
    /**
     * @brief Await local transfer lifetime; the controller must drain this task before closing the pair.
     * @param uuid Node-local nonzero relay ID returned by the corresponding install operation.
     * @pre The pair has been activated on transfer_udp_io.
     * @details The shared listener performs packet I/O; this task waits for the pair's finished event.
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

  private:
    asio::awaitable<void> receive_datagram();
    std::optional<RoutedDatagram> route_datagram(std::span<std::uint8_t> datagram, const udp::endpoint &source);
    std::uint64_t allocate_session_id() const;

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    udp::endpoint listen_endpoint_;
    udp::socket socket_;
    std::size_t capacity_;
    TrafficLimitConfig config_;
    std::unordered_map<std::uint64_t, Binding> bindings_;
    std::unordered_map<std::uint64_t, std::shared_ptr<LocalPair>> local_pairs_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RemotePair>> remote_pairs_;
    bool started_ = false;
    bool stopped_ = false;
};

#endif
