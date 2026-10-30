#ifndef RELAYWEAVE_FLOW_PROTOCOL_H
#define RELAYWEAVE_FLOW_PROTOCOL_H
#include "async_event.h"
#include "lnk_channel.h"
#include <asio/experimental/concurrent_channel.hpp>
#include <atomic>

namespace lnk
{
struct FlowSpec
{
    std::uint64_t id = 0;
    std::uint64_t epoch = 0;
    RelayProtocol transport = RelayProtocol::Tcp;
    std::uint64_t previous = 0;
    std::uint64_t next = 0;
    std::string previous_peer;
    std::string next_peer;
    std::chrono::milliseconds prepare_timeout{10000};
};

enum class EventKind
{
    LinkPrepared,
    LinkReady,
    LinkError,
    LinkClosed,
    FlowPrepared,
    FlowCommitted,
    FlowError,
    FlowClosed
};

struct ProtocolEvent
{
    EventKind kind;
    std::uint64_t epoch;
    std::uint64_t id;
    std::string stage;
    std::string reason;
};

// One sender and one receiver on the transfer executor. DATA credit is returned only
// by complete_receive(), after the caller has written the delivered data downstream.
class FlowStream : public std::enable_shared_from_this<FlowStream>
{
  public:
    static constexpr std::size_t maximum_chunk_size = LnkFrameHeader::maximum_payload;
    /** @brief Retain the shared, thread-safe allocator used by payloads across executors. */
    const std::shared_ptr<std::pmr::memory_resource> pool;
    asio::awaitable<void> send(PooledBuffer data);
    asio::awaitable<void> finish_send();
    asio::awaitable<std::optional<PooledBuffer>> receive();
    void complete_receive();
    void reset(std::string reason);
    void close(std::string reason = "flow closed");
    asio::any_io_executor executor()
    {
        return send_queue_.get_executor();
    }

  private:
    friend class ::FlowProtocol;
    // An engaged value is DATA (including empty DATA); nullopt is an ordered FIN.
    using Queue = asio::experimental::concurrent_channel<void(asio::error_code, std::optional<PooledBuffer>)>;
    FlowStream(asio::any_io_executor transfer, asio::any_io_executor data, std::weak_ptr<FlowProtocol> protocol,
               std::uint64_t id, bool reverse, std::shared_ptr<std::pmr::synchronized_pool_resource> payload_pool);
    const std::uint64_t id_;
    const bool reverse_;
    Queue send_queue_;
    Queue receive_queue_;
    asio::experimental::concurrent_channel<void(asio::error_code)> receive_completed_;
    // Only the data executor uses this local sender's capacity notification.
    asio::experimental::channel<void(asio::error_code)> send_space_;
    // Transfer-executor state. Protocol tasks consult FlowState::phase instead.
    bool closed_ = false;
    bool send_finished_ = false;
    std::string reason_;
    std::weak_ptr<FlowProtocol> protocol_;
};

// Datagram endpoints never wait for queue capacity and never acknowledge delivery.
class FlowDatagram : public std::enable_shared_from_this<FlowDatagram>
{
  public:
    bool try_send(std::span<const std::uint8_t> data);
    asio::awaitable<PooledBuffer> receive();
    void close(std::string reason = "flow closed");
    asio::any_io_executor executor()
    {
        return send_queue_.get_executor();
    }

  private:
    friend class ::FlowProtocol;
    friend struct FlowState;
    using Queue = asio::experimental::concurrent_channel<void(asio::error_code, PooledBuffer)>;
    FlowDatagram(asio::any_io_executor transfer, std::weak_ptr<FlowProtocol> protocol, std::uint64_t id, bool reverse,
                 std::shared_ptr<Queue> received, std::shared_ptr<std::pmr::synchronized_pool_resource> pool);
    const std::uint64_t id_;
    const bool reverse_;
    Queue send_queue_;
    // Flow also owns this bounded queue, allowing datagrams to arrive before binding.
    const std::shared_ptr<Queue> receive_queue_;
    bool closed_ = false;
    std::string reason_;
    std::weak_ptr<FlowProtocol> protocol_;
    const std::shared_ptr<std::pmr::synchronized_pool_resource> pool_;
};

// Shared by Flow routing and outstanding Link writes. Endpoint APIs expose none of this state.
struct FlowState
{
  private:
    struct Stream
    {
      private:
        struct Direction
        {
            std::deque<Frame> pending_frames;
            // Local allocation charge; the cumulative counters below use wire-credit units.
            std::size_t buffered_bytes = 0;
            std::uint64_t sent_bytes = 0;
            std::uint64_t send_limit = 0;
            std::uint64_t received_bytes = 0;
            std::uint64_t released_bytes = 0;
            std::uint64_t receive_limit = 0;
            bool finished = false;
        };

      public:
        Stream(asio::any_io_executor executor, bool endpoint);
        asio::experimental::channel<void(asio::error_code, Frame)> receive_queue;
        std::array<Direction, 2> directions;
        std::shared_ptr<FlowStream> port;
    };

    struct Datagram
    {
        Datagram(asio::any_io_executor executor, bool endpoint);
        std::shared_ptr<FlowDatagram::Queue> receive_queue;
        std::shared_ptr<FlowDatagram> port;
    };

  public:
    enum class Phase
    {
        Prepared,
        Active,
        Closed
    };

    FlowState(asio::any_io_executor executor, FlowSpec spec);

    const std::uint64_t id;
    const std::uint64_t epoch;
    const RelayProtocol transport;
    const std::uint64_t previous;
    const std::uint64_t next;

    Clock::time_point prepare_deadline;
    Phase phase = Phase::Prepared;
    std::string reason;

    // transport selects the state; protocol-specific paths do not probe its presence.
    // Stream state exists for TCP. UDP endpoints have a receive queue before binding.
    std::optional<Stream> stream;
    Datagram datagram;
};
} // namespace lnk

// Node-wide logical Flow protocol. A Flow routes between two adjacent NodeLinks,
// or between one NodeLink and its local endpoint. State belongs to the data executor.
class FlowProtocol : public std::enable_shared_from_this<FlowProtocol>
{
  public:
    FlowProtocol(asio::any_io_executor executor, asio::any_io_executor event_executor, std::string node_id,
                 std::string tcp_address, std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port);
    LnkChannel &links()
    {
        return links_;
    }
    asio::any_io_executor executor() const
    {
        return executor_;
    }
    void activate();
    // A normal return always means all data tasks have drained.
    asio::awaitable<bool> stop();
    asio::awaitable<lnk::ProtocolEvent> receive_event();
    std::uint64_t take_forwarded_bytes() noexcept
    {
        return forwarded_bytes_.exchange(0, std::memory_order_relaxed);
    }

    void prepare_flow(lnk::FlowSpec spec);
    void commit_flow(std::uint64_t id);
    void close_flow(std::uint64_t id, std::string reason = {});
    void invalidate_flows(std::string reason);
    // Data executor: attach the endpoint and start its owned I/O tasks synchronously.
    std::shared_ptr<lnk::FlowStream> bind_stream_flow(std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                      asio::any_io_executor transfer);
    std::shared_ptr<lnk::FlowDatagram> bind_datagram_flow(std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                          asio::any_io_executor transfer);

  private:
    friend class LnkChannel;
    friend class lnk::FlowStream;
    friend class lnk::FlowDatagram;
    using Clock = lnk::Clock;

    // Protocol-specific logic is contained by the service and borrows it for shared resources.
    class Stream
    {
      public:
        explicit Stream(FlowProtocol &protocol) : protocol_(protocol)
        {
        }
        std::shared_ptr<lnk::FlowStream> bind(const std::shared_ptr<lnk::FlowState> &flow, bool reverse,
                                              asio::any_io_executor transfer);
        void close(lnk::FlowState &flow);
        void receive(const std::shared_ptr<lnk::FlowState> &flow, lnk::Frame frame);
        bool try_send(const std::shared_ptr<lnk::FlowState> &flow, bool reverse,
                      std::optional<lnk::PooledBuffer> &data);
        void reset(const std::shared_ptr<lnk::FlowState> &flow, bool reverse, std::string reason);
        void receive_credit(const lnk::NodeLink &link, const LnkFrameHeader &header);
        void publish_credit(const std::shared_ptr<lnk::FlowState> &flow);
        void publish_credit(const std::shared_ptr<lnk::FlowState> &flow, bool reverse);
        void release_buffer(const std::shared_ptr<lnk::FlowState> &flow, bool reverse, std::size_t memory_bytes,
                            std::size_t credit_bytes);
        void close_port(const std::shared_ptr<lnk::FlowStream> &port, std::string reason);
        void reset_port(const std::shared_ptr<lnk::FlowStream> &port, std::string reason);

      private:
        bool route(const std::shared_ptr<lnk::FlowState> &flow, lnk::Frame &frame);
        void flush(const std::shared_ptr<lnk::FlowState> &flow, bool reverse);
        asio::awaitable<void> run(std::shared_ptr<lnk::FlowState> flow);
        // run owns the identity until both loops complete; each loop only borrows it and its port.
        asio::awaitable<void> send_loop(const std::shared_ptr<lnk::FlowState> &flow, lnk::FlowStream &port);
        asio::awaitable<void> receive_loop(const std::shared_ptr<lnk::FlowState> &flow, lnk::FlowStream &port);
        FlowProtocol &protocol_;
    };

    class Datagram
    {
      public:
        explicit Datagram(FlowProtocol &protocol) : protocol_(protocol)
        {
        }
        std::shared_ptr<lnk::FlowDatagram> bind(const std::shared_ptr<lnk::FlowState> &flow, bool reverse,
                                                asio::any_io_executor transfer);
        void close(lnk::FlowState &flow);
        void receive(const std::shared_ptr<lnk::FlowState> &flow, lnk::Frame frame);
        bool try_send(const std::shared_ptr<lnk::FlowState> &flow, bool reverse, lnk::PooledBuffer &data);
        void close_port(const std::shared_ptr<lnk::FlowDatagram> &port, std::string reason);

      private:
        bool route(const std::shared_ptr<lnk::FlowState> &flow, lnk::Frame &frame);
        asio::awaitable<void> run(std::shared_ptr<lnk::FlowState> flow);
        FlowProtocol &protocol_;
    };

    // Same-executor transport boundary: no registered callbacks or extra event queue.
    void receive(const lnk::NodeLink &source, lnk::Frame frame);
    void written(lnk::Frame frame);
    void link_flows_closed(std::uint64_t id);
    void notify_flow(const std::shared_ptr<lnk::FlowState> &flow, lnk::EventKind kind, std::string stage,
                     std::string reason = {});
    void fail_flow(std::shared_ptr<lnk::FlowState> flow, std::string stage, std::string reason);

    std::shared_ptr<lnk::FlowState> require_endpoint_flow(std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                          RelayProtocol transport);
    void emit(lnk::ProtocolEvent event);
    void spawn(asio::awaitable<void> task);
    asio::awaitable<void> run();
    void request_stop(std::string reason);
    void schedule_monitor(Clock::time_point deadline);
    Clock::time_point poll(Clock::time_point now);
    asio::awaitable<void> monitor();
    asio::any_io_executor executor_;
    // Business deliveries may retain their storage after the protocol has stopped.
    std::shared_ptr<std::pmr::synchronized_pool_resource> payload_pool_;
    LnkChannel links_;
    Stream stream_;
    Datagram datagram_;
    std::map<std::uint64_t, std::shared_ptr<lnk::FlowState>> flows_;
    std::map<std::uint64_t, Clock::time_point> retired_;
    std::atomic<std::uint64_t> forwarded_bytes_{0};

    // Control notifications cross executors.
    asio::experimental::concurrent_channel<void(asio::error_code, lnk::ProtocolEvent)> events_;
    std::atomic<bool> event_overflow_ = false;

    // Lifecycle state belongs to cluster_data_io.
    asio::steady_timer monitor_timer_;
    // All Flow directions share one batch deadline; idle nodes have none.
    Clock::time_point credit_deadline_ = Clock::time_point::max();
    AsyncEvent tasks_done_;
    bool stop_ok_ = true;
    std::size_t tasks_ = 0;
    bool activated_ = false;
    bool stopping_ = false;
};
#endif
