#ifndef RELAYWEAVE_LNK_CHANNEL_H
#define RELAYWEAVE_LNK_CHANNEL_H
#include "async_event.h"
#include "tls_channel.h"
#include <asio/experimental/concurrent_channel.hpp>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <utility>

// Exclusive, movable byte storage. The resource must outlive the buffer; access and
// destruction must obey the resource's threading requirements. Bytes are not initialised.
class PooledBuffer
{
  private:
    struct Deleter
    {
        std::pmr::memory_resource *resource = nullptr;
        std::size_t size = 0;

        void operator()(std::uint8_t *storage) const noexcept
        {
            resource->deallocate(storage, size);
        }
    };

  public:
    PooledBuffer() noexcept = default;

    PooledBuffer(std::pmr::memory_resource &resource, std::size_t size)
        : storage_(size ? static_cast<std::uint8_t *>(resource.allocate(size)) : nullptr, Deleter{&resource, size}),
          view_(storage_.get(), size)
    {
    }

    PooledBuffer(std::pmr::memory_resource &resource, std::span<const std::uint8_t> bytes)
        : PooledBuffer(resource, bytes.size())
    {
        if (!bytes.empty())
        {
            std::memcpy(data(), bytes.data(), bytes.size());
        }
    }

    PooledBuffer(PooledBuffer &&other) noexcept
        : storage_(std::move(other.storage_)), view_(std::exchange(other.view_, {}))
    {
    }

    PooledBuffer &operator=(PooledBuffer &&other) noexcept
    {
        if (this != &other)
        {
            storage_ = std::move(other.storage_);
            view_ = std::exchange(other.view_, {});
        }
        return *this;
    }

    std::uint8_t *data() noexcept
    {
        return view_.data();
    }

    const std::uint8_t *data() const noexcept
    {
        return view_.data();
    }

    std::size_t size() const noexcept
    {
        return view_.size();
    }

    std::size_t allocation_size() const noexcept
    {
        return storage_ ? storage_.get_deleter().size : 0;
    }

    std::span<std::uint8_t> bytes() noexcept
    {
        return view_;
    }

    std::span<const std::uint8_t> bytes() const noexcept
    {
        return view_;
    }

    void slice(std::size_t offset, std::size_t size) noexcept
    {
        assert(offset <= view_.size() && size <= view_.size() - offset);
        view_ = view_.subspan(offset, size);
    }

  private:
    std::unique_ptr<std::uint8_t, Deleter> storage_{nullptr, Deleter{}};
    std::span<std::uint8_t> view_;
};

enum class FlowSendStatus
{
    Queued,
    CapacityExceeded, // Closes the flow; this is not retryable backpressure.
    Closed,
    Invalid
};

namespace lnk
{
using Clock = std::chrono::steady_clock;

struct Frame
{
    LnkFrameHeader header;
    PooledBuffer payload;
};

using FrameQueue = asio::experimental::channel<void(asio::error_code, Frame)>;

struct NodeLink
{
    enum class State
    {
        Preparing,
        Resolving,
        Prepared,
        WaitingForPeer,
        Connecting,
        Attaching,
        Ready,
        Closed
    };

    NodeLink(asio::any_io_executor executor, njson params);

    const njson params; // Immutable control notification template; hot fields are typed below.
    const std::uint64_t id;
    const std::uint64_t epoch;
    const RelayProtocol transport;

    std::optional<asio::ip::tcp::socket> socket;
    asio::ip::tcp::resolver resolver;
    std::optional<FrameQueue> writes;
    asio::ip::udp::endpoint endpoint;
    std::size_t pending_data = 0;

    Clock::time_point deadline;
    State state = State::Preparing;
    // UDP Attach and its acknowledgement can arrive in either order.
    bool attached = false;
    bool acknowledged = false;

    Clock::time_point last_response = Clock::now();
    Clock::time_point last_ping = Clock::now();
    std::uint64_t ping = 0;
    std::uint64_t last_ack_ping = 0;
};

struct NodeFlow
{
    enum class State
    {
        Prepared,
        Active,
        Closed
    };

    NodeFlow(asio::any_io_executor executor, njson params);

    const njson params; // Immutable control notification template; hot fields are typed below.
    const std::uint64_t epoch;
    const RelayProtocol transport;
    std::uint64_t previous = 0;
    std::uint64_t next = 0;

    Clock::time_point prepare_deadline;
    State state = State::Prepared;
    std::array<bool, 2> fin{};
    std::string reason;

    FrameQueue received;
    std::size_t bytes = 0;
};

struct Datagram
{
    Frame frame;
    std::uint64_t link_id;
};
} // namespace lnk

class LnkChannel : public std::enable_shared_from_this<LnkChannel>
{
  public:
    LnkChannel(asio::any_io_executor executor, asio::any_io_executor event_executor, std::string node_id,
               std::string tcp_address, std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port);
    void start();
    void activate();
    void rollback() noexcept;
    asio::awaitable<void> stop();
    // Thread-safe control notification receive; resumes on the caller's executor.
    asio::awaitable<CtrlMessage> receive_event();
    asio::any_io_executor executor() const
    {
        return executor_;
    }

    // Physical neighbour connections; prepare parameters are validated by NodeLinkMgr.
    void prepare(njson params);
    void connect(std::uint64_t id);
    void close(std::uint64_t id);

    // Logical paths (control-validated prepare parameters) and endpoint business frames.
    void prepare_flow(njson params);
    void commit_flow(std::uint64_t id);
    void close_flow(std::uint64_t id, std::string reason = {});
    void invalidate_flows(std::string reason);
    FlowSendStatus send_flow(FlowFrame frame);
    // Cross-executor submission; the caller keeps the channel alive until completion.
    asio::awaitable<FlowSendStatus> async_send_flow(FlowFrame frame);
    // DATA submission borrows payload until completion; only the data executor allocates pool storage.
    asio::awaitable<FlowSendStatus> async_send_flow_data(std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                       std::span<const std::uint8_t> payload);
    // Cross-executor receive; pool storage stays on the data executor, completion returns to the caller.
    // Cancellation wakes all pending receivers of this Flow; its owner still controls Flow closure.
    asio::awaitable<FlowFrame> receive_flow(std::uint64_t epoch, std::uint64_t id);

  private:
    using tcp = asio::ip::tcp;
    using udp = asio::ip::udp;
    using Clock = std::chrono::steady_clock;

    void link_flows_closed(std::uint64_t id);
    FlowSendStatus send_flow(const FlowFrame &frame, std::span<const std::uint8_t> payload);
    bool deliver(const std::shared_ptr<lnk::NodeFlow> &flow, lnk::Frame &frame);
    void notify_flow(const std::shared_ptr<lnk::NodeFlow> &flow, CtrlCommand command, std::string stage,
                     std::string reason = {});
    void fail_flow(const std::shared_ptr<lnk::NodeFlow> &flow, std::string stage, std::string reason);
    void expire_preparations(Clock::time_point now);
    void incoming_flow(const lnk::NodeLink &link, lnk::Frame frame);

    void notify(const std::shared_ptr<lnk::NodeLink> &link, CtrlCommand command, std::string reason = {},
                std::string_view stage = {});
    void fail(const std::shared_ptr<lnk::NodeLink> &link, std::string reason, std::string_view stage = {});
    void mark_ready(const std::shared_ptr<lnk::NodeLink> &link);
    bool matches(const lnk::NodeLink &link, const CtrlMessage &message) const;
    CtrlMessage attach_message(const lnk::NodeLink &link, CtrlCommand command) const;
    bool enqueue(const std::shared_ptr<lnk::NodeLink> &link, const LnkFrameHeader &header, PooledBuffer &&payload = {});
    void process(const std::shared_ptr<lnk::NodeLink> &link, const LnkFrameHeader &header, PooledBuffer payload);
    asio::awaitable<void> accept_loop();
    asio::awaitable<void> accept(std::shared_ptr<tcp::socket> socket);
    asio::awaitable<void> prepare_udp(std::shared_ptr<lnk::NodeLink> link);
    asio::awaitable<void> establish(std::shared_ptr<lnk::NodeLink> link);
    asio::awaitable<void> tcp_read(std::shared_ptr<lnk::NodeLink> link);
    asio::awaitable<void> tcp_write(std::shared_ptr<lnk::NodeLink> link);
    asio::awaitable<void> udp_read();
    asio::awaitable<void> udp_write();

    void emit(CtrlMessage message);
    void spawn(asio::awaitable<void> task);
    void schedule_monitor(Clock::time_point deadline);
    asio::awaitable<void> monitor();

    asio::any_io_executor executor_;
    std::string node_id_;
    tcp::acceptor acceptor_;
    udp::socket udp_socket_;
    std::string tcp_address_;
    std::string udp_address_;
    std::uint16_t tcp_port_;
    std::uint16_t udp_port_;
    // Single cluster_data_io owner. Declared before all queues so their buffers die before the pool.
    std::pmr::unsynchronized_pool_resource payload_pool_;

    std::map<std::uint64_t, std::shared_ptr<lnk::NodeLink>> links_;
    std::set<std::shared_ptr<tcp::socket>> accepting_;
    asio::experimental::channel<void(asio::error_code, lnk::Datagram)> udp_writes_;
    std::size_t udp_pending_data_ = 0;

    std::map<std::uint64_t, std::shared_ptr<lnk::NodeFlow>> flows_;
    std::map<std::uint64_t, Clock::time_point> retired_;
    std::size_t buffered_bytes_ = 0;

    // Control notifications cross executors.
    asio::experimental::concurrent_channel<void(asio::error_code, CtrlMessage)> events_;
    std::atomic<bool> event_overflow_ = false;

    // Lifecycle state belongs to cluster_data_io.
    asio::steady_timer monitor_timer_;
    AsyncEvent tasks_done_;
    std::size_t tasks_ = 0;
    bool activated_ = false;
    bool stopping_ = false;
};

#endif
