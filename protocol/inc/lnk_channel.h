#ifndef RELAYWEAVE_LNK_CHANNEL_H
#define RELAYWEAVE_LNK_CHANNEL_H
#include "fair_queue.h"
#include "tls_channel.h"
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <memory_resource>
#include <set>
#include <utility>

class FlowProtocol;
namespace lnk
{
enum class EventKind;
using Clock = std::chrono::steady_clock;
inline constexpr std::size_t max_flow_count = 1000;
struct FlowState;

// Frame storage and its shared allocator belong together with the neighbour transport.
// Exclusive, movable byte storage. Shared resources follow their buffers across executors.
// Bytes are not initialised.
class PooledBuffer
{
  private:
    struct Deleter
    {
        std::size_t size = 0;
        std::shared_ptr<std::pmr::memory_resource> resource;

        void operator()(std::uint8_t *storage) const noexcept
        {
            resource->deallocate(storage, size);
        }
    };

  public:
    PooledBuffer() noexcept = default;

    PooledBuffer(std::shared_ptr<std::pmr::memory_resource> resource, std::size_t size)
        : storage_(size ? static_cast<std::uint8_t *>(resource->allocate(size)) : nullptr, Deleter{size, resource}),
          view_(storage_.get(), size)
    {
    }

    PooledBuffer(std::shared_ptr<std::pmr::memory_resource> resource, std::span<const std::uint8_t> bytes)
        : PooledBuffer(std::move(resource), bytes.size())
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

struct FlowWriteTag
{
    const FlowState *flow;
    bool reverse;
    bool operator==(const FlowWriteTag &) const = default;
};

struct FlowWriteHash
{
    std::size_t operator()(const FlowWriteTag &tag) const noexcept
    {
        return std::hash<const FlowState *>{}(tag.flow) ^ std::size_t(tag.reverse);
    }
};

struct Frame
{
    // Logical charge for frame management; not a measurement of allocator or socket memory.
    static constexpr std::size_t buffer_overhead = 128;
    LnkFrameHeader header;
    PooledBuffer payload;
    // Shared Link writes can outlive map removal. Keep the exact Flow identity for completion.
    // Flow-owned queues leave this empty to avoid a self-reference.
    std::shared_ptr<FlowState> owner;
    // Stable wire-credit units, independent of the allocator's actual block sizes.
    static std::size_t charge_for_payload(std::size_t size) noexcept
    {
        return buffer_overhead + (size ? std::bit_ceil(std::max(size, std::size_t{256})) : 0);
    }
    std::size_t charge() const noexcept
    {
        return charge_for_payload(payload.allocation_size());
    }
};
static_assert(sizeof(Frame) <= Frame::buffer_overhead, "review frame management charge after changing Frame layout");

struct LinkSpec
{
    std::uint64_t id = 0;
    std::uint64_t epoch = 0;
    RelayProtocol transport = RelayProtocol::Tcp;
    std::string peer;
    std::string peer_address;
    std::string token;
    std::uint16_t tcp_port = 0;
    std::uint16_t udp_port = 0;
    bool initiator = false;
};
// Shared only by neighbour transport and Flow routing; not a component interface.
struct NodeLink
{
    enum class Phase
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

    NodeLink(asio::any_io_executor executor, LinkSpec spec);

    const LinkSpec spec;

    std::optional<asio::ip::tcp::socket> socket;
    asio::ip::tcp::resolver resolver;
    asio::experimental::channel<void(asio::error_code)> writable;
    FairQueue<FlowWriteTag, Frame, FlowWriteHash> writes;
    std::deque<Frame> controls;
    // Coalesced outgoing control frames, not a second set of Flow credit counters.
    std::map<std::pair<std::uint64_t, bool>, LnkFrameHeader> credits;
    asio::ip::udp::endpoint endpoint;

    Clock::time_point deadline;
    Phase phase = Phase::Preparing;
    // UDP Attach and its acknowledgement can arrive in either order.
    bool attached = false;
    bool acknowledged = false;

    Clock::time_point last_response = Clock::now();
    Clock::time_point last_ping = Clock::now();
    std::uint64_t ping = 0;
    std::uint64_t last_ack_ping = 0;
};

} // namespace lnk

// Physical neighbour transport. All operations require the protocol's data executor,
// except start/rollback during startup. Its owner drains all transport tasks before destruction.
class LnkChannel
{
  public:
    LnkChannel(FlowProtocol &protocol, asio::any_io_executor executor, std::string node_id, std::string tcp_address,
               std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port);
    void start();
    void rollback() noexcept;
    void prepare(lnk::LinkSpec spec);
    void connect(std::uint64_t id);
    void close(std::uint64_t id);

  private:
    friend class FlowProtocol;
    using tcp = asio::ip::tcp;
    using udp = asio::ip::udp;
    using Clock = lnk::Clock;
    // The channel owns these values directly, so its message layout belongs here.
    struct DatagramWrite
    {
        lnk::Frame frame;
        std::uint64_t link_id;
    };
    bool enqueue(lnk::NodeLink &link, lnk::Frame frame);
    void discard(std::uint64_t link_id, const std::shared_ptr<lnk::FlowState> &flow);
    Clock::time_point poll(Clock::time_point now);
    void close_all();
    void notify(const lnk::NodeLink &link, lnk::EventKind kind, std::string reason = {}, std::string_view stage = {});
    void fail(lnk::NodeLink &link, std::string reason, std::string_view stage = {});
    void mark_ready(lnk::NodeLink &link);
    bool matches(const lnk::NodeLink &link, const CtrlMessage &message) const;
    CtrlMessage attach_message(const lnk::NodeLink &link, CtrlCommand command) const;

    void process(lnk::NodeLink &link, const LnkFrameHeader &header, lnk::PooledBuffer payload);
    asio::awaitable<void> accept_loop();
    asio::awaitable<void> accept(std::shared_ptr<tcp::socket> socket);
    asio::awaitable<void> prepare_udp(std::shared_ptr<lnk::NodeLink> link);
    asio::awaitable<void> establish(std::shared_ptr<lnk::NodeLink> link);
    // The establishment/accept task owns the Link and joins both borrowed I/O loops here.
    asio::awaitable<void> tcp_run(lnk::NodeLink &link);
    asio::awaitable<void> tcp_read(lnk::NodeLink &link);
    asio::awaitable<void> tcp_write(lnk::NodeLink &link);
    asio::awaitable<void> udp_read();
    asio::awaitable<void> udp_write();

    FlowProtocol &protocol_;
    asio::any_io_executor executor_;
    std::string node_id_;
    tcp::acceptor acceptor_;
    udp::socket udp_socket_;
    std::string tcp_address_;
    std::string udp_address_;
    std::uint16_t tcp_port_;
    std::uint16_t udp_port_;

    std::map<std::uint64_t, std::shared_ptr<lnk::NodeLink>> connections_;
    std::set<std::shared_ptr<tcp::socket>> accepting_;
    asio::experimental::channel<void(asio::error_code, DatagramWrite)> udp_writes_;
    std::size_t udp_pending_data_ = 0;
};
#endif
