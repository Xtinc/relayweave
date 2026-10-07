#ifndef RELAYWEAVE_LNK_CHANNEL_H
#define RELAYWEAVE_LNK_CHANNEL_H
#include "tls_channel.h"
#include <array>
#include <map>
#include <optional>
#include <set>

enum class FlowSendStatus
{
    Queued,
    WouldBlock,
    Closed,
    Invalid
};
struct FlowSendResult
{
    FlowSendStatus status = FlowSendStatus::Closed;
    FlowFrame unsent;
};

// All state and tasks in this module belong to the single cluster_data_io executor.
class LnkChannel : public std::enable_shared_from_this<LnkChannel>
{
  public:
    // Control notifications only; business frames are dispatched directly in this data domain.
    asio::awaitable<CtrlMessage> receive_event();
    LnkChannel(asio::any_io_executor executor, std::string node_id, std::string tcp_address, std::uint16_t tcp_port,
               std::string udp_address, std::uint16_t udp_port);
    void start();
    void activate();
    void rollback() noexcept;
    void prepare(njson params);
    void connect(std::uint64_t id);
    void close(std::uint64_t id);
    asio::awaitable<void> stop();
    void prepare_flow(njson params);
    void commit_flow(std::uint64_t id);
    void close_flow(std::uint64_t id, std::string reason = {});
    void invalidate_flows(std::string reason);
    FlowSendResult send_flow(FlowFrame frame);
    asio::awaitable<FlowFrame> receive_flow(std::uint64_t epoch, std::uint64_t id);
    asio::any_io_executor executor() const
    {
        return executor_;
    }

  private:
    using tcp = asio::ip::tcp;
    using udp = asio::ip::udp;
    using Clock = std::chrono::steady_clock;
    using ReceiveQueue = asio::experimental::channel<void(asio::error_code, FlowFrame)>;
    struct NodeFlow
    {
        NodeFlow(asio::any_io_executor executor, njson params);
        const njson params; // Immutable control notification template; hot fields are typed below.
        const std::uint64_t epoch;
        const RelayProtocol transport;
        std::uint64_t previous = 0;
        std::uint64_t next = 0;
        Clock::time_point prepare_deadline;
        ReceiveQueue received;
        std::array<bool, 2> fin{};
        std::size_t bytes = 0;
        bool active = false;
        bool closed = false;
        std::string reason;
    };
    void link_flows_closed(std::uint64_t id);
    FlowSendStatus deliver(const std::shared_ptr<NodeFlow> &flow, FlowFrame &frame);
    void notify_flow(const std::shared_ptr<NodeFlow> &flow, CtrlCommand command, std::string stage,
                     std::string reason = {});
    void fail_flow(std::shared_ptr<NodeFlow> flow, std::string stage, std::string reason);
    void expire_preparations(Clock::time_point now);
    static std::size_t charge(const FlowFrame &frame);

    struct QueuedFrame
    {
        LnkFrameHeader::Buffer header;
        BytesBuf payload;
        bool data;
    };
    using FrameQueue = asio::experimental::channel<void(asio::error_code, QueuedFrame)>;
    struct NodeLink
    {
        NodeLink(asio::any_io_executor executor, njson params);
        const njson params; // Immutable control notification template; hot fields are typed below.
        const std::uint64_t id;
        const std::uint64_t epoch;
        const RelayProtocol transport;
        std::optional<tcp::socket> socket;
        tcp::resolver resolver;
        std::optional<FrameQueue> writes;
        udp::endpoint endpoint;
        Clock::time_point deadline = Clock::now() + std::chrono::seconds(10);
        Clock::time_point last_response = Clock::now();
        Clock::time_point last_ping = Clock::now();
        std::uint64_t ping = 0;
        std::size_t pending_data = 0;
        bool connected = false;
        bool attached = false;
        bool acknowledged = false;
        bool ready = false;
        bool closed = false;
        std::string stage = "prepare";
    };
    struct Datagram
    {
        QueuedFrame frame;
        std::shared_ptr<NodeLink> link;
    };
    void emit(CtrlMessage message);
    void spawn(asio::awaitable<void> task);
    void notify(const std::shared_ptr<NodeLink> &link, CtrlCommand command, std::string reason = {});
    void fail(const std::shared_ptr<NodeLink> &link, std::string reason);
    void mark_ready(const std::shared_ptr<NodeLink> &link);
    bool matches(const NodeLink &link, const CtrlMessage &message) const;
    CtrlMessage attach_message(const NodeLink &link, CtrlCommand command) const;
    bool enqueue(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, BytesBuf &&payload = {});
    void process(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, BytesBuf payload);
    void incoming_flow(const NodeLink &link, FlowFrame frame);
    asio::awaitable<void> accept_loop();
    asio::awaitable<void> accept(std::shared_ptr<tcp::socket> socket);
    asio::awaitable<void> prepare_udp(std::shared_ptr<NodeLink> link);
    asio::awaitable<void> establish(std::shared_ptr<NodeLink> link);
    asio::awaitable<void> read(std::shared_ptr<NodeLink> link);
    asio::awaitable<void> write(std::shared_ptr<NodeLink> link);
    asio::awaitable<void> udp_read();
    asio::awaitable<void> udp_write();
    asio::awaitable<void> monitor();

    asio::any_io_executor executor_;
    std::string node_id_;
    tcp::acceptor acceptor_;
    udp::socket udp_socket_;
    std::string tcp_address_;
    std::string udp_address_;
    std::uint16_t tcp_port_;
    std::uint16_t udp_port_;
    asio::experimental::channel<void(asio::error_code, CtrlMessage)> events_;
    std::string event_error_;
    std::map<std::uint64_t, std::shared_ptr<NodeFlow>> flows_;
    std::map<std::uint64_t, Clock::time_point> retired_;
    std::size_t buffered_bytes_ = 0;
    std::size_t udp_pending_data_ = 0;
    std::map<std::uint64_t, std::shared_ptr<NodeLink>> links_;
    std::set<std::shared_ptr<tcp::socket>> accepting_;
    asio::experimental::channel<void(asio::error_code, Datagram)> udp_writes_;
    asio::steady_timer monitor_timer_;
    asio::steady_timer done_;
    std::size_t tasks_ = 0;
    bool stopping_ = false;
};

#endif
