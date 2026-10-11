#ifndef PROXY_DATAGRAM_MANAGER_HEADER
#define PROXY_DATAGRAM_MANAGER_HEADER

#include "app_common.h"
#include "relay_endpoint.h"
#include "xfr_channel.h"

class DatagramMgr : public std::enable_shared_from_this<DatagramMgr>
{
    using udp = asio::ip::udp;

    enum class Side
    {
        Producer,
        Consumer,
    };

    struct LocalPair
    {
        explicit LocalPair(asio::any_io_executor executor) : attached(executor), finished(executor)
        {
        }
        std::optional<udp::endpoint> &endpoint(Side side);
        const DatagramHeader::Buffer &session_header(Side side) const;
        bool allow(Side side, std::size_t payload_size);

        AsyncEvent attached;
        AsyncEvent finished;
        std::uint64_t producer_session_id = 0;
        std::uint64_t consumer_session_id = 0;
        DatagramHeader::Buffer producer_session_header;
        DatagramHeader::Buffer consumer_session_header;
        std::uint64_t producer_ticket = generate_random_id();
        std::uint64_t consumer_ticket = 0;
        std::optional<udp::endpoint> producer_endpoint;
        std::optional<udp::endpoint> consumer_endpoint;
        TokenBucket rx_limiter;
        TokenBucket tx_limiter;
        SRVTrafficPtr traffic;
        std::string accessor;
        bool active = false;
        bool closed = false;
    };

    struct RemotePair : RelayEndpoint
    {
        RemotePair(asio::any_io_executor executor, int role) : RelayEndpoint(executor, role), received(executor, 16)
        {
        }
        asio::experimental::channel<void(asio::error_code, BytesBuf)> received;
        std::uint64_t session_id = 0;
        std::optional<udp::endpoint> source;
    };

    struct Binding
    {
        std::uint64_t relay_uuid;
        Side side;
    };

    struct RoutedDatagram
    {
        udp::endpoint destination;
        SRVTrafficPtr traffic;
        Side source_side;
    };

  public:
    DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                udp::endpoint listen_endpoint, std::size_t capacity, TrafficLimitConfig config);

    void start();
    // These entry points require the owning transfer executor.
    njson install_remote_pair(int role);
    asio::awaitable<bool> wait_remote_pair(std::uint64_t uuid);
    void bind_remote_pair(std::uint64_t uuid, LnkChannel &channel, std::uint64_t epoch, std::uint64_t flow_id,
                       SRVTrafficPtr traffic, std::string accessor);
    void activate_remote_pair(std::uint64_t uuid);
    asio::awaitable<void> run_remote_pair(std::uint64_t uuid);
    void close_remote_pair(std::uint64_t uuid);
    njson install_local_pair();
    asio::awaitable<bool> wait_local_pair(std::uint64_t uuid);
    void bind_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor);
    void activate_local_pair(std::uint64_t uuid);
    asio::awaitable<void> run_local_pair(std::uint64_t uuid);
    void close_local_pair(std::uint64_t uuid);
    void stop();

  private:
    asio::awaitable<void> receive_datagram();
    std::optional<RoutedDatagram> route_datagram(std::span<std::uint8_t> datagram, const udp::endpoint &source);
    asio::awaitable<void> read_remote_pair(std::shared_ptr<RemotePair> endpoint);
    asio::awaitable<void> write_remote_pair(std::shared_ptr<RemotePair> endpoint);
    std::uint64_t allocate_session_id() const;

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    udp::endpoint listen_endpoint_;
    udp::socket socket_;
    std::size_t capacity_;
    TrafficLimitConfig config_;
    // Close erases resources and bindings before notifying tasks that still hold them.
    std::unordered_map<std::uint64_t, Binding> bindings_;
    std::unordered_map<std::uint64_t, std::shared_ptr<LocalPair>> local_pairs_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RemotePair>> remote_pairs_;
    bool started_ = false;
    bool stopped_ = false;
};

#endif
