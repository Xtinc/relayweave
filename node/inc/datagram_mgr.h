#ifndef PROXY_DATAGRAM_MANAGER_HEADER
#define PROXY_DATAGRAM_MANAGER_HEADER

#include "app_common.h"
#include "relay_endpoint.h"
#include "xfr_channel.h"
#include <tuple>

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

    struct PathEndpoint : RelayEndpoint
    {
        PathEndpoint(asio::any_io_executor executor, int role)
            : RelayEndpoint(executor, role), received(executor, 16)
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
        std::size_t payload_size;
    };

    struct DatagramSend
    {
        DatagramSend(asio::any_io_executor executor, BytesBuf data, udp::endpoint destination)
            : data(std::move(data)), destination(std::move(destination)), done(executor, 1)
        {
        }
        BytesBuf data;
        udp::endpoint destination;
        asio::experimental::channel<void(asio::error_code, std::size_t)> done;
        bool cancelled = false;
    };

  public:
    DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                udp::endpoint listen_endpoint, std::size_t capacity, TrafficLimitConfig config);

    void start();
    // These entry points require the owning transfer executor.
    njson install_endpoint(int role);
    asio::awaitable<bool> wait_endpoint(std::uint64_t uuid);
    void bind_endpoint(std::uint64_t uuid, LnkChannel &channel, std::uint64_t epoch, std::uint64_t flow_id,
                       SRVTrafficPtr traffic, std::string accessor);
    void activate_endpoint(std::uint64_t uuid);
    asio::awaitable<void> run_endpoint(std::uint64_t uuid);
    void close_endpoint(std::uint64_t uuid);
    njson install_pair();
    asio::awaitable<bool> wait_pair(std::uint64_t uuid);
    void bind_pair(std::uint64_t uuid, SRVTrafficPtr traffic, std::string accessor);
    void activate_pair(std::uint64_t uuid);
    asio::awaitable<void> run_pair(std::uint64_t uuid);
    void close_pair(std::uint64_t uuid);
    void stop();

  private:
    asio::awaitable<void> receive_datagram();
    asio::awaitable<void> send_datagrams();
    asio::awaitable<std::tuple<asio::error_code, std::size_t>> send_datagram(BytesBuf data, udp::endpoint destination);
    std::optional<RoutedDatagram> route_datagram(std::span<std::uint8_t> datagram, const udp::endpoint &source);
    asio::awaitable<void> forward_datagram(std::span<const std::uint8_t> datagram, RoutedDatagram route);
    asio::awaitable<void> read_endpoint(std::shared_ptr<PathEndpoint> endpoint);
    asio::awaitable<void> write_endpoint(std::shared_ptr<PathEndpoint> endpoint);
    std::uint64_t allocate_session_id() const;

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    udp::endpoint listen_endpoint_;
    udp::socket socket_;
    asio::experimental::channel<void(asio::error_code, std::shared_ptr<DatagramSend>)> sends_;
    std::size_t capacity_;
    TrafficLimitConfig config_;
    std::unordered_map<std::uint64_t, std::shared_ptr<LocalPair>> local_pairs_;
    std::unordered_map<std::uint64_t, Binding> bindings_;
    std::unordered_map<std::uint64_t, std::shared_ptr<PathEndpoint>> path_endpoints_;
    bool started_ = false;
    bool stopped_ = false;
};

#endif
