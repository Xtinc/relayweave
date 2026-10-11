#ifndef RELAYWEAVE_STREAM_PIPELINE_H
#define RELAYWEAVE_STREAM_PIPELINE_H

#include "app_common.h"
#include "relay_endpoint.h"
#include "xfr_channel.h"
#include <atomic>

struct TcpTransport
{
    using Stream = asio::ip::tcp::socket;

    explicit TcpTransport(asio::ssl::context &) noexcept;

    static constexpr std::string_view name = "TCP";

    asio::awaitable<Stream> prepare(Stream socket, std::chrono::steady_clock::time_point deadline) const;
    static asio::awaitable<void> relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                      TokenBucket &tx_limiter, ServiceTraffic &traffic);
};

struct TlsTransport
{
    using Stream = TLSStream;

    explicit TlsTransport(asio::ssl::context &context) noexcept;

    static constexpr std::string_view name = "TLS";

    asio::awaitable<Stream> prepare(asio::ip::tcp::socket socket,
                                    std::chrono::steady_clock::time_point deadline) const;
    static asio::awaitable<void> relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                      TokenBucket &tx_limiter, ServiceTraffic &traffic);

  private:
    asio::ssl::context &context_;
};

template <typename Transport>
class StreamPipeline : public std::enable_shared_from_this<StreamPipeline<Transport>>
{
    using tcp = asio::ip::tcp;
    using Stream = typename Transport::Stream;

    // Forwarding between an attached Agent and a NodeFlow.
    struct RemotePair : RelayEndpoint
    {
        using RelayEndpoint::RelayEndpoint;
        std::optional<Stream> stream;
    };

    // Single-node forwarding between two attached Agents.
    struct LocalPair
    {
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
    StreamPipeline(asio::any_io_executor executor, asio::ssl::context &ssl_context,
                   std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address, std::uint16_t data_port,
                   std::size_t max_setup_connections, std::size_t capacity,
                   std::chrono::steady_clock::duration setup_timeout, TrafficLimitConfig config);

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
    void wait_for_pending();

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
    // Close erases resources before notifying tasks that still hold them.
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
