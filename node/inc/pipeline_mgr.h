#ifndef RELAYWEAVE_STREAM_PIPELINE_H
#define RELAYWEAVE_STREAM_PIPELINE_H

#include "app_common.h"
#include "tls_channel.h"
#include "xfr_channel.h"
#include <atomic>

struct TcpTransport
{
    using Stream = asio::ip::tcp::socket;

    explicit TcpTransport(asio::ssl::context &) noexcept;

    static constexpr RelayProtocol protocol = RelayProtocol::Tcp;
    static constexpr std::string_view name = "TCP";

    asio::awaitable<Stream> prepare(Stream socket, std::chrono::steady_clock::time_point deadline) const;
    static asio::awaitable<void> relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                      TokenBucket &tx_limiter, ServiceTraffic &traffic);
};

struct TlsTransport
{
    using Stream = TLSStream;

    explicit TlsTransport(asio::ssl::context &context) noexcept;

    static constexpr RelayProtocol protocol = RelayProtocol::Tls;
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

    struct Relay
    {
        Relay(asio::any_io_executor executor, std::uint64_t uuid, std::uint64_t producer_ticket,
              std::uint64_t consumer_ticket, std::string service, const ControlSessionPtr &producer,
              const ControlSessionPtr &consumer, std::uint64_t request_id, const TrafficLimitConfig &config,
              SRVTrafficPtr traffic);

        bool belongs_to(const ControlSessionPtr &session) const;
        bool complete() const noexcept;
        void cancel() noexcept;

        std::uint64_t uuid;
        std::uint64_t producer_ticket;
        std::uint64_t consumer_ticket;
        std::string service;
        std::weak_ptr<ControlSession> producer;
        std::weak_ptr<ControlSession> consumer;
        std::uint64_t request_id;
        std::string accessor;
        asio::steady_timer setup_timer;
        std::optional<Stream> producer_stream;
        std::optional<Stream> consumer_stream;
        TokenBucket rx_limiter;
        TokenBucket tx_limiter;
        SRVTrafficPtr traffic;
        bool relaying = false;
    };

  public:
    StreamPipeline(asio::any_io_executor executor, asio::ssl::context &ssl_context,
                   std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address, std::uint16_t data_port,
                   std::size_t max_setup_connections, std::size_t capacity,
                   std::chrono::steady_clock::duration setup_timeout, TrafficLimitConfig config);

    void start();
    void open(const ControlSessionPtr &producer, const ControlSessionPtr &consumer, std::string service,
              std::uint64_t request_id, SRVTrafficPtr traffic);
    bool reject(const ControlSessionPtr &session, std::uint64_t uuid, std::string reason);
    bool cancel(const ControlSessionPtr &session, std::optional<std::uint64_t> uuid,
                std::optional<std::uint64_t> request_id);
    void disconnect(const ControlSessionPtr &session);
    void stop();
    void wait_for_pending();

  private:
    asio::awaitable<void> accept_loop();
    asio::awaitable<void> run_transfer_session(tcp::socket socket);
    void do_open(const ControlSessionPtr &producer, const ControlSessionPtr &consumer, std::string service,
                 std::uint64_t request_id, SRVTrafficPtr traffic);
    void attach(int role, std::uint64_t uuid, std::uint64_t ticket, Stream stream);
    void start_stream(const std::shared_ptr<Relay> &relay);
    bool erase_stream(const std::shared_ptr<Relay> &relay) noexcept;
    void terminate_stream(std::shared_ptr<Relay> relay, std::optional<std::string> reason = std::nullopt);

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
    std::unordered_map<std::uint64_t, std::shared_ptr<Relay>> relays_;
    std::atomic_size_t pending_sockets_{0};
    bool started_ = false;
    bool stopped_ = false;
};

using TcpPipeline = StreamPipeline<TcpTransport>;
using TlsPipeline = StreamPipeline<TlsTransport>;

extern template class StreamPipeline<TcpTransport>;
extern template class StreamPipeline<TlsTransport>;

#endif
