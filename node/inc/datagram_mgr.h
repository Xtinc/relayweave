#ifndef PROXY_DATAGRAM_MANAGER_HEADER
#define PROXY_DATAGRAM_MANAGER_HEADER

#include "app_common.h"
#include "tls_channel.h"
#include "xfr_channel.h"

class DatagramMgr : public std::enable_shared_from_this<DatagramMgr>
{
    using udp = asio::ip::udp;

    enum class Side
    {
        Producer,
        Consumer,
    };

    enum class RelayState
    {
        WaitingForProducer,
        WaitingForAttach,
        Active,
        Closing,
    };

    struct Relay
    {
        Relay(asio::any_io_executor executor, std::uint64_t uuid, std::uint64_t producer_session_id,
              std::uint64_t consumer_session_id, std::uint64_t producer_ticket, std::uint64_t consumer_ticket,
              std::string service, const ControlSessionPtr &producer, const ControlSessionPtr &consumer,
              std::uint64_t request_id, const TrafficLimitConfig &config, SRVTrafficPtr traffic);

        bool belongs_to(const ControlSessionPtr &session) const;
        bool complete() const noexcept;
        void cancel() noexcept;
        std::optional<udp::endpoint> &endpoint(Side side);
        const DatagramHeader::Buffer &session_header(Side side) const;
        bool allow(Side side, std::size_t payload_size);

        std::uint64_t uuid;
        std::uint64_t producer_session_id;
        std::uint64_t consumer_session_id;
        DatagramHeader::Buffer producer_session_header;
        DatagramHeader::Buffer consumer_session_header;
        std::uint64_t producer_ticket;
        std::uint64_t consumer_ticket;
        std::string service;
        std::weak_ptr<ControlSession> producer;
        std::weak_ptr<ControlSession> consumer;
        std::uint64_t request_id;
        std::string accessor;
        asio::steady_timer service_wait_timer;
        std::optional<udp::endpoint> producer_endpoint;
        std::optional<udp::endpoint> consumer_endpoint;
        TokenBucket rx_limiter;
        TokenBucket tx_limiter;
        SRVTrafficPtr traffic;
        RelayState state = RelayState::WaitingForProducer;
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

  public:
    DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                udp::endpoint listen_endpoint, std::size_t capacity,
                std::chrono::steady_clock::duration service_wait_duration, TrafficLimitConfig config);

    void start();
    void open(const ControlSessionPtr &consumer, std::string service, std::uint64_t request_id,
              ControlSessionPtr producer = {}, SRVTrafficPtr traffic = {});
    void attach_service(const ControlSessionPtr &producer, const std::string &service, SRVTrafficPtr traffic);
    bool reject(const ControlSessionPtr &session, std::uint64_t uuid, std::string reason);
    bool cancel(const ControlSessionPtr &session, std::optional<std::uint64_t> uuid,
                std::optional<std::uint64_t> request_id);
    void disconnect(const ControlSessionPtr &session);
    void stop();

  private:
    asio::awaitable<void> receive_datagram();
    std::optional<RoutedDatagram> route_datagram(std::span<std::uint8_t> datagram, const udp::endpoint &source);
    asio::awaitable<void> forward_datagram(std::span<const std::uint8_t> datagram, RoutedDatagram route);
    void do_open(const ControlSessionPtr &consumer, std::string service, std::uint64_t request_id,
                 ControlSessionPtr producer, SRVTrafficPtr traffic);
    void offer_producer(const std::shared_ptr<Relay> &relay, const ControlSessionPtr &producer);
    void start_relay(const std::shared_ptr<Relay> &relay);
    bool erase_relay(const std::shared_ptr<Relay> &relay) noexcept;
    void terminate_relay(std::shared_ptr<Relay> relay, std::optional<std::string> reason = std::nullopt);
    std::uint64_t allocate_session_id() const;

    asio::any_io_executor executor_;
    std::shared_ptr<RelayIdAllocator> id_allocator_;
    udp::endpoint listen_endpoint_;
    udp::socket socket_;
    std::size_t capacity_;
    std::chrono::steady_clock::duration service_wait_duration_;
    TrafficLimitConfig config_;
    std::unordered_map<std::uint64_t, std::shared_ptr<Relay>> relays_;
    std::unordered_map<std::uint64_t, Binding> bindings_;
    bool started_ = false;
    bool stopped_ = false;
};

#endif
