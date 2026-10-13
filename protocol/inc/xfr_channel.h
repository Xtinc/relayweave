#ifndef PROXY_TRANSFER_CHANNEL_H
#define PROXY_TRANSFER_CHANNEL_H

#include "asio.hpp"
#include "asio/ssl.hpp"
#include "message.h"
#include <atomic>
#include <memory>
#include <span>
#include <string_view>

void set_current_thread_scheduler_policy();

template <class Socket> std::string socket_peer(const Socket &socket)
{
    asio::error_code error;
    const auto endpoint = socket.lowest_layer().remote_endpoint(error);
    if (error)
        return "unknown";
    const auto address = endpoint.address().to_string();
    return (endpoint.address().is_v6() ? "[" + address + "]" : address) + ":" + std::to_string(endpoint.port());
}

class TrafficCounter
{
  public:
    void add(std::size_t bytes) noexcept;
    std::uint64_t total() const noexcept;
    std::uint64_t take_interval() noexcept;

  private:
    static void saturating_add(std::atomic<std::uint64_t> &counter, std::uint64_t value) noexcept;
    alignas(64) std::atomic<std::uint64_t> total_{};
    alignas(64) std::atomic<std::uint64_t> interval_{};
};

// Access is serialized on the owning executor; only one wait() may run at a time.
class TokenBucket
{
  public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    enum class Mode
    {
        Fixed,
        Dynamic
    };

    TokenBucket(std::uint64_t bytes_per_second = 0, std::uint64_t burst_bytes = 0, Mode mode = Mode::Fixed) noexcept;
    bool try_consume(std::size_t bytes = 0);
    asio::awaitable<void> wait(std::size_t bytes = 0);
    void close();

    void feedback(std::uint64_t sequence, FlowPressure action);

  private:
    void set_rate(std::uint64_t bytes_per_second, std::uint64_t burst_bytes);
    TimePoint reserve(std::size_t bytes, TimePoint now);
    TimePoint deadline(TimePoint now) const noexcept;
    void refill(TimePoint now) noexcept;
    Clock::duration duration_for(double bytes) const noexcept;

    Mode mode_;
    std::uint64_t rate_{};
    std::uint64_t burst_{};
    std::uint64_t ceiling_{};
    std::uint64_t sequence_ = 0;
    bool paused_ = false;
    bool closed_ = false;
    double tokens_{};
    TimePoint last_update_{};
    std::unique_ptr<asio::steady_timer> timer_;
};

asio::awaitable<void> relay_tcp(asio::ip::tcp::socket &left, asio::ip::tcp::socket &right,
                                TokenBucket *left_to_right_limiter = nullptr,
                                TokenBucket *right_to_left_limiter = nullptr,
                                TrafficCounter *left_to_right_traffic = nullptr,
                                TrafficCounter *right_to_left_traffic = nullptr);

inline constexpr std::string_view tls_data_framing = "length32-fin-v1";
inline constexpr std::size_t tls_data_maximum_payload = LnkFrameHeader::maximum_payload;

asio::awaitable<std::size_t> read_tls_data(asio::ssl::stream<asio::ip::tcp::socket> &stream,
                                           std::span<std::uint8_t> buffer);
asio::awaitable<void> write_tls_data(asio::ssl::stream<asio::ip::tcp::socket> &stream,
                                     std::span<const std::uint8_t> payload, TrafficCounter *traffic = nullptr);
asio::awaitable<void> finish_tls_data(asio::ssl::stream<asio::ip::tcp::socket> &stream);

asio::awaitable<void> relay_tls(asio::ip::tcp::socket &left, asio::ssl::stream<asio::ip::tcp::socket> &right);
asio::awaitable<void> relay_tls(asio::ssl::stream<asio::ip::tcp::socket> &left,
                                asio::ssl::stream<asio::ip::tcp::socket> &right,
                                TokenBucket *left_to_right_limiter = nullptr,
                                TokenBucket *right_to_left_limiter = nullptr,
                                TrafficCounter *left_to_right_traffic = nullptr,
                                TrafficCounter *right_to_left_traffic = nullptr);

// Stream relays preserve the opposite direction after business EOF and propagate I/O failures.
// Same executor: wait for both normal completions; on failure cancel and drain the sibling.
asio::awaitable<void> await_transfers(asio::awaitable<void> outgoing, asio::awaitable<void> incoming);

asio::awaitable<void> relay_udp_connected(asio::ip::udp::socket &local_socket, asio::ip::udp::socket &transfer_socket,
                                          DatagramHeader::Buffer session_header);

#endif // PROXY_COROUTINE_RELAY_H
