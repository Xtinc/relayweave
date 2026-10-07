#ifndef PROXY_TRANSFER_CHANNEL_H
#define PROXY_TRANSFER_CHANNEL_H

#include "asio.hpp"
#include "asio/ssl.hpp"
#include "message.h"
#include <atomic>

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

class TokenBucket
{
  public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    TokenBucket(std::uint64_t bytes_per_second = 0, std::uint64_t burst_bytes = 0) noexcept;
    TimePoint reserve(std::size_t bytes, TimePoint now);
    bool try_consume(std::size_t bytes);

  private:
    bool try_consume_at(std::size_t bytes, TimePoint now);
    void refill(TimePoint now) noexcept;
    Clock::duration duration_for(double bytes) const noexcept;

    std::uint64_t rate_{};
    std::uint64_t burst_{};
    bool initialised_{};
    double tokens_{};
    TimePoint last_update_{};
};

asio::awaitable<void> relay_tcp(asio::ip::tcp::socket &left, asio::ip::tcp::socket &right,
                                TokenBucket *left_to_right_limiter = nullptr,
                                TokenBucket *right_to_left_limiter = nullptr,
                                TrafficCounter *left_to_right_traffic = nullptr,
                                TrafficCounter *right_to_left_traffic = nullptr);

asio::awaitable<void> relay_tls(asio::ip::tcp::socket &left, asio::ssl::stream<asio::ip::tcp::socket> &right);
asio::awaitable<void> relay_tls(asio::ssl::stream<asio::ip::tcp::socket> &left,
                                asio::ssl::stream<asio::ip::tcp::socket> &right,
                                TokenBucket *left_to_right_limiter = nullptr,
                                TokenBucket *right_to_left_limiter = nullptr,
                                TrafficCounter *left_to_right_traffic = nullptr,
                                TrafficCounter *right_to_left_traffic = nullptr);

asio::awaitable<void> relay_udp_connected(asio::ip::udp::socket &local_socket, asio::ip::udp::socket &transfer_socket,
                                          DatagramHeader::Buffer session_header);

#endif // PROXY_COROUTINE_RELAY_H
