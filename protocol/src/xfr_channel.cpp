#include "xfr_channel.h"
#include "asio/experimental/awaitable_operators.hpp"
#include <algorithm>
#include <limits>

#if defined(_WIN32)
#include <windows.h>
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

using tcp = asio::ip::tcp;
using udp = asio::ip::udp;
using tls_stream = asio::ssl::stream<tcp::socket>;
using namespace asio::experimental::awaitable_operators;
using std::chrono::steady_clock;

constexpr auto use_nothrow_awaitable = asio::as_tuple(asio::use_awaitable);

void set_current_thread_scheduler_policy()
{
#if defined(_WIN32)
    if (!SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL))
    {
        PROXY_ERROR_PRINT("SetThreadPriority failed: error=%lu.", GetLastError());
    }
#elif defined(__linux__)
    pthread_t thread = pthread_self();
    struct sched_param param{};
    const int max_priority = sched_get_priority_max(SCHED_RR);
    const int min_priority = sched_get_priority_min(SCHED_RR);
    if (max_priority < 0)
    {
        PROXY_ERROR_PRINT("sched_get_priority_max failed: error=%s.", std::strerror(errno));
        return;
    }
    param.sched_priority = max_priority > min_priority ? max_priority - 1 : max_priority;
    const auto error = pthread_setschedparam(thread, SCHED_RR, &param);
    if (error != 0)
    {
        if (error == EPERM || error == EACCES)
            PROXY_DEBUG_PRINT("Thread priority unchanged: SCHED_RR requires permission (%s)", std::strerror(error));
        else
            PROXY_ERROR_PRINT("Thread priority failed: SCHED_RR error=%s", std::strerror(error));
        return;
    }
    pthread_setname_np(thread, "audio_thd");
#endif
}

void TrafficCounter::add(std::size_t bytes) noexcept
{
    if (bytes == 0)
    {
        return;
    }
    const auto value = static_cast<std::uint64_t>(bytes);
    saturating_add(total_, value);
    saturating_add(interval_, value);
}

std::uint64_t TrafficCounter::total() const noexcept
{
    return total_.load(std::memory_order_relaxed);
}

std::uint64_t TrafficCounter::take_interval() noexcept
{
    return interval_.exchange(0, std::memory_order_relaxed);
}

void TrafficCounter::saturating_add(std::atomic<std::uint64_t> &counter, std::uint64_t value) noexcept
{
    auto current = counter.load(std::memory_order_relaxed);
    for (;;)
    {
        const auto maximum = std::numeric_limits<std::uint64_t>::max();
        const auto next = value > maximum - current ? maximum : current + value;
        if (counter.compare_exchange_weak(current, next, std::memory_order_relaxed))
        {
            return;
        }
    }
}

TokenBucket::TokenBucket(std::uint64_t bytes_per_second, std::uint64_t burst_bytes) noexcept
    : rate_(bytes_per_second), burst_(burst_bytes), tokens_(static_cast<double>(burst_bytes))
{
}

TokenBucket::TimePoint TokenBucket::reserve(std::size_t bytes, TimePoint now)
{
    if (rate_ == 0 || bytes == 0)
    {
        return now;
    }

    refill(now);

    const auto requested = static_cast<double>(bytes);
    if (last_update_ <= now && tokens_ >= requested)
    {
        tokens_ -= requested;
        return now;
    }

    const auto deficit = std::max(0.0, requested - tokens_);
    tokens_ = 0.0;
    last_update_ = std::max(last_update_, now) + duration_for(deficit);
    return last_update_;
}

bool TokenBucket::try_consume_at(std::size_t bytes, TimePoint now)
{
    refill(now);
    const auto requested = static_cast<double>(bytes);
    if (tokens_ < requested)
    {
        return false;
    }
    tokens_ -= requested;
    return true;
}

bool TokenBucket::try_consume(std::size_t bytes)
{
    if (rate_ == 0 || bytes == 0)
    {
        return true;
    }
    return try_consume_at(bytes, Clock::now());
}

void TokenBucket::refill(TimePoint now) noexcept
{
    if (!initialised_)
    {
        last_update_ = now;
        initialised_ = true;
    }
    if (now > last_update_)
    {
        const auto elapsed = std::chrono::duration<double>(now - last_update_).count();
        tokens_ = std::min(static_cast<double>(burst_), tokens_ + elapsed * static_cast<double>(rate_));
        last_update_ = now;
    }
}

TokenBucket::Clock::duration TokenBucket::duration_for(double bytes) const noexcept
{
    const double nanoseconds = (bytes * 1000000000.0) / static_cast<double>(rate_);
    const auto rounded = static_cast<std::int64_t>(nanoseconds + 0.999999);
    return std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(rounded));
}

static asio::awaitable<bool> wait_for_limit(TokenBucket &limiter, asio::steady_timer &timer, std::size_t bytes)
{
    const auto now = std::chrono::steady_clock::now();
    const auto target = limiter.reserve(bytes, now);
    if (target <= now)
    {
        co_return true;
    }
    timer.expires_at(target);
    auto [error] = co_await timer.async_wait(use_nothrow_awaitable);
    co_return !error;
}

// Log current endpoints and the original error at the I/O completion point.
template <class From, class To>
static void log_transfer_end(const asio::error_code &error, const char *protocol, const char *operation,
                             const From &from, const To &to)
{
    if (error == asio::error::eof || error == asio::error::operation_aborted ||
        error == asio::ssl::error::stream_truncated)
        PROXY_DEBUG_PRINT("Transfer ended %s %s %s -> %s reason=%s", protocol, operation, socket_peer(from).c_str(),
                          socket_peer(to).c_str(), error.message().c_str());
    else
        PROXY_ERROR_PRINT("Transfer failed %s %s %s -> %s reason=%s", protocol, operation,
                          socket_peer(from).c_str(), socket_peer(to).c_str(), error.message().c_str());
}

static asio::awaitable<void> transfer_tcp(tcp::socket &from, tcp::socket &to, TokenBucket *limiter,
                                          TrafficCounter *traffic)
{
    asio::steady_timer timer(from.get_executor());
    std::array<std::uint8_t, 64 * 1024> buffer{};
    for (;;)
    {
        auto [er, nr] = co_await from.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (er)
        {
            log_transfer_end(er, "tcp", "read", from, to);
            if (er == asio::error::eof)
            {
                asio::error_code ignored;
                to.shutdown(tcp::socket::shutdown_send, ignored);
            }
            co_return;
        }

        if (limiter && !co_await wait_for_limit(*limiter, timer, nr))
        {
            co_return;
        }

        auto [ew, nw] = co_await asio::async_write(to, asio::buffer(buffer.data(), nr), use_nothrow_awaitable);
        if (traffic && nw != 0)
        {
            traffic->add(nw);
        }

        if (ew)
        {
            log_transfer_end(ew, "tcp", "write", from, to);
            co_return;
        }
    }
}

asio::awaitable<void> relay_tcp(tcp::socket &left, tcp::socket &right, TokenBucket *left_to_right_limiter,
                                TokenBucket *right_to_left_limiter, TrafficCounter *left_to_right_traffic,
                                TrafficCounter *right_to_left_traffic)
{
    co_await (transfer_tcp(left, right, left_to_right_limiter, left_to_right_traffic) &&
              transfer_tcp(right, left, right_to_left_limiter, right_to_left_traffic));
}

template <typename From, typename To>
static asio::awaitable<void> transfer_tls(From &from, To &to, TokenBucket *limiter, TrafficCounter *traffic)
{
    asio::steady_timer timer(from.get_executor());
    std::array<std::uint8_t, 64 * 1024> buffer{};
    for (;;)
    {
        auto [read_error, read_size] = co_await from.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (read_error)
        {
            log_transfer_end(read_error, "tls", "read", from, to);
            co_return;
        }
        if (limiter && !co_await wait_for_limit(*limiter, timer, read_size))
        {
            co_return;
        }

        auto [write_error, written] =
            co_await asio::async_write(to, asio::buffer(buffer.data(), read_size), use_nothrow_awaitable);
        if (traffic && written != 0)
        {
            traffic->add(written);
        }
        if (write_error)
        {
            log_transfer_end(write_error, "tls", "write", from, to);
            co_return;
        }
    }
}

asio::awaitable<void> relay_tls(tcp::socket &left, tls_stream &right)
{
    co_await (transfer_tls(left, right, nullptr, nullptr) || transfer_tls(right, left, nullptr, nullptr));
}

asio::awaitable<void> relay_tls(tls_stream &left, tls_stream &right, TokenBucket *left_to_right_limiter,
                                TokenBucket *right_to_left_limiter, TrafficCounter *left_to_right_traffic,
                                TrafficCounter *right_to_left_traffic)
{
    co_await (transfer_tls(left, right, left_to_right_limiter, left_to_right_traffic) ||
              transfer_tls(right, left, right_to_left_limiter, right_to_left_traffic));
}

static asio::awaitable<void> send_udp_payloads(udp::socket &local_socket, udp::socket &transfer_socket,
                                               DatagramHeader::Buffer session_header)
{
    std::array<std::uint8_t, DatagramHeader::maximum_user_payload + 1> payload{};
    for (;;)
    {
        auto [er, nr] = co_await local_socket.async_receive(asio::buffer(payload), use_nothrow_awaitable);
        if (er)
        {
            if (er == asio::error::message_size)
            {
                continue;
            }
            log_transfer_end(er, "udp", "receive", local_socket, transfer_socket);
            co_return;
        }

        if (nr > DatagramHeader::maximum_user_payload)
        {
            continue;
        }

        const std::array buffers{asio::buffer(session_header), asio::buffer(payload.data(), nr)};
        auto [wr, nw] = co_await transfer_socket.async_send(buffers, use_nothrow_awaitable);
        if (wr)
        {
            log_transfer_end(wr, "udp", "send", local_socket, transfer_socket);
            co_return;
        }
    }
}

static asio::awaitable<void> receive_udp_payloads(udp::socket &transfer_socket, udp::socket &local_socket,
                                                  DatagramHeader::Buffer session_header)
{
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> datagram{};
    for (;;)
    {
        auto [er, nr] = co_await transfer_socket.async_receive(asio::buffer(datagram), use_nothrow_awaitable);
        if (er)
        {
            if (er == asio::error::message_size)
            {
                continue;
            }
            log_transfer_end(er, "udp", "receive", transfer_socket, local_socket);
            co_return;
        }

        if (nr < DatagramHeader::length || nr > DatagramHeader::maximum_wire_payload)
        {
            continue;
        }

        if (!std::equal(session_header.begin(), session_header.end(), datagram.begin()))
        {
            continue;
        }

        auto [ew, nw] = co_await local_socket.async_send(
            asio::buffer(datagram.data() + DatagramHeader::length, nr - DatagramHeader::length), use_nothrow_awaitable);
        if (ew)
        {
            log_transfer_end(ew, "udp", "send", transfer_socket, local_socket);
            co_return;
        }
    }
}

asio::awaitable<void> relay_udp_connected(udp::socket &local_socket, udp::socket &transfer_socket,
                                          DatagramHeader::Buffer session_header)
{
    co_await (send_udp_payloads(local_socket, transfer_socket, session_header) &&
              receive_udp_payloads(transfer_socket, local_socket, session_header));
}
