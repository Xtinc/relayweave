#include "xfr_channel.h"
#include "asio/experimental/parallel_group.hpp"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>

#if defined(_WIN32)
#include <windows.h>
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

using tcp = asio::ip::tcp;
using udp = asio::ip::udp;
using tls_stream = asio::ssl::stream<tcp::socket>;

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

asio::awaitable<void> TokenBucket::consume(std::size_t bytes)
{
    if (rate_ == 0 || bytes == 0)
    {
        co_return;
    }
    const auto now = Clock::now();
    const auto target = reserve(bytes, now);
    if (target <= now)
    {
        co_return;
    }
    if (!timer_)
    {
        timer_.emplace(co_await asio::this_coro::executor);
    }
    timer_->expires_at(target);
    co_await timer_->async_wait(asio::use_awaitable);
}

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
    std::array<std::uint8_t, 64 * 1024> buffer{};
    for (;;)
    {
        auto [er, nr] = co_await from.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (er)
        {
            log_transfer_end(er, "tcp", "read", from, to);
            if (er == asio::error::eof)
            {
                asio::error_code shutdown_error;
                to.shutdown(tcp::socket::shutdown_send, shutdown_error);
                if (shutdown_error && shutdown_error != asio::error::not_connected)
                {
                    throw asio::system_error(shutdown_error, "shutdown TCP send");
                }
                co_return;
            }
            throw asio::system_error(er, "read TCP stream");
        }

        if (limiter)
        {
            co_await limiter->consume(nr);
        }

        auto [ew, nw] = co_await asio::async_write(to, asio::buffer(buffer.data(), nr), use_nothrow_awaitable);
        if (traffic && nw != 0)
        {
            traffic->add(nw);
        }

        if (ew)
        {
            log_transfer_end(ew, "tcp", "write", from, to);
            throw asio::system_error(ew, "write TCP stream");
        }
    }
}

asio::awaitable<void> relay_tcp(tcp::socket &left, tcp::socket &right, TokenBucket *left_to_right_limiter,
                                TokenBucket *right_to_left_limiter, TrafficCounter *left_to_right_traffic,
                                TrafficCounter *right_to_left_traffic)
{
    return await_transfers(transfer_tcp(left, right, left_to_right_limiter, left_to_right_traffic),
                           transfer_tcp(right, left, right_to_left_limiter, right_to_left_traffic));
}

asio::awaitable<std::size_t> read_tls_data(tls_stream &stream, std::span<std::uint8_t> buffer)
{
    std::array<std::uint8_t, 4> header{};
    co_await asio::async_read(stream, asio::buffer(header), asio::use_awaitable);
    std::uint32_t size = 0;
    for (const auto byte : header)
    {
        size = (size << 8) | byte;
    }
    if (size > tls_data_maximum_payload || size > buffer.size())
    {
        throw std::invalid_argument("Invalid TLS DATA length");
    }
    if (size != 0)
    {
        co_await asio::async_read(stream, asio::buffer(buffer.data(), size), asio::use_awaitable);
    }
    co_return size;
}

asio::awaitable<void> write_tls_data(tls_stream &stream, std::span<const std::uint8_t> payload,
                                    TrafficCounter *traffic)
{
    while (!payload.empty())
    {
        const auto size = std::min(payload.size(), tls_data_maximum_payload);
        const std::array<std::uint8_t, 4> header{
            static_cast<std::uint8_t>(size >> 24), static_cast<std::uint8_t>(size >> 16),
            static_cast<std::uint8_t>(size >> 8), static_cast<std::uint8_t>(size)};
        const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(payload.data(), size)};
        const auto [error, written] = co_await asio::async_write(stream, buffers, use_nothrow_awaitable);
        if (traffic && written > header.size())
        {
            traffic->add(written - header.size());
        }
        if (error)
        {
            throw asio::system_error(error, "write TLS DATA");
        }
        payload = payload.subspan(size);
    }
}

asio::awaitable<void> finish_tls_data(tls_stream &stream)
{
    const std::array<std::uint8_t, 4> fin{};
    co_await asio::async_write(stream, asio::buffer(fin), asio::use_awaitable);
}

// Business FIN is independent of TLS shutdown, including on TLS 1.2 connections.
template <typename From, typename To>
static asio::awaitable<void> transfer_tls(From &from, To &to, TokenBucket *limiter, TrafficCounter *traffic)
{
    std::array<std::uint8_t, tls_data_maximum_payload> buffer{};
    for (;;)
    {
        std::size_t size;
        if constexpr (std::is_same_v<From, tls_stream>)
        {
            size = co_await read_tls_data(from, buffer);
        }
        else
        {
            const auto [error, received] = co_await from.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
            if (error && error != asio::error::eof)
            {
                throw asio::system_error(error, "read local TCP stream");
            }
            size = received;
            // Deliver any final bytes before propagating EOF.
            if (!error && size == 0)
            {
                continue;
            }
        }
        if (size == 0)
        {
            if constexpr (std::is_same_v<To, tls_stream>)
            {
                co_await finish_tls_data(to);
            }
            else
            {
                asio::error_code error;
                to.shutdown(tcp::socket::shutdown_send, error);
                if (error && error != asio::error::not_connected)
                {
                    throw asio::system_error(error, "shutdown local TCP send");
                }
            }
            co_return;
        }
        if (limiter)
        {
            co_await limiter->consume(size);
        }
        if constexpr (std::is_same_v<To, tls_stream>)
        {
            co_await write_tls_data(to, std::span<const std::uint8_t>(buffer.data(), size), traffic);
        }
        else
        {
            const auto [error, written] = co_await asio::async_write(to, asio::buffer(buffer.data(), size), use_nothrow_awaitable);
            if (traffic)
            {
                traffic->add(written);
            }
            if (error)
            {
                throw asio::system_error(error, "write local TCP stream");
            }
        }
    }
}

asio::awaitable<void> relay_tls(tcp::socket &left, tls_stream &right)
{
    return await_transfers(transfer_tls(left, right, nullptr, nullptr), transfer_tls(right, left, nullptr, nullptr));
}

asio::awaitable<void> relay_tls(tls_stream &left, tls_stream &right, TokenBucket *left_to_right_limiter,
                                TokenBucket *right_to_left_limiter, TrafficCounter *left_to_right_traffic,
                                TrafficCounter *right_to_left_traffic)
{
    return await_transfers(transfer_tls(left, right, left_to_right_limiter, left_to_right_traffic),
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
            throw asio::system_error(er, "receive target datagram");
        }

        if (nr > DatagramHeader::maximum_user_payload)
        {
            continue;
        }

        const std::array buffers{asio::buffer(session_header), asio::buffer(payload.data(), nr)};
        auto [wr, nw] = co_await transfer_socket.async_send(buffers, use_nothrow_awaitable);
        if (wr)
        {
            throw asio::system_error(wr, "send relay datagram");
        }
        if (nw != session_header.size() + nr)
        {
            throw std::runtime_error("truncated relay datagram");
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
            throw asio::system_error(er, "receive relay datagram");
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
            throw asio::system_error(ew, "send target datagram");
        }
        if (nw != nr - DatagramHeader::length)
        {
            throw std::runtime_error("truncated target datagram");
        }
    }
}

asio::awaitable<void> relay_udp_connected(udp::socket &local_socket, udp::socket &transfer_socket,
                                          DatagramHeader::Buffer session_header)
{
    return await_transfers(send_udp_payloads(local_socket, transfer_socket, session_header),
                           receive_udp_payloads(transfer_socket, local_socket, session_header));
}

asio::awaitable<void> await_transfers(asio::awaitable<void> outgoing, asio::awaitable<void> incoming)
{
    const auto executor = co_await asio::this_coro::executor;
    const auto [order, outgoing_error, incoming_error] = co_await asio::experimental::make_parallel_group(
        asio::co_spawn(executor, std::move(outgoing), asio::deferred),
        asio::co_spawn(executor, std::move(incoming), asio::deferred)
    ).async_wait(asio::experimental::wait_for_one_error(), asio::use_awaitable);
    // operator&& retains only the outgoing exception if both fail; that can hide the original I/O error.
    for (const auto index : order)
    {
        if (const auto error = index == 0 ? outgoing_error : incoming_error)
        {
            std::rethrow_exception(error);
        }
    }
}
