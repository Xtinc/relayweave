#include "xfr_channel.h"
#include "test_data.h"
#include <algorithm>
#include <array>
#include <asio/experimental/awaitable_operators.hpp>
#include <iomanip>
#include <iostream>
#include <stdexcept>

namespace
{
using Clock = TokenBucket::Clock;
using tcp = asio::ip::tcp;
using TLS = asio::ssl::stream<tcp::socket>;
using namespace asio::experimental::awaitable_operators;

// Pre-flow-control implementation from 8103543. Keep the call out of line, as the
// production implementation is linked from a separate translation unit.
class OriginalBucket
{
  public:
    OriginalBucket(std::uint64_t rate, std::uint64_t burst)
        : rate_(rate), burst_(burst), tokens_(static_cast<double>(burst))
    {
    }
#if defined(_MSC_VER)
    __declspec(noinline)
#else
    __attribute__((noinline))
#endif
    bool try_consume(std::size_t bytes)
    {
        if (!rate_ || !bytes)
            return true;
        const auto now = Clock::now();
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
        if (tokens_ < static_cast<double>(bytes))
            return false;
        tokens_ -= static_cast<double>(bytes);
        return true;
    }

  private:
    std::uint64_t rate_;
    std::uint64_t burst_;
    bool initialised_ = false;
    double tokens_;
    Clock::time_point last_update_{};
};

template <bool ReadGate = false, class Bucket> double measure(Bucket &bucket, std::size_t bytes)
{
    constexpr std::size_t iterations = 3000000;
    std::size_t accepted = 0;
    const auto start = Clock::now();
    for (std::size_t i = 0; i < iterations; ++i)
    {
        if constexpr (ReadGate)
            accepted += bucket.try_consume(0);
        accepted += bucket.try_consume(bytes);
    }
    const auto elapsed = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
    if (accepted != iterations * (ReadGate ? 2 : 1))
        throw std::runtime_error("Benchmark unexpectedly exhausted tokens");
    return elapsed / iterations;
}

double median(std::array<double, 7> samples)
{
    std::sort(samples.begin(), samples.end());
    return samples[samples.size() / 2];
}

void microbenchmark(std::uint64_t rate, std::size_t bytes, const char *label)
{
    std::array<std::array<double, 7>, 4> samples{};
    for (std::size_t round = 0; round < 8; ++round)
    {
        // Rotate order; discard the first round as warmup.
        for (std::size_t offset = 0; offset < samples.size(); ++offset)
        {
            const auto candidate = (round + offset) % samples.size();
            double elapsed;
            if (candidate == 0)
            {
                OriginalBucket bucket(rate, LnkFrameHeader::maximum_payload);
                elapsed = measure(bucket, bytes);
            }
            else
            {
                TokenBucket bucket(rate, LnkFrameHeader::maximum_payload,
                                   candidate == 1 ? TokenBucket::Mode::Fixed : TokenBucket::Mode::Dynamic);
                elapsed = candidate == 3 ? measure<true>(bucket, bytes) : measure(bucket, bytes);
            }
            if (round)
                samples[candidate][round - 1] = elapsed;
        }
    }
    std::cout << label << " bytes=" << bytes << " ns/frame original=" << median(samples[0])
              << " fixed=" << median(samples[1]) << " dynamic=" << median(samples[2])
              << " dynamic_with_read_gate=" << median(samples[3]) << '\n';
}

asio::awaitable<void> send_tcp(tcp::socket &socket, std::size_t total)
{
    std::array<std::uint8_t, LnkFrameHeader::maximum_payload> payload{};
    for (std::size_t sent = 0; sent < total;)
    {
        const auto bytes = std::min(payload.size(), total - sent);
        co_await asio::async_write(socket, asio::buffer(payload.data(), bytes), asio::use_awaitable);
        sent += bytes;
    }
    socket.shutdown(tcp::socket::shutdown_send);
}

asio::awaitable<void> receive_tcp(tcp::socket &socket, std::size_t total)
{
    std::array<std::uint8_t, LnkFrameHeader::maximum_payload> payload;
    for (std::size_t received = 0; received < total;)
        received += co_await socket.async_read_some(asio::buffer(payload), asio::use_awaitable);
    socket.shutdown(tcp::socket::shutdown_send);
}

asio::awaitable<void> send_tls(TLS &socket, std::size_t total)
{
    std::array<std::uint8_t, LnkFrameHeader::maximum_payload> payload{};
    for (std::size_t sent = 0; sent < total;)
    {
        const auto bytes = std::min(payload.size(), total - sent);
        co_await write_tls_data(socket, std::span(payload.data(), bytes));
        sent += bytes;
    }
    co_await finish_tls_data(socket);
}

asio::awaitable<void> receive_tls(TLS &socket, std::size_t total)
{
    std::array<std::uint8_t, LnkFrameHeader::maximum_payload> payload;
    for (std::size_t received = 0; received < total;)
    {
        const auto bytes = co_await read_tls_data(socket, payload);
        if (!bytes)
            throw std::runtime_error("Early TLS FIN");
        received += bytes;
    }
    co_await finish_tls_data(socket);
}

double transfer(bool tls, int candidate)
{
    constexpr std::size_t total = 128 * 1024 * 1024;
    asio::io_context io{1};
    tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
    std::array<tcp::socket, 4> sockets{tcp::socket(io), tcp::socket(io), tcp::socket(io), tcp::socket(io)};
    for (int i : {0, 2})
    {
        sockets[i].connect(acceptor.local_endpoint());
        acceptor.accept(sockets[i + 1]);
    }
    for (auto &socket : sockets)
        socket.set_option(tcp::no_delay(true));
    const auto mode = candidate == 2 ? TokenBucket::Mode::Dynamic : TokenBucket::Mode::Fixed;
    TokenBucket forward(0, 0, mode), reverse(0, 0, mode);
    auto *out = candidate ? &forward : nullptr;
    auto *in = candidate ? &reverse : nullptr;
    double elapsed;
    if (!tls)
    {
        auto result = asio::co_spawn(io, send_tcp(sockets[0], total) && receive_tcp(sockets[3], total) &&
                                            relay_tcp(sockets[1], sockets[2], out, in), asio::use_future);
        const auto start = Clock::now();
        io.run();
        elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        result.get();
    }
    else
    {
        asio::ssl::context client(asio::ssl::context::tls_client), server(asio::ssl::context::tls_server);
        const auto dir = test_data::directory();
        server.use_certificate_chain_file((dir / "tls_channel_test_server.pem").string());
        server.use_private_key_file((dir / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
        client.set_verify_mode(asio::ssl::verify_none);
        TLS source(std::move(sockets[0]), client), left(std::move(sockets[1]), server),
            right(std::move(sockets[2]), client), sink(std::move(sockets[3]), server);
        auto ready = asio::co_spawn(io,
            source.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                left.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable) &&
                right.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                sink.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable), asio::use_future);
        io.run();
        ready.get();
        io.restart();
        auto result = asio::co_spawn(io, send_tls(source, total) && receive_tls(sink, total) &&
                                            relay_tls(left, right, out, in), asio::use_future);
        const auto start = Clock::now();
        io.run();
        elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        result.get();
    }
    return static_cast<double>(total) / (1024 * 1024) / elapsed;
}

void stream_benchmark(bool tls)
{
    std::array<std::array<double, 7>, 3> samples{};
    for (std::size_t round = 0; round < 8; ++round)
        for (std::size_t offset = 0; offset < samples.size(); ++offset)
        {
            const auto candidate = (round + offset) % samples.size();
            const auto speed = transfer(tls, static_cast<int>(candidate));
            if (round)
                samples[candidate][round - 1] = speed;
        }
    std::cout << (tls ? "TLS" : "TCP") << " loopback MiB/s no_bucket=" << median(samples[0])
              << " fixed=" << median(samples[1]) << " dynamic=" << median(samples[2]) << '\n';
}
} // namespace

int main()
{
    try
    {
        std::cout << std::fixed << std::setprecision(2)
                  << "bucket_bytes original=" << sizeof(OriginalBucket) << " current=" << sizeof(TokenBucket) << '\n';
        microbenchmark(0, 1400, "unlimited");
        microbenchmark(1ULL << 40, 1400, "finite");
        stream_benchmark(false);
        stream_benchmark(true);
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
