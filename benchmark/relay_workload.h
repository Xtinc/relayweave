#pragma once

#include "benchmark.h"
#include "xfr_channel.h"
#include <asio/experimental/awaitable_operators.hpp>
#include <cstring>
#include <future>
#include <type_traits>

// Socket setup, payload validation and executor shared by the three relay examples.
namespace relay_bench
{
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;
using TLS = asio::ssl::stream<tcp::socket>;
using bench::require;
using namespace asio::experimental::awaitable_operators;
using namespace std::chrono_literals;

void connect(tcp::socket &client, tcp::socket &server)
{
    tcp::acceptor acceptor(client.get_executor(), {asio::ip::address_v4::loopback(), 0});
    client.connect(acceptor.local_endpoint());
    acceptor.accept(server);
    client.set_option(tcp::no_delay(true));
    server.set_option(tcp::no_delay(true));
}

template <class Stream>
asio::awaitable<void> send(Stream &stream, std::size_t chunks, std::size_t size, unsigned direction)
{
    BytesBuf data(size);
    for (std::size_t i = 8; i < size; ++i)
        data[i] = static_cast<std::uint8_t>(i * 31 + direction * 67);
    for (std::uint64_t sequence = 0; sequence < chunks; ++sequence)
    {
        for (unsigned i = 0; i < 8; ++i)
            data[i] = static_cast<std::uint8_t>(sequence >> (i * 8));
        if constexpr (std::is_same_v<Stream, TLS>)
            co_await write_tls_data(stream, data);
        else
            co_await asio::async_write(stream, asio::buffer(data), asio::use_awaitable);
    }
    if constexpr (std::is_same_v<Stream, TLS>)
        co_await finish_tls_data(stream);
    else
        stream.shutdown(tcp::socket::shutdown_send);
}

template <class Stream>
asio::awaitable<void> receive(Stream &stream, std::size_t chunks, std::size_t size, unsigned direction)
{
    BytesBuf data(tls_data_maximum_payload);
    BytesBuf expected(size);
    for (std::size_t i = 8; i < size; ++i)
        expected[i] = static_cast<std::uint8_t>(i * 31 + direction * 67);
    std::size_t offset = 0;
    for (;;)
    {
        std::size_t received;
        if constexpr (std::is_same_v<Stream, TLS>)
            received = co_await read_tls_data(stream, data);
        else
        {
            const auto [error, size] = co_await stream.async_read_some(asio::buffer(data), asio::as_tuple(asio::use_awaitable));
            if (error && error != asio::error::eof)
                throw asio::system_error(error);
            received = size;
        }
        if (received == 0)
            break;
        require(received <= chunks * size - std::min(offset, chunks * size), "relay delivered extra bytes");
        for (std::size_t i = 0; i < received;)
        {
            const auto local = offset % size;
            if (local == 0)
                for (unsigned byte = 0; byte < 8; ++byte)
                    expected[byte] = static_cast<std::uint8_t>((offset / size) >> (byte * 8));
            const auto count = std::min(size - local, received - i);
            require(std::memcmp(data.data() + i, expected.data() + local, count) == 0,
                    "relay corrupted or reordered business data");
            offset += count;
            i += count;
        }
    }
    require(offset == chunks * size, "relay FIN truncated business data");
}

template <class Stream>
asio::awaitable<void> business(Stream &stream, std::size_t chunks, std::size_t size, unsigned direction)
{
    co_await (send(stream, chunks, size, direction) && receive(stream, chunks, size, 1 - direction));
}

void run(asio::io_context &io, asio::awaitable<void> task)
{
    auto result = asio::co_spawn(io, std::move(task), asio::cancel_after(30s, asio::use_future));
    io.run();
    result.get();
}

} // namespace relay_bench
