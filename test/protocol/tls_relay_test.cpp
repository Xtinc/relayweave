#include "xfr_channel.h"
#include "tls_channel.h"
#include "test_data.h"
#include <asio/experimental/awaitable_operators.hpp>
#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
#include <iostream>

namespace
{
using tcp = asio::ip::tcp;
using TLS = asio::ssl::stream<tcp::socket>;
using namespace asio::experimental::awaitable_operators;
using namespace std::chrono_literals;

void require(bool value, const char *message)
{
    if (!value)
    {
        throw std::runtime_error(message);
    }
}

asio::ssl::context make_context()
{
    asio::ssl::context identity(asio::ssl::context::tls);
    const auto dir = test_data::directory();
    identity.use_certificate_chain_file((dir / "tls_channel_test_client.pem").string());
    identity.use_private_key_file((dir / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
    asio::ssl::context context(asio::ssl::context::tls);
    configure_tls_rpk(context, identity);
    return context;
}

struct Fixture
{
    asio::io_context io{1};
    asio::ssl::context client_context;
    asio::ssl::context server_context;
    TLS left_client{io, client_context};
    TLS left_server{io, server_context};
    tcp::socket local{io};
    tcp::socket app{io};

    Fixture()
        : client_context(make_context()), server_context(make_context())
    {
        tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
        left_client.next_layer().connect(acceptor.local_endpoint());
        acceptor.accept(left_server.next_layer());
        app.connect(acceptor.local_endpoint());
        acceptor.accept(local);
    }

    asio::awaitable<void> handshake()
    {
        co_await (left_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  left_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
    }

    void run(asio::awaitable<void> task)
    {
        auto result = asio::co_spawn(io, std::move(task), asio::cancel_after(5s, asio::use_future));
        io.run();
        result.get();
    }
};

asio::awaitable<void> receive_data(TLS &stream, const BytesBuf &expected)
{
    BytesBuf buffer(tls_data_maximum_payload);
    std::size_t offset = 0;
    for (;;)
    {
        std::array<std::uint8_t, 4> header{};
        co_await asio::async_read(stream, asio::buffer(header), asio::use_awaitable);
        const auto size = decode_tls_data_size(header);
        if (size == 0)
        {
            break;
        }
        co_await asio::async_read(stream, asio::buffer(buffer.data(), size), asio::use_awaitable);
        require(size <= expected.size() - offset &&
                std::equal(buffer.begin(), buffer.begin() + size, expected.begin() + offset), "TLS DATA changed");
        offset += size;
    }
    require(offset == expected.size(), "TLS FIN truncated business DATA");
}

asio::awaitable<void> receive_tcp(tcp::socket &socket, const BytesBuf &expected)
{
    BytesBuf data(expected.size());
    co_await asio::async_read(socket, asio::buffer(data), asio::use_awaitable);
    require(data == expected, "TCP DATA changed through TLS access");
    std::uint8_t byte;
    const auto [error, size] = co_await socket.async_read_some(asio::buffer(&byte, 1), asio::as_tuple(asio::use_awaitable));
    require(error == asio::error::eof && size == 0, "Business FIN did not half-close local TCP");
}

asio::awaitable<void> access_business(Fixture &f, bool reverse)
{
    const BytesBuf request(tls_data_maximum_payload - 11, 0x5a);
    const BytesBuf reply(tls_data_maximum_payload, 0xa5);
    for (bool responding : {false, true})
    {
        const auto &payload = responding ? reply : request;
        if (reverse != responding)
        {
            const auto header = encode_tls_data_size(payload.size());
            const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(payload)};
            co_await asio::async_write(f.left_client, buffers, asio::use_awaitable);
            const std::array<std::uint8_t, 4> fin{};
            co_await asio::async_write(f.left_client, asio::buffer(fin), asio::use_awaitable);
            co_await receive_tcp(f.app, payload);
        }
        else
        {
            co_await asio::async_write(f.app, asio::buffer(payload), asio::use_awaitable);
            f.app.shutdown(tcp::socket::shutdown_send);
            co_await receive_data(f.left_client, payload);
        }
    }
}

asio::awaitable<void> access_tls(Fixture &f, bool reverse)
{
    co_await f.handshake();
    co_await await_transfers(relay_tls(f.local, f.left_server),
                             access_business(f, reverse));
}

// Unexpected TLS EOF must cancel the idle reverse direction and retain its error.
asio::awaitable<void> truncated(Fixture &f, bool partial_body)
{
    co_await f.handshake();
    if (partial_body)
    {
        const std::array<std::uint8_t, 6> broken{0, 0, 0, 10, 1, 2};
        co_await asio::async_write(f.left_client, asio::buffer(broken), asio::use_awaitable);
    }
    f.left_client.next_layer().shutdown(tcp::socket::shutdown_send);
    bool failed = false;
    try { co_await relay_tls(f.local, f.left_server); }
    catch (const asio::system_error &error)
    {
        failed = error.code() == asio::ssl::error::stream_truncated;
    }
    require(failed, "Unexpected TLS truncation became normal business FIN");
}

asio::awaitable<void> invalid_length(Fixture &f, std::size_t size)
{
    co_await f.handshake();
    const auto header = encode_tls_data_size(size);
    co_await asio::async_write(f.left_client, asio::buffer(header), asio::use_awaitable);
    bool rejected = false;
    try { co_await relay_tls(f.local, f.left_server); }
    catch (const std::invalid_argument &) { rejected = true; }
    require(rejected, "Oversized TLS DATA was accepted");
}

}

int main()
{
    try
    {
        for (const bool reverse : {false, true})
        {
            Fixture access;
            access.run(access_tls(access, reverse));
        }
        for (const bool partial_body : {false, true})
        {
            Fixture broken;
            broken.run(truncated(broken, partial_body));
        }
        for (const auto size : {tls_data_maximum_payload + 1, std::size_t{0xffffffff}})
        {
            Fixture oversized;
            oversized.run(invalid_length(oversized, size));
        }
        std::cout << "[PASS] TLS 1.3 RPK business FIN, large replies, and truncation failures\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
