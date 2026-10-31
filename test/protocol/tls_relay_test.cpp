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

asio::ssl::context make_context(int version, bool server)
{
    asio::ssl::context context(server ? asio::ssl::context::tls_server : asio::ssl::context::tls_client);
    const auto dir = test_data::directory();
    const std::string prefix = server ? "tls_channel_test_server" : "tls_channel_test_client";
    context.use_certificate_chain_file((dir / (prefix + ".pem")).string());
    context.use_private_key_file((dir / (prefix + ".key")).string(), asio::ssl::context::pem);
    context.load_verify_file((dir / (server ? "tls_channel_test_client_ca.pem" : "tls_channel_test_ca.pem")).string());
    require(SSL_CTX_set_min_proto_version(context.native_handle(), version) == 1 &&
            SSL_CTX_set_max_proto_version(context.native_handle(), version) == 1, "Cannot pin TLS version");
    return context;
}

struct Fixture
{
    asio::io_context io{1};
    asio::ssl::context client_context;
    asio::ssl::context server_context;
    TLS left_client{io, client_context};
    TLS left_server{io, server_context};
    TLS right_client{io, client_context};
    TLS right_server{io, server_context};
    tcp::socket local{io};
    tcp::socket app{io};

    explicit Fixture(int version)
        : client_context(make_context(version, false)), server_context(make_context(version, true))
    {
        tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
        left_client.next_layer().connect(acceptor.local_endpoint());
        acceptor.accept(left_server.next_layer());
        right_client.next_layer().connect(acceptor.local_endpoint());
        acceptor.accept(right_server.next_layer());
        app.connect(acceptor.local_endpoint());
        acceptor.accept(local);
        configure_tls_client(left_client, "localhost");
        configure_tls_client(right_client, "localhost");
        configure_tls_server(left_server);
        configure_tls_server(right_server);
    }

    asio::awaitable<void> handshake()
    {
        co_await (left_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  left_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable) &&
                  right_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  right_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
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

asio::awaitable<void> tls_business(TLS &requester, TLS &responder)
{
    const BytesBuf request(tls_data_maximum_payload - 11, 0x5a);
    const BytesBuf reply(tls_data_maximum_payload, 0xa5);
    // The service deliberately waits for business FIN before producing its reply.
    for (bool responding : {false, true})
    {
        auto &sender = responding ? responder : requester;
        auto &receiver = responding ? requester : responder;
        const auto &payload = responding ? reply : request;
        const auto header = encode_tls_data_size(payload.size());
        const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(payload)};
        co_await asio::async_write(sender, buffers, asio::use_awaitable);
        const std::array<std::uint8_t, 4> fin{};
        co_await asio::async_write(sender, asio::buffer(fin), asio::use_awaitable);
        co_await receive_data(receiver, payload);
    }
}

asio::awaitable<void> single_tls(Fixture &f, bool reverse)
{
    co_await f.handshake();
    TrafficCounter outgoing;
    TrafficCounter incoming;
    co_await await_transfers(relay_tls(f.left_server, f.right_server, nullptr, nullptr, &outgoing, &incoming),
        tls_business(reverse ? f.right_client : f.left_client, reverse ? f.left_client : f.right_client));
    require(outgoing.total() == (reverse ? tls_data_maximum_payload : tls_data_maximum_payload - 11),
            "TLS outgoing traffic includes framing or lost bytes");
    require(incoming.total() == (reverse ? tls_data_maximum_payload - 11 : tls_data_maximum_payload),
            "TLS incoming traffic includes framing or lost bytes");
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
    try { co_await relay_tls(f.left_server, f.right_server); }
    catch (const asio::system_error &error)
    {
        failed = error.code() == asio::ssl::error::stream_truncated;
    }
    require(failed, "Unexpected TLS truncation became normal business FIN");
}

asio::awaitable<void> invalid_length(Fixture &f)
{
    co_await f.handshake();
    const std::array<std::uint8_t, 4> header{0xff, 0xff, 0xff, 0xff};
    co_await asio::async_write(f.left_client, asio::buffer(header), asio::use_awaitable);
    bool rejected = false;
    try { co_await relay_tls(f.left_server, f.right_server); }
    catch (const std::invalid_argument &) { rejected = true; }
    require(rejected, "Oversized TLS DATA was accepted");
}

asio::awaitable<void> break_during_limit(Fixture &f, bool reverse)
{
    const BytesBuf payload(60, 0x5a);
    const auto header = encode_tls_data_size(payload.size());
    const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(payload)};
    co_await asio::async_write(reverse ? f.right_client : f.left_client, buffers, asio::use_awaitable);
    asio::steady_timer timer(f.io, 20ms);
    co_await timer.async_wait(asio::use_awaitable);
    auto &peer = reverse ? f.left_client : f.right_client;
    peer.next_layer().set_option(asio::socket_base::linger(true, 0));
    peer.next_layer().close();
}

asio::awaitable<void> limit_failure(Fixture &f, bool reverse)
{
    co_await f.handshake();
    TokenBucket limiter(1, 0); // One minute of debt must be cancelled by the other direction's RST.
    bool failed = false;
    try
    {
        co_await await_transfers(
            relay_tls(f.left_server, f.right_server, reverse ? nullptr : &limiter, reverse ? &limiter : nullptr),
            break_during_limit(f, reverse));
    }
    catch (const asio::system_error &error)
    {
        failed = error.code() == asio::error::connection_reset;
    }
    require(failed, "TLS rate-limit wait hid the original I/O failure or did not drain");
}
}

int main()
{
    try
    {
        for (const auto version : {TLS1_2_VERSION, TLS1_3_VERSION})
        {
            for (const bool reverse : {false, true})
            {
                Fixture single(version);
                single.run(single_tls(single, reverse));
                Fixture access(version);
                access.run(access_tls(access, reverse));
                Fixture limited(version);
                limited.run(limit_failure(limited, reverse));
            }
            for (const bool partial_body : {false, true})
            {
                Fixture broken(version);
                broken.run(truncated(broken, partial_body));
            }
            Fixture oversized(version);
            oversized.run(invalid_length(oversized));
        }
        std::cout << "[PASS] TLS 1.2/1.3 Single/Multi FIN, large replies, traffic, and truncation failures\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
