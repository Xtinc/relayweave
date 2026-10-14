#include "benchmark.h"
#include "test_data.h"
#include "tls_channel.h"
#include "xfr_channel.h"
#include "proxy_server.h"
#include <asio/experimental/awaitable_operators.hpp>
#include <cstring>
#include <future>
#include <openssl/ssl.h>
#include <type_traits>

namespace
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

asio::ssl::context context(bool server, int version)
{
    asio::ssl::context result(server ? asio::ssl::context::tls_server : asio::ssl::context::tls_client);
    const auto dir = test_data::directory();
    const std::string prefix = server ? "tls_channel_test_server" : "tls_channel_test_client";
    result.use_certificate_chain_file((dir / (prefix + ".pem")).string());
    result.use_private_key_file((dir / (prefix + ".key")).string(), asio::ssl::context::pem);
    result.load_verify_file((dir / (server ? "tls_channel_test_client_ca.pem" : "tls_channel_test_ca.pem")).string());
    require(SSL_CTX_set_min_proto_version(result.native_handle(), version) == 1 &&
            SSL_CTX_set_max_proto_version(result.native_handle(), version) == 1, "cannot pin TLS version");
    return result;
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
        for (std::size_t i = 0; i < received; ++i, ++offset)
        {
            const auto local = offset % size;
            const auto expected = local < 8 ? static_cast<std::uint8_t>((offset / size) >> (local * 8))
                                            : static_cast<std::uint8_t>(local * 31 + direction * 67);
            require(data[i] == expected, "relay corrupted or reordered business data");
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

void tcp_relay(bench::Report &report, std::size_t size)
{
    asio::io_context io(1);
    tcp::socket left_client(io), left_server(io), right_client(io), right_server(io);
    connect(left_client, left_server);
    connect(right_client, right_server);
    TrafficCounter outgoing, incoming;
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer timer;
        co_await (relay_tcp(left_server, right_server, nullptr, nullptr, &outgoing, &incoming) &&
                  business(left_client, report.iterations, size, 0) && business(right_client, report.iterations, size, 1));
        report.add("relay/tcp/" + std::to_string(size), report.iterations * 2, report.iterations * size * 2,
                   timer, outgoing.total() + incoming.total());
        require(outgoing.total() == report.iterations * size && incoming.total() == report.iterations * size,
                "TCP relay counters lost payload bytes");
    };
    run(io, task());
}

void tls_relay(bench::Report &report, int version, bool access)
{
    asio::io_context io(1);
    auto client_context = context(false, version);
    auto server_context = context(true, version);
    TLS left_client(io, client_context), left_server(io, server_context);
    TLS right_client(io, client_context), right_server(io, server_context);
    tcp::socket app(io), local(io);
    connect(right_client.next_layer(), right_server.next_layer());
    configure_tls_client(right_client, "localhost");
    configure_tls_server(right_server);
    if (access)
        connect(app, local);
    else
    {
        connect(left_client.next_layer(), left_server.next_layer());
        configure_tls_client(left_client, "localhost");
        configure_tls_server(left_server);
    }
    const auto label = std::string(access ? "access_tcp_tls" : "tls_tls") + (version == TLS1_2_VERSION ? "/1.2" : "/1.3");
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer handshake;
        co_await (right_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  right_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
        if (!access)
            co_await (left_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                      left_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
        report.add("handshake/" + label, access ? 1 : 2, 0, handshake, access ? 1 : 2,
                   {{"tls_version", SSL_get_version(right_client.native_handle())}, {"cipher", SSL_get_cipher_name(right_client.native_handle())}});
        constexpr std::size_t size = 32768;
        TrafficCounter outgoing, incoming;
        bench::Timer timer;
        if (access)
            co_await (relay_tls(local, right_server) && business(app, report.iterations, size, 0) &&
                      business(right_client, report.iterations, size, 1));
        else
        {
            co_await (relay_tls(left_server, right_server, nullptr, nullptr, &outgoing, &incoming) &&
                      business(left_client, report.iterations, size, 0) && business(right_client, report.iterations, size, 1));
            require(outgoing.total() == report.iterations * size && incoming.total() == report.iterations * size,
                    "TLS counters include framing or lost payload");
        }
        report.add("relay/" + label, report.iterations * 2, report.iterations * size * 2, timer, report.iterations * size * 2,
                   {{"chunk_bytes", size}, {"duplex", true}, {"includes_integrity_check", true}});
    };
    run(io, task());
}

void udp_relay(bench::Report &report)
{
    asio::io_context io(1);
    const udp::endpoint endpoint(asio::ip::address_v4::loopback(), 0);
    udp::socket target(io, endpoint), transfer(io, endpoint), target_peer(io, endpoint), transfer_peer(io, endpoint);
    target.connect(target_peer.local_endpoint());
    target_peer.connect(target.local_endpoint());
    transfer.connect(transfer_peer.local_endpoint());
    transfer_peer.connect(transfer.local_endpoint());
    const auto header = DatagramHeader::encode(1);
    auto task = [&]() -> asio::awaitable<void> {
        BytesBuf data(1400, 0x5a), wire(data.size() + header.size()), returned(data.size());
        bench::Timer timer;
        for (std::uint64_t sequence = 0; sequence < report.iterations; ++sequence)
        {
            std::memcpy(data.data(), &sequence, sizeof(sequence));
            co_await target_peer.async_send(asio::buffer(data), asio::use_awaitable);
            const auto size = co_await transfer_peer.async_receive(asio::buffer(wire), asio::use_awaitable);
            require(size == wire.size() && std::equal(header.begin(), header.end(), wire.begin()) &&
                    std::equal(data.begin(), data.end(), wire.begin() + header.size()), "UDP relay changed packet/header");
            co_await transfer_peer.async_send(asio::buffer(wire), asio::use_awaitable);
            const auto response = co_await target_peer.async_receive(asio::buffer(returned), asio::use_awaitable);
            require(response == data.size() && returned == data, "UDP reverse relay changed packet");
        }
        report.add("relay/udp_roundtrip", report.iterations, report.iterations * data.size() * 2, timer, report.iterations,
                   {{"packet_bytes", data.size()}, {"in_flight", 1}});
    };
    auto combined = [&]() -> asio::awaitable<void> {
        co_await (relay_udp_connected(target, transfer, header) || task());
    };
    run(io, combined());
}

void control(bench::Report &report)
{
    asio::io_context io(1);
    auto client_context = context(false, TLS1_3_VERSION);
    auto server_context = context(true, TLS1_3_VERSION);
    tcp::socket client_socket(io), server_socket(io);
    connect(client_socket, server_socket);
    auto client = std::make_shared<TLSChannel>(std::move(client_socket), client_context, TLSChannelRole::C);
    auto server = std::make_shared<TLSChannel>(std::move(server_socket), server_context, TLSChannelRole::S);
    auto responder = [&]() -> asio::awaitable<void> {
        for (std::size_t i = 0; i < report.iterations; ++i)
            server->send(co_await server->async_receive());
    };
    auto requester = [&]() -> asio::awaitable<void> {
        constexpr std::size_t window = 32;
        for (std::size_t base = 0; base < report.iterations; base += window)
        {
            const auto count = std::min(window, report.iterations - base);
            for (std::size_t i = 0; i < count; ++i)
                client->send(CtrlMessage(CtrlCommand::ServiceLookup, njson{{"sequence", base + i}, {"service", "benchmark"}}));
            for (std::size_t i = 0; i < count; ++i)
            {
                const auto message = co_await client->async_receive();
                require(message.type() == CtrlCommand::ServiceLookup && message.params->at("sequence") == base + i,
                        "TLS control session reordered messages");
            }
        }
    };
    auto task = [&]() -> asio::awaitable<void> {
        ScopeGuard close([&] { client->disconnect(); server->disconnect(); });
        co_await (client->start("localhost") && server->start(""));
        bench::Timer timer;
        co_await (responder() && requester());
        report.add("control/tls_message_roundtrip", report.iterations, 0, timer, report.iterations, {{"window", 32}});
        co_await (client->async_disconnect() && server->async_disconnect());
    };
    try { run(io, task()); }
    catch (...) { client->disconnect(); server->disconnect(); io.restart(); io.run(); throw; }
}

void proxy(bench::Report &report, bool socks, bool churn)
{
    asio::io_context io(1);
    tcp::acceptor http_port(io, {asio::ip::address_v4::loopback(), 0});
    tcp::acceptor socks_port(io, {asio::ip::address_v4::loopback(), 0});
    tcp::acceptor origin(io, {asio::ip::address_v4::loopback(), 0});
    ProxyConfig config;
    config.http.address = "127.0.0.1";
    config.http.port = http_port.local_endpoint().port();
    config.socks5 = {"127.0.0.1", socks_port.local_endpoint().port()};
    http_port.close();
    socks_port.close();
    auto server = std::make_shared<ProxyServer>(io, config);
    server->start();
    const auto connections = churn ? report.iterations : 1;
    const auto chunks = churn ? 1 : report.iterations;
    const auto size = churn ? 32 : 32768;
    const BytesBuf payload(size, 0x5a);
    auto upstream = [&]() -> asio::awaitable<void> {
        BytesBuf received(size);
        for (std::size_t i = 0; i < connections; ++i)
        {
            auto socket = co_await origin.async_accept(asio::use_awaitable);
            socket.set_option(tcp::no_delay(true));
            for (std::size_t chunk = 0; chunk < chunks; ++chunk)
            {
                co_await asio::async_read(socket, asio::buffer(received), asio::use_awaitable);
                require(received == payload, "proxy upstream payload changed");
                co_await asio::async_write(socket, asio::buffer(received), asio::use_awaitable);
            }
            socket.shutdown(tcp::socket::shutdown_send);
        }
    };
    auto client = [&]() -> asio::awaitable<void> {
        BytesBuf received(size);
        for (std::size_t i = 0; i < connections; ++i)
        {
            tcp::socket socket(io);
            co_await socket.async_connect({asio::ip::address_v4::loopback(), socks ? config.socks5.port : config.http.port},
                                          asio::use_awaitable);
            socket.set_option(tcp::no_delay(true));
            if (socks)
            {
                const std::array<std::uint8_t, 3> greeting{5, 1, 0};
                co_await asio::async_write(socket, asio::buffer(greeting), asio::use_awaitable);
                std::array<std::uint8_t, 2> method{};
                co_await asio::async_read(socket, asio::buffer(method), asio::use_awaitable);
                require(method == std::array<std::uint8_t, 2>{5, 0}, "SOCKS negotiation failed");
                const auto port = origin.local_endpoint().port();
                const std::array<std::uint8_t, 10> request{5, 1, 0, 1, 127, 0, 0, 1,
                    static_cast<std::uint8_t>(port >> 8), static_cast<std::uint8_t>(port)};
                co_await asio::async_write(socket, asio::buffer(request), asio::use_awaitable);
                std::array<std::uint8_t, 10> reply{};
                co_await asio::async_read(socket, asio::buffer(reply), asio::use_awaitable);
                require(reply[0] == 5 && reply[1] == 0 && reply[3] == 1, "SOCKS CONNECT failed");
            }
            else
            {
                const std::string request = "CONNECT 127.0.0.1:" + std::to_string(origin.local_endpoint().port()) +
                                            " HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
                co_await asio::async_write(socket, asio::buffer(request), asio::use_awaitable);
                std::string reply;
                co_await asio::async_read_until(socket, asio::dynamic_buffer(reply, config.http.max_header_bytes),
                                                "\r\n\r\n", asio::use_awaitable);
                require(reply.find("200 Connection Established") != std::string::npos, "HTTP CONNECT failed");
            }
            for (std::size_t chunk = 0; chunk < chunks; ++chunk)
            {
                co_await asio::async_write(socket, asio::buffer(payload), asio::use_awaitable);
                co_await asio::async_read(socket, asio::buffer(received), asio::use_awaitable);
                require(received == payload, "proxy response payload changed");
            }
            socket.shutdown(tcp::socket::shutdown_send);
            std::uint8_t byte = 0;
            const auto [error, extra] = co_await socket.async_read_some(asio::buffer(&byte, 1), asio::as_tuple(asio::use_awaitable));
            require(error == asio::error::eof && extra == 0, "proxy did not propagate EOF");
        }
    };
    auto task = [&]() -> asio::awaitable<void> {
        ScopeGuard close([&] { server->stop(); });
        bench::Timer timer;
        co_await (upstream() && client());
        report.add(std::string(socks ? "proxy/socks5/" : "proxy/http_connect/") + (churn ? "connection_churn" : "payload_roundtrip"),
                   churn ? connections : chunks, connections * chunks * size * 2, timer, connections * chunks,
                   {{"connections", connections}, {"payload_bytes_per_direction", size}, {"includes_handshake", true}, {"in_flight", 1}});
    };
    run(io, task());
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 256);
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            tcp_relay(report, 1400);
            tcp_relay(report, 32768);
            for (const int version : {TLS1_2_VERSION, TLS1_3_VERSION})
            {
                tls_relay(report, version, false);
                tls_relay(report, version, true);
            }
            udp_relay(report);
            control(report);
            for (const bool socks : {false, true})
            {
                proxy(report, socks, false);
                proxy(report, socks, true);
            }
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
