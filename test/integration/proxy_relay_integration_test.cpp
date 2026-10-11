#include "test_data.h"
#include "node_test_config.h"
#include "proxy_server.h"
#include "relay_agent.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <openssl/ssl.h>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
constexpr auto use_nothrow_awaitable = asio::as_tuple(asio::use_awaitable);

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

std::uint16_t unused_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
    return acceptor.local_endpoint().port();
}

std::uint16_t unique_port(asio::io_context &io, std::initializer_list<std::uint16_t> used)
{
    for (;;)
    {
        const auto port = unused_port(io);
        if (std::ranges::find(used, port) == used.end())
        {
            return port;
        }
    }
}

void configure_common(asio::ssl::context &context)
{
    context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1);
}

void configure_server(asio::ssl::context &context)
{
    const auto data = test_data::directory();
    configure_common(context);
    context.use_certificate_chain_file((data / "tls_channel_test_server.pem").string());
    context.use_private_key_file((data / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
    context.load_verify_file((data / "tls_channel_test_client_ca.pem").string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server certificate and key do not match");
}

void configure_client(asio::ssl::context &context)
{
    const auto data = test_data::directory();
    configure_common(context);
    context.load_verify_file((data / "tls_channel_test_ca.pem").string());
    context.use_certificate_chain_file((data / "tls_channel_test_client.pem").string());
    context.use_private_key_file((data / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client certificate and key do not match");
}

TLSChannelConfig channel_config()
{
    TLSChannelConfig config;
    config.handshake_timeout = 2s;
    config.disconnect_timeout = 2s;
    config.heartbeat_interval = 100ms;
    config.heartbeat_timeout = 2s;
    return config;
}

asio::awaitable<void> http_origin_session(tcp::socket socket)
{
    std::string request;
    co_await asio::async_read_until(socket, asio::dynamic_buffer(request, 8192), "\r\n\r\n",
                                    asio::cancel_after(2s, asio::use_awaitable));
    constexpr std::string_view response =
        "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nrelay-chain";
    co_await asio::async_write(socket, asio::buffer(response), asio::use_awaitable);
}

asio::awaitable<void> echo_session(tcp::socket socket)
{
    std::array<char, 128> buffer{};
    for (;;)
    {
        auto [read_error, size] = co_await socket.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (read_error)
        {
            co_return;
        }
        auto [write_error, _] =
            co_await asio::async_write(socket, asio::buffer(buffer.data(), size), use_nothrow_awaitable);
        if (write_error)
        {
            co_return;
        }
    }
}

asio::awaitable<void> origin_accept_loop(tcp::acceptor &acceptor, bool http)
{
    for (;;)
    {
        auto [error, socket] = co_await acceptor.async_accept(use_nothrow_awaitable);
        if (error)
        {
            co_return;
        }
        if (http)
        {
            asio::co_spawn(acceptor.get_executor(), http_origin_session(std::move(socket)), asio::detached);
        }
        else
        {
            asio::co_spawn(acceptor.get_executor(), echo_session(std::move(socket)), asio::detached);
        }
    }
}

asio::awaitable<void> http_chain_probe(std::uint16_t proxy_port, std::uint16_t origin_port)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), proxy_port},
                                  asio::cancel_after(2s, asio::use_awaitable));
    const auto request = "GET http://127.0.0.1:" + std::to_string(origin_port) +
                         "/through-relay HTTP/1.1\r\nHost: ignored\r\n\r\n";
    co_await asio::async_write(socket, asio::buffer(request), asio::cancel_after(2s, asio::use_awaitable));
    std::string response;
    std::array<char, 1024> buffer{};
    for (;;)
    {
        auto [error, size] = co_await socket.async_read_some(
            asio::buffer(buffer), asio::cancel_after(3s, use_nothrow_awaitable));
        response.append(buffer.data(), size);
        if (error == asio::error::eof)
        {
            break;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
    }
    require(response.find("200 OK") != std::string::npos && response.ends_with("relay-chain"),
            "HTTP response did not traverse the RelayWeave tunnel");
}

asio::awaitable<void> socks_chain_probe(std::uint16_t proxy_port, std::uint16_t echo_port)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), proxy_port},
                                  asio::cancel_after(2s, asio::use_awaitable));
    constexpr std::array<std::uint8_t, 3> greeting{0x05, 0x01, 0x00};
    co_await asio::async_write(socket, asio::buffer(greeting), asio::use_awaitable);
    std::array<std::uint8_t, 2> method{};
    co_await asio::async_read(socket, asio::buffer(method), asio::cancel_after(3s, asio::use_awaitable));
    require(method == std::array<std::uint8_t, 2>{0x05, 0x00}, "SOCKS5 handshake did not traverse the tunnel");

    constexpr std::string_view host = "127.0.0.1";
    std::vector<std::uint8_t> request{0x05, 0x01, 0x00, 0x03, static_cast<std::uint8_t>(host.size())};
    request.insert(request.end(), host.begin(), host.end());
    request.push_back(static_cast<std::uint8_t>(echo_port >> 8));
    request.push_back(static_cast<std::uint8_t>(echo_port));
    co_await asio::async_write(socket, asio::buffer(request), asio::use_awaitable);
    std::array<std::uint8_t, 4> reply{};
    co_await asio::async_read(socket, asio::buffer(reply), asio::cancel_after(3s, asio::use_awaitable));
    require(reply[0] == 0x05 && reply[1] == 0x00, "SOCKS5 CONNECT failed through the tunnel");
    std::vector<std::uint8_t> endpoint(reply[3] == 0x04 ? 18 : 6);
    co_await asio::async_read(socket, asio::buffer(endpoint), asio::cancel_after(3s, asio::use_awaitable));

    constexpr std::string_view payload = "socks-relay-chain";
    co_await asio::async_write(socket, asio::buffer(payload), asio::use_awaitable);
    std::string echoed(payload.size(), '\0');
    co_await asio::async_read(socket, asio::buffer(echoed), asio::cancel_after(3s, asio::use_awaitable));
    require(echoed == payload, "SOCKS5 payload did not traverse the RelayWeave tunnel");
}

template <typename Probe> void eventually(Probe probe, std::string_view description)
{
    std::string last_failure;
    for (int attempt = 0; attempt < 40; ++attempt)
    {
        asio::io_context io;
        auto result = asio::co_spawn(io, probe(), asio::use_future);
        io.run();
        try
        {
            result.get();
            return;
        }
        catch (const std::exception &exception)
        {
            last_failure = exception.what();
            std::this_thread::sleep_for(100ms);
        }
    }
    throw std::runtime_error(std::string(description) + " did not become ready: " + last_failure);
}

} // namespace

int main()
{
    try
    {
        asio::ssl::context server_context(asio::ssl::context::tls_server);
        asio::ssl::context producer_context(asio::ssl::context::tls_client);
        asio::ssl::context consumer_context(asio::ssl::context::tls_client);
        configure_server(server_context);
        configure_client(producer_context);
        configure_client(consumer_context);

        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        asio::io_context proxy_io(1);
        const auto control_port = unused_port(control_io);
        const auto tcp_port = unique_port(transfer_tcp_io, {control_port});
        const auto tls_port = unique_port(transfer_tcp_io, {control_port, tcp_port});
        const auto proxy_socks_port = unique_port(proxy_io, {control_port, tcp_port, tls_port});
        const auto proxy_http_port = unique_port(proxy_io, {control_port, tcp_port, tls_port, proxy_socks_port});
        const auto http_origin_port = unique_port(proxy_io, {proxy_socks_port, proxy_http_port});
        const auto echo_port = unique_port(proxy_io, {proxy_socks_port, proxy_http_port, http_origin_port});
        const auto local_http_port = unique_port(transfer_tcp_io, {control_port, tcp_port, tls_port});
        const auto local_socks_port = unique_port(transfer_tcp_io, {control_port, tcp_port, tls_port, local_http_port});

        tcp::acceptor http_origin(proxy_io, {asio::ip::address_v4::loopback(), http_origin_port});
        tcp::acceptor echo_origin(proxy_io, {asio::ip::address_v4::loopback(), echo_port});
        asio::co_spawn(proxy_io, origin_accept_loop(http_origin, true), asio::detached);
        asio::co_spawn(proxy_io, origin_accept_loop(echo_origin, false), asio::detached);

        ProxyConfig proxy_config;
        proxy_config.socks5 = {"127.0.0.1", proxy_socks_port};
        proxy_config.http.address = "127.0.0.1";
        proxy_config.http.port = proxy_http_port;
        auto proxy = std::make_shared<ProxyServer>(proxy_io, proxy_config);
        proxy->start();

        NodeConfig node_config = make_test_node_config();
        node_config.control.address = "127.0.0.1";
        node_config.control.port = control_port;
        node_config.control.max_services = 8;
        node_config.control.max_services_per_session = 4;
        node_config.tcp = {"127.0.0.1", tcp_port};
        node_config.tls = {"127.0.0.1", tls_port};
        node_config.datagram.address = "127.0.0.1";
        node_config.datagram.port = tcp_port;
        node_config.channel = channel_config();
        auto node = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data.io, server_context,
                                                std::move(node_config));
        node->start();

        AgentConfig producer_config;
        producer_config.host = "127.0.0.1";
        producer_config.port = control_port;
        producer_config.connect_timeout = 1s;
        producer_config.reconnect_initial_delay = 50ms;
        producer_config.reconnect_max_delay = 200ms;
        producer_config.channel = channel_config();
        producer_config.services = {
            {"proxy-http", "127.0.0.1", proxy_http_port, RelayProtocol::Tls},
            {"proxy-socks5", "127.0.0.1", proxy_socks_port, RelayProtocol::Tls},
        };
        auto producer =
            std::make_shared<RelayAgent>(control_io, transfer_tcp_io, producer_context, std::move(producer_config));

        AgentConfig consumer_config;
        consumer_config.host = "127.0.0.1";
        consumer_config.port = control_port;
        consumer_config.connect_timeout = 1s;
        consumer_config.reconnect_initial_delay = 50ms;
        consumer_config.reconnect_max_delay = 200ms;
        consumer_config.relay_open_timeout = 2s;
        consumer_config.channel = channel_config();
        consumer_config.forwards = {
            {"proxy-http", "127.0.0.1", local_http_port, RelayProtocol::Tls},
            {"proxy-socks5", "127.0.0.1", local_socks_port, RelayProtocol::Tls},
        };
        auto consumer =
            std::make_shared<RelayAgent>(control_io, transfer_tcp_io, consumer_context, std::move(consumer_config));

        producer->start();
        consumer->start();
        std::thread control_thread([&]() { control_io.run(); });
        std::thread transfer_tcp_thread([&]() { transfer_tcp_io.run(); });
        std::thread transfer_udp_thread([&]() { transfer_udp_io.run(); });
        std::thread proxy_thread([&]() { proxy_io.run(); });

        eventually([=]() { return http_chain_probe(local_http_port, http_origin_port); }, "HTTP proxy chain");
        eventually([=]() { return socks_chain_probe(local_socks_port, echo_port); }, "SOCKS5 proxy chain");

        auto consumer_stopped = asio::co_spawn(control_io, consumer->async_stop(), asio::use_future);
        auto producer_stopped = asio::co_spawn(control_io, producer->async_stop(), asio::use_future);
        consumer_stopped.get();
        producer_stopped.get();
        node->stop();
        std::promise<void> proxy_stopped;
        auto proxy_stopped_result = proxy_stopped.get_future();
        asio::post(proxy_io, [&]() {
            proxy->stop();
            asio::error_code ignored;
            http_origin.close(ignored);
            echo_origin.close(ignored);
            proxy_stopped.set_value();
        });
        proxy_stopped_result.wait();
        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();
        proxy_thread.join();

        std::cout << "[PASS] HTTP and SOCKS5 application proxy through RelayWeave TLS tunnels\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
