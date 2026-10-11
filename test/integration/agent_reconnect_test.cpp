#include "test_data.h"
#include "node_test_config.h"
#include "relay_node.h"
#include "relay_agent.h"

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <openssl/ssl.h>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

struct DataFiles
{
    std::filesystem::path server_ca;
    std::filesystem::path server_certificate;
    std::filesystem::path server_private_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_private_key;
};

DataFiles load_data_files()
{
    const auto directory = test_data::directory();
    DataFiles files{directory / "tls_channel_test_ca.pem",     directory / "tls_channel_test_server.pem",
                    directory / "tls_channel_test_server.key", directory / "tls_channel_test_client_ca.pem",
                    directory / "tls_channel_test_client.pem", directory / "tls_channel_test_client.key"};
    for (const auto &path : {files.server_ca, files.server_certificate, files.server_private_key, files.client_ca,
                             files.client_certificate, files.client_private_key})
    {
        require(std::filesystem::is_regular_file(path), "Missing test data file: " + path.string());
    }
    return files;
}

void configure_common(asio::ssl::context &context)
{
    context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1);
}

void configure_server(asio::ssl::context &context, const DataFiles &files)
{
    configure_common(context);
    context.use_certificate_chain_file(files.server_certificate.string());
    context.use_private_key_file(files.server_private_key.string(), asio::ssl::context::pem);
    context.load_verify_file(files.client_ca.string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const DataFiles &files)
{
    configure_common(context);
    context.load_verify_file(files.server_ca.string());
    context.use_certificate_chain_file(files.client_certificate.string());
    context.use_private_key_file(files.client_private_key.string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client certificate and key do not match");
}

std::uint16_t unused_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
}

TLSChannelConfig channel_config()
{
    TLSChannelConfig config;
    config.handshake_timeout = 1s;
    config.disconnect_timeout = 1s;
    config.heartbeat_interval = 100ms;
    config.heartbeat_timeout = 2s;
    return config;
}

NodeConfig server_config(std::uint16_t control_port, std::uint16_t transfer_port)
{
    NodeConfig config = make_test_node_config();
    config.control.address = "127.0.0.1";
    config.control.port = control_port;
    config.tcp.address = "127.0.0.1";
    config.tcp.port = transfer_port;
    config.tls.address = "127.0.0.1";
    config.tls.port = 0;
    config.datagram.address = "127.0.0.1";
    config.datagram.port = transfer_port;
    config.tcp.setup_timeout = 1s;
    config.datagram.setup_timeout = 1s;
    config.channel = channel_config();
    return config;
}

AgentConfig client_config(std::uint16_t control_port)
{
    AgentConfig config;
    config.host = "127.0.0.1";
    config.port = control_port;
    config.connect_timeout = 500ms;
    config.reconnect_initial_delay = 50ms;
    config.reconnect_max_delay = 200ms;
    config.relay_open_timeout = 1s;
    config.channel = channel_config();
    return config;
}

asio::awaitable<void> echo_session(tcp::socket socket)
{
    std::array<std::uint8_t, 4096> buffer{};
    for (;;)
    {
        auto [read_error, length] = co_await socket.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (read_error == asio::error::eof)
        {
            asio::error_code ignored;
            socket.shutdown(tcp::socket::shutdown_send, ignored);
            co_return;
        }
        if (read_error)
        {
            co_return;
        }
        auto [write_error, _] =
            co_await asio::async_write(socket, asio::buffer(buffer.data(), length), use_nothrow_awaitable);
        if (write_error)
        {
            co_return;
        }
    }
}

asio::awaitable<void> echo_accept_loop(tcp::acceptor &acceptor)
{
    for (;;)
    {
        auto [error, socket] = co_await acceptor.async_accept(use_nothrow_awaitable);
        if (error)
        {
            co_return;
        }
        asio::co_spawn(acceptor.get_executor(), echo_session(std::move(socket)), asio::detached);
    }
}

asio::awaitable<void> relay_round_trip(std::uint16_t forward_port)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), forward_port),
                                  asio::cancel_after(500ms, asio::use_awaitable));
    const std::string payload = "client reconnect payload";
    co_await asio::async_write(socket, asio::buffer(payload), asio::cancel_after(1s, asio::use_awaitable));
    std::string received(payload.size(), '\0');
    co_await asio::async_read(socket, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Relay round trip payload mismatch");
}

asio::awaitable<std::shared_ptr<tcp::socket>> open_active_relay(std::uint16_t forward_port)
{
    const auto executor = co_await asio::this_coro::executor;
    auto socket = std::make_shared<tcp::socket>(executor);
    co_await socket->async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), forward_port),
                                   asio::cancel_after(500ms, asio::use_awaitable));
    const std::string payload = "active relay";
    co_await asio::async_write(*socket, asio::buffer(payload), asio::cancel_after(1s, asio::use_awaitable));
    std::string received(payload.size(), '\0');
    co_await asio::async_read(*socket, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Active relay setup payload mismatch");
    co_return socket;
}

asio::awaitable<void> require_relay_closed(const std::shared_ptr<tcp::socket> &socket)
{
    std::array<std::uint8_t, 1> byte{};
    auto [error, _] =
        co_await socket->async_read_some(asio::buffer(byte), asio::cancel_after(2s, use_nothrow_awaitable));
    require(error && error != asio::error::timed_out && error != asio::error::operation_aborted,
            "Active relay survived its control epoch");
}

void wait_for_round_trip(asio::io_context &io, std::uint16_t forward_port)
{
    std::string last_failure;
    for (int attempt = 0; attempt < 30; ++attempt)
    {
        auto result = asio::co_spawn(io, relay_round_trip(forward_port), asio::use_future);
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
    throw std::runtime_error("Relay did not become ready: " + last_failure);
}
} // namespace

int main()
{
    std::thread control_thread;
    std::thread transfer_thread;
    std::thread server_udp_thread;
    try
    {
        const auto files = load_data_files();
        asio::ssl::context server_context(asio::ssl::context::tls_server);
        asio::ssl::context producer_context(asio::ssl::context::tls_client);
        asio::ssl::context consumer_context(asio::ssl::context::tls_client);
        configure_server(server_context, files);
        configure_client(producer_context, files);
        configure_client(consumer_context, files);

        asio::io_context control_io(1);
        asio::io_context transfer_io(1);
        asio::io_context server_udp_io(1);
        TestClusterDataIO cluster_data;
        auto control_work = asio::make_work_guard(control_io);
        auto transfer_work = asio::make_work_guard(transfer_io);
        const auto control_port = unused_port(control_io);
        auto transfer_port = unused_port(transfer_io);
        while (transfer_port == control_port)
        {
            transfer_port = unused_port(transfer_io);
        }
        auto target_port = unused_port(transfer_io);
        while (target_port == control_port || target_port == transfer_port)
        {
            target_port = unused_port(transfer_io);
        }
        auto forward_port = unused_port(transfer_io);
        while (forward_port == control_port || forward_port == transfer_port || forward_port == target_port)
        {
            forward_port = unused_port(transfer_io);
        }

        tcp::acceptor echo_acceptor(transfer_io, tcp::endpoint(asio::ip::address_v4::loopback(), target_port));
        asio::co_spawn(transfer_io, echo_accept_loop(echo_acceptor), asio::detached);

        auto server = std::make_shared<RelayNode>(control_io, transfer_io, server_udp_io, cluster_data.io, server_context,
                                                  server_config(control_port, transfer_port));
        server->start();

        auto producer_settings = client_config(control_port);
        producer_settings.services.push_back(AgentServiceConfig{"echo", "127.0.0.1", target_port});
        auto producer =
            std::make_shared<RelayAgent>(control_io, transfer_io, producer_context, std::move(producer_settings));
        auto consumer_settings = client_config(control_port);
        consumer_settings.forwards.push_back(AgentForwardConfig{"echo", "127.0.0.1", forward_port});
        auto consumer =
            std::make_shared<RelayAgent>(control_io, transfer_io, consumer_context, std::move(consumer_settings));
        std::weak_ptr<RelayAgent> weak_producer = producer;
        std::weak_ptr<RelayAgent> weak_consumer = consumer;
        producer->start();
        consumer->start();

        control_thread = std::thread([&control_io]() { control_io.run(); });
        transfer_thread = std::thread([&transfer_io]() { transfer_io.run(); });
        server_udp_thread = std::thread([&server_udp_io]() { server_udp_io.run(); });
        ScopeGuard thread_cleanup([&]() noexcept {
            control_io.stop();
            transfer_io.stop();
            server_udp_io.stop();
            if (control_thread.joinable())
            {
                control_thread.join();
            }
            if (transfer_thread.joinable())
            {
                transfer_thread.join();
            }
            if (server_udp_thread.joinable())
            {
                server_udp_thread.join();
            }
        });
        wait_for_round_trip(control_io, forward_port);

        auto active_relay_result =
            asio::co_spawn(control_io, open_active_relay(forward_port), asio::use_future);
        auto active_relay = active_relay_result.get();
        server->stop();
        server.reset();
        server_udp_thread.join();
        auto active_relay_closed =
            asio::co_spawn(control_io, require_relay_closed(active_relay), asio::use_future);
        active_relay_closed.get();
        active_relay.reset();
        server_udp_io.restart();
        auto replacement = std::make_shared<RelayNode>(control_io, transfer_io, server_udp_io, cluster_data.io, server_context,
                                                       server_config(control_port, transfer_port));
        replacement->start();
        server_udp_thread = std::thread([&server_udp_io]() { server_udp_io.run(); });
        wait_for_round_trip(control_io, forward_port);

        auto consumer_stopped = asio::co_spawn(control_io, consumer->async_stop(), asio::use_future);
        auto producer_stopped = asio::co_spawn(control_io, producer->async_stop(), asio::use_future);
        consumer_stopped.get();
        producer_stopped.get();
        consumer.reset();
        producer.reset();
        replacement->stop();
        replacement.reset();
        asio::post(transfer_io, [&echo_acceptor]() {
            asio::error_code ignored;
            echo_acceptor.close(ignored);
        });
        control_work.reset();
        transfer_work.reset();
        server_udp_thread.join();
        control_thread.join();
        transfer_thread.join();
        thread_cleanup.dismiss();
        require(weak_producer.expired() && weak_consumer.expired(), "Stopped clients retained asynchronous work");
        std::cout << "[PASS] Client forwarding survives control server restart\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
