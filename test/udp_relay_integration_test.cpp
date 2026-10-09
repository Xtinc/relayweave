#include "node_test_config.h"
#include "relay_agent.h"
#include "relay_node.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
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
using udp = asio::ip::udp;

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

struct Certificates
{
    std::filesystem::path server_ca;
    std::filesystem::path server_certificate;
    std::filesystem::path server_private_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_private_key;
};

Certificates certificates(const char *program)
{
    const auto directory = std::filesystem::absolute(program).parent_path() / "data";
    return {directory / "tls_channel_test_ca.pem",     directory / "tls_channel_test_server.pem",
            directory / "tls_channel_test_server.key", directory / "tls_channel_test_client_ca.pem",
            directory / "tls_channel_test_client.pem", directory / "tls_channel_test_client.key"};
}

void configure_server(asio::ssl::context &context, const Certificates &files)
{
    context.use_certificate_chain_file(files.server_certificate.string());
    context.use_private_key_file(files.server_private_key.string(), asio::ssl::context::pem);
    context.load_verify_file(files.client_ca.string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const Certificates &files)
{
    context.load_verify_file(files.server_ca.string());
    context.use_certificate_chain_file(files.client_certificate.string());
    context.use_private_key_file(files.client_private_key.string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client certificate and key do not match");
}

std::uint16_t unused_tcp_port(asio::io_context &io)
{
    tcp::acceptor socket(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return socket.local_endpoint().port();
}

std::uint16_t unused_udp_port(asio::io_context &io)
{
    udp::socket socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    return socket.local_endpoint().port();
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

AgentConfig client_config(std::uint16_t control_port)
{
    AgentConfig config;
    config.host = "127.0.0.1";
    config.port = control_port;
    config.connect_timeout = 1s;
    config.reconnect_initial_delay = 50ms;
    config.reconnect_max_delay = 200ms;
    // All protocols use the Agent establishment budget; running UDP has no expiry.
    config.relay_open_timeout = 1s;
    config.channel = channel_config();
    return config;
}

asio::awaitable<void> udp_echo(udp::socket &socket)
{
    std::array<std::uint8_t, 65535> buffer{};
    for (;;)
    {
        udp::endpoint source;
        auto [read_error, size] =
            co_await socket.async_receive_from(asio::buffer(buffer), source, use_nothrow_awaitable);
        if (read_error)
        {
            co_return;
        }
        auto [write_error, sent] =
            co_await socket.async_send_to(asio::buffer(buffer.data(), size), source, use_nothrow_awaitable);
        if (write_error || sent != size)
        {
            co_return;
        }
    }
}

void expect_round_trip(udp::socket &socket, const BytesBuf &payload)
{
    for (int attempt = 0; attempt < 40; ++attempt)
    {
        socket.send(asio::buffer(payload));
        for (int poll = 0; poll < 15; ++poll)
        {
            std::array<std::uint8_t, 65535> received{};
            asio::error_code error;
            const auto size = socket.receive(asio::buffer(received), 0, error);
            if (!error)
            {
                require(size == payload.size() && std::equal(payload.begin(), payload.end(), received.begin()),
                        "UDP relay payload mismatch");
                return;
            }
            if (error != asio::error::would_block && error != asio::error::try_again)
            {
                throw asio::system_error(error, "UDP relay receive failed");
            }
            std::this_thread::sleep_for(10ms);
        }
    }
    throw std::runtime_error("UDP relay did not become ready");
}

void expect_drop(udp::socket &socket, const BytesBuf &payload)
{
    socket.send(asio::buffer(payload));
    for (int poll = 0; poll < 30; ++poll)
    {
        std::array<std::uint8_t, 65535> received{};
        asio::error_code error;
        socket.receive(asio::buffer(received), 0, error);
        if (!error)
        {
            throw std::runtime_error("UDP datagram expected to be dropped was forwarded");
        }
        if (error != asio::error::would_block && error != asio::error::try_again)
        {
            throw asio::system_error(error, "UDP relay drop check failed");
        }
        std::this_thread::sleep_for(10ms);
    }
}

void run_9000_pps_baseline(udp::socket &socket)
{
    constexpr std::size_t packet_count = 18000;
    constexpr auto duration = std::chrono::seconds(2);
    std::array<std::uint8_t, 8> payload{};
    std::size_t received = 0;

    const auto drain = [&]() {
        for (;;)
        {
            std::array<std::uint8_t, 64> response{};
            asio::error_code error;
            const auto size = socket.receive(asio::buffer(response), 0, error);
            if (!error)
            {
                if (size == payload.size())
                {
                    ++received;
                }
                continue;
            }
            if (error == asio::error::would_block || error == asio::error::try_again)
            {
                return;
            }
            throw asio::system_error(error, "UDP baseline receive failed");
        }
    };

    drain();
    const auto started = std::chrono::steady_clock::now();
    for (std::size_t index = 0; index < packet_count; ++index)
    {
        const auto sequence = static_cast<std::uint32_t>(index);
        payload[0] = static_cast<std::uint8_t>(sequence >> 24);
        payload[1] = static_cast<std::uint8_t>(sequence >> 16);
        payload[2] = static_cast<std::uint8_t>(sequence >> 8);
        payload[3] = static_cast<std::uint8_t>(sequence);
        socket.send(asio::buffer(payload));
        drain();
        std::this_thread::sleep_until(started + duration * (index + 1) / packet_count);
    }
    const auto sent_at = std::chrono::steady_clock::now();
    const auto drain_deadline = sent_at + std::chrono::seconds(2);
    while (received < packet_count && std::chrono::steady_clock::now() < drain_deadline)
    {
        drain();
        std::this_thread::sleep_for(1ms);
    }
    drain();

    const auto elapsed = std::chrono::duration<double>(sent_at - started).count();
    const auto loss = 100.0 * static_cast<double>(packet_count - std::min(packet_count, received)) /
                      static_cast<double>(packet_count);
    std::cout << "[BASELINE] sent=" << packet_count << ", received=" << received
              << ", send_rate_pps=" << static_cast<double>(packet_count) / elapsed << ", loss=" << loss << "%\n";
}
} // namespace

int main(int argc, char *argv[])
{
    try
    {
        require(argc > 0, "Missing executable path");
        const auto files = certificates(argv[0]);

        asio::ssl::context server_context(asio::ssl::context::tls_server);
        asio::ssl::context producer_context(asio::ssl::context::tls_client);
        asio::ssl::context consumer_context(asio::ssl::context::tls_client);
        configure_server(server_context, files);
        configure_client(producer_context, files);
        configure_client(consumer_context, files);

        asio::io_context port_io;
        const auto control_port = unused_tcp_port(port_io);
        const auto tcp_data_port = unused_tcp_port(port_io);
        const auto udp_data_port = unused_udp_port(port_io);
        const auto target_port = unused_udp_port(port_io);
        const auto forward_port = unused_udp_port(port_io);
        auto second_forward_port = unused_udp_port(port_io);
        while (second_forward_port == udp_data_port || second_forward_port == target_port ||
               second_forward_port == forward_port)
        {
            second_forward_port = unused_udp_port(port_io);
        }

        asio::io_context server_control_io(1);
        asio::io_context server_transfer_tcp_io(1);
        asio::io_context server_transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        asio::io_context consumer_control_io(1);
        asio::io_context consumer_transfer_io(1);
        asio::io_context producer_control_io(1);
        asio::io_context producer_transfer_io(1);
        asio::io_context echo_io(1);

        udp::socket echo_socket(echo_io, udp::endpoint(asio::ip::address_v4::loopback(), target_port));
        asio::co_spawn(echo_io, udp_echo(echo_socket), asio::detached);
        std::thread echo_thread([&echo_io]() { echo_io.run(); });

        NodeConfig settings = make_test_node_config();
        settings.control.address = "127.0.0.1";
        settings.control.port = control_port;
        settings.tcp.address = "127.0.0.1";
        settings.tcp.port = tcp_data_port;
        settings.tls.address = "127.0.0.1";
        settings.tls.port = 0;
        settings.datagram.address = "127.0.0.1";
        settings.datagram.port = udp_data_port;
        settings.tcp.max_relays = 8;
        settings.datagram.max_relays = 8;
        settings.tcp.setup_timeout = 1s;
        settings.datagram.setup_timeout = 1s;
        settings.channel = channel_config();
        settings.datagram.traffic.rx_bytes_per_second = 100000000;
        settings.datagram.traffic.rx_burst_bytes = DatagramHeader::maximum_user_payload;
        settings.datagram.traffic.tx_bytes_per_second = 100000000;
        settings.datagram.traffic.tx_burst_bytes = DatagramHeader::maximum_user_payload;
        auto server = std::make_shared<RelayNode>(server_control_io, server_transfer_tcp_io,
                                                  server_transfer_udp_io, cluster_data.io, server_context, std::move(settings));
        server->start();
        std::thread server_control_thread([&server_control_io]() { server_control_io.run(); });
        std::thread server_transfer_tcp_thread([&server_transfer_tcp_io]() { server_transfer_tcp_io.run(); });
        std::thread server_transfer_udp_thread([&server_transfer_udp_io]() { server_transfer_udp_io.run(); });

        auto consumer_settings = client_config(control_port);
        consumer_settings.forwards.push_back(
            AgentForwardConfig{"udp-echo", "127.0.0.1", forward_port, RelayProtocol::Udp});
        consumer_settings.forwards.push_back(
            AgentForwardConfig{"udp-echo", "127.0.0.1", second_forward_port, RelayProtocol::Udp});
        const auto self_forward_port = unused_udp_port(port_io);
        consumer_settings.services.push_back({"udp-self", "127.0.0.1", target_port, RelayProtocol::Udp});
        consumer_settings.forwards.push_back({"udp-self", "127.0.0.1", self_forward_port, RelayProtocol::Udp});
        auto consumer = std::make_shared<RelayAgent>(consumer_control_io, consumer_transfer_io, consumer_context,
                                                    std::move(consumer_settings));
        consumer->start();
        std::thread consumer_control_thread([&consumer_control_io]() { consumer_control_io.run(); });
        std::thread consumer_transfer_thread([&consumer_transfer_io]() { consumer_transfer_io.run(); });

        auto producer_settings = client_config(control_port);
        producer_settings.services.push_back(
            AgentServiceConfig{"udp-echo", "127.0.0.1", target_port, RelayProtocol::Udp});
        auto producer = std::make_shared<RelayAgent>(producer_control_io, producer_transfer_io, producer_context,
                                                    std::move(producer_settings));
        std::this_thread::sleep_for(200ms);
        producer->start();
        std::thread producer_control_thread([&producer_control_io]() { producer_control_io.run(); });
        std::thread producer_transfer_thread([&producer_transfer_io]() { producer_transfer_io.run(); });

        asio::io_context probe_io;
        udp::socket probe(probe_io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
        probe.connect(udp::endpoint(asio::ip::address_v4::loopback(), forward_port));
        probe.non_blocking(true);
        udp::socket second_probe(probe_io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
        second_probe.connect(udp::endpoint(asio::ip::address_v4::loopback(), second_forward_port));
        second_probe.non_blocking(true);

        udp::socket self_probe(probe_io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
        self_probe.connect(udp::endpoint(asio::ip::address_v4::loopback(), self_forward_port));
        self_probe.non_blocking(true);

        const BytesBuf maximum_payload(DatagramHeader::maximum_user_payload, 0x5a);
        expect_round_trip(probe, maximum_payload);
        expect_round_trip(self_probe, maximum_payload);

        const BytesBuf oversized_payload(DatagramHeader::maximum_user_payload + 1, 0x6b);
        expect_drop(probe, oversized_payload);

        const BytesBuf payload{'u', 'd', 'p', '-', 'r', 'e', 'l', 'a', 'y'};
        expect_round_trip(probe, payload);
        expect_round_trip(second_probe, payload);

        const auto original_probe_port = probe.local_endpoint().port();
        probe.close();
        auto rebound_probe_port = unused_udp_port(port_io);
        while (rebound_probe_port == original_probe_port)
        {
            rebound_probe_port = unused_udp_port(port_io);
        }
        probe.open(udp::v4());
        probe.bind(udp::endpoint(asio::ip::address_v4::loopback(), rebound_probe_port));
        probe.connect(udp::endpoint(asio::ip::address_v4::loopback(), forward_port));
        probe.non_blocking(true);
        expect_round_trip(probe, payload);

        const auto attach_shaped_payload = WireMessage::pack(RelayAttach::to_msg({RelayAttach::Producer, 7, 9}));
        expect_round_trip(probe, attach_shaped_payload);

        asio::co_spawn(producer_control_io, producer->async_stop(), asio::use_future).get();
        producer_control_thread.join();
        producer_transfer_thread.join();
        std::this_thread::sleep_for(300ms);

        asio::io_context replacement_control_io(1);
        asio::io_context replacement_transfer_io(1);
        asio::ssl::context replacement_context(asio::ssl::context::tls_client);
        configure_client(replacement_context, files);
        auto replacement_settings = client_config(control_port);
        replacement_settings.services.push_back(
            AgentServiceConfig{"udp-echo", "127.0.0.1", target_port, RelayProtocol::Udp});
        auto replacement = std::make_shared<RelayAgent>(replacement_control_io, replacement_transfer_io,
                                                       replacement_context, std::move(replacement_settings));
        replacement->start();
        std::thread replacement_control_thread([&replacement_control_io]() { replacement_control_io.run(); });
        std::thread replacement_transfer_thread([&replacement_transfer_io]() { replacement_transfer_io.run(); });
        expect_round_trip(probe, payload);
        expect_round_trip(second_probe, payload);

        if (std::getenv("PROXY_UDP_9000_PPS"))
        {
            run_9000_pps_baseline(probe);
        }

        auto replacement_stopped =
            asio::co_spawn(replacement_control_io, replacement->async_stop(), asio::use_future);
        auto consumer_stopped = asio::co_spawn(consumer_control_io, consumer->async_stop(), asio::use_future);
        replacement_stopped.get();
        consumer_stopped.get();
        server->stop();
        asio::post(echo_io, [&echo_socket]() {
            asio::error_code ignored;
            echo_socket.close(ignored);
        });
        replacement_control_thread.join();
        replacement_transfer_thread.join();
        consumer_control_thread.join();
        consumer_transfer_thread.join();
        server_control_thread.join();
        server_transfer_tcp_thread.join();
        server_transfer_udp_thread.join();
        echo_thread.join();

        std::cout << "[PASS] Native UDP proactive setup, forwarding, and TLS lifecycle\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
