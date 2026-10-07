#include "node_test_config.h"
#include "relay_node.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

constexpr std::size_t udp_payload_bytes = 1400;
constexpr std::size_t relay_payload_bytes = udp_payload_bytes - DatagramHeader::length;
constexpr std::uint64_t benchmark_tag = 0x55445042454e4348ULL;
constexpr std::uint64_t warmup_tag = 0x5741524d55505021ULL;

struct Options
{
    std::chrono::seconds duration{5};
    std::uint64_t rate_pps = 0;
    std::chrono::milliseconds drain_timeout{1000};
};

struct Certificates
{
    std::filesystem::path server_ca;
    std::filesystem::path server_certificate;
    std::filesystem::path server_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_key;
};

struct ActiveRelay
{
    std::shared_ptr<TLSChannel> producer;
    std::shared_ptr<TLSChannel> consumer;
    std::uint64_t producer_session_id;
    std::uint64_t consumer_session_id;
};

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

void print_usage(std::string_view program)
{
    std::cout << "Usage: " << program
              << " [--duration-seconds N] [--rate-pps N] [--drain-ms N]\n"
                 "  --rate-pps 0 means unpaced saturation mode (default).\n";
}

std::uint64_t parse_unsigned(std::string_view text, std::string_view option)
{
    std::size_t consumed = 0;
    const auto value = std::stoull(std::string(text), &consumed);
    if (consumed != text.size())
    {
        throw std::invalid_argument(std::string(option) + " must be an unsigned integer");
    }
    return value;
}

Options parse_options(int argc, char *argv[])
{
    Options options;
    for (int index = 1; index < argc; ++index)
    {
        const std::string_view option = argv[index];
        if (option == "--help")
        {
            print_usage(argc > 0 ? argv[0] : "benchmark_udp_node");
            std::exit(0);
        }
        if (index + 1 >= argc)
        {
            throw std::invalid_argument(std::string(option) + " requires a value");
        }
        const std::string_view value = argv[++index];
        if (option == "--duration-seconds")
        {
            const auto seconds = parse_unsigned(value, option);
            require(seconds >= 1 && seconds <= 60, "--duration-seconds must be in [1, 60]");
            options.duration = std::chrono::seconds(seconds);
        }
        else if (option == "--rate-pps")
        {
            options.rate_pps = parse_unsigned(value, option);
            require(options.rate_pps <= 10'000'000, "--rate-pps must not exceed 10000000");
        }
        else if (option == "--drain-ms")
        {
            const auto milliseconds = parse_unsigned(value, option);
            require(milliseconds <= 10'000, "--drain-ms must not exceed 10000");
            options.drain_timeout = std::chrono::milliseconds(milliseconds);
        }
        else
        {
            throw std::invalid_argument("Unknown option: " + std::string(option));
        }
    }
    return options;
}

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
    context.use_private_key_file(files.server_key.string(), asio::ssl::context::pem);
    context.load_verify_file(files.client_ca.string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const Certificates &files)
{
    context.load_verify_file(files.server_ca.string());
    context.use_certificate_chain_file(files.client_certificate.string());
    context.use_private_key_file(files.client_key.string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client certificate and key do not match");
}

std::uint16_t unused_tcp_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
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
    config.heartbeat_interval = 1s;
    config.heartbeat_timeout = 10s;
    return config;
}

asio::awaitable<std::shared_ptr<TLSChannel>> connect_control(asio::ssl::context &context, std::uint16_t port)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), port),
                                  asio::cancel_after(2s, asio::use_awaitable));
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, channel_config());
    co_await channel->start("127.0.0.1");
    co_return channel;
}

asio::awaitable<CtrlMessage> receive_command(const std::shared_ptr<TLSChannel> &channel, std::string expected)
{
    auto message = co_await channel->async_receive();
    require(message.command == expected, "Expected " + expected + ", received " + message.command);
    co_return message;
}

const njson &params_of(const CtrlMessage &message)
{
    require(message.params && message.params->is_object(), "Control response params are invalid");
    return *message.params;
}

std::array<std::uint8_t, udp_payload_bytes> make_datagram(std::uint64_t session_id, std::uint64_t tag)
{
    std::array<std::uint8_t, udp_payload_bytes> datagram{};
    const auto session_header = DatagramHeader::encode(session_id);
    const auto tag_header = DatagramHeader::encode(tag);
    std::copy(session_header.begin(), session_header.end(), datagram.begin());
    std::copy(tag_header.begin(), tag_header.end(), datagram.begin() + DatagramHeader::length);
    return datagram;
}

void send_attach(udp::socket &socket, const udp::endpoint &server, int role, std::uint64_t uuid,
                 std::uint64_t ticket)
{
    const auto attach = WireMessage::pack(RelayAttach::to_msg({role, uuid, ticket}));
    const auto sent = socket.send_to(asio::buffer(attach), server);
    require(sent == attach.size(), "UDP relay attach was partially sent");
}

asio::awaitable<ActiveRelay> setup_relay(asio::ssl::context &producer_context,
                                         asio::ssl::context &consumer_context, std::uint16_t control_port,
                                         const udp::endpoint &server, udp::socket &source, udp::socket &sink)
{
    auto producer = co_await connect_control(producer_context, control_port);
    auto consumer = co_await connect_control(consumer_context, control_port);

    producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 1U}, {"service", "udp-benchmark"}, {"protocol", "udp"}}});
    co_await receive_command(producer, "service.ok");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 2U}, {"service", "udp-benchmark"}, {"protocol", "udp"}}});
    const auto opened = co_await receive_command(consumer, "relay.opened");
    const auto offered = co_await receive_command(producer, "relay.offer");
    const auto consumer_session_id = params_of(opened).at("session_id").get<std::uint64_t>();
    const auto producer_session_id = params_of(offered).at("session_id").get<std::uint64_t>();
    const auto uuid = params_of(opened).at("uuid").get<std::uint64_t>();
    const auto consumer_ticket = params_of(opened).at("ticket").get<std::uint64_t>();
    const auto producer_ticket = params_of(offered).at("ticket").get<std::uint64_t>();

    send_attach(source, server, RelayAttach::Producer, uuid, producer_ticket);
    send_attach(sink, server, RelayAttach::Consumer, uuid, consumer_ticket);
    co_await receive_command(producer, "relay.ready");
    co_await receive_command(consumer, "relay.ready");

    co_return ActiveRelay{std::move(producer), std::move(consumer), producer_session_id, consumer_session_id};
}

asio::awaitable<void> disconnect_relay(ActiveRelay relay)
{
    co_await relay.producer->async_disconnect();
    co_await relay.consumer->async_disconnect();
}

void write_sequence(std::array<std::uint8_t, udp_payload_bytes> &datagram, std::uint64_t sequence)
{
    constexpr auto offset = DatagramHeader::length * 2;
    for (std::size_t index = 0; index < sizeof(sequence); ++index)
    {
        datagram[offset + index] = static_cast<std::uint8_t>(sequence >> ((sizeof(sequence) - index - 1) * 8));
    }
}

void wait_for_send_slot(std::chrono::steady_clock::time_point deadline)
{
    for (;;)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            return;
        }
        const auto remaining = deadline - now;
        if (remaining > 500us)
        {
            std::this_thread::sleep_for(remaining - 250us);
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

void run_benchmark(const Options &options, udp::socket &source, udp::socket &sink, const udp::endpoint &server,
                   std::uint64_t producer_session_id, std::uint64_t consumer_session_id)
{
    asio::error_code ignored;
    source.set_option(asio::socket_base::send_buffer_size(4 * 1024 * 1024), ignored);
    sink.set_option(asio::socket_base::receive_buffer_size(4 * 1024 * 1024), ignored);
    sink.non_blocking(true);

    const auto expected_session = DatagramHeader::encode(consumer_session_id);
    const auto expected_tag = DatagramHeader::encode(benchmark_tag);
    const auto expected_warmup_tag = DatagramHeader::encode(warmup_tag);
    std::atomic<bool> stop_receiver{false};
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> invalid{0};

    std::thread receiver([&]() {
        std::array<std::uint8_t, udp_payload_bytes + 1> buffer{};
        udp::endpoint peer;
        while (!stop_receiver.load(std::memory_order_relaxed))
        {
            asio::error_code error;
            const auto size = sink.receive_from(asio::buffer(buffer), peer, 0, error);
            if (!error)
            {
                if (size == udp_payload_bytes &&
                    std::equal(expected_session.begin(), expected_session.end(), buffer.begin()) &&
                    std::equal(expected_tag.begin(), expected_tag.end(),
                               buffer.begin() + DatagramHeader::length))
                {
                    received.fetch_add(1, std::memory_order_relaxed);
                }
                else if (size == udp_payload_bytes &&
                         std::equal(expected_session.begin(), expected_session.end(), buffer.begin()) &&
                         std::equal(expected_warmup_tag.begin(), expected_warmup_tag.end(),
                                    buffer.begin() + DatagramHeader::length))
                {
                    continue;
                }
                else
                {
                    invalid.fetch_add(1, std::memory_order_relaxed);
                }
                continue;
            }
            if (error != asio::error::would_block && error != asio::error::try_again)
            {
                invalid.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::yield();
        }
    });

    auto warmup = make_datagram(producer_session_id, warmup_tag);
    for (std::uint64_t sequence = 0; sequence < 1000; ++sequence)
    {
        write_sequence(warmup, sequence);
        source.send_to(asio::buffer(warmup), server, 0, ignored);
    }
    std::this_thread::sleep_for(200ms);

    auto datagram = make_datagram(producer_session_id, benchmark_tag);
    std::uint64_t sent = 0;
    std::uint64_t send_errors = 0;
    std::uint64_t attempts = 0;
    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + options.duration;
    for (;;)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            break;
        }
        if (options.rate_pps != 0)
        {
            const auto target = started +
                                std::chrono::nanoseconds((attempts * 1'000'000'000ULL) / options.rate_pps);
            wait_for_send_slot(target);
            if (target >= deadline)
            {
                break;
            }
        }

        write_sequence(datagram, attempts++);
        asio::error_code error;
        const auto size = source.send_to(asio::buffer(datagram), server, 0, error);
        if (!error && size == datagram.size())
        {
            ++sent;
        }
        else
        {
            ++send_errors;
        }
    }
    const auto finished = std::chrono::steady_clock::now();
    const auto received_in_window = received.load(std::memory_order_relaxed);
    std::this_thread::sleep_for(options.drain_timeout);
    const auto delivered = received.load(std::memory_order_relaxed);
    stop_receiver.store(true, std::memory_order_relaxed);
    receiver.join();

    const auto elapsed = std::chrono::duration<double>(finished - started).count();
    const auto offered_pps = static_cast<double>(sent) / elapsed;
    const auto forwarded_pps = static_cast<double>(received_in_window) / elapsed;
    const auto delivered_pps = static_cast<double>(delivered) / elapsed;
    const auto loss = sent == 0
                          ? 0.0
                          : 100.0 * static_cast<double>(sent - std::min(sent, delivered)) / static_cast<double>(sent);
    const auto forwarded_mbps = forwarded_pps * static_cast<double>(udp_payload_bytes) * 8.0 / 1'000'000.0;

    std::cout << std::fixed << std::setprecision(2)
              << "[BENCHMARK] udp_payload_bytes=" << udp_payload_bytes
              << ", relay_payload_bytes=" << relay_payload_bytes << ", duration_seconds=" << elapsed
              << ", target_rate_pps=" << options.rate_pps << '\n'
              << "[BENCHMARK] sent=" << sent << ", send_errors=" << send_errors
              << ", received_in_window=" << received_in_window << ", received_after_drain=" << delivered
              << ", invalid=" << invalid.load(std::memory_order_relaxed) << '\n'
              << "[BENCHMARK] offered_pps=" << offered_pps << ", forwarded_pps=" << forwarded_pps
              << ", delivered_pps=" << delivered_pps << ", loss_percent=" << loss
              << ", forwarded_udp_mbps=" << forwarded_mbps << '\n';
}
} // namespace

int main(int argc, char *argv[])
{
    try
    {
        const auto options = parse_options(argc, argv);
        const auto files = certificates(argc > 0 ? argv[0] : "benchmark_udp_node");

        asio::ssl::context server_context(asio::ssl::context::tls_server);
        asio::ssl::context producer_context(asio::ssl::context::tls_client);
        asio::ssl::context consumer_context(asio::ssl::context::tls_client);
        configure_server(server_context, files);
        configure_client(producer_context, files);
        configure_client(consumer_context, files);

        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        asio::io_context endpoint_io(1);
        const auto control_port = unused_tcp_port(control_io);
        const auto tcp_port = unused_tcp_port(transfer_tcp_io);
        const auto udp_port = unused_udp_port(endpoint_io);
        const udp::endpoint server_endpoint(asio::ip::address_v4::loopback(), udp_port);
        udp::socket source(endpoint_io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
        udp::socket sink(endpoint_io, udp::endpoint(asio::ip::address_v4::loopback(), 0));

        NodeConfig config = make_test_node_config();
        config.control.address = "127.0.0.1";
        config.control.port = control_port;
        config.tcp.address = "127.0.0.1";
        config.tcp.port = tcp_port;
        config.tls.address = "127.0.0.1";
        config.tls.port = 0;
        config.datagram.address = "127.0.0.1";
        config.datagram.port = udp_port;
        config.tcp.max_relays = 1;
        config.datagram.max_relays = 1;
        config.tcp.setup_timeout = 5s;
        config.datagram.service_wait_timeout = 5s;
        config.channel = channel_config();

        auto server = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, server_context,
                                                  std::move(config));
        server->start();
        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_tcp_thread([&transfer_tcp_io]() { transfer_tcp_io.run(); });
        std::thread transfer_udp_thread([&transfer_udp_io]() { transfer_udp_io.run(); });

        try
        {
            auto relay_future = asio::co_spawn(
                control_io,
                setup_relay(producer_context, consumer_context, control_port, server_endpoint, source, sink),
                asio::use_future);
            auto relay = relay_future.get();
            run_benchmark(options, source, sink, server_endpoint, relay.producer_session_id,
                          relay.consumer_session_id);
            asio::co_spawn(control_io, disconnect_relay(std::move(relay)), asio::use_future).get();
            server->stop();
        }
        catch (...)
        {
            try
            {
                server->stop();
            }
            catch (...)
            {
            }
            control_io.stop();
            transfer_tcp_io.stop();
            transfer_udp_io.stop();
            control_thread.join();
            transfer_tcp_thread.join();
            transfer_udp_thread.join();
            throw;
        }

        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
