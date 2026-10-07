#include "node_test_config.h"
#include "relay_node.h"

#include <algorithm>
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
    std::filesystem::path server_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_key;
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

asio::awaitable<std::shared_ptr<TLSChannel>> connect_control(asio::ssl::context &context, std::uint16_t port)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), port),
                                  asio::cancel_after(1s, asio::use_awaitable));
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

BytesBuf udp_datagram(std::uint64_t session_id, std::span<const std::uint8_t> payload = {})
{
    const auto header = DatagramHeader::encode(session_id);
    BytesBuf datagram;
    datagram.reserve(header.size() + payload.size());
    datagram.insert(datagram.end(), header.begin(), header.end());
    datagram.insert(datagram.end(), payload.begin(), payload.end());
    return datagram;
}

BytesBuf udp_attach(int role, std::uint64_t uuid, std::uint64_t ticket)
{
    return WireMessage::pack(RelayAttach::to_msg({role, uuid, ticket}));
}

asio::awaitable<void> send_datagram(udp::socket &socket, const udp::endpoint &server, const BytesBuf &datagram)
{
    const auto sent = co_await socket.async_send_to(asio::buffer(datagram), server, asio::use_awaitable);
    require(sent == datagram.size(), "UDP test datagram was partially sent");
}

asio::awaitable<BytesBuf> receive_datagram(udp::socket &socket)
{
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload> buffer{};
    udp::endpoint source;
    auto [error, size] = co_await socket.async_receive_from(
        asio::buffer(buffer), source, asio::cancel_after(1s, use_nothrow_awaitable));
    if (error)
    {
        throw asio::system_error(error, "UDP test receive failed");
    }
    co_return BytesBuf(buffer.begin(), buffer.begin() + size);
}

asio::awaitable<void> require_no_datagram(udp::socket &socket, std::string message)
{
    std::array<std::uint8_t, 64> buffer{};
    udp::endpoint source;
    auto [error, _] = co_await socket.async_receive_from(
        asio::buffer(buffer), source, asio::cancel_after(150ms, use_nothrow_awaitable));
    if (!error)
    {
        throw std::runtime_error(std::move(message));
    }
    require(error == asio::error::timed_out || error == asio::error::operation_aborted,
            "Unexpected UDP drop-check error: " + error.message());
}

asio::awaitable<void> require_no_control_message(const std::shared_ptr<TLSChannel> &channel, std::string message)
{
    try
    {
        const auto unexpected = co_await channel->async_receive(250ms);
        throw std::runtime_error(std::move(message) + ": " + unexpected.command);
    }
    catch (const asio::system_error &error)
    {
        require(error.code() == asio::error::timed_out || error.code() == asio::error::operation_aborted ||
                    error.code() == asio::experimental::error::channel_cancelled,
                "Unexpected control receive error: " + error.code().message());
    }
}

asio::awaitable<void> verify_routing(asio::ssl::context &producer_context, asio::ssl::context &consumer_context,
                                     std::uint16_t control_port, std::uint16_t udp_port)
{
    auto producer = co_await connect_control(producer_context, control_port);
    auto consumer = co_await connect_control(consumer_context, control_port);
    producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 2U}, {"service", "tcp-session"}, {"protocol", "tcp"}}});
    co_await receive_command(producer, "service.ok");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 10U}, {"service", "tcp-session"}, {"protocol", "tcp"}}});
    const auto tcp_opened = co_await receive_command(consumer, "relay.opened");
    co_await receive_command(producer, "relay.offer");
    const auto tcp_uuid = params_of(tcp_opened).at("uuid").get<std::uint64_t>();

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 20U}, {"service", "udp-session"}, {"protocol", "udp"}}});
    const auto opened = co_await receive_command(consumer, "relay.opened");
    const auto &consumer_params = params_of(opened);
    const auto udp_uuid = consumer_params.at("uuid").get<std::uint64_t>();
    require(udp_uuid > tcp_uuid, "TCP and UDP managers did not share a monotonic UUID allocator");
    const auto consumer_id = consumer_params.at("session_id").get<std::uint64_t>();
    const auto consumer_ticket = consumer_params.at("ticket").get<std::uint64_t>();

    const auto executor = co_await asio::this_coro::executor;
    udp::socket consumer_socket(executor, udp::endpoint(udp::v4(), 0));
    const udp::endpoint server(asio::ip::address_v4::loopback(), udp_port);
    co_await send_datagram(consumer_socket, server,
                           udp_attach(RelayAttach::Consumer, udp_uuid, consumer_ticket));

    producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 1U}, {"service", "udp-session"}, {"protocol", "udp"}}});
    const auto first_registration_response = co_await producer->async_receive();
    const auto second_registration_response = co_await producer->async_receive();
    const auto responses_are_ok_and_offer =
        (first_registration_response.command == "service.ok" &&
         second_registration_response.command == "relay.offer") ||
        (first_registration_response.command == "relay.offer" &&
         second_registration_response.command == "service.ok");
    require(responses_are_ok_and_offer,
            "Expected service.ok and relay.offer after UDP registration, received " +
                first_registration_response.command + " and " + second_registration_response.command);
    const auto &offered = first_registration_response.command == "relay.offer" ? first_registration_response
                                                                                : second_registration_response;
    const auto &producer_params = params_of(offered);
    const auto producer_id = producer_params.at("session_id").get<std::uint64_t>();
    const auto producer_ticket = producer_params.at("ticket").get<std::uint64_t>();
    require(consumer_id != 0 && producer_id != 0 && consumer_id != producer_id,
            "UDP sides did not receive distinct non-zero session IDs");
    require(consumer_ticket != 0 && producer_ticket != 0 && consumer_ticket != producer_ticket,
            "UDP sides did not receive distinct non-zero attach tickets");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 11U}, {"service", "tcp-session"}, {"protocol", "tcp"}}});
    const auto tcp_capacity = co_await receive_command(consumer, "relay.error");
    require(params_of(tcp_capacity).at("reason") == "relay capacity reached",
            "TCP capacity was not enforced independently");
    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 21U}, {"service", "udp-session"}, {"protocol", "udp"}}});
    const auto udp_capacity = co_await receive_command(consumer, "relay.error");
    require(params_of(udp_capacity).at("reason") == "relay capacity reached",
            "UDP capacity was not enforced independently");

    consumer->send(CtrlMessage{"relay.cancel", njson{{"uuid", tcp_uuid}}});
    const auto tcp_cancelled = co_await receive_command(consumer, "relay.error");
    require(params_of(tcp_cancelled).at("protocol") == "tcp", "TCP cancellation reached the wrong manager");
    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 12U}, {"service", "tcp-session"}, {"protocol", "tcp"}}});
    const auto replacement_opened = co_await receive_command(consumer, "relay.opened");
    co_await receive_command(producer, "relay.offer");
    const auto replacement_uuid = params_of(replacement_opened).at("uuid").get<std::uint64_t>();
    require(replacement_uuid > udp_uuid, "Relay UUIDs were not monotonic across alternating protocols");
    producer->send(CtrlMessage{"relay.reject", njson{{"uuid", replacement_uuid}}});
    const auto tcp_rejected = co_await receive_command(consumer, "relay.error");
    require(params_of(tcp_rejected).at("protocol") == "tcp", "TCP rejection reached the wrong manager");

    udp::socket producer_socket(executor, udp::endpoint(udp::v4(), 0));
    udp::socket attacker_socket(executor, udp::endpoint(udp::v4(), 0));

    const BytesBuf early_payload{'e', 'a', 'r', 'l', 'y'};
    co_await send_datagram(consumer_socket, server, udp_datagram(consumer_id, early_payload));
    co_await require_no_datagram(producer_socket, "Payload sent before relay attach was forwarded");

    co_await send_datagram(attacker_socket, server,
                           udp_attach(RelayAttach::Producer, udp_uuid, producer_ticket ^ 1U));
    co_await send_datagram(producer_socket, server,
                           udp_attach(RelayAttach::Producer, udp_uuid, producer_ticket ^ 1U));
    co_await send_datagram(consumer_socket, server,
                           udp_attach(RelayAttach::Consumer, udp_uuid, producer_ticket));
    co_await send_datagram(producer_socket, server,
                           udp_attach(RelayAttach::Producer, udp_uuid, producer_ticket));
    co_await send_datagram(consumer_socket, server,
                           udp_attach(RelayAttach::Consumer, udp_uuid, consumer_ticket));
    co_await receive_command(producer, "relay.ready");
    co_await receive_command(consumer, "relay.ready");

    const BytesBuf hijack_payload{'h', 'i', 'j', 'a', 'c', 'k'};
    auto unknown_id = producer_id ^ 0x8000000000000000ULL;
    if (unknown_id == 0 || unknown_id == consumer_id)
    {
        unknown_id ^= 1;
    }
    co_await send_datagram(attacker_socket, server, udp_datagram(unknown_id, hijack_payload));
    co_await require_no_datagram(consumer_socket, "Unknown UDP session ID was forwarded");

    co_await send_datagram(attacker_socket, server, udp_datagram(producer_id, hijack_payload));
    co_await require_no_datagram(consumer_socket, "A different endpoint reused the Producer session ID");
    co_await send_datagram(attacker_socket, server,
                           udp_attach(RelayAttach::Producer, udp_uuid, producer_ticket));
    co_await require_no_datagram(consumer_socket, "A different endpoint replayed the Producer attach ticket");

    const auto attach_shaped_payload = WireMessage::pack(RelayAttach::to_msg({RelayAttach::Producer, 7, 9}));
    co_await send_datagram(producer_socket, server, udp_datagram(producer_id, attach_shaped_payload));
    const auto forwarded = co_await receive_datagram(consumer_socket);
    require(forwarded.size() == DatagramHeader::length + attach_shaped_payload.size(),
            "Forwarded UDP datagram has the wrong size");
    const auto forwarded_id = DatagramHeader::decode(
        std::span(forwarded.data(), DatagramHeader::length));
    require(forwarded_id && *forwarded_id == consumer_id, "Server did not rewrite the destination session ID");
    require(std::equal(attach_shaped_payload.begin(), attach_shaped_payload.end(),
                       forwarded.begin() + DatagramHeader::length),
            "Server modified UDP user payload");

    const BytesBuf tx_payload{'u', 'd', 'p', '-', 't', 'x'};
    co_await send_datagram(consumer_socket, server, udp_datagram(consumer_id, tx_payload));
    const auto reverse_forwarded = co_await receive_datagram(producer_socket);
    require(reverse_forwarded.size() == DatagramHeader::length + tx_payload.size(),
            "Reverse UDP datagram has the wrong size");
    require(std::equal(tx_payload.begin(), tx_payload.end(),
                       reverse_forwarded.begin() + DatagramHeader::length),
            "Server modified reverse UDP user payload");

    co_await send_datagram(producer_socket, server, udp_datagram(producer_id));
    const auto empty = co_await receive_datagram(consumer_socket);
    require(empty.size() == DatagramHeader::length, "Zero-length UDP payload was not forwarded");

    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 30U}}});
    const auto traffic = co_await receive_command(consumer, "server.traffic.reported");
    const auto &services = params_of(traffic).at("services");
    require(services.size() == 2, "Traffic response did not report both registered services");
    const auto find_service_stats = [&](const char *name) {
        return std::ranges::find_if(
            services, [name](const njson &entry) { return entry.at("service") == name; });
    };
    require(find_service_stats("tcp-session") != services.end(), "TCP service was missing from traffic response");
    const auto stats = find_service_stats("udp-session");
    require(stats != services.end(), "UDP service was missing from traffic response");
    require(stats->at("protocol") == "udp", "UDP traffic response reported the wrong protocol");
    require(stats->at("rx_bytes") == attach_shaped_payload.size(),
            "UDP RX traffic included headers, invalid datagrams, or dropped payloads");
    require(stats->at("tx_bytes") == tx_payload.size(), "UDP TX payload bytes were counted incorrectly");

    asio::steady_timer sample_wait(co_await asio::this_coro::executor);
    sample_wait.expires_after(1100ms);
    co_await sample_wait.async_wait(asio::use_awaitable);
    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 31U}}});
    const auto sampled = co_await receive_command(consumer, "server.traffic.reported");
    const auto &sampled_services = params_of(sampled).at("services");
    const auto sampled_stats = std::ranges::find_if(sampled_services, [](const njson &entry) {
        return entry.at("service") == "udp-session";
    });
    require(sampled_stats != sampled_services.end(), "Sampled UDP service was missing from traffic response");
    require(sampled_stats->at("rx_bytes_per_second").get<std::uint64_t>() > 0,
            "UDP RX bandwidth EMA was not sampled");
    require(sampled_stats->at("tx_bytes_per_second").get<std::uint64_t>() > 0,
            "UDP TX bandwidth EMA was not sampled");

    co_await producer->async_disconnect();
    const auto closed = co_await receive_command(consumer, "relay.closed");
    require(params_of(closed).at("protocol") == "udp", "UDP close notification lost its protocol");
    co_await send_datagram(consumer_socket, server, udp_datagram(consumer_id, early_payload));
    co_await require_no_datagram(producer_socket, "Expired UDP session ID remained routable");

    auto disconnecting_consumer = co_await connect_control(consumer_context, control_port);
    disconnecting_consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 38U}, {"service", "disconnect-waiting"}, {"protocol", "udp"}}});
    co_await receive_command(disconnecting_consumer, "relay.opened");
    co_await disconnecting_consumer->async_disconnect();

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 39U}, {"service", "after-disconnect"}, {"protocol", "udp"}}});
    const auto after_disconnect = co_await receive_command(consumer, "relay.opened");
    require(params_of(after_disconnect).at("request_id") == 39U,
            "Disconnecting a waiting Consumer did not release UDP capacity");
    consumer->send(CtrlMessage{"relay.cancel", njson{{"uuid", params_of(after_disconnect).at("uuid")}}});
    co_await receive_command(consumer, "relay.error");

    auto late_producer = co_await connect_control(producer_context, control_port);
    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 42U}, {"service", "near-timeout"}, {"protocol", "udp"}}});
    const auto near_timeout_opened = co_await receive_command(consumer, "relay.opened");
    asio::steady_timer near_timeout_wait(executor);
    near_timeout_wait.expires_after(2200ms);
    co_await near_timeout_wait.async_wait(asio::use_awaitable);

    late_producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 42U}, {"service", "near-timeout"}, {"protocol", "udp"}}});
    const auto late_first = co_await late_producer->async_receive();
    const auto late_second = co_await late_producer->async_receive();
    require((late_first.command == "service.ok" && late_second.command == "relay.offer") ||
                (late_first.command == "relay.offer" && late_second.command == "service.ok"),
            "Producer registration near the service wait deadline did not match the waiting Relay");
    const auto &near_timeout_offer = late_first.command == "relay.offer" ? late_first : late_second;
    const auto &near_consumer_params = params_of(near_timeout_opened);
    const auto &near_producer_params = params_of(near_timeout_offer);
    asio::steady_timer attach_wait(executor);
    attach_wait.expires_after(1100ms);
    co_await attach_wait.async_wait(asio::use_awaitable);
    udp::socket near_consumer_socket(executor, udp::endpoint(udp::v4(), 0));
    udp::socket near_producer_socket(executor, udp::endpoint(udp::v4(), 0));
    const auto near_uuid = near_consumer_params.at("uuid").get<std::uint64_t>();
    co_await send_datagram(near_consumer_socket, server,
                           udp_attach(RelayAttach::Consumer, near_uuid,
                                      near_consumer_params.at("ticket").get<std::uint64_t>()));
    co_await send_datagram(near_producer_socket, server,
                           udp_attach(RelayAttach::Producer, near_uuid,
                                      near_producer_params.at("ticket").get<std::uint64_t>()));
    co_await receive_command(consumer, "relay.ready");
    co_await receive_command(late_producer, "relay.ready");
    consumer->send(CtrlMessage{"relay.cancel", njson{{"uuid", near_uuid}}});
    co_await receive_command(consumer, "relay.closed");
    const auto producer_closed = co_await receive_command(late_producer, "relay.closed");
    require(params_of(producer_closed).at("uuid") == near_uuid,
            "UDP Producer was not notified when the active Relay closed");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 40U}, {"service", "never-online"}, {"protocol", "udp"}}});
    const auto waiting = co_await receive_command(consumer, "relay.opened");
    require(params_of(waiting).at("request_id") == 40U, "UDP waiting relay returned the wrong request ID");
    const auto timed_out = co_await consumer->async_receive(4s);
    require(timed_out.command == "relay.error", "UDP waiting relay did not time out");
    require(params_of(timed_out).at("request_id") == 40U &&
                params_of(timed_out).at("reason") == "service wait timed out",
            "UDP waiting relay returned the wrong timeout error");

    late_producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 43U}, {"service", "never-online"}, {"protocol", "udp"}}});
    const auto late_registration = co_await receive_command(late_producer, "service.ok");
    require(params_of(late_registration).at("request_id") == 43U, "Late service registration returned the wrong ID");
    co_await require_no_control_message(late_producer, "Timed-out UDP Relay was revived by late registration");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 41U}, {"service", "still-offline"}, {"protocol", "udp"}}});
    const auto replacement = co_await receive_command(consumer, "relay.opened");
    require(params_of(replacement).at("request_id") == 41U,
            "UDP waiting timeout did not release relay capacity");
    consumer->send(CtrlMessage{"relay.cancel", njson{{"uuid", params_of(replacement).at("uuid")}}});
    const auto cancelled = co_await receive_command(consumer, "relay.error");
    require(params_of(cancelled).at("request_id") == 41U, "Replacement UDP waiting relay was not cancelled");

    auto aborting_consumer = co_await connect_control(consumer_context, control_port);
    aborting_consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 44U}, {"service", "never-online"}, {"protocol", "udp"}}});
    const auto aborting_opened = co_await receive_command(aborting_consumer, "relay.opened");
    const auto aborting_offer = co_await receive_command(late_producer, "relay.offer");
    const auto aborting_uuid = params_of(aborting_opened).at("uuid").get<std::uint64_t>();
    require(params_of(aborting_offer).at("uuid") == aborting_uuid, "UDP offer used the wrong Relay UUID");
    co_await aborting_consumer->async_disconnect();
    const auto abandoned = co_await receive_command(late_producer, "relay.closed");
    require(params_of(abandoned).at("uuid") == aborting_uuid,
            "UDP Producer was not released when the Consumer disconnected before ready");

    co_await late_producer->async_disconnect();
    co_await consumer->async_disconnect();
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

        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        const auto control_port = unused_tcp_port(control_io);
        const auto tcp_port = unused_tcp_port(transfer_tcp_io);
        const auto udp_port = unused_udp_port(transfer_udp_io);

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
        config.tcp.setup_timeout = 3s;
        config.datagram.service_wait_timeout = 3s;
        config.channel = channel_config();
        auto server = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data.io, server_context,
                                                  std::move(config));
        server->start();

        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_tcp_thread([&transfer_tcp_io]() { transfer_tcp_io.run(); });
        std::thread transfer_udp_thread([&transfer_udp_io]() { transfer_udp_io.run(); });
        auto result = asio::co_spawn(control_io,
                                     verify_routing(producer_context, consumer_context, control_port, udp_port),
                                     asio::use_future);
        try
        {
            result.get();
        }
        catch (...)
        {
            control_io.stop();
            transfer_tcp_io.stop();
            transfer_udp_io.stop();
            control_thread.join();
            transfer_tcp_thread.join();
            transfer_udp_thread.join();
            throw;
        }
        server->stop();
        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();

        std::cout << "[PASS] UDP session routing, endpoint pinning, and cleanup\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
