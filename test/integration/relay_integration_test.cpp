#include "test_data.h"
#include "node_test_config.h"
#include "relay_agent.h"
#include "relay_node.h"

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
struct DataFiles
{
    std::filesystem::path server_ca;
    std::filesystem::path server_certificate;
    std::filesystem::path server_private_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_private_key;
};

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

DataFiles load_data_files()
{
    const auto directory = test_data::directory();
    DataFiles files{directory / "tls_channel_test_ca.pem",     directory / "tls_channel_test_server.pem",
                    directory / "tls_channel_test_server.key", directory / "tls_channel_test_client_ca.pem",
                    directory / "tls_channel_test_client.pem", directory / "tls_channel_test_client.key"};
    for (const auto &path :
         {files.server_ca, files.server_certificate, files.server_private_key, files.client_ca,
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
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server test certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const DataFiles &files)
{
    configure_common(context);
    context.load_verify_file(files.server_ca.string());
    context.use_certificate_chain_file(files.client_certificate.string());
    context.use_private_key_file(files.client_private_key.string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client test certificate and key do not match");
}

std::uint16_t unused_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
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

const njson &params_of(const CtrlMessage &message)
{
    require(message.params.has_value() && message.params->is_object(),
            "Control response params must be an object: " + message.command);
    return *message.params;
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
    require(message.command == expected, "Expected control command " + expected + ", received " + message.command);
    co_return message;
}

asio::awaitable<tcp::socket> connect_relay_half(std::uint16_t transfer_port, int role, uint64_t uuid,
                                                uint64_t ticket)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), transfer_port),
                                  asio::cancel_after(1s, asio::use_awaitable));
    const auto frame = WireMessage::pack(RelayAttach::to_msg({role, uuid, ticket}));
    co_await asio::async_write(socket, asio::buffer(frame), asio::cancel_after(1s, asio::use_awaitable));
    co_return std::move(socket);
}

asio::awaitable<tcp::socket> connect_transfer_socket(std::uint16_t transfer_port)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), transfer_port),
                                  asio::cancel_after(1s, asio::use_awaitable));
    co_return std::move(socket);
}

asio::awaitable<void> require_socket_closed(tcp::socket &socket, const std::string &message)
{
    std::array<std::uint8_t, 1> buffer{};
    auto [error, _] =
        co_await socket.async_read_some(asio::buffer(buffer), asio::cancel_after(1s, use_nothrow_awaitable));
    if (!error || error == asio::error::timed_out || error == asio::error::operation_aborted)
    {
        throw std::runtime_error(message);
    }
}

asio::awaitable<void> verify_incomplete_relay_lifecycle(asio::ssl::context &producer_context,
                                                        asio::ssl::context &consumer_context,
                                                        std::uint16_t control_port, std::uint16_t transfer_port)
{
    auto producer = co_await connect_control(producer_context, control_port);
    auto consumer = co_await connect_control(consumer_context, control_port);

    asio::steady_timer probe_wait(co_await asio::this_coro::executor);
    probe_wait.expires_after(1300ms);
    co_await probe_wait.async_wait(asio::use_awaitable);

    consumer->send(CtrlMessage{"server.load", njson{{"request_id", 9U}}});
    const auto load = co_await receive_command(consumer, "server.loaded");
    const auto &load_params = params_of(load);
    require(load_params.at("request_id") == 9U, "Server load response returned the wrong request ID");
    for (const auto *field : {"control_queue_delay_us", "transfer_tcp_queue_delay_us",
                              "transfer_udp_queue_delay_us"})
    {
        const auto &delay = load_params.at(field);
        require(delay.is_number_unsigned(), std::string("Invalid server load field: ") + field);
        require(delay.get<std::uint32_t>() != std::numeric_limits<std::uint32_t>::max(),
                std::string("Server load field did not receive its first sample: ") + field);
    }
    require(load_params.at("transfer_tcp_queue_delay_us").get<std::uint32_t>() >= 50'000,
            "TCP transfer executor blocking work was not reflected in its queue delay");
    require(load_params.at("transfer_udp_queue_delay_us").get<std::uint32_t>() >= 50'000,
            "UDP transfer executor blocking work was not reflected in its queue delay");

    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 30U}}});
    const auto empty_traffic = co_await receive_command(consumer, "server.traffic.reported");
    require(params_of(empty_traffic).at("request_id") == 30U, "Traffic response returned the wrong request ID");
    require(params_of(empty_traffic).at("services").empty(), "Unregistered services appeared in traffic response");

    producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 1U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto registered = co_await receive_command(producer, "service.ok");
    require(params_of(registered).at("service") == "lifecycle", "Lifecycle service registration failed");

    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 31U}}});
    const auto initial_traffic = co_await receive_command(consumer, "server.traffic.reported");
    const auto &initial_services = params_of(initial_traffic).at("services");
    require(initial_services.size() == 1, "Registered service was missing from traffic response");
    const auto &initial_stats = initial_services.at(0);
    require(initial_stats.at("service") == "lifecycle" && initial_stats.at("protocol") == "tcp",
            "Traffic response identified the wrong service");
    for (const auto *field : {"rx_bytes", "tx_bytes", "rx_bytes_per_second", "tx_bytes_per_second"})
    {
        require(initial_stats.at(field).is_number_unsigned() && initial_stats.at(field) == 0,
                std::string("New service traffic field was not zero: ") + field);
    }
    require(initial_stats.at("accessors").empty(), "New service reported an active accessor");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 8U}, {"service", "lifecycle"}, {"protocol", "udp"}}});
    const auto protocol_mismatch = co_await receive_command(consumer, "relay.error");
    require(params_of(protocol_mismatch).at("reason") == "service protocol mismatch",
            "Service protocol mismatch was not rejected");

    producer->send(CtrlMessage{
        "service.register", njson{{"request_id", 2U}, {"service", "extra"}, {"protocol", "tcp"}}});
    const auto service_limit = co_await receive_command(producer, "service.error");
    require(params_of(service_limit).at("reason") == "session service capacity reached",
            "Per-session service capacity was not enforced");

    auto stalled_transfer_one = co_await connect_transfer_socket(transfer_port);
    auto stalled_transfer_two = co_await connect_transfer_socket(transfer_port);
    auto excess_transfer = co_await connect_transfer_socket(transfer_port);
    co_await require_socket_closed(excess_transfer, "Transfer setup connection capacity was not enforced");
    asio::error_code ignored;
    stalled_transfer_one.close(ignored);
    stalled_transfer_two.close(ignored);

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 10U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto first_opened = co_await receive_command(consumer, "relay.opened");
    const auto first_offer = co_await receive_command(producer, "relay.offer");
    const auto first_uuid = params_of(first_opened).at("uuid").get<uint64_t>();
    require(params_of(first_offer).at("uuid").get<uint64_t>() == first_uuid,
            "Producer and Consumer received different relay UUIDs");
    require(params_of(first_opened).at("ticket").get<uint64_t>() != params_of(first_offer).at("ticket").get<uint64_t>(),
            "Producer and Consumer received the same relay ticket");

    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 310U}}});
    const auto pending_traffic = co_await receive_command(consumer, "server.traffic.reported");
    require(params_of(pending_traffic).at("services").at(0).at("accessors").empty(),
            "Incomplete relay was reported as an active service accessor");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 11U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto capacity_error = co_await receive_command(consumer, "relay.error");
    require(params_of(capacity_error).at("request_id") == 11U,
            "Zero-half Incomplete Pipe did not consume pipeline capacity");
    require(params_of(capacity_error).at("reason") == "relay capacity reached", "Unexpected capacity rejection reason");

    consumer->send(CtrlMessage{"relay.cancel", njson{{"request_id", 10U}}});
    const auto cancelled = co_await receive_command(consumer, "relay.error");
    require(params_of(cancelled).at("request_id") == 10U, "Cancelled Pipe returned the wrong request ID");
    require(params_of(cancelled).at("reason") == "relay cancelled", "Unexpected relay cancellation reason");
    co_await receive_command(producer, "relay.closed");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 12U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto second_opened = co_await receive_command(consumer, "relay.opened");
    const auto second_offer = co_await receive_command(producer, "relay.offer");
    const auto second_uuid = params_of(second_opened).at("uuid").get<uint64_t>();
    require(params_of(second_offer).at("uuid").get<uint64_t>() == second_uuid,
            "Pipeline capacity was not reusable after cancellation");
    require(second_uuid > first_uuid, "Pipe UUIDs were not allocated monotonically");
    const auto second_producer_ticket = params_of(second_offer).at("ticket").get<uint64_t>();
    const auto second_consumer_ticket = params_of(second_opened).at("ticket").get<uint64_t>();

    const auto wrong_ticket = second_producer_ticket == 1 ? uint64_t{2} : second_producer_ticket ^ uint64_t{1};
    auto wrong_ticket_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, second_uuid, wrong_ticket);
    co_await require_socket_closed(wrong_ticket_half, "Incorrect relay ticket was not rejected");
    auto wrong_role_ticket_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, second_uuid, second_consumer_ticket);
    co_await require_socket_closed(wrong_role_ticket_half, "Consumer ticket was accepted for Producer role");

    auto producer_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, second_uuid, second_producer_ticket);
    auto duplicate_producer_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, second_uuid, second_producer_ticket);
    co_await require_socket_closed(duplicate_producer_half, "Duplicate Producer half was not rejected");

    producer->send(CtrlMessage{"relay.reject", njson{{"uuid", second_uuid}, {"reason", "test rejection"}}});
    const auto rejected = co_await receive_command(consumer, "relay.error");
    require(params_of(rejected).at("request_id") == 12U, "Rejected Pipe returned the wrong request ID");
    require(params_of(rejected).at("reason") == "test rejection", "Producer rejection reason was not preserved");
    co_await receive_command(producer, "relay.closed");
    co_await require_socket_closed(producer_half, "Rejecting a one-half Pipe did not close its data socket");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 13U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    co_await receive_command(consumer, "relay.opened");
    co_await receive_command(producer, "relay.offer");
    const auto zero_half_timeout = co_await receive_command(consumer, "relay.error");
    require(params_of(zero_half_timeout).at("request_id") == 13U,
            "Zero-half Pipe timeout returned the wrong request ID");
    require(params_of(zero_half_timeout).at("reason") == "relay setup timed out",
            "Zero-half Pipe did not expire through its setup timer");
    co_await receive_command(producer, "relay.closed");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 14U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto one_half_opened = co_await receive_command(consumer, "relay.opened");
    const auto one_half_offer = co_await receive_command(producer, "relay.offer");
    const auto one_half_uuid = params_of(one_half_opened).at("uuid").get<uint64_t>();
    const auto one_half_ticket = params_of(one_half_offer).at("ticket").get<uint64_t>();
    auto timed_out_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, one_half_uuid, one_half_ticket);
    const auto one_half_timeout = co_await receive_command(consumer, "relay.error");
    require(params_of(one_half_timeout).at("request_id") == 14U,
            "One-half Pipe timeout returned the wrong request ID");
    require(params_of(one_half_timeout).at("reason") == "relay setup timed out",
            "One-half Pipe did not expire through its setup timer");
    co_await receive_command(producer, "relay.closed");
    co_await require_socket_closed(timed_out_half, "One-half Pipe timeout did not close its data socket");

    consumer->send(CtrlMessage{
        "relay.open", njson{{"request_id", 15U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    const auto active_opened = co_await receive_command(consumer, "relay.opened");
    const auto active_offer = co_await receive_command(producer, "relay.offer");
    const auto active_uuid = params_of(active_opened).at("uuid").get<uint64_t>();
    const auto active_producer_ticket = params_of(active_offer).at("ticket").get<uint64_t>();
    const auto active_consumer_ticket = params_of(active_opened).at("ticket").get<uint64_t>();
    auto active_producer_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Producer, active_uuid, active_producer_ticket);
    auto active_consumer_half =
        co_await connect_relay_half(transfer_port, RelayAttach::Consumer, active_uuid, active_consumer_ticket);
    co_await receive_command(producer, "relay.ready");
    co_await receive_command(consumer, "relay.ready");

    const BytesBuf rx_payload{'s', 'e', 'r', 'v', 'i', 'c', 'e', '-', 'r', 'x'};
    const BytesBuf tx_payload{'s', 'e', 'r', 'v', 'i', 'c', 'e', '-', 't', 'x', '-', 'd', 'a', 't', 'a'};
    co_await asio::async_write(active_producer_half, asio::buffer(rx_payload), asio::use_awaitable);
    BytesBuf received_rx(rx_payload.size());
    co_await asio::async_read(active_consumer_half, asio::buffer(received_rx), asio::use_awaitable);
    require(received_rx == rx_payload, "RX traffic test payload was not forwarded");
    co_await asio::async_write(active_consumer_half, asio::buffer(tx_payload), asio::use_awaitable);
    BytesBuf received_tx(tx_payload.size());
    co_await asio::async_read(active_producer_half, asio::buffer(received_tx), asio::use_awaitable);
    require(received_tx == tx_payload, "TX traffic test payload was not forwarded");

    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 32U}}});
    const auto counted_traffic = co_await receive_command(consumer, "server.traffic.reported");
    const auto &counted_stats = params_of(counted_traffic).at("services").at(0);
    require(counted_stats.at("rx_bytes") == rx_payload.size(), "TCP RX payload bytes were counted incorrectly");
    require(counted_stats.at("tx_bytes") == tx_payload.size(), "TCP TX payload bytes were counted incorrectly");
    const auto &active_accessors = counted_stats.at("accessors");
    require(active_accessors.size() == 1 && !active_accessors.begin().key().empty() &&
                active_accessors.begin().value() == 1,
            "Active service accessor was not reported");

    probe_wait.expires_after(1100ms);
    co_await probe_wait.async_wait(asio::use_awaitable);
    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 33U}}});
    const auto sampled_traffic = co_await receive_command(consumer, "server.traffic.reported");
    const auto &sampled_stats = params_of(sampled_traffic).at("services").at(0);
    require(sampled_stats.at("rx_bytes_per_second").get<std::uint64_t>() > 0,
            "TCP RX bandwidth EMA was not sampled");
    require(sampled_stats.at("tx_bytes_per_second").get<std::uint64_t>() > 0,
            "TCP TX bandwidth EMA was not sampled");

    co_await producer->async_disconnect();
    co_await require_socket_closed(active_producer_half,
                                   "Producer control disconnect did not close the active Producer half");
    co_await require_socket_closed(active_consumer_half,
                                   "Producer control disconnect did not close the active Consumer half");

    probe_wait.expires_after(100ms);
    co_await probe_wait.async_wait(asio::use_awaitable);
    auto replacement = co_await connect_control(producer_context, control_port);
    replacement->send(CtrlMessage{
        "service.register", njson{{"request_id", 34U}, {"service", "lifecycle"}, {"protocol", "tcp"}}});
    co_await receive_command(replacement, "service.ok");
    consumer->send(CtrlMessage{"server.traffic", njson{{"request_id", 35U}}});
    const auto reset_traffic = co_await receive_command(consumer, "server.traffic.reported");
    const auto &reset_stats = params_of(reset_traffic).at("services").at(0);
    require(reset_stats.at("rx_bytes") == 0 && reset_stats.at("tx_bytes") == 0,
            "Re-registered service retained old traffic totals");
    require(reset_stats.at("rx_bytes_per_second") == 0 && reset_stats.at("tx_bytes_per_second") == 0,
            "Re-registered service retained old bandwidth EMA");
    require(reset_stats.at("accessors").empty(), "Re-registered service retained old accessors");
    co_await replacement->async_disconnect();
    co_await consumer->async_disconnect();
}

asio::awaitable<void> echo_session(tcp::socket socket)
{
    std::array<std::uint8_t, 8192> buffer{};
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

asio::awaitable<void> relay_round_trip(std::uint16_t port, bool half_close)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), port),
                                  asio::cancel_after(1s, asio::use_awaitable));

    const std::string payload = "relay integration payload";
    co_await asio::async_write(socket, asio::buffer(payload), asio::cancel_after(1s, asio::use_awaitable));
    if (half_close)
    {
        asio::error_code shutdown_error;
        socket.shutdown(tcp::socket::shutdown_send, shutdown_error);
        if (shutdown_error)
        {
            throw asio::system_error(shutdown_error, "Local test socket half-close failed");
        }
    }
    else
    {
        std::string received(payload.size(), '\0');
        co_await asio::async_read(socket, asio::buffer(received),
                                  asio::cancel_after(2s, asio::use_awaitable));
        require(received == payload, "TLS relay round trip payload mismatch");
        co_return;
    }

    std::string received;
    std::array<char, 256> buffer{};
    for (;;)
    {
        auto [error, length] =
            co_await socket.async_read_some(asio::buffer(buffer), asio::cancel_after(2s, use_nothrow_awaitable));
        received.append(buffer.data(), length);
        if (error == asio::error::eof)
        {
            break;
        }
        if (error)
        {
            throw asio::system_error(error, "Pipe test read failed");
        }
    }
    require(received == payload, "Pipe round trip payload mismatch");
}
} // namespace

int main()
{
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
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        const auto control_port = unused_port(control_io);
        auto transfer_port = unused_port(transfer_tcp_io);
        while (transfer_port == control_port)
        {
            transfer_port = unused_port(transfer_tcp_io);
        }
        auto tls_transfer_port = unused_port(transfer_tcp_io);
        while (tls_transfer_port == control_port || tls_transfer_port == transfer_port)
        {
            tls_transfer_port = unused_port(transfer_tcp_io);
        }
        const auto target_port = unused_port(transfer_tcp_io);
        const auto forward_port = unused_port(transfer_tcp_io);

        tcp::acceptor echo_acceptor(transfer_tcp_io, tcp::endpoint(asio::ip::address_v4::loopback(), target_port));
        asio::co_spawn(transfer_tcp_io, echo_accept_loop(echo_acceptor), asio::detached);

        NodeConfig server_config = make_test_node_config();
        server_config.control.address = "127.0.0.1";
        server_config.control.port = control_port;
        server_config.tcp.address = "127.0.0.1";
        server_config.tcp.port = transfer_port;
        server_config.tls.address = "127.0.0.1";
        server_config.tls.port = tls_transfer_port;
        server_config.datagram.address = "127.0.0.1";
        server_config.datagram.port = transfer_port;
        server_config.control.max_services = 2;
        server_config.control.max_services_per_session = 1;
        server_config.tcp.max_setup_connections = 2;
        server_config.tcp.max_relays = 1;
        server_config.tls.max_setup_connections = 2;
        server_config.tls.max_relays = 1;
        server_config.datagram.max_relays = 1;
        server_config.tcp.setup_timeout = 500ms;
        server_config.tls.setup_timeout = 500ms;
        server_config.datagram.setup_timeout = 500ms;
        server_config.channel = channel_config();
        auto server = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data.io, server_context,
                                                  std::move(server_config));
        server->start();

        AgentConfig producer_config;
        producer_config.host = "127.0.0.1";
        producer_config.port = control_port;
        producer_config.connect_timeout = 1s;
        producer_config.reconnect_initial_delay = 50ms;
        producer_config.reconnect_max_delay = 200ms;
        producer_config.channel = channel_config();
        producer_config.services.push_back(
            AgentServiceConfig{"echo", "127.0.0.1", target_port, RelayProtocol::Tls});
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
        consumer_config.forwards.push_back(
            AgentForwardConfig{"echo", "127.0.0.1", forward_port, RelayProtocol::Tls});
        auto consumer =
            std::make_shared<RelayAgent>(control_io, transfer_tcp_io, consumer_context, std::move(consumer_config));

        asio::post(transfer_tcp_io, []() { std::this_thread::sleep_for(1200ms); });
        asio::post(transfer_udp_io, []() { std::this_thread::sleep_for(1200ms); });

        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_tcp_thread([&transfer_tcp_io]() { transfer_tcp_io.run(); });
        std::thread transfer_udp_thread([&transfer_udp_io]() { transfer_udp_io.run(); });
        auto lifecycle_result = asio::co_spawn(
            control_io,
            verify_incomplete_relay_lifecycle(producer_context, consumer_context, control_port, transfer_port),
            asio::use_future);
        try
        {
            lifecycle_result.get();
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

        require(!control_io.stopped(), "Control io_context stopped after lifecycle verification");
        require(!transfer_tcp_io.stopped(), "TCP transfer io_context stopped after lifecycle verification");
        require(!transfer_udp_io.stopped(), "UDP transfer io_context stopped after lifecycle verification");
        producer->start();
        consumer->start();
        std::size_t completed_round_trips = 0;
        std::string last_failure;
        for (std::size_t round = 0; round < 2; ++round)
        {
            bool succeeded = false;
            for (int attempt = 0; attempt < 20 && !succeeded; ++attempt)
            {
                auto result = asio::co_spawn(control_io, relay_round_trip(forward_port, false), asio::use_future);
                try
                {
                    result.get();
                    succeeded = true;
                    ++completed_round_trips;
                }
                catch (const std::exception &exception)
                {
                    last_failure = exception.what();
                    std::this_thread::sleep_for(100ms);
                }
            }
            require(succeeded, "Pipe round trip did not become ready: " + last_failure);
        }

        tcp::socket stopping_transfer(control_io);
        stopping_transfer.connect(tcp::endpoint(asio::ip::address_v4::loopback(), transfer_port));
        auto stopping_transfer_closed = asio::co_spawn(
            control_io,
            require_socket_closed(stopping_transfer, "Server stop did not close a transfer setup socket"),
            asio::use_future);
        std::weak_ptr<RelayAgent> weak_consumer = consumer;
        std::weak_ptr<RelayAgent> weak_producer = producer;
        auto consumer_stopped = asio::co_spawn(control_io, consumer->async_stop(), asio::use_future);
        auto producer_stopped = asio::co_spawn(control_io, producer->async_stop(), asio::use_future);
        consumer.reset();
        producer.reset();
        consumer_stopped.get();
        producer_stopped.get();
        server->stop();
        stopping_transfer_closed.get();
        asio::post(transfer_tcp_io, [&echo_acceptor]() {
            asio::error_code ignored;
            echo_acceptor.close(ignored);
        });
        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();

        require(weak_consumer.expired() && weak_producer.expired(), "Stopped Agents are retained by Forwarder tasks");
        require(completed_round_trips == 2, "Pipeline capacity was not released after the first transfer");
        std::cout << "[PASS] Server lifecycle, resource limits, Pipe forwarding, and half-close\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
