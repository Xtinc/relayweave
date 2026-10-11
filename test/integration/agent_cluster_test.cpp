#include "test_data.h"
#include "node_test_config.h"
#include "relay_agent.h"
#include "relay_node.h"

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <openssl/ssl.h>
#include <set>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

void require(bool condition, const std::string &message)
{
    if (!condition)
        throw std::runtime_error(message);
}

void configure_server(asio::ssl::context &context, const std::filesystem::path &data)
{
    context.use_certificate_chain_file((data / "tls_channel_test_server.pem").string());
    context.use_private_key_file((data / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
    context.load_verify_file((data / "tls_channel_test_client_ca.pem").string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const std::filesystem::path &data)
{
    context.load_verify_file((data / "tls_channel_test_ca.pem").string());
    context.use_certificate_chain_file((data / "tls_channel_test_client.pem").string());
    context.use_private_key_file((data / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Client certificate and key do not match");
}

std::uint16_t unused_port(asio::io_context &io, std::set<std::uint16_t> &used)
{
    for (;;)
    {
        tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
        const auto port = acceptor.local_endpoint().port();
        if (used.insert(port).second)
            return port;
    }
}

TLSChannelConfig channel_config()
{
    TLSChannelConfig config;
    config.handshake_timeout = 1s;
    config.disconnect_timeout = 200ms;
    config.heartbeat_interval = 100ms;
    config.heartbeat_timeout = 2s;
    return config;
}

NodeConfig server_config(std::string node_id, ClusterConfig::Role role, std::uint16_t cluster_port,
                           std::uint16_t control_port, std::uint16_t transfer_port)
{
    auto config = make_test_node_config();
    const bool is_master = node_id == "master";
    config.cluster = {role, std::move(node_id), "127.0.0.1", cluster_port};
    configure_test_cluster_data(config.cluster);
    config.control.address = is_master ? "0.0.0.0" : "127.0.0.1";
    config.control.advertise_address = is_master ? "localhost" : "127.0.0.1";
    config.control.port = control_port;
    config.tcp.address = "127.0.0.1";
    config.tcp.port = transfer_port;
    config.tls.address = "127.0.0.1";
    config.tls.port = 0;
    config.datagram.address = "127.0.0.1";
    config.datagram.port = 0;
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
    config.relay_open_timeout = 2s;
    config.channel = channel_config();
    return config;
}

asio::awaitable<void> echo_session(tcp::socket socket)
{
    std::array<char, 1024> data{};
    for (;;)
    {
        auto [read_error, size] = co_await socket.async_read_some(asio::buffer(data), use_nothrow_awaitable);
        if (read_error)
            co_return;
        auto [write_error, _] =
            co_await asio::async_write(socket, asio::buffer(data.data(), size), use_nothrow_awaitable);
        if (write_error)
            co_return;
    }
}

asio::awaitable<void> echo_loop(tcp::acceptor &acceptor)
{
    for (;;)
    {
        auto [error, socket] = co_await acceptor.async_accept(use_nothrow_awaitable);
        if (error)
            co_return;
        asio::co_spawn(acceptor.get_executor(), echo_session(std::move(socket)), asio::detached);
    }
}

asio::awaitable<void> round_trip(tcp::socket &socket, std::string payload)
{
    co_await asio::async_write(socket, asio::buffer(payload), asio::cancel_after(1s, asio::use_awaitable));
    std::string received(payload.size(), '\0');
    co_await asio::async_read(socket, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Cluster relay payload mismatch");
}

asio::awaitable<void> verify_two_services(std::uint16_t forward_a, std::uint16_t forward_b)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket a(executor);
    tcp::socket b(executor);
    co_await a.async_connect({asio::ip::address_v4::loopback(), forward_a},
                             asio::cancel_after(1s, asio::use_awaitable));
    co_await b.async_connect({asio::ip::address_v4::loopback(), forward_b},
                             asio::cancel_after(1s, asio::use_awaitable));
    co_await round_trip(a, "service-a-1");
    co_await round_trip(b, "service-b-1");
    co_await round_trip(a, "service-a-2");
    co_await round_trip(b, "service-b-2");
}

asio::awaitable<void> verify_service(std::uint16_t port)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port},
                                  asio::cancel_after(1s, asio::use_awaitable));
    co_await round_trip(socket, "recovered-service");
}

asio::awaitable<std::shared_ptr<TLSChannel>> open_control(asio::ssl::context &context, std::uint16_t port)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, channel_config());
    co_await channel->start("127.0.0.1");
    channel->send(CtrlMessage{"server.identify", njson::object()});
    require((co_await channel->async_receive(1s)).command == "server.identified",
            "Cluster status client was not identified");
    co_return channel;
}

asio::awaitable<std::map<std::string, njson>> collect_cluster_status(const std::shared_ptr<TLSChannel> &channel,
                                                                     std::uint64_t request_id,
                                                                     std::size_t expected_nodes)
{
    std::map<std::string, njson> reports;
    while (reports.size() < expected_nodes)
    {
        const auto message = co_await channel->async_receive(2s);
        require(message.command == "server.status.reported" && message.params,
                "Missing cluster status response");
        const auto &params = *message.params;
        require(config::require_unsigned(params, "request_id", true) == request_id,
                "Cluster status request ID changed");
        require(!params.contains("page_index") && !params.contains("page_count"),
                "Cluster status leaked transport pagination metadata");
        const auto node_id = config::required_string(params, "node_id", "server.status.reported");
        require(!params.contains("source") && !params.contains("requester_node") && !params.contains("session_id"),
                "Cluster status leaked internal routing fields");
        config::require_unsigned(params, "uptime_ms");
        require(!params.contains("address") && !params.contains("port") &&
                    !params.contains("control_queue_delay_us") &&
                    !params.contains("transfer_tcp_queue_delay_us") &&
                    !params.contains("transfer_udp_queue_delay_us"),
                "Cluster status duplicated topology data");
        require(params.contains("services") && params.at("services").is_array(),
                "Cluster status services were not reported");
        std::string previous_service;
        for (const auto &service : params.at("services"))
        {
            const auto name = config::message_service(service);
            config::message_protocol(service);
            config::require_unsigned(service, "rx_bytes");
            config::require_unsigned(service, "tx_bytes");
            config::require_unsigned(service, "rx_bytes_per_second");
            config::require_unsigned(service, "tx_bytes_per_second");
            require(service.contains("accessors") && service.at("accessors").is_object(),
                    "Cluster status service accessors were not reported");
            require(previous_service.empty() || previous_service < name,
                    "Cluster status services were not sorted");
            previous_service = name;
        }
        require(reports.emplace(node_id, params).second, "A node reported cluster status more than once");
    }
    co_return reports;
}

asio::awaitable<void> verify_cluster_status(asio::ssl::context &context, std::uint16_t master_port,
                                            std::uint16_t slave_port, std::size_t expected_nodes)
{
    auto master = co_await open_control(context, master_port);
    auto slave = co_await open_control(context, slave_port);
    constexpr std::uint64_t request_id = 71;
    master->send(CtrlMessage{"server.cluster", njson{{"request_id", request_id},
                                                      {"source", "forged-source"},
                                                      {"node_id", "forged-node"}}});
    slave->send(CtrlMessage{"server.cluster", njson{{"request_id", request_id},
                                                     {"source", "forged-source"},
                                                     {"node_id", "forged-node"}}});
    auto master_reports = co_await collect_cluster_status(master, request_id, expected_nodes);
    auto slave_reports = co_await collect_cluster_status(slave, request_id, expected_nodes);
    require(master_reports.contains("master") && master_reports.contains("slave"),
            "Master query did not collect both cluster nodes");
    require(slave_reports.contains("master") && slave_reports.contains("slave"),
            "Slave query did not collect both cluster nodes");
    require(master_reports.at("master").at("services").size() == 1 &&
                master_reports.at("master").at("services").at(0).at("service") == "service-a" &&
                master_reports.at("slave").at("services").size() == 1 &&
                master_reports.at("slave").at("services").at(0).at("service") == "service-b",
            "Cluster status did not include the registered service traffic");
    co_await master->async_disconnect();
    co_await slave->async_disconnect();
}

asio::awaitable<njson> query_topology(const std::shared_ptr<TLSChannel> &channel, std::uint64_t request_id)
{
    channel->send(CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request_id}}});
    auto response = co_await channel->async_receive(2s);
    require(response.type() == CtrlCommand::TopologySnapshot, "Missing topology snapshot response");
    const auto &params = config::message_params(response);
    Topology::validate_snapshot(params, "topology.snapshot");
    require(config::require_unsigned(params, "request_id", true) == request_id,
            "Topology request ID was changed");
    require(!params.contains("source") && !params.contains("requester_node") && !params.contains("session_id") &&
                !params.contains("node_id") && !params.contains("page_index") && !params.contains("page_count"),
            "Topology snapshot leaked internal fields");
    co_return njson{{"epoch", params.at("epoch")}, {"version", params.at("snapshot_version")},
                    {"nodes", params.at("nodes")}, {"links", params.at("links")}};
}

asio::awaitable<void> verify_topology(asio::ssl::context &context, std::uint16_t master_port,
                                      std::uint16_t slave_port)
{
    for (const auto port : {master_port, slave_port})
    {
        auto channel = co_await open_control(context, port);
        const auto snapshot = co_await query_topology(channel, 81);
        require(snapshot.at("nodes").size() == 2, "Topology snapshot did not contain both Nodes");
        co_await channel->async_disconnect();
    }
}

asio::awaitable<void> discovery_peer(tcp::acceptor &acceptor, asio::ssl::context &context,
                                    std::uint16_t unavailable_port, std::uint16_t service_port)
{
    auto socket = co_await acceptor.async_accept(asio::use_awaitable);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::S, channel_config());
    co_await channel->start({});
    std::uint64_t previous_request = 0;
    auto location = [&](std::uint64_t request, bool available) {
        channel->send(CtrlMessage{"service.located", njson{{"request_id", request},
            {"service", "service-b"}, {"protocol", "tcp"},
            {"node_id", available ? "master" : "unavailable"}, {"address", "127.0.0.1"},
            {"port", available ? service_port : unavailable_port}}});
    };
    try
    {
        for (;;)
        {
            auto message = co_await channel->async_receive();
            if (message.command == "server.identify")
                channel->send(CtrlMessage{"server.identified", njson{{"node_id", "discovery"}}});
            else if (message.command == "service.lookup")
            {
                const auto request = config::require_unsigned(config::message_params(message), "request_id", true);
                if (!previous_request)
                {
                    previous_request = request;
                    location(request, false);
                }
                else
                {
                    // An old response must not win over the current reachable location.
                    location(previous_request, false);
                    location(request, true);
                }
            }
        }
    }
    catch (const asio::system_error &)
    {
    }
    co_await channel->async_disconnect();
}

asio::awaitable<void> verify_discovery(asio::ssl::context &context, std::uint16_t entry_port,
                                      std::uint16_t remote_port)
{
    const auto executor = co_await asio::this_coro::executor;
    std::array<std::shared_ptr<TLSChannel>, 2> sessions;
    for (auto &session : sessions)
    {
        tcp::socket socket(executor);
        co_await socket.async_connect({asio::ip::address_v4::loopback(), entry_port}, asio::use_awaitable);
        session = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, channel_config());
        co_await session->start("127.0.0.1");
    }
    // Request IDs are local to each client: identical IDs must not mix sessions.
    sessions[0]->send(CtrlMessage{"service.lookup", njson{{"request_id", 17},
        {"service", "service-a"}, {"protocol", "tcp"}}});
    sessions[1]->send(CtrlMessage{"service.lookup", njson{{"request_id", 17},
        {"service", "service-b"}, {"protocol", "tcp"}}});
    for (std::size_t index = 0; index < sessions.size(); ++index)
    {
        auto response = co_await sessions[index]->async_receive(2s);
        const auto &params = config::message_params(response);
        require(response.command == "service.located", "Missing service location response");
        require(config::require_unsigned(params, "request_id") == 17, "Request ID was changed");
        require(config::message_service(params) == (index ? "service-b" : "service-a"),
                "Location routed to the wrong client session");
        require(params.at("node_id") == (index ? "slave" : "master"), "Incorrect location source");
        require(params.at("address") == (index ? "127.0.0.1" : "localhost"),
                "Configured control advertise address was not returned");
        require(params.at("port") == (index ? remote_port : entry_port), "Incorrect control port");
        require(!params.contains("source") && !params.contains("session_id") &&
                    !params.contains("requester_node"), "Internal routing fields leaked to client");
        co_await sessions[index]->async_disconnect();
    }
}
} // namespace

int main()
{
    try
    {
        const auto data = test_data::directory();
        asio::ssl::context server_context(asio::ssl::context::tls_server);
        asio::ssl::context client_context(asio::ssl::context::tls_client);
        configure_server(server_context, data);
        configure_client(client_context, data);

        asio::io_context control_io(1);
        asio::io_context transfer_io(1);
        asio::io_context udp_io(1);
        TestClusterDataIO cluster_data;
        auto control_work = asio::make_work_guard(control_io);
        auto transfer_work = asio::make_work_guard(transfer_io);
        auto udp_work = asio::make_work_guard(udp_io);
        std::set<std::uint16_t> used;
        const auto cluster_port = unused_port(control_io, used);
        const auto control_a = unused_port(control_io, used);
        const auto control_b = unused_port(control_io, used);
        const auto transfer_a = unused_port(transfer_io, used);
        const auto transfer_b = unused_port(transfer_io, used);
        const auto target_a = unused_port(transfer_io, used);
        const auto target_b = unused_port(transfer_io, used);
        const auto forward_a = unused_port(transfer_io, used);
        const auto forward_b = unused_port(transfer_io, used);

        tcp::acceptor echo_a(transfer_io, {asio::ip::address_v4::loopback(), target_a});
        tcp::acceptor echo_b(transfer_io, {asio::ip::address_v4::loopback(), target_b});
        asio::co_spawn(transfer_io, echo_loop(echo_a), asio::detached);
        asio::co_spawn(transfer_io, echo_loop(echo_b), asio::detached);

        auto master = std::make_shared<RelayNode>(
            control_io, transfer_io, udp_io, cluster_data.io, server_context,
            server_config("master", ClusterConfig::Role::Master, cluster_port, control_a, transfer_a));
        auto slave = std::make_shared<RelayNode>(
            control_io, transfer_io, udp_io, cluster_data.io, server_context,
            server_config("slave", ClusterConfig::Role::Slave, cluster_port, control_b, transfer_b));
        master->start();
        slave->start();

        auto producer_a_config = client_config(control_a);
        producer_a_config.services.push_back(
            AgentServiceConfig{"service-a", "127.0.0.1", target_a, RelayProtocol::Tcp});
        auto producer_b_config = client_config(control_b);
        producer_b_config.services.push_back(
            AgentServiceConfig{"service-b", "127.0.0.1", target_b, RelayProtocol::Tcp});
        auto consumer_config = client_config(control_a);
        consumer_config.forwards.push_back(
            AgentForwardConfig{"service-a", "127.0.0.1", forward_a, RelayProtocol::Tcp});
        consumer_config.forwards.push_back(
            AgentForwardConfig{"service-b", "127.0.0.1", forward_b, RelayProtocol::Tcp});

        auto producer_a = std::make_shared<RelayAgent>(control_io, transfer_io, client_context,
                                                      std::move(producer_a_config));
        auto producer_b = std::make_shared<RelayAgent>(control_io, transfer_io, client_context,
                                                      std::move(producer_b_config));
        auto consumer =
            std::make_shared<RelayAgent>(control_io, transfer_io, client_context, std::move(consumer_config));
        std::shared_ptr<RelayAgent> recovery_client;
        producer_a->start();
        producer_b->start();
        consumer->start();

        std::thread control_thread([&] { control_io.run(); });
        std::thread transfer_thread([&] { transfer_io.run(); });
        std::thread udp_thread([&] { udp_io.run(); });
        auto cleanup = [&]() noexcept {
            const auto stop_agent = [&](auto &agent) noexcept {
                if (!agent)
                    return;
                try
                {
                    asio::co_spawn(control_io, agent->async_stop(), asio::use_future).get();
                }
                catch (...)
                {
                }
            };
            const auto stop_node = [](auto &node) noexcept {
                if (!node)
                    return;
                try
                {
                    node->stop();
                }
                catch (...)
                {
                }
            };

            stop_agent(recovery_client);
            stop_agent(consumer);
            stop_agent(producer_b);
            stop_agent(producer_a);
            stop_node(slave);
            stop_node(master);
            asio::post(transfer_io, [&] {
                asio::error_code ignored;
                echo_a.close(ignored);
                echo_b.close(ignored);
            });

            recovery_client.reset();
            consumer.reset();
            producer_b.reset();
            producer_a.reset();
            slave.reset();
            master.reset();
            control_work.reset();
            transfer_work.reset();
            udp_work.reset();
            if (control_thread.joinable()) control_thread.join();
            if (transfer_thread.joinable()) transfer_thread.join();
            if (udp_thread.joinable()) udp_thread.join();
        };
        ScopeGuard cleanup_guard([&cleanup]() noexcept { cleanup(); });

        std::string last_error;
        bool passed = false;
        for (int attempt = 0; attempt < 40 && !passed; ++attempt)
        {
            try
            {
                asio::co_spawn(control_io, verify_two_services(forward_a, forward_b), asio::use_future).get();
                passed = true;
            }
            catch (const std::exception &exception)
            {
                last_error = exception.what();
                std::this_thread::sleep_for(200ms);
            }
        }
        require(passed, "Two-server client routing did not become ready: " + last_error);
        asio::co_spawn(control_io, verify_cluster_status(client_context, control_a, control_b, 2),
                       asio::use_future).get();
        asio::co_spawn(control_io, verify_topology(client_context, control_a, control_b), asio::use_future).get();
        asio::co_spawn(control_io, verify_discovery(client_context, control_a, control_b),
                       asio::use_future).get();

        auto wait_for_service = [&](std::uint16_t port, const std::string &scenario) {
            std::string error;
            for (int attempt = 0; attempt < 60; ++attempt)
            {
                try
                {
                    asio::co_spawn(control_io, verify_service(port), asio::use_future).get();
                    return;
                }
                catch (const std::exception &exception)
                {
                    error = exception.what();
                    std::this_thread::sleep_for(200ms);
                }
            }
            throw std::runtime_error(scenario + ": " + error);
        };

        auto wait_for_cluster = [&](const std::string &scenario) {
            std::string error;
            for (int attempt = 0; attempt < 60; ++attempt)
            {
                try
                {
                    asio::co_spawn(control_io, verify_cluster_status(client_context, control_a, control_b, 2),
                                   asio::use_future).get();
                    return;
                }
                catch (const std::exception &exception)
                {
                    error = exception.what();
                    std::this_thread::sleep_for(200ms);
                }
            }
            throw std::runtime_error(scenario + ": " + error);
        };

        // Recover through ordinary service discovery while the primary is online.
        slave->stop();
        slave = std::make_shared<RelayNode>(
            control_io, transfer_io, udp_io, cluster_data.io, server_context,
            server_config("slave", ClusterConfig::Role::Slave, cluster_port, control_b, transfer_b));
        slave->start();
        wait_for_service(forward_b, "Slave recovery");
        wait_for_cluster("Slave cluster recovery");

        master->stop();
        master = std::make_shared<RelayNode>(
            control_io, transfer_io, udp_io, cluster_data.io, server_context,
            server_config("master", ClusterConfig::Role::Master, cluster_port, control_a, transfer_a));
        master->start();
        wait_for_service(forward_a, "Entry recovery");
        wait_for_cluster("Cluster recovery");

        // Move B to the entry node while its old server stays connected.
        asio::co_spawn(control_io, producer_b->async_stop(), asio::use_future).get();
        auto migrated_config = client_config(control_a);
        migrated_config.services.push_back(
            AgentServiceConfig{"service-b", "127.0.0.1", target_b, RelayProtocol::Tcp});
        producer_b = std::make_shared<RelayAgent>(control_io, transfer_io, client_context,
                                                std::move(migrated_config));
        asio::post(control_io, [producer_b] { producer_b->start(); });
        // Synchronize with producer shutdown and registration before testing the old route.
        std::this_thread::sleep_for(500ms);
        wait_for_service(forward_b, "Service migration");
        asio::co_spawn(control_io, verify_two_services(forward_a, forward_b), asio::use_future).get();

        // An unreachable first location must not suppress subsequent discovery.
        tcp::acceptor discovery_acceptor(control_io, {asio::ip::address_v4::loopback(), 0});
        const auto unavailable = unused_port(control_io, used);
        const auto recovery_forward = unused_port(transfer_io, used);
        auto discovery_task = asio::co_spawn(control_io,
            discovery_peer(discovery_acceptor, server_context, unavailable, control_a), asio::use_future);
        auto recovery_config = client_config(discovery_acceptor.local_endpoint().port());
        recovery_config.forwards.push_back(
            AgentForwardConfig{"service-b", "127.0.0.1", recovery_forward, RelayProtocol::Tcp});
        recovery_client = std::make_shared<RelayAgent>(control_io, transfer_io, client_context,
                                                       std::move(recovery_config));
        asio::post(control_io, [recovery_client] { recovery_client->start(); });
        wait_for_service(recovery_forward, "Initial connection failure and stale discovery response");
        asio::co_spawn(control_io, recovery_client->async_stop(), asio::use_future).get();
        discovery_task.get();
        recovery_client.reset();

        cleanup();
        cleanup_guard.dismiss();

        std::cout << "[PASS] One client used services on two cluster servers concurrently\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
