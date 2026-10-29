#include "test_data.h"
#include "node_test_config.h"
#include "relay_agent.h"
#include "relay_node.h"
#include "node_session.h"
#include "xfr_channel.h"

#include <array>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <openssl/ssl.h>
#include <set>
#include <stdexcept>
#include <thread>
#include "agent_control.h"
#include "member_access.h"

TEST_MEMBER(RelayNode, registry_)
TEST_MEMBER(RelayNode, relay_sessions_)
TEST_MEMBER(RelayAgent, control_)
TEST_MEMBER(AgentControl, service_locations_)
TEST_MEMBER(RelayAgent, connection_)

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
        {
            return port;
        }
    }
}

std::uint16_t unused_udp_port(asio::io_context &io, std::set<std::uint16_t> &used)
{
    for (;;)
    {
        udp::socket socket(io, {asio::ip::address_v4::loopback(), 0});
        const auto port = socket.local_endpoint().port();
        if (used.insert(port).second)
        {
            return port;
        }
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
                           std::uint16_t control_port, std::uint16_t transfer_port, std::uint16_t datagram_port,
                           std::set<std::uint16_t> &used)
{
    auto config = make_test_node_config();
    const bool is_master = node_id == "master";
    config.cluster = {role, std::move(node_id), "127.0.0.1", cluster_port};
    asio::io_context port_io;
    config.cluster.tcp_port = unused_port(port_io, used);
    config.cluster.udp_port = unused_udp_port(port_io, used);
    config.control.address = is_master ? "0.0.0.0" : "127.0.0.1";
    config.control.advertise_address = is_master ? "localhost" : "127.0.0.1";
    config.control.port = control_port;
    config.tcp.address = "127.0.0.1";
    config.tcp.port = transfer_port;
    config.tcp.setup_timeout = 1s;
    config.tls.address = "127.0.0.1";
    config.tls.port = unused_port(port_io, used);
    config.datagram.address = "127.0.0.1";
    config.datagram.port = datagram_port;
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
        {
            co_return;
        }
        auto [write_error, _] =
            co_await asio::async_write(socket, asio::buffer(data.data(), size), use_nothrow_awaitable);
        if (write_error)
        {
            co_return;
        }
    }
}

asio::awaitable<void> echo_loop(tcp::acceptor &acceptor)
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

asio::awaitable<void> round_trip(tcp::socket &socket, std::string payload)
{
    co_await asio::async_write(socket, asio::buffer(payload), asio::cancel_after(1s, asio::use_awaitable));
    std::string received(payload.size(), '\0');
    co_await asio::async_read(socket, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Cluster relay payload mismatch");
}

asio::awaitable<void> verify_entries(RelayAgent &consumer, const std::vector<std::shared_ptr<RelayNode>> &nodes,
                                      const std::vector<std::uint16_t> &ports,
                                      std::vector<tcp::socket> &clients)
{
    const auto executor = co_await asio::this_coro::executor;
    const auto deadline = std::chrono::steady_clock::now() + 8s;
    for (;;)
    {
        const auto &locations = test_access::AgentControl_service_locations_(test_access::RelayAgent_control_(consumer));
        const auto ready = std::ranges::count_if(locations, [](const auto &entry) { return entry.first.protocol == RelayProtocol::Tcp && !entry.second.node.empty(); });
        if (ready == 10)
        {
            break;
        }
        require(std::chrono::steady_clock::now() < deadline, "Ten services did not become discoverable");
        asio::steady_timer timer(executor, 20ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
    // Keep all application sessions open while inspecting the actual TLS control registries.
    for (std::size_t i = 0; i < ports.size(); ++i)
    {
        clients.emplace_back(executor);
        co_await clients.back().async_connect({asio::ip::address_v4::loopback(), ports[i]}, asio::use_awaitable);
        co_await round_trip(clients.back(), "entry-" + std::to_string(i));
    }
    require(test_access::RelayNode_registry_(*nodes.front()).session_count() == 1,
            "Consumer opened more than one primary control connection");
    for (std::size_t i = 1; i < nodes.size(); ++i)
    {
        require(test_access::RelayNode_registry_(*nodes[i]).session_count() == 1,
                "Consumer opened an extra entrance control connection");
    }
    for (auto &client : clients)
    {
        co_await round_trip(client, "still-open");
    }
}

asio::awaitable<void> verify_primary_reconnect(RelayAgent &consumer, std::vector<tcp::socket> &clients)
{
    const auto &connection = test_access::RelayAgent_connection_(consumer);
    const auto original = connection.route().channel;
    original.lock()->disconnect();
    for (auto &client : clients)
    {
        std::array<char, 1> data{};
        const auto [error, size] = co_await client.async_read_some(asio::buffer(data),
            asio::cancel_after(1s, use_nothrow_awaitable));
        require(size == 0 && (error == asio::error::eof || error == asio::error::connection_reset),
                "Primary disconnection left an old business connection running");
    }
    clients.clear();
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!connection.ready() || !original.expired())
    {
        require(std::chrono::steady_clock::now() < deadline, "Primary did not reconnect after disconnection");
        asio::steady_timer timer(co_await asio::this_coro::executor, 10ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
}

asio::awaitable<void> wait_empty(const std::vector<std::shared_ptr<RelayNode>> &nodes)
{
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    for (;;)
    {
        if (std::ranges::all_of(nodes, [](const auto &node) { return test_access::RelayNode_relay_sessions_(*node).empty(); }))
        {
            co_return;
        }
        require(std::chrono::steady_clock::now() < deadline, "Remote Agent sessions were not released");
        asio::steady_timer timer(co_await asio::this_coro::executor, 10ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
}

asio::awaitable<void> verify_detach(asio::ssl::context &context, std::uint16_t port,
                                   const std::vector<std::shared_ptr<RelayNode>> &nodes)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, channel_config());
    co_await channel->start("127.0.0.1");
    channel->send(CtrlMessage{CtrlCommand::RelayOpen, njson{{"request_id", 1}, {"service", "service-0"},
        {"protocol", "tcp"}, {"node_id", "entry-0"}}});
    const auto reply = co_await channel->async_receive(2s);
    require(reply.type() == CtrlCommand::RelayOpened, "Remote request was not opened");
    require(!reply.params->contains("agent_session_id") && !reply.params->contains("source"), "Cluster routing fields leaked to Agent");
    // No data attach: only the primary session departure can release this pending remote relay promptly.
    co_await channel->async_disconnect();
    co_await wait_empty(nodes);
}
asio::awaitable<void> echo_datagrams(udp::socket &socket)
{
    std::array<char, 1024> data{};
    for (;;)
    {
        udp::endpoint source;
        const auto [error, size] = co_await socket.async_receive_from(asio::buffer(data), source, use_nothrow_awaitable);
        if (error)
        {
            co_return;
        }
        co_await socket.async_send_to(asio::buffer(data.data(), size), source, asio::use_awaitable);
    }
}

asio::awaitable<void> verify_unavailable(RelayAgent &consumer, const std::vector<std::uint16_t> &ports,
                                       const std::vector<std::shared_ptr<RelayNode>> &nodes)
{
    const auto executor = co_await asio::this_coro::executor;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    const auto &locations = test_access::AgentControl_service_locations_(test_access::RelayAgent_control_(consumer));
    for (;;)
    {
        if (locations.at({"service-8", RelayProtocol::Tcp}).request &&
            locations.at({"service-9", RelayProtocol::Tcp}).request)
        {
            break;
        }
        require(std::chrono::steady_clock::now() < deadline, "Offline services were not queried");
        asio::steady_timer timer(executor, 10ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
    for (const auto index : {8U, 9U})
    {
        tcp::socket socket(executor);
        co_await socket.async_connect({asio::ip::address_v4::loopback(), ports[index]}, asio::use_awaitable);
        std::array<char, 1> data{};
        const auto [error, size] = co_await socket.async_read_some(asio::buffer(data),
            asio::cancel_after(500ms, use_nothrow_awaitable));
        require(error == asio::error::eof && size == 0, "Unavailable TCP service changed its existing close behavior");
        require(test_access::RelayNode_relay_sessions_(*nodes[index + 1]).empty(), "Offline service allocated a relay");
    }
    require(locations.at({"late-udp", RelayProtocol::Udp}).node.empty(), "Offline UDP service became available");
}

asio::awaitable<void> verify_late_udp(const std::shared_ptr<RelayNode> &node, std::uint16_t forward_port)
{
    const auto executor = co_await asio::this_coro::executor;
    const auto deadline = std::chrono::steady_clock::now() + 8s;
    for (;;)
    {
        const auto &sessions = test_access::RelayNode_relay_sessions_(*node);
        if (std::ranges::any_of(sessions, [](const auto &session) {
                const auto report = session->status_report();
                return report && report->at("service") == "late-udp";
            }))
        {
            break;
        }
        require(std::chrono::steady_clock::now() < deadline, "Late UDP producer did not establish proactively");
        asio::steady_timer timer(executor, 20ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
    udp::socket socket(executor, udp::v4());
    co_await socket.async_connect({asio::ip::address_v4::loopback(), forward_port}, asio::use_awaitable);
    const std::string payload = "late-udp-producer";
    co_await socket.async_send(asio::buffer(payload), asio::use_awaitable);
    std::array<char, 128> received{};
    const auto size = co_await socket.async_receive(asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(std::string_view(received.data(), size) == payload, "Late UDP service did not forward data");
}

asio::awaitable<std::shared_ptr<TLSChannel>> connect_control(asio::ssl::context &context, std::uint16_t port)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, channel_config());
    co_await channel->start("127.0.0.1");
    co_return channel;
}

asio::awaitable<tcp::socket> attach_stream(const njson &endpoint, int role)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect({asio::ip::make_address(endpoint.at("data_address").get<std::string>()),
                                  config::message_data_port(endpoint)}, asio::use_awaitable);
    const auto frame = WireMessage::pack(RelayAttach::to_msg({role,
        endpoint.at("uuid").get<std::uint64_t>(), endpoint.at("ticket").get<std::uint64_t>()}));
    co_await asio::async_write(socket, asio::buffer(frame), asio::use_awaitable);
    co_return socket;
}

asio::awaitable<void> verify_attach_wait(asio::ssl::context &context, std::uint16_t primary_port,
                                        std::uint16_t producer_port,
                                        const std::vector<std::shared_ptr<RelayNode>> &nodes)
{
    auto consumer = co_await connect_control(context, primary_port);
    auto producer = co_await connect_control(context, producer_port);
    producer->send(CtrlMessage{CtrlCommand::ServiceRegister, njson{{"request_id", 1},
        {"service", "delayed-attach"}, {"protocol", "tcp"}}});
    require((co_await producer->async_receive(1s)).type() == CtrlCommand::ServiceOk, "Delayed producer was not registered");
    // Complete a delayed attach, time out a missing half, then cancel an attaching request.
    for (unsigned request = 1; request <= 3; ++request)
    {
        njson params{{"request_id", request}, {"service", "delayed-attach"}, {"protocol", "tcp"}, {"node_id", "entry-0"}};
        consumer->send(CtrlMessage{CtrlCommand::RelayOpen, std::move(params)});
        const auto opened = co_await consumer->async_receive(2s);
        const auto offer = co_await producer->async_receive(2s);
        require(opened.type() == CtrlCommand::RelayOpened && offer.type() == CtrlCommand::RelayOffer,
                "Delayed attach did not get endpoints");
        auto consumer_data = co_await attach_stream(*opened.params, RelayAttach::Consumer);
        if (request == 1)
        {
            const std::string payload = "delayed-peer";
            co_await asio::async_write(consumer_data, asio::buffer(payload), asio::use_awaitable);
            bool waiting = false;
            try
            {
                co_await consumer->async_receive(100ms);
            }
            catch (const asio::system_error &error)
            {
                waiting = error.code() == asio::experimental::error::channel_cancelled ||
                          error.code() == asio::error::operation_aborted;
            }
            require(waiting, "Relay became ready or failed before its producer attached");
            auto producer_data = co_await attach_stream(*offer.params, RelayAttach::Producer);
            require((co_await consumer->async_receive(1s)).type() == CtrlCommand::RelayReady &&
                    (co_await producer->async_receive(1s)).type() == CtrlCommand::RelayReady,
                    "Delayed second attach did not wake both sides");
            std::string received(payload.size(), '\0');
            co_await asio::async_read(producer_data, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
            require(received == payload, "Data did not cross the delayed relay");
            consumer_data.close();
            producer_data.close();

        }
        else
        {
            if (request == 3)
            {
                consumer->send(CtrlMessage{CtrlCommand::RelayCancel, njson{{"request_id", request}, {"node_id", "entry-0"}}});
            }
            const auto error = co_await consumer->async_receive(2s);
            require(error.type() == CtrlCommand::RelayError && error.params->at("request_id") == request,
                    "Missing peer did not terminate the original request");
            const auto reason = error.params->at("reason").get<std::string>();
            require(request == 2 ? reason.find("timed out") != std::string::npos : reason == "relay cancelled",
                    "Attach timeout or cancellation lost its reason");
            require((co_await producer->async_receive(1s)).type() == CtrlCommand::RelayClosed,
                    "Pending producer was not released");
        }
        co_await wait_empty(nodes);
    }
    co_await producer->async_disconnect();
    co_await consumer->async_disconnect();
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
        const auto primary_port = unused_port(control_io, used);
        tcp::acceptor echo(transfer_io, {asio::ip::address_v4::loopback(), 0});
        asio::co_spawn(transfer_io, echo_loop(echo), asio::detached);
        udp::socket udp_echo(transfer_io, {asio::ip::address_v4::loopback(), 0});
        asio::co_spawn(transfer_io, echo_datagrams(udp_echo), asio::detached);
        udp::socket forward_reservation(transfer_io, {asio::ip::address_v4::loopback(), 0});
        const auto udp_forward = forward_reservation.local_endpoint().port();
        std::vector<std::shared_ptr<RelayNode>> nodes;
        std::vector<std::shared_ptr<RelayAgent>> agents;
        std::vector<std::uint16_t> forwards;
        std::vector<std::uint16_t> entry_ports;
        std::vector<tcp::socket> clients;
        nodes.push_back(std::make_shared<RelayNode>(control_io, transfer_io, udp_io, cluster_data.io, server_context,
            server_config("master", ClusterConfig::Role::Master, cluster_port, primary_port,
                          unused_port(transfer_io, used), unused_udp_port(udp_io, used), used)));
        auto consumer_config = client_config(primary_port);
        consumer_config.routing_max_nodes = 1;
        for (unsigned i = 0; i < 10; ++i)
        {
            const auto control_port = unused_port(control_io, used);
            entry_ports.push_back(control_port);
            const auto service = "service-" + std::to_string(i);
            nodes.push_back(std::make_shared<RelayNode>(control_io, transfer_io, udp_io, cluster_data.io, server_context,
                server_config("entry-" + std::to_string(i), ClusterConfig::Role::Slave, cluster_port, control_port,
                              unused_port(transfer_io, used), unused_udp_port(udp_io, used), used)));
            auto producer_config = client_config(control_port);
            producer_config.services.push_back({service, "127.0.0.1", echo.local_endpoint().port(), RelayProtocol::Tcp});
            if (i == 8)
            {
                producer_config.services.push_back({"late-udp", "127.0.0.1", udp_echo.local_endpoint().port(), RelayProtocol::Udp});
            }
            agents.push_back(std::make_shared<RelayAgent>(control_io, transfer_io, client_context, std::move(producer_config)));
            forwards.push_back(unused_port(transfer_io, used));
            consumer_config.forwards.push_back({service, "127.0.0.1", forwards.back(), RelayProtocol::Tcp});
        }
        consumer_config.forwards.push_back({"late-udp", "127.0.0.1", udp_forward, RelayProtocol::Udp});
        agents.push_back(std::make_shared<RelayAgent>(control_io, transfer_io, client_context, std::move(consumer_config)));
        for (std::size_t i = 0; i + 1 < nodes.size(); ++i)
        {
            nodes[i]->start();
        }
        forward_reservation.close();
        agents.back()->start();
        for (std::size_t i = 0; i < 8; ++i)
        {
            agents[i]->start();
        }
        agents[9]->start();
        std::thread control_thread([&] { control_io.run(); });
        std::thread transfer_thread([&] { transfer_io.run(); });
        std::thread udp_thread([&] { udp_io.run(); });
        ScopeGuard cleanup([&]() noexcept {
            for (const auto &agent : agents)
            {
                agent->stop();
            }
            for (const auto &node : nodes)
            {
                node->stop();
            }
            asio::post(control_io, [&] { clients.clear(); });
            asio::post(transfer_io, [&] { asio::error_code ignored; echo.close(ignored); udp_echo.close(ignored); });
            control_work.reset(); transfer_work.reset(); udp_work.reset();
            control_thread.join(); transfer_thread.join(); udp_thread.join();
        });
        asio::co_spawn(control_io, verify_unavailable(*agents.back(), forwards, nodes), asio::use_future).get();
        asio::post(control_io, [&] {
            agents[8]->start();
            nodes.back()->start();
        });
        asio::co_spawn(control_io, verify_entries(*agents.back(), nodes, forwards, clients), asio::use_future).get();
        asio::co_spawn(control_io, verify_late_udp(nodes[9], udp_forward), asio::use_future).get();
        asio::co_spawn(control_io, verify_primary_reconnect(*agents.back(), clients), asio::use_future).get();
        asio::co_spawn(control_io, verify_entries(*agents.back(), nodes, forwards, clients), asio::use_future).get();
        asio::co_spawn(control_io, verify_late_udp(nodes[9], udp_forward), asio::use_future).get();
        agents.back()->stop();
        asio::co_spawn(control_io, wait_empty(nodes), asio::use_future).get();
        asio::co_spawn(control_io, verify_detach(client_context, primary_port, nodes), asio::use_future).get();
        asio::co_spawn(control_io, verify_attach_wait(client_context, primary_port, entry_ports[0], nodes),
                       asio::use_future).get();
        std::cout << "[PASS] Late producer/Node recovery, proactive UDP, delayed attach, timeout and cancellation\n";
        std::cout << "[PASS] Ten entrances share one Agent control; disconnection closes old business and reconnect establishes new business\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
