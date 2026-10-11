#include "cluster_mgr.h"
#include "node_fixture.h"
#include <deque>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

namespace
{
void require(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

struct Inbox
{
    std::deque<std::pair<std::string, CtrlMessage>> messages;

    void push(CtrlMessage message)
    {
        std::string source;
        if (message.params && message.params->is_object())
        {
            const auto iterator = message.params->find("source");
            if (iterator != message.params->end() && iterator->is_string())
                source = iterator->get<std::string>();
        }
        messages.emplace_back(std::move(source), std::move(message));
    }

    std::size_t count(std::string_view command) const
    {
        return std::ranges::count_if(messages, [&](const auto &entry) { return entry.second.command == command; });
    }
};

asio::awaitable<void> receive_until(const std::shared_ptr<TLSChannel> &server, Inbox &inbox,
                                    std::function<bool()> predicate)
{
    while (!predicate())
    {
        inbox.push(co_await server->async_receive());
    }
}

void configure(asio::ssl::context &context, const std::filesystem::path &data, bool client_identity = false)
{
    context.load_verify_file((data / "tls_channel_test_ca.pem").string());
    context.use_certificate_chain_file(
        (data / (client_identity ? "tls_channel_test_client.pem" : "tls_channel_test_server.pem")).string());
    context.use_private_key_file(
        (data / (client_identity ? "tls_channel_test_client.key" : "tls_channel_test_server.key")).string(),
        asio::ssl::context::pem);
}

asio::awaitable<std::shared_ptr<TLSChannel>> connect(asio::ssl::context &context, std::uint16_t port,
                                                     std::string host = "127.0.0.1")
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
    TLSChannelConfig config;
    config.disconnect_timeout = 100ms;
    auto channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::C, config);
    co_await channel->start(std::move(host));
    co_return channel;
}

asio::awaitable<void> verify(asio::io_context &io, asio::ssl::context &context, asio::ssl::context &client_context,
                             asio::ssl::context &anonymous_context)
{
    asio::io_context tcp_io(1);
    asio::io_context udp_io(1);
    TestClusterDataIO cluster_data;
    tcp::acceptor reservation(io, {asio::ip::address_v4::loopback(), 0});
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    TLSChannelConfig channel;
    channel.handshake_timeout = 300ms;
    channel.disconnect_timeout = 100ms;
    channel.heartbeat_interval = 100ms;
    channel.heartbeat_timeout = 2s;

    Inbox a_inbox;
    Inbox b_inbox;
    std::vector<std::shared_ptr<RelayNode>> owners;
    std::vector<std::shared_ptr<ClusterMgr>> nodes;
    auto make_owner = [&](const ClusterConfig &cluster) {
        auto settings = make_test_node_config();
        settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address =
            "127.0.0.1";
        settings.cluster = cluster;
        settings.channel = channel;
        auto owner = std::make_shared<RelayNode>(io, tcp_io, udp_io, cluster_data.io, context, std::move(settings));
        owners.push_back(owner);
        return owner;
    };
    auto make = [&](ClusterConfig::Role role, std::string node_id, std::shared_ptr<RelayNode> &owner) {
        ClusterConfig config{role, std::move(node_id), "127.0.0.1", port};
        owner = make_owner(config);
        auto node = std::make_shared<ClusterMgr>(
            *owner, io.get_executor(), context, std::move(config), channel, 16);
        node->start();
        nodes.push_back(node);
        return node;
    };

    std::shared_ptr<RelayNode> master_owner;
    auto master = make(ClusterConfig::Role::Master, "master", master_owner);
    std::shared_ptr<TLSChannel> a;
    std::shared_ptr<TLSChannel> b;
    std::exception_ptr failure;
    try
    {
        a = co_await connect(context, port);
        b = co_await connect(context, port);
        a->send(CtrlMessage{"cluster.join", njson{{"node_id", "a"}}});
        b->send(CtrlMessage{"cluster.join", njson{{"node_id", "b"}}});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("cluster.joined") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("cluster.joined") == 1; });

        const auto broadcast = [&](const std::shared_ptr<TLSChannel> &peer, CtrlMessage message) {
            if (!message.params) message.params = njson::object();
            (*message.params)["target"] = "all";
            peer->send(std::move(message));
        };
        broadcast(a, CtrlMessage{"notice", njson{{"request_id", 1}}});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("notice") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("notice") == 1; });
        require(a_inbox.messages.back().first == "a" && b_inbox.messages.back().first == "a",
                "Broadcast lost the authenticated source");
        require(!a_inbox.messages.back().second.params->contains("target") &&
                    !b_inbox.messages.back().second.params->contains("target"),
                "Cluster target leaked into a broadcast delivery");

        a->send(CtrlMessage{"direct", njson{{"target", "b"}, {"request_id", 2}}});
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("direct") == 1; });
        require(b_inbox.messages.back().first == "a" &&
                    b_inbox.messages.back().second.params->at("request_id") == 2 &&
                    !b_inbox.messages.back().second.params->contains("target"),
                "Directed message lost its source or leaked its target");
        master->broadcast(CtrlMessage{"direct.barrier"});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("direct.barrier") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("direct.barrier") == 1; });
        require(a_inbox.count("direct") == 0 && b_inbox.count("direct") == 1,
                "Directed message reached a non-target peer");

        broadcast(b, CtrlMessage{"reported", njson{{"request_id", 1}, {"value", 42}}});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("reported") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("reported") == 1; });
        require(a_inbox.messages.back().first == "b" &&
                    a_inbox.messages.back().second.params->at("request_id") == 1,
                "Protocol response lost source or request_id");
        master->broadcast(CtrlMessage{"master.notice"});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("master.notice") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("master.notice") == 1; });
        master->broadcast(CtrlMessage{std::string(CtrlMessage::max_command_length + 1, 'x')});
        master->broadcast(CtrlMessage{"after.invalid"});
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("after.invalid") == 1; });

        auto duplicate = co_await connect(context, port);
        duplicate->send(CtrlMessage{"cluster.join", njson{{"node_id", "a"}}});
        require((co_await duplicate->async_receive()).type() == CtrlCommand::ClusterError,
                "Duplicate node joined the room");
        co_await duplicate->async_disconnect();

        auto reserved = co_await connect(context, port);
        reserved->send(CtrlMessage{"cluster.join", njson{{"node_id", "all"}}});
        require((co_await reserved->async_receive()).command == "cluster.error",
                "Reserved all node_id joined the room");
        // A rejected peer may send another frame instead of closing voluntarily.
        reserved->send(CtrlMessage{"ignored"});
        co_await asio::co_spawn(io, reserved->async_wait_closed(), asio::cancel_after(1s, asio::use_awaitable));
        co_await reserved->async_disconnect();

        // A server certificate is a valid member identity; ordinary client identities are rejected.
        for (auto *invalid_context : {&client_context, &anonymous_context})
        {
            bool rejected = false;
            std::shared_ptr<TLSChannel> peer;
            try
            {
                peer = co_await connect(*invalid_context, port);
                peer->send(CtrlMessage{"cluster.join", njson{{"node_id", "unauthorized"}}});
                co_await peer->async_receive();
            }
            catch (const std::exception &)
            {
                rejected = true;
            }
            if (peer)
                co_await peer->async_disconnect();
            require(rejected, "Untrusted or absent member certificate accepted");
        }

        bool wrong_host = false;
        try
        {
            co_await connect(context, port, "wrong-host.invalid");
        }
        catch (const std::exception &)
        {
            wrong_host = true;
        }
        require(wrong_host, "Master hostname verification bypassed");

        auto raw = co_await connect(context, port);
        raw->send(CtrlMessage{"cluster.join", njson{{"node_id", "raw"}}});
        require((co_await raw->async_receive()).command == "cluster.joined", "Raw peer did not join");
        raw->send(CtrlMessage{"identity", njson{{"target", "all"}, {"source", "forged"}}});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("identity") == 1; });
        require(a_inbox.messages.back().first == "raw", "Peer forged the broadcast source");
        auto raw_delivery = co_await raw->async_receive();
        while (raw_delivery.command == "topology.members")
        {
            const auto &members = config::message_params(raw_delivery);
            require(members.at("master") == "master" && members.at("members").is_array(),
                    "Cluster membership announcement is invalid");
            raw_delivery = co_await raw->async_receive();
        }
        require(raw_delivery.command == "identity" && raw_delivery.params->at("source") == "raw",
                "Raw peer did not receive its own broadcast");
        require(!raw_delivery.params->contains("target"), "Raw broadcast leaked its target");
        raw->send(CtrlMessage{"cluster.invalid"});
        bool invalid_closed = false;
        try
        {
            co_await raw->async_receive();
        }
        catch (const std::exception &)
        {
            invalid_closed = true;
        }
        require(invalid_closed, "Invalid cluster command did not close the session");
        co_await raw->async_disconnect();

        // Stop master sessions during TLS handshake and slaves while connected.
        tcp::socket stalled(io);
        co_await stalled.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
        co_await master->async_stop();
        co_await a->async_disconnect();
        co_await b->async_disconnect();

        // Pending handshakes count against the cluster connection limit.
        ClusterConfig limited_config{ClusterConfig::Role::Master, "limited", "127.0.0.1", port};
        auto limited_owner = make_owner(limited_config);
        auto limited = std::make_shared<ClusterMgr>(*limited_owner, io.get_executor(), context,
                                                    std::move(limited_config), channel, 1);
        nodes.push_back(limited);
        limited->start();
        tcp::socket first(io);
        tcp::socket second(io);
        co_await first.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
        co_await second.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
        std::array<char, 1> byte{};
        auto [limit_error, size] =
            co_await second.async_read_some(asio::buffer(byte), asio::cancel_after(1s, use_nothrow_awaitable));
        require(limit_error == asio::error::eof || limit_error == asio::error::connection_reset,
                "Cluster connection limit did not reject pending peer");
        co_await limited->async_stop();

        // A slave cancels its in-progress handshake through the connector.
        tcp::acceptor silent_master(io, {asio::ip::address_v4::loopback(), 0});
        ClusterConfig handshaking_config{ClusterConfig::Role::Slave, "handshaking", "127.0.0.1",
                                         silent_master.local_endpoint().port()};
        auto handshaking_owner = make_owner(handshaking_config);
        auto handshaking = std::make_shared<ClusterMgr>(*handshaking_owner, io.get_executor(), context,
                                                        std::move(handshaking_config), channel, 1);
        nodes.push_back(handshaking);
        handshaking->start();
        auto silent_socket = co_await silent_master.async_accept(asio::use_awaitable);
        co_await handshaking->async_stop();
    }
    catch (...)
    {
        failure = std::current_exception();
    }

    if (a) co_await a->async_disconnect();
    if (b) co_await b->async_disconnect();
    for (const auto &node : nodes)
        co_await node->async_stop();
    if (failure)
        std::rethrow_exception(failure);
}

void verify_relay_node(asio::ssl::context &context)
{
    asio::io_context control(1);
    asio::io_context tcp_io(1);
    asio::io_context udp_io(1);
    TestClusterDataIO cluster_data;
    tcp::acceptor reservation(control, {asio::ip::address_v4::loopback(), 0});
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    auto settings = make_test_node_config();
    settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address = "127.0.0.1";
    settings.cluster.control_port = port;
    settings.channel.disconnect_timeout = 100ms;
    auto master = std::make_shared<RelayNode>(control, tcp_io, udp_io, cluster_data.io, context, settings);
    settings.cluster.role = ClusterConfig::Role::Slave;
    settings.cluster.node_id = "slave";
    configure_test_cluster_data(settings.cluster);
    auto slave = std::make_shared<RelayNode>(control, tcp_io, udp_io, cluster_data.io, context, settings);
    master->start();
    slave->start();
    // A real cluster peer observes wire replies; RelayNode needs no generic inbox.
    auto scenario = [&](std::uint64_t previous_epoch) -> asio::awaitable<std::uint64_t> {
        auto peer = co_await connect(context, port);
        peer->send(CtrlMessage{"cluster.join", njson{{"node_id", "observer"}}});
        std::uint64_t epoch = 0;
        bool members_ready = false;
        while (!members_ready)
        {
            auto message = co_await peer->async_receive();
            if (message.type() == CtrlCommand::TopologyMembers)
            {
                epoch = message.params->at("epoch").get<std::uint64_t>();
                members_ready = std::ranges::any_of(message.params->at("members"), [](const njson &member) {
                    return member.at("node_id") == "slave";
                });
            }
        }
        require(epoch && epoch != previous_epoch, "Restart failed to publish a fresh membership epoch");
        // Invalid local messages and malformed business requests must leave the slave connected.
        test_node::cluster(slave).broadcast(CtrlMessage{"cluster.invalid"});
        peer->send(CtrlMessage{"service.lookup", njson{{"target", "slave"}}});
        for (const auto &target : {"slave", "test-master"})
        {
            peer->send(CtrlMessage{CtrlCommand::ServerStatusQuery,
                                  njson{{"target", target}, {"request_id", 1u}, {"session_id", 1u}}});
            CtrlMessage report;
            do
            {
                report = co_await peer->async_receive();
            } while (report.type() != CtrlCommand::ServerStatusReport);
            require(report.params->at("source") == target && report.params->at("request_id") == 1u &&
                        report.params->at("requester_node") == "observer" && !report.params->contains("target"),
                    "Real cluster query lost its routing metadata");
        }
        co_await peer->async_disconnect();
        co_return epoch;
    };

    auto result = asio::co_spawn(control, scenario(0), asio::use_future);
    std::thread control_thread([&] { control.run(); });
    std::thread tcp_thread([&] { tcp_io.run(); });
    std::thread udp_thread([&] { udp_io.run(); });
    std::exception_ptr failure;
    try
    {
        const auto epoch = result.get();
        master->stop();
        auto restarted = make_test_node_config();
        restarted.control.address = restarted.tcp.address = restarted.tls.address = restarted.datagram.address = "127.0.0.1";
        restarted.cluster.control_port = port;
        restarted.channel.disconnect_timeout = 100ms;
        master = std::make_shared<RelayNode>(control, tcp_io, udp_io, cluster_data.io, context, std::move(restarted));
        master->start();
        asio::co_spawn(control, scenario(epoch), asio::use_future).get();
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    master->stop();
    slave->stop();
    control_thread.join();
    tcp_thread.join();
    udp_thread.join();
    if (failure)
        std::rethrow_exception(failure);
}
} // namespace

int main()
{
    try
    {
        const auto data = std::filesystem::path(__FILE__).parent_path() / "data";
        asio::io_context io(1);
        asio::ssl::context context(asio::ssl::context::tls);
        asio::ssl::context client_context(asio::ssl::context::tls);
        asio::ssl::context anonymous_context(asio::ssl::context::tls);
        configure(context, data);
        configure(client_context, data, true);
        anonymous_context.load_verify_file((data / "tls_channel_test_ca.pem").string());
        auto result = asio::co_spawn(io, verify(io, context, client_context, anonymous_context), asio::use_future);
        io.run();
        result.get();
        verify_relay_node(context);
        std::cout << "[PASS] cluster room, broadcast, mTLS, reconnect and shutdown\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
