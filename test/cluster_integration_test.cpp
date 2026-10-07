#include "cluster_mgr.h"
#include "node_test_config.h"
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

asio::awaitable<void> until(std::function<bool()> predicate)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + 7s;
    while (!predicate())
    {
        require(std::chrono::steady_clock::now() < deadline, "Timed out waiting for cluster message");
        timer.expires_after(10ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
}

asio::awaitable<void> receive_until(const std::shared_ptr<RelayNode> &server, Inbox &inbox,
                                    std::function<bool()> predicate)
{
    while (!predicate())
    {
        inbox.push(co_await server->async_receive_cluster());
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
    asio::io_context tcp_io(1), udp_io(1);
    tcp::acceptor reservation(io, {asio::ip::address_v4::loopback(), 0});
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    TLSChannelConfig channel;
    channel.handshake_timeout = 300ms;
    channel.disconnect_timeout = 100ms;
    channel.heartbeat_interval = 100ms;
    channel.heartbeat_timeout = 2s;

    Inbox master_inbox, a_inbox, b_inbox, duplicate_inbox;
    std::vector<std::shared_ptr<RelayNode>> owners;
    std::vector<std::shared_ptr<ClusterMgr>> nodes;
    auto make_owner = [&](const ClusterConfig &cluster) {
        auto settings = make_test_node_config();
        settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address =
            "127.0.0.1";
        settings.cluster = cluster;
        settings.channel = channel;
        auto owner = std::make_shared<RelayNode>(io, tcp_io, udp_io, context, std::move(settings));
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

    std::shared_ptr<RelayNode> master_owner, a_owner, b_owner;
    auto master = make(ClusterConfig::Role::Master, "master", master_owner);
    auto a = make(ClusterConfig::Role::Slave, "a", a_owner);
    auto b = make(ClusterConfig::Role::Slave, "b", b_owner);
    std::exception_ptr failure;
    try
    {
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("cluster.joined") == 1; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("cluster.joined") == 1; });

        asio::steady_timer live_channel(io);
        live_channel.expires_after(1100ms);
        co_await live_channel.async_wait(asio::use_awaitable);
        require(a_inbox.count("cluster.joined") == 1 && b_inbox.count("cluster.joined") == 1,
                "Slave reconnected while its channel was alive");

        a->broadcast(CtrlMessage{"notice", njson{{"request_id", 1}}});
        co_await receive_until(master_owner, master_inbox, [&] { return master_inbox.count("notice") == 1; });
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("notice") == 1; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("notice") == 1; });
        require(master_inbox.messages.back().first == "a" && b_inbox.messages.back().first == "a",
                "Broadcast source was not taken from the joined session");
        require(!master_inbox.messages.back().second.params->contains("target") &&
                    !a_inbox.messages.back().second.params->contains("target") &&
                    !b_inbox.messages.back().second.params->contains("target"),
                "Cluster target leaked into a broadcast delivery");

        a->send("b", CtrlMessage{"direct", njson{{"request_id", 2}}});
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("direct") == 1; });
        require(b_inbox.messages.back().first == "a" &&
                    b_inbox.messages.back().second.params->at("request_id") == 2 &&
                    !b_inbox.messages.back().second.params->contains("target"),
                "Directed message lost its authenticated source or leaked its target");
        master->broadcast(CtrlMessage{"direct.barrier"});
        co_await receive_until(master_owner, master_inbox,
                               [&] { return master_inbox.count("direct.barrier") == 1; });
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("direct.barrier") == 1; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("direct.barrier") == 1; });
        require(master_inbox.count("direct") == 0 && a_inbox.count("direct") == 0 &&
                    b_inbox.count("direct") == 1,
                "Directed message was delivered to a non-target node");

        b->broadcast(CtrlMessage{"reported", njson{{"request_id", 1}, {"value", 42}}});
        co_await receive_until(master_owner, master_inbox, [&] { return master_inbox.count("reported") == 1; });
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("reported") == 1; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("reported") == 1; });
        require(master_inbox.messages.back().first == "b" &&
                    master_inbox.messages.back().second.params->at("request_id") == 1,
                "Protocol response lost source or request_id");

        master->broadcast(CtrlMessage{"master.notice"});
        co_await receive_until(master_owner, master_inbox,
                               [&] { return master_inbox.count("master.notice") == 1; });
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("master.notice") == 1; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("master.notice") == 1; });

        master->broadcast(CtrlMessage{std::string(CtrlMessage::max_command_length + 1, 'x')});
        co_await receive_until(master_owner, master_inbox, [&] { return master_inbox.count("cluster.error") == 1; });
        master->broadcast(CtrlMessage{"after.invalid"});
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("after.invalid") == 1; });

        std::shared_ptr<RelayNode> duplicate_owner;
        auto duplicate = make(ClusterConfig::Role::Slave, "a", duplicate_owner);
        co_await receive_until(duplicate_owner, duplicate_inbox,
                               [&] { return duplicate_inbox.count("cluster.error") != 0; });
        require(duplicate_inbox.count("cluster.joined") == 0, "Duplicate node joined the room");
        co_await duplicate->async_stop();

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
        co_await receive_until(master_owner, master_inbox, [&] { return master_inbox.count("identity") == 1; });
        require(master_inbox.messages.back().first == "raw", "Peer forged the broadcast source");
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

        co_await master->async_stop();
        master = make(ClusterConfig::Role::Master, "master", master_owner);
        co_await receive_until(a_owner, a_inbox, [&] { return a_inbox.count("cluster.joined") == 2; });
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("cluster.joined") == 2; });
        require(a_inbox.count("notice") == 1 && b_inbox.count("notice") == 1, "History replayed after reconnect");
        a->broadcast(CtrlMessage{"after.reconnect"});
        co_await receive_until(b_owner, b_inbox, [&] { return b_inbox.count("after.reconnect") == 1; });

        // Stop master sessions during TLS handshake and slaves while connected.
        tcp::socket stalled(io);
        co_await stalled.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
        co_await master->async_stop();
        co_await a->async_stop();
        co_await b->async_stop();

        // Pending handshakes count against the cluster connection limit.
        ClusterConfig limited_config{ClusterConfig::Role::Master, "limited", "127.0.0.1", port};
        auto limited_owner = make_owner(limited_config);
        auto limited = std::make_shared<ClusterMgr>(*limited_owner, io.get_executor(), context,
                                                    std::move(limited_config), channel, 1);
        nodes.push_back(limited);
        limited->start();
        tcp::socket first(io), second(io);
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

    for (const auto &node : nodes)
        co_await node->async_stop();
    if (failure)
        std::rethrow_exception(failure);
}

void verify_relay_node(asio::ssl::context &context)
{
    asio::io_context control(1), tcp_io(1), udp_io(1);
    tcp::acceptor reservation(control, {asio::ip::address_v4::loopback(), 0});
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    Inbox master_inbox, slave_inbox;
    auto settings = make_test_node_config();
    settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address = "127.0.0.1";
    settings.cluster.port = port;
    settings.channel.disconnect_timeout = 100ms;
    auto master = std::make_shared<RelayNode>(control, tcp_io, udp_io, context, settings);
    settings.cluster.role = ClusterConfig::Role::Slave;
    settings.cluster.node_id = "slave";
    auto slave = std::make_shared<RelayNode>(control, tcp_io, udp_io, context, settings);
    master->start();
    slave->start();
    auto collect = [&control](const std::shared_ptr<RelayNode> &server, Inbox &inbox) {
        asio::co_spawn(
            control,
            [server, &inbox]() -> asio::awaitable<void> {
                try
                {
                    for (;;)
                    {
                        auto received = co_await server->async_receive_cluster();
                        inbox.push(std::move(received));
                    }
                }
                catch (const asio::system_error &)
                {
                }
            },
            asio::detached);
    };
    collect(master, master_inbox);
    collect(slave, slave_inbox);
    auto scenario = [&]() -> asio::awaitable<void> {
        co_await until([&] { return slave_inbox.count("cluster.joined") == 1; });
        slave->broadcast_cluster(CtrlMessage{"cluster.invalid"});
        co_await until([&] { return slave_inbox.count("cluster.error") == 1; });
        // Business validation failures must not tear down the cluster connection.
        slave->send_cluster("test-master", CtrlMessage{"service.lookup"});
        slave->broadcast_cluster(CtrlMessage{"app.broadcast"});
        co_await until(
            [&] { return master_inbox.count("app.broadcast") == 1 && slave_inbox.count("app.broadcast") == 1; });
        require(master_inbox.messages.back().first == "slave", "RelayNode lost the broadcast source");
        slave->send_cluster("test-master", CtrlMessage{"app.direct"});
        co_await until([&] { return master_inbox.count("app.direct") == 1; });
        master->broadcast_cluster(CtrlMessage{"app.barrier"});
        co_await until(
            [&] { return master_inbox.count("app.barrier") == 1 && slave_inbox.count("app.barrier") == 1; });
        require(slave_inbox.count("app.direct") == 0, "RelayNode directed message reached a non-target node");
        require(slave_inbox.count("cluster.joined") == 1, "Invalid local command disconnected the slave");
    };

    auto result = asio::co_spawn(control, scenario(), asio::use_future);
    std::thread control_thread([&] { control.run(); });
    std::thread tcp_thread([&] { tcp_io.run(); });
    std::thread udp_thread([&] { udp_io.run(); });
    std::exception_ptr failure;
    try
    {
        result.get();
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
        asio::ssl::context context(asio::ssl::context::tls), client_context(asio::ssl::context::tls);
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
