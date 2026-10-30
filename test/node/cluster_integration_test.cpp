#include "test_data.h"
#include "cluster_mgr.h"
#include "node_fixture.h"
#include <deque>
#include <iostream>
#include <set>
#include <thread>

using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

TEST_MEMBER(RelayNode, registry_)

namespace
{
void require(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

CtrlMessage budget_message(std::string target, std::size_t payload_size)
{
    return CtrlMessage{"budget.notice", njson{{"target", std::move(target)}, {"value", std::string(payload_size, 'x')}}};
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
    // Full-budget CBOR encoding can occupy this single executor for seconds in Debug builds.
    channel.heartbeat_timeout = 30s;

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
        require(node->broadcast(CtrlMessage{"before.start"}) == ClusterMgr::SendResult::Stopped, "Stopped cluster accepted a send");
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
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("notice") == 1; });
        require(b_inbox.messages.back().first == "a", "Broadcast lost the authenticated source");
        require(!b_inbox.messages.back().second.params->contains("target"),
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
        require(a_inbox.count("notice") == 0, "Generic broadcast was echoed back to its source");

        broadcast(b, CtrlMessage{"reported", njson{{"request_id", 1}, {"value", 42}}});
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("reported") == 1; });
        require(a_inbox.messages.back().first == "b" &&
                    a_inbox.messages.back().second.params->at("request_id") == 1,
                "Protocol response lost source or request_id");
        for (const auto command : {CtrlCommand::ServiceLookup, CtrlCommand::ServerStatusQuery, CtrlCommand::RelayDetach})
        {
            const auto name = ctrl_command_name(command);
            broadcast(a, CtrlMessage(command, njson{{"request_id", 41U}, {"session_id", 1U},
                {"agent_session_id", 1U}, {"service", "missing-service"}, {"protocol", "tcp"}}));
            co_await receive_until(b, b_inbox, [&] { return b_inbox.count(name) == 1; });
            const auto barriers = a_inbox.count("origin.barrier");
            master->send("a", CtrlMessage{"origin.barrier"});
            co_await receive_until(a, a_inbox, [&] { return a_inbox.count("origin.barrier") > barriers; });
            std::cout << "[MESSAGES] command=" << name << " source_deliveries=" << a_inbox.count(name)
                      << " other_peer_deliveries=" << b_inbox.count(name) << '\n';
            require(a_inbox.count(name) == 0, "Broadcast was echoed back to its source");
            require(b_inbox.messages.back().first == "a", "Filtered broadcast lost its authenticated source");
        }
        require(master->broadcast(CtrlMessage{"master.notice"}) == ClusterMgr::SendResult::Submitted, "Master rejected a broadcast submission");
        co_await receive_until(a, a_inbox, [&] { return a_inbox.count("master.notice") == 1; });
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("master.notice") == 1; });
        require(b_inbox.count("reported") == 0, "Response broadcast was echoed back to its source");
        require(master->send({}, CtrlMessage{"invalid.target"}) == ClusterMgr::SendResult::InvalidMessage, "Empty target was accepted");
        require(master->broadcast(CtrlMessage{"invalid.params", njson::array()}) == ClusterMgr::SendResult::InvalidMessage, "Invalid parameters were accepted");
        require(master->broadcast(CtrlMessage{"cluster.invalid"}) == ClusterMgr::SendResult::InvalidMessage, "Reserved command was accepted");
        require(master->broadcast(CtrlMessage{std::string(CtrlMessage::max_command_length + 1, 'x')}) == ClusterMgr::SendResult::InvalidMessage,
                "Encoding failure was reported as a successful submission");
        require(master->broadcast(CtrlMessage{"after.invalid"}) == ClusterMgr::SendResult::Submitted, "Invalid send prevented the next submission");
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("after.invalid") == 1; });

        // Leave room for conservative estimation of this message's small envelope.
        // Authentication by the largest identity must still fit the final wire budget.
        const auto legal_size = cluster_max_message_length - 256;
        const std::string longest_id(cluster_max_node_id_length, 'n');
        auto large = co_await connect(context, port);
        large->send(CtrlMessage{CtrlCommand::ClusterJoin, njson{{"node_id", longest_id}}});
        Inbox large_inbox;
        co_await receive_until(large, large_inbox, [&] { return large_inbox.count("cluster.joined") == 1; });
        auto receive_budget = [&](const std::shared_ptr<TLSChannel> &peer, Inbox &inbox) -> asio::awaitable<void> {
            co_await receive_until(peer, inbox, [&] { return inbox.count("budget.notice") == 1; });
            const auto &[source, message] = inbox.messages.back();
            require(source == longest_id && !message.params->contains("target"),
                    "Boundary message lost authentication or leaked its target");
            require(message.params->at("value").get_ref<const std::string &>().size() == legal_size,
                    "Boundary message changed its payload");
            inbox.messages.pop_back();
        };
        large->send(budget_message("b", legal_size));
        co_await receive_budget(b, b_inbox);
        large->send(budget_message("all", legal_size));
        co_await receive_budget(a, a_inbox);
        co_await receive_budget(b, b_inbox);
        master->send(longest_id, CtrlMessage{"budget.origin.barrier"});
        co_await receive_until(large, large_inbox, [&] { return large_inbox.count("budget.origin.barrier") == 1; });
        require(large_inbox.count("budget.notice") == 0, "Large broadcast was echoed back to its source");

        // Fits the incoming wire limit, but the largest authenticated source would push it over.
        large->send(budget_message("b", WireMessage::max_message_length - 128));
        large->send(CtrlMessage{"budget.barrier", njson{{"target", "b"}}});
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("budget.barrier") == 1; });
        require(b_inbox.count("budget.notice") == 0, "Oversized input was forwarded to a healthy target");
        require(master->send("b", budget_message("b", cluster_max_message_length)) ==
                    ClusterMgr::SendResult::InvalidMessage, "Local send consumed the reserved routing budget");
        require(master->send("b", budget_message("b", legal_size)) ==
                    ClusterMgr::SendResult::Submitted, "Local send rejected a legal near-limit input");
        co_await receive_until(b, b_inbox, [&] { return b_inbox.count("budget.notice") == 1; });
        require(b_inbox.messages.back().first == "master", "Maximum local send lost its source");
        b_inbox.messages.pop_back();
        co_await large->async_disconnect();

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

        auto oversized_id = co_await connect(context, port);
        oversized_id->send(CtrlMessage{CtrlCommand::ClusterJoin,
                                      njson{{"node_id", std::string(cluster_max_node_id_length + 1, 'n')}}});
        require((co_await oversized_id->async_receive()).type() == CtrlCommand::ClusterError,
                "Oversized remote identity joined the cluster");
        co_await oversized_id->async_disconnect();

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
        master->send("raw", CtrlMessage{"identity.barrier"});
        auto raw_delivery = co_await raw->async_receive();
        while (raw_delivery.command == "topology.members")
        {
            const auto &members = config::message_params(raw_delivery);
            require(members.at("master") == "master" && members.at("members").is_array(),
                    "Cluster membership announcement is invalid");
            raw_delivery = co_await raw->async_receive();
        }
        require(raw_delivery.command == "identity.barrier" && raw_delivery.params->at("source") == "master",
                "Raw peer received its own broadcast or lost the ordering barrier");
        require(!raw_delivery.params->contains("target"), "Raw unicast leaked its target");
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

        // Overflow must drop pending messages and close joined and handshaking peers.
        tcp::socket stalled(io);
        co_await stalled.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
        auto result = ClusterMgr::SendResult::Submitted;
        for (std::size_t i = 0; i < channel.max_queued_messages + 2 &&
                                result == ClusterMgr::SendResult::Submitted; ++i)
        {
            result = master->send("b", CtrlMessage{"queued.notice"});
        }
        require(result == ClusterMgr::SendResult::QueueFull, "Master overflow did not return QueueFull");
        require(!co_await master->async_stop(), "Overflow did not preserve its failure result");
        bool closed = false;
        try
        {
            for (;;)
            {
                const auto message = co_await b->async_receive(2s);
                require(message.command != "queued.notice", "Failed master delivered a discarded message");
            }
        }
        catch (const asio::system_error &error)
        {
            closed = error.code() == asio::experimental::error::channel_closed ||
                     error.code() == asio::experimental::error::channel_cancelled;
        }
        require(closed, "Overflow did not close the remote control channel");
        require(master->send("b", CtrlMessage{"after.stop"}) == ClusterMgr::SendResult::Stopped, "Stopped master accepted submission");
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
        require(handshaking->send("master", CtrlMessage{"not.joined"}) == ClusterMgr::SendResult::NotConnected, "Unjoined slave accepted submission");
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

asio::awaitable<CtrlMessage> client_reply(const std::shared_ptr<TLSChannel> &client, CtrlCommand command)
{
    auto message = co_await client->async_receive(3s);
    require(message.type() == command, "Public query returned an unexpected response");
    for (const auto *field : {"source", "target", "session_id", "requester_node"})
    {
        require(!message.params->contains(field), "Internal routing field leaked to the client");
    }
    co_return message;
}

asio::awaitable<CtrlMessage> peer_query(const std::shared_ptr<TLSChannel> &peer, std::uint64_t request)
{
    for (;;)
    {
        auto message = co_await peer->async_receive(3s);
        if (message.type() == CtrlCommand::ServiceLookup && message.params->at("request_id") == request)
        {
            co_return message;
        }
    }
}

CtrlMessage located_reply(const CtrlMessage &query)
{
    const auto &params = *query.params;
    return CtrlMessage(CtrlCommand::ServiceLocated,
        njson{{"target", params.at("source")}, {"requester_node", params.at("source")},
              {"session_id", params.at("session_id")}, {"request_id", params.at("request_id")},
              {"source", "forged"}, {"node_id", "forged"}, {"address", "127.0.0.1"},
              {"service", params.at("service")}, {"protocol", params.at("protocol")}});
}

asio::awaitable<void> verify_public_queries(const std::shared_ptr<TLSChannel> &peer,
                                           asio::ssl::context &client_context, const std::shared_ptr<RelayNode> &master,
                                           std::uint16_t master_port, std::uint16_t slave_port)
{
    auto local = co_await connect(client_context, slave_port);
    auto remote = co_await connect(client_context, master_port);
    local->send(CtrlMessage(CtrlCommand::ServiceRegister,
                           njson{{"request_id", 100U}, {"service", "public-query"}, {"protocol", "tcp"}}));
    co_await client_reply(local, CtrlCommand::ServiceOk);
    njson snapshot;
    for (const auto &client : {local, remote})
    {
        client->send(CtrlMessage(CtrlCommand::ServiceLookup,
                                njson{{"request_id", 101U}, {"service", "public-query"}, {"protocol", "tcp"}}));
        const auto service = co_await client_reply(client, CtrlCommand::ServiceLocated);
        require(service.params->at("node_id") == "slave" && service.params->at("address") == "127.0.0.1" &&
                    !service.params->contains("port"),
                "Local/remote service discovery changed its owner or endpoint");
        client->send(CtrlMessage(CtrlCommand::TopologyQuery, njson{{"request_id", 103U}}));
        const auto topology = co_await client_reply(client, CtrlCommand::TopologySnapshot);
        require(topology.params->at("nodes").size() >= 2, "Topology query lost cluster members");
        snapshot = *topology.params;
        test_node::cluster(master).send("observer", CtrlMessage{"topology.barrier"});
        std::size_t unrelated_queries = 0;
        for (;;)
        {
            const auto observed = co_await peer->async_receive(3s);
            if (observed.command == "topology.barrier")
            {
                break;
            }
            if (observed.type() == CtrlCommand::TopologyQuery)
            {
                ++unrelated_queries;
            }
        }
        std::cout << "[MESSAGES] topology unrelated_peer_queries=" << unrelated_queries << '\n';
        require(unrelated_queries == 0, "Topology query reached a non-master Node");
        client->send(CtrlMessage(CtrlCommand::ServerCluster, njson{{"request_id", 104U}}));
        std::set<std::string> reported;
        for (unsigned i = 0; i < 2; ++i)
        {
            const auto status = co_await client_reply(client, CtrlCommand::ServerStatusReported);
            reported.insert(status.params->at("node_id").get<std::string>());
            require(status.params->at("request_id") == 104U && status.params->at("relays").empty() &&
                        status.params->at("uptime_ms").get<std::uint64_t>() < 30'000,
                    "Status query changed its request, ready-only relays or startup clock");
        }
        require(reported == std::set<std::string>{"slave", "test-master"}, "Cluster status lost or duplicated a node");
        client->send(CtrlMessage(CtrlCommand::ServerLoad, njson{{"request_id", 105U}}));
        const auto load = co_await client_reply(client, CtrlCommand::ServerLoaded);
        require(load.params->contains("transfer_udp_queue_delay_us"), "Load query lost executor metrics");
    }
    local->send(CtrlMessage(CtrlCommand::ServiceList, njson{{"request_id", 106U}}));
    const auto listed = co_await client_reply(local, CtrlCommand::ServiceListed);
    require(listed.params->at("services") == njson::array({"public-query"}), "Service list lost the registration");
    local->send(CtrlMessage(CtrlCommand::ServerTraffic, njson{{"request_id", 107U}}));
    const auto traffic = co_await client_reply(local, CtrlCommand::ServerTrafficReported);
    require(traffic.params->at("services").size() == 1, "Traffic query lost the registered service");

    // The observer holds a real lookup so replies can exercise routing without timing races.
    remote->send(CtrlMessage(CtrlCommand::ServiceLookup,
                             njson{{"request_id", 201U}, {"service", "observer-service"}, {"protocol", "tcp"}}));
    const auto query = co_await peer_query(peer, 201U);
    auto forged_snapshot = snapshot;
    forged_snapshot["target"] = query.params->at("source");
    forged_snapshot["requester_node"] = query.params->at("source");
    forged_snapshot["session_id"] = query.params->at("session_id");
    forged_snapshot["request_id"] = query.params->at("request_id");
    forged_snapshot["source"] = "test-master";
    peer->send(CtrlMessage(CtrlCommand::TopologySnapshot, std::move(forged_snapshot)));
    auto wrong = located_reply(query);
    (*wrong.params)["requester_node"] = "another-node";
    (*wrong.params)["address"] = "wrong.invalid";
    peer->send(std::move(wrong));
    peer->send(located_reply(query));
    const auto correct = co_await client_reply(remote, CtrlCommand::ServiceLocated);
    require(correct.params->at("node_id") == "observer" && correct.params->at("address") == "127.0.0.1",
            "Reply accepted a wrong requester or trusted a forged source/node ID");

    remote->send(CtrlMessage(CtrlCommand::ServiceLookup,
                             njson{{"request_id", 202U}, {"service", "observer-service"}, {"protocol", "tcp"}}));
    const auto late = co_await peer_query(peer, 202U);
    co_await remote->async_disconnect();
    const auto old_session = late.params->at("session_id").get<std::uint64_t>();
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (test_access::RelayNode_registry_(*master).contains(old_session))
    {
        require(std::chrono::steady_clock::now() < deadline, "Disconnected client remained registered");
        asio::steady_timer wait(co_await asio::this_coro::executor, 1ms);
        co_await wait.async_wait(asio::use_awaitable);
    }
    auto replacement = co_await connect(client_context, master_port);
    replacement->send(CtrlMessage(CtrlCommand::ServiceLookup,
                             njson{{"request_id", 203U}, {"service", "observer-service"}, {"protocol", "tcp"}}));
    const auto fresh = co_await peer_query(peer, 203U);
    require(fresh.params->at("session_id") != old_session, "Replacement connection reused a retired session ID");
    peer->send(located_reply(late));
    peer->send(located_reply(fresh));
    const auto current = co_await client_reply(replacement, CtrlCommand::ServiceLocated);
    require(current.params->at("request_id") == 203U, "Late reply reached the replacement client");
    co_await local->async_disconnect();
    co_await replacement->async_disconnect();
}

void verify_relay_node(asio::ssl::context &context, asio::ssl::context &client_context)
{
    asio::io_context control(1);
    asio::io_context tcp_io(1);
    asio::io_context udp_io(1);
    TestClusterDataIO cluster_data;
    tcp::acceptor reservation(control, {asio::ip::address_v4::loopback(), 0});
    const auto port = reservation.local_endpoint().port();
    reservation.close();

    auto settings = make_test_node_config();
    context.load_verify_file((test_data::directory() / "tls_channel_test_client_ca.pem").string());
    settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address = "127.0.0.1";
    settings.cluster.control_port = port;
    settings.channel.disconnect_timeout = 100ms;
    tcp::acceptor client_reservation(control, {asio::ip::address_v4::loopback(), 0});
    const auto master_port = client_reservation.local_endpoint().port();
    client_reservation.close();
    settings.control.port = master_port;
    auto master = std::make_shared<RelayNode>(control, tcp_io, udp_io, cluster_data.io, context, settings);
    master->start();
    settings.cluster.role = ClusterConfig::Role::Slave;
    settings.cluster.node_id = "slave";
    configure_test_cluster_data(settings.cluster);
    client_reservation.open(tcp::v4());
    client_reservation.bind({asio::ip::address_v4::loopback(), 0});
    const auto slave_port = client_reservation.local_endpoint().port();
    client_reservation.close();
    settings.control.port = slave_port;
    auto slave = std::make_shared<RelayNode>(control, tcp_io, udp_io, cluster_data.io, context, settings);
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
        require(test_node::cluster(slave).broadcast(CtrlMessage{"cluster.invalid"}) == ClusterMgr::SendResult::InvalidMessage,
                "Slave accepted a reserved command");
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
        require(test_node::cluster(slave).send("test-master", CtrlMessage{"slave.notice"}) == ClusterMgr::SendResult::Submitted,
                "Joined slave rejected a valid submission");
        co_await verify_public_queries(peer, client_context, master, master_port, slave_port);
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
        restarted.control.port = master_port;
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
        const auto data = test_data::directory();
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
        verify_relay_node(context, client_context);
        std::cout << "[PASS] cluster room, broadcast, mTLS, reconnect and shutdown\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
