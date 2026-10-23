#include "agent_stop.h"
#include "node_connection_mgr.h"
#include "relay_agent.h"
#include "xfr_channel.h"
#include "test_data.h"
#include <future>
#include <iostream>
#include <string_view>

#include "member_access.h"
TEST_MEMBER(NodeConnection, running_)
TEST_MEMBER(NodeConnection, channel_)
TEST_MEMBER(RelayAgent, connections_)
TEST_MEMBER(RelayAgent, control_)
TEST_MEMBER(AgentControl, running_)
TEST_MEMBER(AgentControl, service_locations_)
TEST_MEMBER(AgentControl, next_request_id_)
TEST_MEMBER(AgentControl, entry_waits_)
TEST_MEMBER(NodeConnectionMgr, connections_)

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using Clock = std::chrono::steady_clock;

void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

template <class Predicate> asio::awaitable<void> wait_for(Predicate predicate)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = Clock::now() + 2s;
    while (!predicate())
    {
        require(Clock::now() < deadline, "Control connection condition timed out");
        timer.expires_after(1ms);
        co_await timer.async_wait(asio::use_awaitable);
    }
}

// Real mTLS peer. Delaying identification separates socket/TLS readiness from Node readiness.
class Peer
{
  public:
    Peer(asio::io_context &io, asio::ssl::context &context, std::string node)
        : acceptor(io, {asio::ip::address_v4::loopback(), 0}), context(context), node(std::move(node)) {}

    asio::awaitable<void> run()
    {
        while (acceptor.is_open())
        {
            auto [error, socket] = co_await acceptor.async_accept(use_nothrow_awaitable);
            if (error)
            {
                co_return;
            }
            ++accepted;
            channel = std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::S);
            try
            {
                co_await channel->start({});
                const auto identify = co_await channel->async_receive();
                require(identify.type() == CtrlCommand::ServerIdentify, "Missing server.identify");
                ++identified;
                if (!hold_identification)
                {
                    identify_node();
                }
                while (acceptor.is_open())
                {
                    co_await channel->async_receive();
                }
            }
            catch (const asio::system_error &)
            {
            }
            co_await channel->async_disconnect();
            channel.reset();
        }
    }

    void identify_node()
    {
        channel->send(CtrlMessage(CtrlCommand::ServerIdentified, njson{{"node_id", node}}));
    }

    void stop()
    {
        acceptor.close();
        if (channel)
        {
            channel->disconnect();
        }
    }

    tcp::acceptor acceptor;
    asio::ssl::context &context;
    std::string node;
    std::shared_ptr<TLSChannel> channel;
    bool hold_identification = false;
    std::size_t accepted = 0;
    std::size_t identified = 0;
};

asio::awaitable<std::shared_ptr<NodeConnection>> exercise(NodeConnectionMgr &manager, Peer &primary, Peer &entry,
                                                        std::shared_ptr<RelayAgent> agent)
{
    co_await wait_for([&] { return manager.primary()->ready(); });
    const auto port = entry.acceptor.local_endpoint().port();
    auto connection = manager.ensure_connection("entry", "127.0.0.1", port);
    require(connection == manager.ensure_connection("entry", "ignored-alias", 1),
            "Node identity did not reuse the existing connection");
    require(connection == manager.ensure_connection({}, "127.0.0.1", port),
            "Endpoint did not reuse the existing connection");
    co_await wait_for([&] { return entry.identified == 1; });
    require(!connection->ready(), "TLS connection became ready before node identification");
    entry.identify_node();
    co_await wait_for([&] { return connection->ready(); });
    require(entry.accepted == 1, "Reused connection started duplicate tasks");

    auto &control = test_access::RelayAgent_control_(*agent);
    const ServiceKey service{"ready-entry", RelayProtocol::Tcp};
    test_access::AgentControl_running_(control) = true;
    test_access::AgentControl_service_locations_(control)[service] = {0, "entry", "127.0.0.1", port};
    control.handle_control_message(*manager.primary(), CtrlMessage(CtrlCommand::ServiceError,
        njson{{"request_id", 0u}, {"service", service.service}, {"protocol", "tcp"}, {"reason", "stale reply"}}));
    require(test_access::AgentControl_service_locations_(control).at(service).node == "entry",
            "Unsolicited service error cleared an already located service");
    const auto request = test_access::AgentControl_next_request_id_(control);
    auto selected = co_await agent->select_relay(service, "entry", Clock::now() + 1s);
    require(selected.connection == connection && test_access::AgentControl_entry_waits_(control).empty() &&
                test_access::AgentControl_next_request_id_(control) == request,
            "Ready entrance allocated an unnecessary request or wait");
    selected.connection.reset();

    // A full lookup table limits new waits, but must not reject a ready entrance.
    auto &waits = test_access::AgentControl_entry_waits_(control);
    const auto deadline = Clock::now() + 1s;
    for (std::uint64_t i = 0; i < 1000; ++i)
    {
        waits.try_emplace(request + i, primary.acceptor.get_executor(), service, "pending-entry", deadline);
    }
    selected = co_await agent->select_relay(service, "entry", deadline);
    require(selected.connection == connection && waits.size() == 1000 &&
                test_access::AgentControl_next_request_id_(control) == request,
            "Pending lookup capacity rejected a ready entrance or allocated another wait");
    selected.connection.reset();

    test_access::AgentControl_service_locations_(control).at(service).node = "new-entry";
    bool full = false;
    try
    {
        co_await agent->select_relay(service, "new-entry", deadline);
    }
    catch (const std::runtime_error &error)
    {
        full = std::string_view(error.what()) == "entry lookup capacity reached";
    }
    require(full && waits.size() == 1000 && test_access::AgentControl_next_request_id_(control) == request,
            "New entrance lookup bypassed the pending lookup limit");
    test_access::AgentControl_service_locations_(control).at(service).node = "entry";
    waits.clear();

    bool rejected = false;
    try
    {
        manager.ensure_connection("another-node", "127.0.0.1", port);
    }
    catch (const std::invalid_argument &)
    {
        rejected = true;
    }
    require(rejected && connection->node_id() == "entry", "Endpoint identity conflict changed a live connection");

    // A retained business handle prevents idle collection, regardless of elapsed time.
    auto now = Clock::now();
    manager.collect_idle_connections(now);
    manager.collect_idle_connections(now + 120s);
    require(connection->ready(), "Idle collection stopped a connection held by a business");
    const std::weak_ptr<NodeConnection> old = connection;
    connection.reset();
    manager.collect_idle_connections(now);
    manager.collect_idle_connections(now + 59s);
    require(test_access::NodeConnection_running_(*old.lock()), "Idle connection stopped before sixty seconds");
    manager.collect_idle_connections(now + 60s);

    // New requests retain the stopping object until its run task has fully drained.
    auto stopping = manager.find_node("entry");
    require(stopping == old.lock() && !stopping->ready(), "Stopping connection disappeared from the index early");
    require(stopping == manager.ensure_connection("entry", "127.0.0.1", port),
            "Selection during shutdown started a competing connection");
    require(entry.accepted == 1 && manager.primary()->ready(), "Idle collection affected primary or duplicated entry");
    stopping.reset();
    co_await wait_for([&] { return old.expired(); });
    require(old.expired(), "Finished connection retained its pool reference");
    require(!manager.find_node("entry"), "Finished connection retained its Node index");
    entry.hold_identification = false;
    auto replacement = manager.ensure_connection("entry", "127.0.0.1", port);
    co_await wait_for([&] { return replacement->ready(); });
    require(entry.accepted == 2, "Finished connection could not be replaced");

    // A listening TCP endpoint that never speaks TLS leaves the client awaiting its handshake.
    tcp::acceptor stalled(primary.acceptor.get_executor(), {asio::ip::address_v4::loopback(), 0});
    auto handshaking = manager.ensure_connection("stalled", "127.0.0.1", stalled.local_endpoint().port());
    co_await wait_for([&] { return !test_access::NodeConnection_channel_(*handshaking).expired(); });
    const auto interrupted_channel = test_access::NodeConnection_channel_(*handshaking);

    co_await test_agent::stop_from_owner(agent);
    require(test_access::NodeConnectionMgr_connections_(manager).size() == 0,
            "Agent stop returned before every connection finished");
    require(!handshaking->ready() && interrupted_channel.expired(),
            "Stopping during TLS handshake published readiness or retained the channel");
    primary.stop();
    entry.stop();
    require(!replacement->ready(), "Drained connection remained ready");
    co_return replacement;
}
} // namespace

int main()
{
    try
    {
        const auto data = test_data::directory();
        asio::ssl::context server(asio::ssl::context::tls_server);
        asio::ssl::context client(asio::ssl::context::tls_client);
        server.use_certificate_chain_file((data / "tls_channel_test_server.pem").string());
        server.use_private_key_file((data / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
        server.load_verify_file((data / "tls_channel_test_client_ca.pem").string());
        client.use_certificate_chain_file((data / "tls_channel_test_client.pem").string());
        client.use_private_key_file((data / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
        client.load_verify_file((data / "tls_channel_test_ca.pem").string());
        asio::io_context io(1);
        Peer primary(io, server, "primary");
        Peer entry(io, server, "entry");
        entry.hold_identification = true;
        AgentConfig config;
        config.host = "127.0.0.1";
        config.port = primary.acceptor.local_endpoint().port();
        config.channel.disconnect_timeout = 100ms;
        asio::io_context transfer(1);
        auto agent = std::make_shared<RelayAgent>(io, transfer, client, config);
        auto &manager = test_access::RelayAgent_connections_(*agent);
        manager.start();
        auto primary_done = asio::co_spawn(io, primary.run(), asio::use_future);
        auto entry_done = asio::co_spawn(io, entry.run(), asio::use_future);
        auto result = asio::co_spawn(io, exercise(manager, primary, entry, agent), asio::use_future);
        const auto deadline = Clock::now() + 8s;
        while (result.wait_for(0ms) != std::future_status::ready)
        {
            require(Clock::now() < deadline, "Connection lifecycle test did not drain");
            io.restart();
            io.run_for(5ms);
            transfer.restart();
            transfer.poll();
        }
        auto retained = result.get();
        io.run();
        primary_done.get();
        entry_done.get();
        const std::weak_ptr<RelayAgent> owner = agent;
        const std::weak_ptr<NodeConnection> retired = retained;
        agent.reset();
        require(owner.expired(), "Drained connection or background task retained the Agent");
        require(!retired.expired(), "Connection handle did not survive its drained owner");
        retained.reset();
        require(retired.expired(), "Drained connection outlived its final business handle");
        std::cout << "[PASS] Node identity, pooled reuse, idle shutdown, handshake cancellation, blocking drain and owner release\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
