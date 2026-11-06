#include "agent_stop.h"
#include "node_connection.h"
#include "relay_agent.h"
#include "xfr_channel.h"
#include "test_data.h"
#include <future>
#include <iostream>
#include <string_view>

#include "member_access.h"
TEST_MEMBER(NodeConnection, channel_)
TEST_MEMBER(RelayAgent, connection_)
TEST_MEMBER(RelayAgent, control_)
TEST_MEMBER(AgentControl, running_)
TEST_MEMBER(AgentControl, service_locations_)
TEST_MEMBER(AgentControl, next_request_id_)

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

asio::awaitable<void> exercise(NodeConnection &connection, Peer &primary, Peer &entry,
                               std::shared_ptr<RelayAgent> agent)
{
    co_await wait_for([&] { return primary.identified == 1; });
    require(!connection.ready(), "Primary became ready before server identification");
    primary.identify_node();
    co_await wait_for([&] { return connection.ready(); });
    auto &control = test_access::RelayAgent_control_(*agent);
    test_access::AgentControl_running_(control) = true;
    const ServiceKey service{"ready-entry", RelayProtocol::Tcp};
    const auto request = test_access::AgentControl_next_request_id_(control);
    for (unsigned i = 0; i < 10; ++i)
    {
        const auto destination = "entry-" + std::to_string(i);
        test_access::AgentControl_service_locations_(control)[service] =
            {0, destination, "127.0.0.1"};
        auto selected = co_await agent->select_relay(service, destination, Clock::now() + 1s);
        require(selected.server.node == destination &&
                    selected.server.channel.lock() == test_access::NodeConnection_channel_(connection).lock(),
                "Destination selection did not reuse the primary channel");
    }
    require(entry.accepted == 0 && primary.accepted == 1 &&
                test_access::AgentControl_next_request_id_(control) == request,
            "Ten entrances opened extra controls or allocated entry lookup requests");
    bool expired = false;
    try
    {
        co_await agent->select_relay(service, "entry-9", Clock::now());
    }
    catch (const std::runtime_error &)
    {
        expired = true;
    }
    require(expired, "Selection renewed an expired establishment budget");
    primary.node = "replacement-primary";
    primary.hold_identification = false;
    primary.channel->disconnect();
    co_await wait_for([&] { return connection.ready() && connection.node_id() == primary.node; });
    primary.hold_identification = true;
    primary.channel->disconnect();
    co_await wait_for([&] { return primary.identified == 3; });
    require(!connection.ready(), "Primary retry skipped identification");
    const auto channel = test_access::NodeConnection_channel_(connection);
    co_await test_agent::stop_from_owner(agent);
    require(!connection.ready() && channel.expired(), "Stop did not drain the sole primary control");
    primary.stop();
    entry.stop();
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
        primary.hold_identification = true;
        AgentConfig config;
        config.host = "127.0.0.1";
        config.port = primary.acceptor.local_endpoint().port();
        config.channel.disconnect_timeout = 100ms;
        asio::io_context transfer(1);
        auto agent = std::make_shared<RelayAgent>(io, transfer, client, config);
        auto &connection = test_access::RelayAgent_connection_(*agent);
        connection.start();
        auto primary_done = asio::co_spawn(io, primary.run(), asio::use_future);
        auto entry_done = asio::co_spawn(io, entry.run(), asio::use_future);
        auto result = asio::co_spawn(io, exercise(connection, primary, entry, agent), asio::use_future);
        const auto deadline = Clock::now() + 8s;
        while (result.wait_for(0ms) != std::future_status::ready)
        {
            require(Clock::now() < deadline, "Connection lifecycle test did not drain");
            io.restart();
            io.run_for(5ms);
            transfer.restart();
            transfer.poll();
        }
        result.get();
        io.run();
        primary_done.get();
        entry_done.get();
        const std::weak_ptr<RelayAgent> owner = agent;
        agent.reset();
        require(owner.expired(), "Drained connection or background task retained the Agent");
        std::cout << "[PASS] Primary identification, ten logical entrances, reconnect cancellation, drain and owner release\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
