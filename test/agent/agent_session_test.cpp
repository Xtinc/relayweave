#include "node_address_fixture.h"
#include "test_data.h"
#include "agent_session.h"
#include "forwarder.h"
#include <array>
#include <future>
#include <iostream>
#include <string_view>

#include "member_access.h"
TEST_MEMBER(AgentSession, target)
TEST_MEMBER(AgentSession, selection)
TEST_MEMBER(AgentSession, endpoint)
TEST_MEMBER(AgentSession, data)
TEST_MEMBER(AgentSession, phase)
TEST_MEMBER(AgentSession, node_closed)
TEST_MEMBER(AgentSession, reason)
TEST_MEMBER(AgentSession, deadline)
TEST_MEMBER(AgentSession, handle)

namespace
{
struct AgentSessionTestAccess
{
    static std::shared_ptr<AgentSession> producer(Forwarder &owner, std::uint16_t target_port,
                                                 std::uint16_t entry_port)
    {
        auto session = std::make_shared<AgentSession>(owner, 7, ServiceKey{"drain-test", RelayProtocol::Tcp},
                                                     std::string{}, std::chrono::steady_clock::now() + std::chrono::seconds(2));
        test_access::AgentSession_target(*session) = AgentServiceConfig{"drain-test", "127.0.0.1", target_port};
        test_access::AgentSession_handle(*session, CtrlMessage(CtrlCommand::RelayOpened,
            njson{{"network_ports", test_node_address("127.0.0.1").to_params().at("network_ports")}, {"uuid", std::uint64_t{1}}, {"ticket", std::uint64_t{2}},
                  {"flow_id", std::uint64_t{3}}, {"data_port", entry_port}}));
        return session;
    }

    static void ready(AgentSession &session)
    {
        test_access::AgentSession_handle(session, CtrlMessage(CtrlCommand::RelayReady, njson::object()));
    }

    static void closed(AgentSession &session)
    {
        test_access::AgentSession_handle(session, CtrlMessage(CtrlCommand::RelayClosed, njson{{"reason", "stream complete"}}));
    }

    static void validate_close_messages(Forwarder &owner)
    {
        for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Udp})
        {
            for (const auto command : {CtrlCommand::RelayClosed, CtrlCommand::RelayError})
            {
                for (const auto field : {"reason", "stage"})
                {
                    auto session = std::make_shared<AgentSession>(
                        owner, 7, ServiceKey{"close-test", protocol}, std::string{},
                        std::chrono::steady_clock::now() + std::chrono::seconds(2));
                    test_access::AgentSession_endpoint(*session).emplace().uuid = 1;
                    test_access::AgentSession_phase(*session) = std::remove_reference_t<decltype(test_access::AgentSession_phase(*session))>::Ready;
                    if (protocol == RelayProtocol::Udp)
                    {
                        std::get<1>(test_access::AgentSession_data(*session)).transfer.open(asio::ip::udp::v4());
                    }
                    else
                    {
                        std::get<0>(test_access::AgentSession_data(*session)).transfer.open(asio::ip::tcp::v4());
                    }
                    const auto is_open = [&] {
                        return protocol == RelayProtocol::Udp
                                   ? std::get<1>(test_access::AgentSession_data(*session)).transfer.is_open()
                                   : std::get<0>(test_access::AgentSession_data(*session)).transfer.is_open();
                    };
                    const njson valid{{"reason", "stream complete"}, {"stage", "transfer"}};
                    auto invalid = valid;
                    invalid[field] = 1;
                    bool rejected = false;
                    try
                    {
                        test_access::AgentSession_handle(*session, CtrlMessage(command, std::move(invalid)));
                    }
                    catch (const std::invalid_argument &)
                    {
                        rejected = true;
                    }
                    if (!rejected || test_access::AgentSession_node_closed(*session) || !test_access::AgentSession_reason(*session).empty() || !is_open())
                    {
                        throw std::runtime_error("Invalid terminal metadata changed the session or closed its socket");
                    }

                    test_access::AgentSession_handle(*session, CtrlMessage(command, valid));
                    if (!test_access::AgentSession_node_closed(*session))
                    {
                        throw std::runtime_error("Invalid metadata prevented a later valid terminal notification");
                    }
                    if (command == CtrlCommand::RelayClosed && protocol == RelayProtocol::Tcp)
                    {
                        if (!test_access::AgentSession_reason(*session).empty() || !is_open())
                        {
                            throw std::runtime_error("Normal stream completion stopped local data draining");
                        }
                    }
                    else if (test_access::AgentSession_reason(*session) != "transfer: stream complete" || is_open())
                    {
                        throw std::runtime_error("UDP closure or relay error did not cancel the session and close I/O");
                    }
                }
            }
        }
    }

    static void validate_endpoint(Forwarder &owner)
    {
        auto session = std::make_shared<AgentSession>(owner, 7, ServiceKey{"endpoint-test", RelayProtocol::Tcp},
                                                     std::string{}, std::chrono::steady_clock::now());
        bool rejected = false;
        try
        {
            test_access::AgentSession_handle(*session, CtrlMessage(CtrlCommand::RelayOpened,
                njson{{"network_ports", test_node_address("127.0.0.1").to_params().at("network_ports")}, {"uuid", 1U}, {"ticket", 2U}, {"data_port", 0U}}));
        }
        catch (const std::invalid_argument &error)
        {
            rejected = std::string_view(error.what()) == "data_port is outside the valid range";
        }
        if (!rejected || test_access::AgentSession_endpoint(*session))
        {
            throw std::runtime_error("Invalid attachment changed endpoint state or hid its original error");
        }
    }

    static void verify(const AgentSession &session, std::string_view expected_reason, bool failed)
    {
        if (!test_access::AgentSession_node_closed(session))
        {
            throw std::runtime_error("Node completion lost the local transfer result");
        }
        if (failed && test_access::AgentSession_reason(session).find(asio::error::make_error_code(asio::error::connection_reset).message()) ==
                          std::string::npos)
        {
            throw std::runtime_error("Drain failure lost the original socket error: " + test_access::AgentSession_reason(session));
        }
        if (!failed && test_access::AgentSession_reason(session) != expected_reason)
        {
            throw std::runtime_error("Expected cancellation was replaced by a drain failure");
        }
    }
};

using tcp = asio::ip::tcp;
using namespace std::chrono_literals;

enum class End
{
    Normal,
    LateNodeClose,
    SlowGreeting,
    Reset,
    Cancel,
    CancelEmpty,
};

asio::awaitable<void> exchange(AgentSession &session, tcp::acceptor &target, tcp::acceptor &entry, End end,
                               const bool &session_finished, const std::shared_ptr<TLSChannel> &control)
{
    auto application = co_await target.async_accept(asio::cancel_after(1s, asio::use_awaitable));
    auto node = co_await entry.async_accept(asio::cancel_after(1s, asio::use_awaitable));
    if (end == End::SlowGreeting)
    {
        asio::steady_timer greeting(co_await asio::this_coro::executor, 1200ms);
        co_await greeting.async_wait(asio::use_awaitable);
    }
    RelayAttach::ConnectionId id{};
    id[0] = 13;
    co_await asio::async_write(node, asio::buffer(id), asio::use_awaitable);
    const auto attachment = RelayAttach::from_msg(co_await control->async_receive(1s));
    if (!attachment.connection_id || *attachment.connection_id != id || attachment.role != RelayAttach::Producer ||
        attachment.uuid != 1 || attachment.ticket != 2)
    {
        throw std::runtime_error("Agent control attachment changed");
    }
    AgentSessionTestAccess::ready(session);

    if (end == End::Normal || end == End::LateNodeClose || end == End::SlowGreeting)
    {
        const std::array<std::uint8_t, 3> payload{1, 2, 3};
        co_await asio::async_write(node, asio::buffer(payload), asio::use_awaitable);
        if (end != End::LateNodeClose)
        {
            AgentSessionTestAccess::closed(session);
        }
        node.shutdown(tcp::socket::shutdown_send);
        std::array<std::uint8_t, 3> received;
        co_await asio::async_read(application, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
        if (received != payload)
        {
            throw std::runtime_error("Node completion stopped local data draining");
        }
        std::array<std::uint8_t, 1> byte;
        const auto [error, size] = co_await application.async_read_some(
            asio::buffer(byte), asio::cancel_after(1s, use_nothrow_awaitable));
        if (error != asio::error::eof)
        {
            throw std::runtime_error("Node FIN did not finish the local direction");
        }
        application.shutdown(tcp::socket::shutdown_send);
        if (end == End::LateNodeClose)
        {
            const auto [node_error, node_size] = co_await node.async_read_some(
                asio::buffer(byte), asio::cancel_after(1s, use_nothrow_awaitable));
            if (node_error != asio::error::eof)
            {
                throw std::runtime_error("Local FIN did not reach the Node");
            }
            asio::steady_timer observe(co_await asio::this_coro::executor, 20ms);
            co_await observe.async_wait(asio::use_awaitable);
            if (!session_finished)
            {
                throw std::runtime_error("Completed data transfer waited for an unnecessary control notification");
            }
            AgentSessionTestAccess::closed(session);
        }
    }
    else if (end == End::CancelEmpty)
    {
        session.cancel("");
        AgentSessionTestAccess::closed(session);
    }
    else
    {
        AgentSessionTestAccess::closed(session);
        if (end == End::Reset)
        {
            application.set_option(asio::socket_base::linger(true, 0));
            application.close();
        }
        else
        {
            session.cancel("test stop");
        }
    }
}

asio::awaitable<void> exercise(asio::io_context &control, asio::io_context &transfer,
                               asio::ssl::context &ssl, End end)
{
    auto agent = std::make_shared<RelayAgent>(control, transfer, ssl, AgentConfig{});
    AgentConfig config;
    config.channel.handshake_timeout = 1s;
    config.connect_timeout = 1s;
    config.relay_open_timeout = 2s;
    auto owner = std::make_unique<Forwarder>(*agent, transfer.get_executor(), ssl, config);
    owner->start();
    if (end == End::Normal)
    {
        AgentSessionTestAccess::validate_close_messages(*owner);
        AgentSessionTestAccess::validate_endpoint(*owner);
    }
    tcp::acceptor target(transfer, {asio::ip::address_v4::loopback(), 0});
    tcp::acceptor entry(transfer, {asio::ip::address_v4::loopback(), 0});
    auto session = AgentSessionTestAccess::producer(*owner, target.local_endpoint().port(), entry.local_endpoint().port());
    asio::ssl::context server_ssl(asio::ssl::context::tls);
    const auto files = test_data::directory();
    server_ssl.use_certificate_chain_file((files / "tls_channel_test_server.pem").string());
    server_ssl.use_private_key_file((files / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
    server_ssl.load_verify_file((files / "tls_channel_test_client_ca.pem").string());
    tcp::acceptor control_listener(transfer, {asio::ip::address_v4::loopback(), 0});
    tcp::socket control_socket(transfer);
    co_await control_socket.async_connect(control_listener.local_endpoint(), asio::use_awaitable);
    auto peer = std::make_shared<TLSChannel>(co_await control_listener.async_accept(asio::use_awaitable),
                                           server_ssl, TLSChannelRole::S);
    auto channel = std::make_shared<TLSChannel>(std::move(control_socket), ssl, TLSChannelRole::C);
    co_await await_transfers(peer->start({}), channel->start("localhost"));
    test_access::AgentSession_selection(*session).server = ServerRoute{channel, "test-node"};
    bool session_finished = false;
    const auto run_session = [&]() -> asio::awaitable<void> {
        co_await session->run();
        session_finished = true;
    };
    co_await await_transfers(run_session(), exchange(*session, target, entry, end, session_finished, peer));
    if (end != End::Normal && end != End::LateNodeClose && end != End::SlowGreeting)
    {
        session->cancel("late cancellation");
    }
    const auto expected_reason = end == End::Cancel ? "test stop" : (end == End::CancelEmpty ? "relay cancelled" : "");
    AgentSessionTestAccess::verify(*session, expected_reason, end == End::Reset);
    co_await owner->async_stop();
    co_await channel->async_disconnect();
    co_await peer->async_disconnect();
}

} // namespace

int main()
{
    try
    {
        asio::ssl::context ssl(asio::ssl::context::tls_client);
        const auto files = test_data::directory();
        ssl.use_certificate_chain_file((files / "tls_channel_test_client.pem").string());
        ssl.use_private_key_file((files / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
        ssl.load_verify_file((files / "tls_channel_test_ca.pem").string());
        for (const auto end : {End::Normal, End::LateNodeClose, End::SlowGreeting, End::Reset, End::Cancel, End::CancelEmpty})
        {
            asio::io_context control(1);
            asio::io_context transfer(1);
            auto result = asio::co_spawn(transfer, exercise(control, transfer, ssl, end), asio::use_future);
            transfer.run();
            result.get();
        }
        std::cout << "[PASS] Node completion retains local drain success, failure and first cancellation cause\n";
        std::cout << "[PASS] Invalid terminal metadata preserves state and permits a later valid notification\n";
        std::cout << "[PASS] Establishment deadline preserves the original attachment failure\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
