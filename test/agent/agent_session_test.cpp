#include "agent_session.h"
#include "forwarder.h"
#include <array>
#include <future>
#include <iostream>
#include <string_view>

#include "member_access.h"
TEST_MEMBER(RelayAgent, state_)
TEST_MEMBER(RelayAgent, service_locations_)
TEST_MEMBER(RelayAgent, entry_waits_)
TEST_MEMBER(RelayAgent, connections_)
TEST_MEMBER(RelayAgent, forwarder_)
TEST_MEMBER(AgentSession, target)
TEST_MEMBER(AgentSession, selection)
TEST_MEMBER(AgentSession, endpoint)
TEST_MEMBER(AgentSession, data)
TEST_MEMBER(AgentSession, ready)
TEST_MEMBER(AgentSession, node_closed)
TEST_MEMBER(AgentSession, reason)
TEST_MEMBER(AgentSession, deadline)
TEST_MEMBER(AgentSession, handle)

namespace
{
struct RelayAgentTestAccess
{
    static std::shared_ptr<Forwarder> prepare(RelayAgent &agent, std::uint16_t port)
    {
        const ServiceKey service{"entry-wait", RelayProtocol::Tcp};
        test_access::RelayAgent_state_(agent) = std::remove_cvref_t<decltype(test_access::RelayAgent_state_(agent))>::Running;
        *test_access::RelayAgent_service_locations_(agent).find_primary(service) = {0, "127.0.0.1", port};
        test_access::RelayAgent_service_locations_(agent).set_secondary(service, "pending-node");
        return test_access::RelayAgent_forwarder_(agent);
    }

    static std::chrono::steady_clock::time_point pending_deadline(const RelayAgent &agent)
    {
        if (test_access::RelayAgent_entry_waits_(agent).size() != 1)
        {
            throw std::runtime_error("Expected one pending entry lookup");
        }
        return test_access::RelayAgent_entry_waits_(agent).begin()->second->deadline;
    }

    static void verify_released(const RelayAgent &agent)
    {
        if (!test_access::RelayAgent_entry_waits_(agent).empty() || test_access::RelayAgent_connections_(agent).size() != 0)
        {
            throw std::runtime_error("Cancelled entry lookup retained its wait or connection");
        }
    }
};

struct AgentSessionTestAccess
{
    static std::shared_ptr<AgentSession> producer(Forwarder &owner, std::uint16_t target_port,
                                                 std::uint16_t entry_port)
    {
        auto session = std::make_shared<AgentSession>(owner, 7, ServiceKey{"drain-test", RelayProtocol::Tcp},
                                                     std::string{}, std::chrono::steady_clock::now() + std::chrono::seconds(2));
        test_access::AgentSession_target(*session) = AgentServiceConfig{"drain-test", "127.0.0.1", target_port};
        test_access::AgentSession_selection(*session).server.id = "test-node";
        test_access::AgentSession_selection(*session).server.host = "127.0.0.1";
        test_access::AgentSession_endpoint(*session) = njson{{"uuid", std::uint64_t{1}}, {"ticket", std::uint64_t{2}},
                                 {"flow_id", std::uint64_t{3}}, {"data_port", entry_port}};
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
                    test_access::AgentSession_endpoint(*session) = njson{{"uuid", std::uint64_t{1}}};
                    test_access::AgentSession_ready(*session) = true;
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

    static asio::awaitable<void> expired_endpoint(Forwarder &owner)
    {
        auto session = producer(owner, 1, 0);
        test_access::AgentSession_deadline(*session) = std::chrono::steady_clock::now();
        co_await session->run();
        if (test_access::AgentSession_reason(*session).find("data_port is outside the valid range") == std::string::npos)
        {
            throw std::runtime_error("Expired establishment deadline hid the original attachment failure");
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
    Reset,
    Cancel,
    CancelEmpty,
};

asio::awaitable<void> exchange(AgentSession &session, tcp::acceptor &target, tcp::acceptor &entry, End end)
{
    auto application = co_await target.async_accept(asio::cancel_after(1s, asio::use_awaitable));
    auto node = co_await entry.async_accept(asio::cancel_after(1s, asio::use_awaitable));
    auto attach = WireMessage::pack(RelayAttach::to_msg({RelayAttach::Producer, 1, 2}));
    BytesBuf received_attach(attach.size());
    co_await asio::async_read(node, asio::buffer(received_attach), asio::cancel_after(1s, asio::use_awaitable));
    if (received_attach != attach)
    {
        throw std::runtime_error("Agent data attachment changed");
    }
    AgentSessionTestAccess::ready(session);

    if (end == End::Normal)
    {
        const std::array<std::uint8_t, 3> payload{1, 2, 3};
        co_await asio::async_write(node, asio::buffer(payload), asio::use_awaitable);
        AgentSessionTestAccess::closed(session);
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
            session.cancel(end == End::CancelEmpty ? "" : "test stop");
        }
    }
}

asio::awaitable<void> exercise(asio::io_context &control, asio::io_context &transfer,
                               asio::ssl::context &ssl, End end)
{
    auto agent = std::make_shared<RelayAgent>(control, transfer, ssl, AgentConfig{});
    auto owner = std::make_shared<Forwarder>(*agent, transfer.get_executor(), ssl, std::nullopt,
                                            1s, 1s, 2s, std::vector<AgentServiceConfig>{},
                                            std::vector<AgentForwardConfig>{});
    owner->start();
    if (end == End::Normal)
    {
        AgentSessionTestAccess::validate_close_messages(*owner);
        co_await AgentSessionTestAccess::expired_endpoint(*owner);
    }
    tcp::acceptor target(transfer, {asio::ip::address_v4::loopback(), 0});
    tcp::acceptor entry(transfer, {asio::ip::address_v4::loopback(), 0});
    auto session = AgentSessionTestAccess::producer(*owner, target.local_endpoint().port(), entry.local_endpoint().port());
    co_await await_transfers(session->run(), exchange(*session, target, entry, end));
    if (end != End::Normal)
    {
        session->cancel("late cancellation");
    }
    const auto expected_reason = end == End::Cancel ? "test stop" : (end == End::CancelEmpty ? "relay cancelled" : "");
    AgentSessionTestAccess::verify(*session, expected_reason, end == End::Reset);
    co_await owner->async_stop();
}

asio::awaitable<void> exercise_entry_wait(asio::io_context &control, asio::io_context &transfer,
                                         asio::ssl::context &ssl, bool stop_agent)
{
    tcp::acceptor reserve(transfer, {asio::ip::address_v4::loopback(), 0});
    const auto forward_port = reserve.local_endpoint().port();
    reserve.close();
    tcp::acceptor entry(control, {asio::ip::address_v4::loopback(), 0});
    AgentConfig config;
    config.connect_timeout = 4s;
    config.channel.handshake_timeout = 4s;
    config.relay_open_timeout = 2s;
    config.forwards.push_back({"entry-wait", "127.0.0.1", forward_port, RelayProtocol::Tcp});
    auto agent = std::make_shared<RelayAgent>(control, transfer, ssl, std::move(config));
    auto owner = RelayAgentTestAccess::prepare(*agent, entry.local_endpoint().port());
    tcp::socket application(transfer);
    co_await asio::co_spawn(
        transfer,
        [owner, &application, forward_port]() -> asio::awaitable<void> {
            owner->start();
            owner->set_service(ServiceKey{"entry-wait", RelayProtocol::Tcp}, "pending-node");
            co_await application.async_connect({asio::ip::address_v4::loopback(), forward_port}, asio::use_awaitable);
        },
        asio::use_awaitable);

    // Accept TCP but leave the control TLS handshake pending throughout the lookup.
    auto peer = co_await entry.async_accept(asio::cancel_after(2s, asio::use_awaitable));
    const auto deadline = RelayAgentTestAccess::pending_deadline(*agent);
    if (stop_agent)
    {
        co_await agent->async_stop();
        if (std::chrono::steady_clock::now() >= deadline)
        {
            throw std::runtime_error("Agent shutdown waited for the entry lookup deadline");
        }
    }
    else
    {
        co_await asio::co_spawn(
            transfer,
            [owner, &application]() -> asio::awaitable<void> {
                owner->clear_service(ServiceKey{"entry-wait", RelayProtocol::Tcp});
                std::array<std::uint8_t, 1> byte;
                const auto [error, size] = co_await application.async_read_some(
                    asio::buffer(byte), asio::cancel_after(1s, use_nothrow_awaitable));
                if (error != asio::error::eof)
                {
                    throw std::runtime_error("Local cancellation did not close the application socket");
                }
            },
            asio::use_awaitable);
        asio::steady_timer observe(control, 50ms);
        co_await observe.async_wait(asio::use_awaitable);
        if (RelayAgentTestAccess::pending_deadline(*agent) != deadline)
        {
            throw std::runtime_error("Local cancellation reset the entry lookup deadline");
        }
        co_await asio::co_spawn(transfer, owner->async_stop(), asio::use_awaitable);
        if (std::chrono::steady_clock::now() < deadline)
        {
            throw std::runtime_error("Local cancellation interrupted the independent entry lookup");
        }
        co_await agent->async_stop();
    }
    RelayAgentTestAccess::verify_released(*agent);
}
} // namespace

int main()
{
    try
    {
        asio::ssl::context ssl(asio::ssl::context::tls_client);
        for (const auto end : {End::Normal, End::Reset, End::Cancel, End::CancelEmpty})
        {
            asio::io_context control(1);
            asio::io_context transfer(1);
            auto result = asio::co_spawn(transfer, exercise(control, transfer, ssl, end), asio::use_future);
            transfer.run();
            result.get();
        }
        for (const bool stop_agent : {false, true})
        {
            asio::io_context control(1);
            asio::io_context transfer(1);
            auto result = asio::co_spawn(control, exercise_entry_wait(control, transfer, ssl, stop_agent), asio::use_future);
            const auto deadline = std::chrono::steady_clock::now() + 5s;
            while (result.wait_for(0ms) != std::future_status::ready)
            {
                if (std::chrono::steady_clock::now() >= deadline)
                {
                    throw std::runtime_error("Entry cancellation regression did not finish");
                }
                control.restart();
                control.run_for(5ms);
                transfer.restart();
                transfer.poll();
            }
            result.get();
        }
        std::cout << "[PASS] Node completion retains local drain success, failure and first cancellation cause\n";
        std::cout << "[PASS] Invalid terminal metadata preserves state and permits a later valid notification\n";
        std::cout << "[PASS] Establishment deadline preserves the original attachment failure\n";
        std::cout << "[PASS] Entry waits retain their deadline after local cancellation and wake on Agent shutdown\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
