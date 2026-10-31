#include "agent_session.h"
#include "forwarder.h"
#include "frame_io.h"
#include "member_access.h"
#include "test_data.h"
#include <algorithm>
#include <array>
#include <future>
#include <iostream>

TEST_MEMBER(RelayAgent, connection_)
TEST_MEMBER(RelayAgent, control_)
TEST_MEMBER(RelayAgent, forwarder_)
TEST_MEMBER(NodeConnection, connected_)
TEST_MEMBER(NodeConnection, channel_)
TEST_MEMBER(NodeConnection, node_id_)
TEST_MEMBER(AgentControl, running_)
TEST_MEMBER(AgentControl, service_locations_)
TEST_MEMBER(Forwarder, datagram_forwards_)
TEST_MEMBER(Forwarder, relays_)
TEST_MEMBER(Forwarder, active_tasks_)
TEST_MEMBER(AgentSession, data)
TEST_MEMBER(AgentSession, reason)

namespace
{
using namespace std::chrono_literals;
using udp = asio::ip::udp;
using tcp = asio::ip::tcp;

void require(bool condition, const char *reason)
{
    if (!condition)
        throw std::runtime_error(reason);
}

asio::awaitable<void> pause(std::chrono::steady_clock::duration duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor, duration);
    co_await timer.async_wait(asio::use_awaitable);
}

asio::awaitable<void> no_packet(udp::socket &socket)
{
    std::array<std::uint8_t, 65536> buffer;
    const auto [error, size] = co_await socket.async_receive(asio::buffer(buffer),
                                                          asio::cancel_after(20ms, use_nothrow_awaitable));
    require(error == asio::error::operation_aborted && size == 0, "Unexpected or misrouted UDP packet");
}

asio::awaitable<void> expect_payload(udp::socket &socket, const BytesBuf &expected)
{
    std::array<std::uint8_t, 65536> buffer;
    const auto size = co_await socket.async_receive(asio::buffer(buffer), asio::cancel_after(500ms, asio::use_awaitable));
    require(size == expected.size() && std::equal(expected.begin(), expected.end(), buffer.begin()),
            "UDP payload changed or reached the wrong client");
}

asio::awaitable<CtrlMessage> command(TLSChannel &peer, CtrlCommand expected)
{
    for (;;)
    {
        auto message = co_await peer.async_receive(500ms);
        if (message.type() == expected)
            co_return message;
        require(message.type() == CtrlCommand::RelayCancel, "Unexpected relay control command");
    }
}

struct Attached
{
    std::uint64_t request;
    std::uint64_t uuid;
    std::uint64_t session;
    udp::endpoint source;
};

asio::awaitable<Attached> attach(Forwarder &owner, TLSChannel &peer, const ServerRoute &route,
                                udp::socket &node, std::uint64_t uuid, bool multi = false)
{
    const auto opened = co_await command(peer, CtrlCommand::RelayOpen);
    const auto request = opened.params->at("request_id").get<std::uint64_t>();
    njson endpoint{{"protocol", "udp"}, {"request_id", request}, {"uuid", uuid}, {"ticket", uuid + 1},
                   {"session_id", uuid + 2}, {"data_address", "127.0.0.1"},
                   {"data_port", node.local_endpoint().port()}};
    if (multi)
        endpoint["flow_id"] = uuid + 3;
    owner.relay_message(route, CtrlMessage(CtrlCommand::RelayOpened, std::move(endpoint)));
    std::array<std::uint8_t, 2048> buffer;
    udp::endpoint source;
    const auto size = co_await node.async_receive_from(asio::buffer(buffer), source,
                                                      asio::cancel_after(500ms, asio::use_awaitable));
    const auto attachment = RelayAttach::from_msg(decode_ctrl_datagram(std::span(buffer).first(size)));
    require(attachment.uuid == uuid && attachment.ticket == uuid + 1 && attachment.role == RelayAttach::Consumer,
            "Client did not attach its independent UDP session");
    co_return Attached{request, uuid, uuid + 2, source};
}

asio::awaitable<void> forwarded(udp::socket &node, const Attached &session, const BytesBuf &expected)
{
    std::array<std::uint8_t, 65536> buffer;
    udp::endpoint source;
    const auto size = co_await node.async_receive_from(asio::buffer(buffer), source,
                                                      asio::cancel_after(500ms, asio::use_awaitable));
    const auto header = DatagramHeader::encode(session.session);
    require(source == session.source && size == header.size() + expected.size() &&
                std::equal(header.begin(), header.end(), buffer.begin()) &&
                std::equal(expected.begin(), expected.end(), buffer.begin() + header.size()),
            "Wrong first packet, source socket or session header");
}

asio::awaitable<void> reply(udp::socket &node, const Attached &session, const BytesBuf &payload)
{
    const auto header = DatagramHeader::encode(session.session);
    const std::array buffers{asio::buffer(header), asio::buffer(payload)};
    co_await node.async_send_to(buffers, session.source, asio::use_awaitable);
}

void ready(Forwarder &owner, const ServerRoute &route, const Attached &session)
{
    owner.relay_message(route, CtrlMessage(CtrlCommand::RelayReady,
                                          njson{{"protocol", "udp"}, {"uuid", session.uuid}}));
}

asio::awaitable<void> exercise(asio::io_context &control, asio::io_context &transfer,
                               asio::ssl::context &server_ssl, asio::ssl::context &client_ssl)
{
    // Both contexts are pumped by one thread. Sessions still select through control_io.
    tcp::acceptor listener(transfer, {asio::ip::address_v4::loopback(), 0});
    tcp::socket socket(transfer);
    co_await socket.async_connect(listener.local_endpoint(), asio::use_awaitable);
    auto peer = std::make_shared<TLSChannel>(co_await listener.async_accept(asio::use_awaitable), server_ssl,
                                            TLSChannelRole::S);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), client_ssl, TLSChannelRole::C);
    co_await await_transfers(peer->start({}), channel->start("localhost"));
    AgentConfig config;
    config.udp_idle_timeout = 400ms;
    config.relay_open_timeout = 300ms;
    config.forwards = {{"udp-test", "127.0.0.1", 0, RelayProtocol::Udp},
                       {"udp-test", "127.0.0.1", 0, RelayProtocol::Udp}};
    auto agent = std::make_shared<RelayAgent>(control, transfer, client_ssl, config);
    auto &owner = *test_access::RelayAgent_forwarder_(*agent);
    auto &connection = test_access::RelayAgent_connection_(*agent);
    test_access::NodeConnection_connected_(connection) = true;
    test_access::NodeConnection_channel_(connection) = channel;
    test_access::NodeConnection_node_id_(connection) = "node";
    auto &agent_control = test_access::RelayAgent_control_(*agent);
    test_access::AgentControl_running_(agent_control) = true;
    const ServiceKey service{"udp-test", RelayProtocol::Udp};
    test_access::AgentControl_service_locations_(agent_control)[service] = {0, "node", "127.0.0.1"};
    const ServerRoute route{channel, "node"};
    owner.start();
    owner.set_service(service, "node");
    auto &forwards = test_access::Forwarder_datagram_forwards_(owner);
    auto &sessions = test_access::Forwarder_relays_(owner);
    require(sessions.empty(), "Service discovery proactively opened a UDP session");
    udp::socket node(transfer, {asio::ip::address_v4::loopback(), 0});
    udp::socket a(transfer, {asio::ip::address_v4::loopback(), 0});
    udp::socket b(transfer, {asio::ip::address_v4::loopback(), 0});
    udp::socket c(transfer, {asio::ip::make_address("127.0.0.2"), 0});
    a.connect(forwards[0].listener_socket.local_endpoint());
    b.connect(a.remote_endpoint());
    c.connect(a.remote_endpoint());
    const BytesBuf first{1, 2, 3};
    const BytesBuf dropped{4, 5};
    co_await a.async_send(asio::buffer(first), asio::use_awaitable);
    const auto sa = co_await attach(owner, *peer, route, node, 100);
    auto held = sessions.at(sa.request);
    for (int i = 0; i < 3; ++i)
        co_await a.async_send(asio::buffer(dropped), asio::use_awaitable);
    co_await pause(10ms);
    require(std::get<1>(test_access::AgentSession_data(*held)).first_packet == first,
            "Opening datagrams replaced the first packet");
    ready(owner, route, sa);
    co_await forwarded(node, sa, first);
    co_await no_packet(node);
    require(!std::get<1>(test_access::AgentSession_data(*held)).first_packet,
            "First packet was retained after successful forwarding");

    // Empty first packet and different-IP client both create their own associations.
    const BytesBuf empty;
    co_await b.async_send(asio::buffer(empty), asio::use_awaitable);
    const auto sb = co_await attach(owner, *peer, route, node, 200);
    ready(owner, route, sb);
    co_await forwarded(node, sb, empty);
    co_await c.async_send(asio::buffer(dropped), asio::use_awaitable);
    const auto sc = co_await attach(owner, *peer, route, node, 300);
    ready(owner, route, sc);
    co_await forwarded(node, sc, dropped);
    require(sa.source != sb.source && sa.source != sc.source && sb.source != sc.source && forwards[0].clients.size() == 3,
            "Clients shared a transfer socket or endpoint association");
    // Delayed replies are delivered by fixed session ownership, independently of last local sender.
    co_await reply(node, sc, dropped);
    co_await reply(node, sa, first);
    co_await reply(node, sb, empty);
    co_await expect_payload(a, first);
    co_await expect_payload(b, empty);
    co_await expect_payload(c, dropped);

    // The same source socket on another listener gets an independent association.
    a.connect(forwards[1].listener_socket.local_endpoint());
    co_await a.async_send(asio::buffer(first), asio::use_awaitable);
    const auto sd = co_await attach(owner, *peer, route, node, 400);
    ready(owner, route, sd);
    co_await forwarded(node, sd, first);
    require(sd.source != sa.source && forwards[1].clients.size() == 1, "Listeners shared a client association");

    // Final path limit can be smaller than the accepted local first packet.
    udp::socket oversized(transfer, udp::v4());
    oversized.connect(a.remote_endpoint());
    const BytesBuf too_large(LnkFrameHeader::maximum_payload + 1, 0xaa);
    co_await oversized.async_send(asio::buffer(too_large), asio::use_awaitable);
    const auto se = co_await attach(owner, *peer, route, node, 500, true);
    ready(owner, route, se);
    co_await no_packet(node);
    co_await oversized.async_send(asio::buffer(first), asio::use_awaitable);
    co_await forwarded(node, se, first);

    // Shared capacity rejects new clients without disturbing an existing association.
    for (std::uint64_t id = 10000; sessions.size() < 1000; ++id)
        sessions.emplace(id, held); // Represent capacity already owned by other business tasks.
    udp::socket full(transfer, udp::v4());
    full.connect(a.remote_endpoint());
    co_await full.async_send(asio::buffer(first), asio::use_awaitable);
    co_await pause(10ms);
    require(sessions.size() == 1000 && !forwards[1].clients.contains(full.local_endpoint()),
            "Capacity exhaustion allocated a new UDP association");
    co_await a.async_send(asio::buffer(dropped), asio::use_awaitable);
    co_await forwarded(node, sd, dropped);
    std::erase_if(sessions, [](const auto &entry) { return entry.first >= 10000; });

    // A shared setup capacity gates new clients across forwards; ready sessions do not consume it.
    udp::socket opening(transfer, udp::v4());
    opening.connect(a.remote_endpoint());
    co_await opening.async_send(asio::buffer(first), asio::use_awaitable);
    const auto sf = co_await attach(owner, *peer, route, node, 600);
    // Represent 99 other opening business tasks; together with sf this fills the fixed 100 slots.
    for (std::uint64_t id = 10000; id < 10099; ++id)
        sessions.emplace(id, sessions.at(sf.request));
    udp::socket limited(transfer, udp::v4());
    limited.connect(b.remote_endpoint());
    co_await limited.async_send(asio::buffer(first), asio::use_awaitable);
    co_await pause(10ms);
    require(!forwards[0].clients.contains(limited.local_endpoint()), "Opening capacity exhaustion allocated an association");
    co_await a.async_send(asio::buffer(dropped), asio::use_awaitable);
    co_await forwarded(node, sd, dropped);
    auto failed = sessions.at(sf.request);
    owner.relay_message(route, CtrlMessage(CtrlCommand::RelayError,
        njson{{"protocol", "udp"}, {"request_id", sf.request}, {"reason", "test failure"}}));
    co_await pause(10ms);
    require(!sessions.contains(sf.request) && !forwards[1].clients.contains(opening.local_endpoint()) &&
                !std::get<1>(test_access::AgentSession_data(*failed)).first_packet,
            "Failed establishment retained its first packet or association");
    ready(owner, route, sf); // Late readiness must not restore the old association.
    require(!sessions.contains(sf.request), "Late ready revived a failed association");
    co_await limited.async_send(asio::buffer(first), asio::use_awaitable);
    const auto admitted = co_await attach(owner, *peer, route, node, 650);
    ready(owner, route, admitted);
    co_await forwarded(node, admitted, first);
    require(forwards[0].clients.contains(limited.local_endpoint()),
            "Failed UDP setup did not release opening capacity");
    co_await opening.async_send(asio::buffer(first), asio::use_awaitable);
    const auto after_ready = co_await attach(owner, *peer, route, node, 660);
    ready(owner, route, after_ready);
    co_await forwarded(node, after_ready, first);
    require(after_ready.request != sf.request, "Ready UDP setup did not release opening capacity");
    std::erase_if(sessions, [](const auto &entry) { return entry.first >= 10000; });

    // Empty outbound packets alone also maintain the association.
    for (int i = 0; i < 5; ++i)
    {
        co_await pause(120ms);
        co_await a.async_send(asio::buffer(empty), asio::use_awaitable);
        co_await forwarded(node, sd, empty);
    }
    require(sessions.contains(sd.request), "Outbound empty packets did not refresh idle activity");

    // Keep one association alive by inbound empty packets alone, then let it expire.
    for (int i = 0; i < 5; ++i)
    {
        co_await pause(120ms);
        co_await reply(node, sd, empty);
        co_await expect_payload(a, empty);
    }
    require(sessions.contains(sd.request), "Inbound empty packets did not refresh idle activity");
    // Oversized local packets and an invalid return header must not extend the idle lifetime.
    const BytesBuf invalid_local(DatagramHeader::maximum_user_payload + 1, 0xcc);
    auto invalid_return = sd;
    ++invalid_return.session;
    for (int i = 0; i < 9; ++i)
    {
        co_await pause(100ms);
        co_await a.async_send(asio::buffer(invalid_local), asio::use_awaitable);
        co_await reply(node, invalid_return, empty);
    }
    require(sessions.empty() && forwards[0].clients.empty() && forwards[1].clients.empty(),
            "Idle expiry retained sessions or client indexes");
    co_await reply(node, sd, dropped);
    co_await no_packet(a);
    co_await a.async_send(asio::buffer(first), asio::use_awaitable);
    const auto fresh = co_await attach(owner, *peer, route, node, 700);
    ready(owner, route, fresh);
    co_await forwarded(node, fresh, first);
    require(fresh.request != sd.request, "Idle client reused a closed session identity");

    // Establishment uses the original absolute deadline, even if more local packets arrive.
    co_await limited.async_send(asio::buffer(first), asio::use_awaitable);
    const auto timeout = co_await attach(owner, *peer, route, node, 800);
    auto timed = sessions.at(timeout.request);
    for (int i = 0; i < 3; ++i)
    {
        co_await pause(80ms);
        co_await limited.async_send(asio::buffer(dropped), asio::use_awaitable);
    }
    co_await pause(100ms);
    require(!sessions.contains(timeout.request) && !std::get<1>(test_access::AgentSession_data(*timed)).first_packet &&
                !test_access::AgentSession_reason(*timed).empty(), "Local packets renewed the establishment deadline");
    owner.set_service(service, "other-node");
    for (const auto &[id, session] : sessions)
        require(!test_access::AgentSession_reason(*session).empty(), "Service migration left old sessions alive");
    owner.control_disconnected();
    co_await owner.async_stop();
    co_await owner.async_stop();
    require(sessions.empty() && forwards[0].clients.empty() && forwards[1].clients.empty() &&
                test_access::Forwarder_active_tasks_(owner) == 0, "Shutdown did not drain UDP resources");
    for (;;)
    {
        try
        {
            co_await peer->async_receive(20ms);
        }
        catch (const asio::system_error &error)
        {
            require(error.code() == asio::error::operation_aborted ||
                        error.code() == asio::experimental::error::channel_cancelled,
                    "Mock control channel failed while draining");
            break;
        }
    }
    channel->disconnect();
    peer->disconnect();
    co_await await_transfers(channel->async_disconnect(), peer->async_disconnect());

    // Stop before the expiry coroutine starts must also drain without waiting for its timer.
    config.udp_idle_timeout = 60s;
    auto second = std::make_unique<Forwarder>(*agent, transfer.get_executor(), client_ssl, config);
    second->start();
    co_await second->async_stop();
    require(test_access::Forwarder_active_tasks_(*second) == 0, "Unstarted idle task prevented shutdown");
}
} // namespace

int main()
{
    try
    {
        asio::ssl::context server_ssl(asio::ssl::context::tls_server), client_ssl(asio::ssl::context::tls_client);
        const auto data = test_data::directory();
        server_ssl.use_certificate_chain_file((data / "tls_channel_test_server.pem").string());
        server_ssl.use_private_key_file((data / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
        server_ssl.load_verify_file((data / "tls_channel_test_client_ca.pem").string());
        client_ssl.load_verify_file((data / "tls_channel_test_ca.pem").string());
        client_ssl.use_certificate_chain_file((data / "tls_channel_test_client.pem").string());
        client_ssl.use_private_key_file((data / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
        asio::io_context control(1), transfer(1);
        auto result = asio::co_spawn(transfer, exercise(control, transfer, server_ssl, client_ssl), asio::use_future);
        const auto deadline = std::chrono::steady_clock::now() + 8s;
        while (result.wait_for(0ms) != std::future_status::ready)
        {
            require(std::chrono::steady_clock::now() < deadline, "UDP Forwarder test timed out");
            control.restart();
            control.poll();
            transfer.restart();
            transfer.run_for(1ms);
        }
        result.get();
        std::cout << "[PASS] UDP first-packet buffering, endpoint isolation, opening capacity, idle and shutdown\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
