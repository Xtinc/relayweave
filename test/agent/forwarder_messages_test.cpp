#include "agent_session.h"
#include "forwarder.h"
#include "member_access.h"
#include "test_data.h"
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
TEST_MEMBER(Forwarder, start_relay)
TEST_MEMBER(Forwarder, relays_)
TEST_MEMBER(Forwarder, active_tasks_)

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

void require(bool condition, const char *reason)
{
    if (!condition)
        throw std::runtime_error(reason);
}

std::string service_name(RelayProtocol protocol)
{
    return "invalid-" + std::string(relay_protocol_name(protocol));
}

njson endpoint_params(RelayProtocol protocol)
{
    return njson{{"service", service_name(protocol)}, {"protocol", relay_protocol_name(protocol)},
                 {"data_address", "127.0.0.1"}, {"data_port", 1U}, {"uuid", 11U}, {"ticket", 12U},
                 {"session_id", 13U}, {"tls_framing", tls_data_framing}};
}

asio::awaitable<void> start_consumer(Forwarder &owner, RelayProtocol protocol, std::uint64_t request)
{
    auto session = std::make_shared<AgentSession>(owner, request, ServiceKey{service_name(protocol), protocol},
                                                 "node", std::chrono::steady_clock::now() + 30s);
    test_access::Forwarder_start_relay(owner, std::move(session));
    co_return;
}

asio::awaitable<void> verify_drained(Forwarder &owner)
{
    require(test_access::Forwarder_relays_(owner).empty() && test_access::Forwarder_active_tasks_(owner) == 0,
            "Invalid message retained a session or an owned task");
    co_return;
}

asio::awaitable<void> exercise(asio::io_context &control, asio::io_context &transfer,
                               asio::ssl::context &server_ssl, asio::ssl::context &client_ssl)
{
    tcp::acceptor listener(control, {asio::ip::address_v4::loopback(), 0});
    tcp::socket socket(control);
    co_await socket.async_connect(listener.local_endpoint(), asio::use_awaitable);
    auto peer = std::make_shared<TLSChannel>(co_await listener.async_accept(asio::use_awaitable),
                                            server_ssl, TLSChannelRole::S);
    auto channel = std::make_shared<TLSChannel>(std::move(socket), client_ssl, TLSChannelRole::C);
    co_await await_transfers(peer->start({}), channel->start("localhost"));

    // Reserve a non-listening TCP port so valid offers fail attachment deterministically.
    tcp::socket unavailable(transfer);
    unavailable.open(tcp::v4());
    unavailable.bind({asio::ip::address_v4::loopback(), 0});
    AgentConfig config;
    for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
        config.services.push_back({service_name(protocol), "127.0.0.1", unavailable.local_endpoint().port(), protocol});
    auto agent = std::make_shared<RelayAgent>(control, transfer, client_ssl, config);
    auto &owner = *test_access::RelayAgent_forwarder_(*agent);
    owner.start();
    // Supply an already identified primary; selection and session cleanup use production paths.
    auto &connection = test_access::RelayAgent_connection_(*agent);
    test_access::NodeConnection_connected_(connection) = true;
    test_access::NodeConnection_channel_(connection) = channel;
    test_access::NodeConnection_node_id_(connection) = "node";
    auto &agent_control = test_access::RelayAgent_control_(*agent);
    test_access::AgentControl_running_(agent_control) = true;
    for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
        test_access::AgentControl_service_locations_(agent_control)[{service_name(protocol), protocol}] =
            {0, "node", "127.0.0.1"};

    const auto deliver = [&](CtrlCommand command, njson params) {
        // Match AgentControl's posted transfer callback, without a coroutine catching its exceptions.
        asio::post(transfer, [&owner, route = ServerRoute{channel, "node"},
                             message = CtrlMessage(command, std::move(params))]() mutable {
            owner.relay_message(std::move(route), std::move(message));
        });
    };
    std::uint64_t request = 100;
    for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
    {
        std::vector<njson> invalid;
        auto add = [&](std::string_view field, njson value) {
            auto params = endpoint_params(protocol);
            params[field] = std::move(value);
            invalid.push_back(std::move(params));
        };
        add("data_port", 0U);
        add("data_port", 65536U);
        add("data_address", 1U);
        add("ticket", 0U);
        auto missing = endpoint_params(protocol);
        missing.erase("ticket");
        invalid.push_back(std::move(missing));
        if (protocol == RelayProtocol::Tls)
        {
            add("tls_framing", "unsupported");
            add("tls_framing", 1U);
        }
        if (protocol == RelayProtocol::Udp)
            add("session_id", 0U);
        for (auto params : invalid)
        {
            deliver(CtrlCommand::RelayOffer, params);
            const auto rejected = co_await peer->async_receive(500ms);
            require(rejected.type() == CtrlCommand::RelayReject && rejected.params->at("uuid") == 11U &&
                        rejected.params->at("node_id") == "node" &&
                        rejected.params->at("reason").get<std::string>().starts_with("invalid relay.offer:"),
                    "Invalid offer did not promptly reject the original Node identity");
            co_await asio::co_spawn(transfer, verify_drained(owner), asio::use_awaitable);

            co_await asio::co_spawn(transfer, start_consumer(owner, protocol, ++request), asio::use_awaitable);
            const auto opened = co_await peer->async_receive(500ms);
            require(opened.type() == CtrlCommand::RelayOpen && opened.params->at("request_id") == request,
                    "Consumer did not submit its opening request");
            params["request_id"] = request;
            deliver(CtrlCommand::RelayOpened, std::move(params));
            const auto cancelled = co_await peer->async_receive(500ms);
            require(cancelled.type() == CtrlCommand::RelayCancel && cancelled.params->at("request_id") == request &&
                        !cancelled.params->contains("uuid"),
                    "Invalid opened endpoint did not wake and cancel the pending consumer by request");
            co_await asio::co_spawn(transfer, verify_drained(owner), asio::use_awaitable);
        }
    }

    // Cover errors thrown before endpoint parsing and malformed terminal metadata.
    for (const auto field : {"uuid", "protocol", "reason", "stage"})
    {
        co_await asio::co_spawn(transfer, start_consumer(owner, RelayProtocol::Tcp, ++request), asio::use_awaitable);
        require((co_await peer->async_receive(500ms)).type() == CtrlCommand::RelayOpen, "Missing consumer open");
        auto params = endpoint_params(RelayProtocol::Tcp);
        params["request_id"] = request;
        params["reason"] = "node failure";
        params[field] = field == std::string_view("uuid") ? njson(0U) : njson(1U);
        const auto command = field == std::string_view("reason") || field == std::string_view("stage")
                                 ? CtrlCommand::RelayError : CtrlCommand::RelayOpened;
        deliver(command, std::move(params));
        const auto cancelled = co_await peer->async_receive(500ms);
        require(cancelled.type() == CtrlCommand::RelayCancel && cancelled.params->at("request_id") == request,
                "Malformed identity or terminal metadata escaped the callback or delayed cleanup");
        co_await asio::co_spawn(transfer, verify_drained(owner), asio::use_awaitable);
    }

    // Numeric JSON equality must not treat a floating-point value as a request identity.
    co_await asio::co_spawn(transfer, start_consumer(owner, RelayProtocol::Tcp, ++request), asio::use_awaitable);
    require((co_await peer->async_receive(500ms)).type() == CtrlCommand::RelayOpen, "Missing consumer open");
    auto fractional_request = endpoint_params(RelayProtocol::Tcp);
    fractional_request["request_id"] = static_cast<double>(request);
    deliver(CtrlCommand::RelayOpened, std::move(fractional_request));
    const auto unmatched = co_await peer->async_receive(500ms);
    require(unmatched.type() == CtrlCommand::RelayCancel && unmatched.params->at("uuid") == 11U &&
                !unmatched.params->contains("request_id"), "Floating-point request matched an existing consumer");
    asio::post(transfer, [&owner] { owner.invalidate_relays("test cleanup"); });
    const auto cleanup = co_await peer->async_receive(500ms);
    require(cleanup.type() == CtrlCommand::RelayCancel && cleanup.params->at("request_id") == request &&
                !cleanup.params->contains("uuid"), "Invalid request installed an endpoint in the pending consumer");
    co_await asio::co_spawn(transfer, verify_drained(owner), asio::use_awaitable);

    // A later well-formed offer still creates a producer and uses its normal rejection cleanup.
    auto valid = endpoint_params(RelayProtocol::Tcp);
    valid["uuid"] = 99U;
    deliver(CtrlCommand::RelayOffer, std::move(valid));
    const auto rejected = co_await peer->async_receive(500ms);
    require(rejected.type() == CtrlCommand::RelayReject && rejected.params->at("uuid") == 99U &&
                !rejected.params->at("reason").get<std::string>().starts_with("invalid relay.offer:"),
            "A malformed message prevented later valid producer handling");
    co_await asio::co_spawn(transfer, verify_drained(owner), asio::use_awaitable);
    co_await asio::co_spawn(transfer, owner.async_stop(), asio::use_awaitable);
    channel->disconnect();
    peer->disconnect();
    co_await await_transfers(channel->async_disconnect(), peer->async_disconnect());
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
        asio::io_context control(1), transfer(1);
        auto result = asio::co_spawn(control, exercise(control, transfer, server, client), asio::use_future);
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (result.wait_for(0ms) != std::future_status::ready)
        {
            require(std::chrono::steady_clock::now() < deadline, "Malformed-message regression did not drain");
            control.restart();
            control.poll();
            transfer.restart();
            transfer.poll();
        }
        result.get();
        std::cout << "[PASS] Malformed relay messages reject/cancel promptly and drain without stopping transfer_io\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
