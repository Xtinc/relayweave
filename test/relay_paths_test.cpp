#include "node_test_config.h"
#include "relay_agent.h"
#include "forwarder.h"
#include <array>
#include <future>
#include <iostream>

// Compile-time fixture access only: seed the existing LRU so full Agent paths do not require raw ICMP.
struct RelayAgentTestAccess
{
    static asio::awaitable<void> select_cached_path(RelayAgent &agent, ServiceKey service, std::uint64_t epoch,
                                                    std::vector<std::string> path)
    {
        for (unsigned attempt = 0; attempt < 200; ++attempt)
        {
            const auto destination = agent.service_locations_.secondary_key(service);
            if (agent.routing_.epoch() == epoch && destination)
            {
                agent.path_cache_.put(path.back(), std::vector<RouteGraph::Path>{{path, 1.0}}, AgentRouting::Clock::now());
                if (agent.calculate_service_paths(service, *destination) != path)
                {
                    asio::steady_timer timer(co_await asio::this_coro::executor, std::chrono::milliseconds(20));
                    co_await timer.async_wait(asio::use_awaitable);
                    continue;
                }
                // Clear a UDP relay that may have opened before topology/cache availability.
                co_await asio::co_spawn(agent.transfer_executor_,
                    [forwarder = agent.forwarder_, service]() -> asio::awaitable<void> {
                        forwarder->clear_service(service);
                        co_return;
                    }, asio::use_awaitable);
                agent.path_cache_.put(path.back(), std::vector<RouteGraph::Path>{{path, 1.0}}, AgentRouting::Clock::now());
                // Refresh availability without connecting to the service Node.
                asio::post(agent.transfer_executor_, [forwarder = agent.forwarder_, service, node = *destination] {
                    forwarder->set_service(service, node);
                });
                co_return;
            }
            asio::steady_timer timer(co_await asio::this_coro::executor, std::chrono::milliseconds(20));
            co_await timer.async_wait(asio::use_awaitable);
        }
        throw std::runtime_error("Consumer Agent service/topology did not become ready");
    }
    static asio::awaitable<void> verify_entry_lifetime(RelayAgent &agent, std::uint64_t epoch)
    {
        const ServiceKey service{"auto-tcp", RelayProtocol::Tcp};
        co_await select_cached_path(agent, service, epoch, {"a", "b", "master"});
        if (agent.connections_.size() != 1 || agent.connections_.find_secondary("master"))
        {
            throw std::runtime_error("Service discovery connected to the tail Node before route selection");
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        const auto first = co_await agent.select_relay(service, "master", deadline);
        const auto second = co_await agent.select_relay(service, "master", deadline);
        if (first.server.id != second.server.id || agent.connections_.size() != 2 ||
            !agent.connections_.find_secondary("a") || agent.connections_.find_secondary("master"))
        {
            throw std::runtime_error("Multi relay did not reuse the head control without connecting the tail");
        }
        agent.release_entry(first.lease);
        if (!agent.connections_.find_secondary("a"))
        {
            throw std::runtime_error("Releasing one relay dropped the shared entry control");
        }
        agent.release_entry(second.lease);
        if (agent.connections_.size() != 1 || !agent.entry_waits_.empty())
        {
            throw std::runtime_error("The last relay did not release the entry control");
        }
    }

};

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

void require(bool condition, std::string reason)
{
    if (!condition)
    {
        throw std::runtime_error(std::move(reason));
    }
}

asio::awaitable<void> pause(std::chrono::milliseconds duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor, duration);
    co_await timer.async_wait(asio::use_awaitable);
}

std::uint16_t tcp_port(asio::io_context &io)
{
    tcp::acceptor socket(io, {asio::ip::address_v4::loopback(), 0});
    return socket.local_endpoint().port();
}

std::uint16_t udp_port(asio::io_context &io)
{
    udp::socket socket(io, {asio::ip::address_v4::loopback(), 0});
    return socket.local_endpoint().port();
}

asio::awaitable<std::shared_ptr<TLSChannel>> connect_control(asio::ssl::context &ssl, const NodeConfig &config)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::make_address(config.control.address), config.control.port},
                                 asio::cancel_after(2s, asio::use_awaitable));
    auto channel = std::make_shared<TLSChannel>(std::move(socket), ssl, TLSChannelRole::C);
    co_await channel->start("localhost");
    co_return channel;
}

asio::awaitable<CtrlMessage> receive(const std::shared_ptr<TLSChannel> &channel, CtrlCommand command)
{
    auto message = co_await channel->async_receive(4s);
    require(message.type() == command, "Expected " + std::string(ctrl_command_name(command)) + ", got " + message.command +
            " " + config::message_params(message).dump());
    co_return message;
}

asio::awaitable<tcp::socket> attach_tcp(const njson &endpoint, const NodeConfig &node, int role)
{
    tcp::socket socket(co_await asio::this_coro::executor);
    co_await socket.async_connect({asio::ip::make_address(node.control.address), config::message_data_port(endpoint)},
        asio::cancel_after(1s, asio::use_awaitable));
    const auto frame = WireMessage::pack(RelayAttach::to_msg({role,
        endpoint.at("uuid").get<std::uint64_t>(), endpoint.at("ticket").get<std::uint64_t>()}));
    co_await asio::async_write(socket, asio::buffer(frame), asio::use_awaitable);
    co_return socket;
}

// A real Agent/Node data handshake. Ownership of all data sockets remains in this coroutine.
asio::awaitable<void> exercise(const std::shared_ptr<TLSChannel> &consumer, const std::shared_ptr<TLSChannel> &producer,
                               asio::ssl::context &ssl, const NodeConfig &entry, const NodeConfig &exit,
                               std::uint64_t epoch, std::uint64_t request, RelayProtocol protocol,
                               const std::vector<std::string> &path, bool bad_ticket = false, std::string_view service_name = {})
{
    const auto service = service_name.empty() ? "manual-" + std::string(relay_protocol_name(protocol)) : std::string(service_name);
    consumer->send(CtrlMessage(CtrlCommand::RelayOpen,
        njson{{"request_id", request}, {"service", service}, {"protocol", relay_protocol_name(protocol)},
              {"epoch", epoch}, {"path", path}, {"budget_ms", 1200}}));
    const auto opened = co_await receive(consumer, CtrlCommand::RelayOpened);
    const auto offer = co_await receive(producer, CtrlCommand::RelayOffer);
    const auto &first = *opened.params;
    const auto &last = *offer.params;
    require(first.at("flow_id") == last.at("flow_id") && first.at("epoch") == epoch,
            "Flow identity lost across endpoint notifications");
    require(!first.contains("data_address") && !last.contains("data_address") &&
            !first.contains("consumer_session_id") && !last.contains("consumer_session_id") &&
            !first.contains("path") && !last.contains("path") && !last.contains("request_id"),
            "Internal route fields leaked to Agent");
    const auto expected_entry_port = protocol == RelayProtocol::Tcp ? entry.tcp.port :
        protocol == RelayProtocol::Tls ? entry.tls.port : entry.datagram.port;
    const auto expected_exit_port = protocol == RelayProtocol::Tcp ? exit.tcp.port :
        protocol == RelayProtocol::Tls ? exit.tls.port : exit.datagram.port;
    require(first.at("data_port") == expected_entry_port && last.at("data_port") == expected_exit_port &&
            expected_entry_port != expected_exit_port && expected_entry_port != entry.control.port,
            "Data/control endpoints were confused");
    const auto executor = co_await asio::this_coro::executor;
    std::vector<tcp::socket> streams;
    std::vector<TLSStream> tls_streams;
    std::vector<udp::socket> datagrams;
    for (int role : {RelayAttach::Consumer, RelayAttach::Producer})
    {
        const auto &descriptor = role == RelayAttach::Consumer ? first : last;
        const auto &settings = role == RelayAttach::Consumer ? entry : exit;
        const auto ticket = descriptor.at("ticket").get<std::uint64_t>() ^ (bad_ticket && role == RelayAttach::Consumer ? 1 : 0);
        const auto frame = WireMessage::pack(RelayAttach::to_msg({role, descriptor.at("uuid").get<std::uint64_t>(), ticket}));
        if (protocol == RelayProtocol::Udp)
        {
            datagrams.emplace_back(executor, udp::v4());
            datagrams.back().connect({asio::ip::make_address(settings.control.address), config::message_data_port(descriptor)});
            co_await datagrams.back().async_send(asio::buffer(frame), asio::use_awaitable);
            // Repeated attach must never replace the bound source endpoint.
            udp::socket duplicate(executor, udp::v4());
            duplicate.connect(datagrams.back().remote_endpoint());
            co_await duplicate.async_send(asio::buffer(frame), asio::use_awaitable);
            if (bad_ticket)
            {
                const auto header = DatagramHeader::encode(descriptor.at("session_id").get<std::uint64_t>());
                const std::array<std::uint8_t, 3> payload{1, 2, 3};
                const std::array buffers{asio::buffer(header), asio::buffer(payload)};
                co_await datagrams.back().async_send(buffers, asio::use_awaitable);
            }
        }
        else
        {
            tcp::socket socket(executor);
            co_await socket.async_connect({asio::ip::make_address(settings.control.address), config::message_data_port(descriptor)},
                                         asio::cancel_after(1s, asio::use_awaitable));
            if (protocol == RelayProtocol::Tls)
            {
                tls_streams.emplace_back(std::move(socket), ssl);
                configure_tls_client(tls_streams.back(), "localhost");
                co_await tls_streams.back().async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
                co_await asio::async_write(tls_streams.back(), asio::buffer(frame), asio::use_awaitable);
            }
            else
            {
                streams.push_back(std::move(socket));
                co_await asio::async_write(streams.back(), asio::buffer(frame), asio::use_awaitable);
                if (!bad_ticket && role == RelayAttach::Consumer)
                {
                    tcp::socket duplicate(executor);
                    co_await duplicate.async_connect(streams.back().remote_endpoint(), asio::use_awaitable);
                    co_await asio::async_write(duplicate, asio::buffer(frame), asio::use_awaitable);
                    std::array<std::uint8_t, 1> byte{};
                    const auto [error, size] = co_await duplicate.async_read_some(asio::buffer(byte),
                        asio::cancel_after(300ms, use_nothrow_awaitable));
                    require(error == asio::error::eof || error == asio::error::connection_reset,
                            "Duplicate attach replaced or retained a second stream");
                }
                if (!bad_ticket)
                {
                    const std::array<std::uint8_t, 3> early{1, 2, 3};
                    co_await asio::async_write(streams.back(), asio::buffer(early), asio::use_awaitable);
                }
            }
        }
    }
    if (bad_ticket)
    {
        const auto error = co_await receive(consumer, CtrlCommand::RelayError);
        require(error.params->at("request_id") == request && error.params->at("stage") == "attach",
                "Wrong ticket passed the readiness barrier: " + error.params->dump());
        co_await receive(producer, CtrlCommand::RelayClosed);
        if (protocol == RelayProtocol::Udp)
        {
            require(datagrams.back().available() == 0, "UDP business data passed an incomplete readiness barrier");
        }
        co_return;
    }
    co_await receive(consumer, CtrlCommand::RelayReady);
    co_await receive(producer, CtrlCommand::RelayReady);
    const std::array<std::uint8_t, 3> expected{1, 2, 3};
    if (protocol == RelayProtocol::Udp)
    {
        require(first.at("session_id") != last.at("session_id"), "UDP endpoints share a session ID");
        for (std::size_t side = 0; side < 2; ++side)
        {
            const auto &descriptor = side == 0 ? first : last;
            const auto header = DatagramHeader::encode(descriptor.at("session_id").get<std::uint64_t>());
            const std::array buffers{asio::buffer(header), asio::buffer(expected)};
            co_await datagrams[side].async_send(buffers, asio::use_awaitable);
            std::array<std::uint8_t, 32> data;
            const auto n = co_await datagrams[1 - side].async_receive(asio::buffer(data),
                asio::cancel_after(1s, asio::use_awaitable));
            const auto other_header = DatagramHeader::encode((side == 0 ? last : first).at("session_id").get<std::uint64_t>());
            require(n == other_header.size() + expected.size() &&
                std::equal(other_header.begin(), other_header.end(), data.begin()) &&
                std::equal(expected.begin(), expected.end(), data.begin() + other_header.size()),
                "UDP path changed payload, direction or local session");
            udp::socket wrong_source(executor, udp::v4());
            wrong_source.connect(datagrams[side].remote_endpoint());
            co_await wrong_source.async_send(buffers, asio::use_awaitable);
            BytesBuf payload(LnkFrameHeader::maximum_payload + 1, 0x5a);
            const std::array<asio::const_buffer, 2> oversized{asio::buffer(header), asio::buffer(payload)};
            co_await datagrams[side].async_send(oversized, asio::use_awaitable);
            payload.pop_back();
            const std::array<asio::const_buffer, 2> maximum{asio::buffer(header), asio::buffer(payload)};
            co_await datagrams[side].async_send(maximum, asio::use_awaitable);
            BytesBuf maximum_received(other_header.size() + payload.size() + 1);
            const auto maximum_size = co_await datagrams[1 - side].async_receive(asio::buffer(maximum_received),
                asio::cancel_after(1s, asio::use_awaitable));
            require(maximum_size == other_header.size() + payload.size() &&
                std::equal(payload.begin(), payload.end(), maximum_received.begin() + other_header.size()),
                "UDP maximum payload failed, or a wrong-source/oversized datagram passed");
            co_await datagrams[side].async_send(asio::buffer(header), asio::use_awaitable);
            const auto empty_size = co_await datagrams[1 - side].async_receive(asio::buffer(data),
                asio::cancel_after(1s, asio::use_awaitable));
            require(empty_size == other_header.size(), "Empty UDP payload was lost");
        }
        consumer->send(CtrlMessage(CtrlCommand::RelayCancel, njson{{"request_id", request}}));
    }
    else
    {
        auto transfer = [&]<typename Stream>(Stream &left, Stream &right) -> asio::awaitable<void> {
            std::array<std::uint8_t, 3> data;
            if (protocol == RelayProtocol::Tcp)
            {
                // The bytes sent before readiness stayed at their sockets until both endpoints were ready.
                co_await asio::async_read(left, asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
                require(data == expected, "Reverse pre-ready bytes were lost");
                co_await asio::async_read(right, asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
                require(data == expected, "Forward pre-ready bytes were lost");
            }
            BytesBuf payload(8193);
            for (std::size_t i = 0; i < payload.size(); ++i)
            {
                payload[i] = static_cast<std::uint8_t>(i);
            }
            if (protocol == RelayProtocol::Tcp && request == 10 + static_cast<unsigned>(RelayProtocol::Tcp))
            {
                co_await pause(1300ms);
            }
            const auto started = std::chrono::steady_clock::now();
            co_await asio::async_write(left, asio::buffer(payload), asio::use_awaitable);
            BytesBuf received(payload.size());
            co_await asio::async_read(right, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
            require(received == payload, "Multi-frame stream payload changed");
            require(std::chrono::steady_clock::now() - started >= 400ms,
                    "Service endpoint did not enforce its configured stream rate limit");
            left.lowest_layer().shutdown(tcp::socket::shutdown_send);
            const auto [eof, n] = co_await right.async_read_some(asio::buffer(data),
                asio::cancel_after(1s, use_nothrow_awaitable));
            require(eof == asio::error::eof || eof == asio::ssl::error::stream_truncated,
                    "FIN did not half-close the destination socket");
            co_await asio::async_write(right, asio::buffer(expected), asio::use_awaitable);
            co_await asio::async_read(left, asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
            require(data == expected, "FIN closed the opposite business direction");
            right.lowest_layer().shutdown(tcp::socket::shutdown_send);
            const auto [final_eof, final_size] = co_await left.async_read_some(asio::buffer(data),
                asio::cancel_after(1s, use_nothrow_awaitable));
            require(final_eof == asio::error::eof || final_eof == asio::ssl::error::stream_truncated,
                    "Reverse FIN was lost during cleanup");
        };
        if (protocol == RelayProtocol::Tls)
        {
            co_await transfer(tls_streams[0], tls_streams[1]);
        }
        else
        {
            co_await transfer(streams[0], streams[1]);
        }
    }
    const auto consumer_closed = co_await receive(consumer, CtrlCommand::RelayClosed);
    const auto producer_closed = co_await receive(producer, CtrlCommand::RelayClosed);
    if (protocol != RelayProtocol::Udp)
    {
        require(consumer_closed.params->at("reason") == "stream complete" &&
                producer_closed.params->at("reason") == "stream complete",
                "Orderly FIN completion was treated as an error");
    }
}

void integration()
{
    asio::io_context control(1);
    asio::io_context transfer(1);
    asio::io_context udp_io(1);
    asio::io_context data(1);
    asio::io_context client(1);
    asio::io_context agent_control(1);
    asio::io_context agent_transfer(1);
    auto cw = asio::make_work_guard(control);
    auto tw = asio::make_work_guard(transfer);
    auto uw = asio::make_work_guard(udp_io);
    auto dw = asio::make_work_guard(data);
    auto aw = asio::make_work_guard(agent_control);
    auto atw = asio::make_work_guard(agent_transfer);
    auto client_work = asio::make_work_guard(client);
    const auto files = std::filesystem::path(__FILE__).parent_path() / "data";
    asio::ssl::context server_ssl(asio::ssl::context::tls_server);
    auto sample = make_test_node_config();
    sample.certificate_chain = files / "tls_multinode_test_server.pem";
    sample.server_ca_file = sample.certificate_chain;
    server_ssl.use_certificate_chain_file(sample.certificate_chain.string());
    server_ssl.use_private_key_file(sample.private_key.string(), asio::ssl::context::pem);
    server_ssl.load_verify_file((files / "tls_channel_test_client_ca.pem").string());
    asio::ssl::context client_ssl(asio::ssl::context::tls_client);
    client_ssl.load_verify_file(sample.server_ca_file.string());
    client_ssl.use_certificate_chain_file((files / "tls_channel_test_client.pem").string());
    client_ssl.use_private_key_file((files / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
    const auto cluster_port = tcp_port(control);
    const auto cluster_tcp = tcp_port(data);
    const auto cluster_udp = udp_port(data);
    const std::vector<std::string> names{"master", "a", "b", "c", "d"};
    std::vector<NodeConfig> configs;
    std::vector<std::shared_ptr<RelayNode>> nodes;
    for (std::size_t index = 0; index < names.size(); ++index)
    {
        auto config = make_test_node_config();
        config.certificate_chain = sample.certificate_chain;
        config.server_ca_file = sample.server_ca_file;
        const auto host = "127.0.0." + std::to_string(index + 1);
        config.control.address = host;
        config.control.advertise_address = host;
        config.control.port = tcp_port(control);
        config.tcp.address = host;
        config.tcp.port = tcp_port(transfer);
        config.tls.address = host;
        config.tls.port = tcp_port(transfer);
        config.datagram.address = host;
        config.datagram.port = udp_port(udp_io);
        if (names[index] == "c")
        {
            config.tcp.traffic.tx_bytes_per_second = 8192;
            config.tcp.traffic.tx_burst_bytes = 4096;
            config.tls.traffic.tx_bytes_per_second = 8192;
            config.tls.traffic.tx_burst_bytes = 4096;
        }
        config.cluster = {index == 0 ? ClusterConfig::Role::Master : ClusterConfig::Role::Slave,
                          names[index], "127.0.0.1", cluster_port, cluster_tcp, cluster_udp};
        config.channel.disconnect_timeout = 100ms;
        auto node = std::make_shared<RelayNode>(control, transfer, udp_io, data, server_ssl, config);
        node->start();
        nodes.push_back(node);
        configs.push_back(config);
    }
    AgentConfig agent_config;
    agent_config.host = configs[0].control.address;
    agent_config.port = configs[0].control.port;
    agent_config.channel.disconnect_timeout = 100ms;
    agent_config.server_name = "localhost";
    agent_config.relay_open_timeout = 2s;
    tcp::acceptor target(client, {asio::ip::address_v4::loopback(), 0});
    udp::socket udp_target(client, {asio::ip::address_v4::loopback(), 0});
    for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
    {
        agent_config.services.push_back({"auto-" + std::string(relay_protocol_name(protocol)), "127.0.0.1",
            protocol == RelayProtocol::Udp ? udp_target.local_endpoint().port() : target.local_endpoint().port(), protocol});
    }
    agent_config.services.push_back({"auto-bad", "127.0.0.1", tcp_port(client), RelayProtocol::Tcp});
    auto agent = std::make_shared<RelayAgent>(agent_control, agent_transfer, client_ssl, agent_config);
    agent->start();
    std::thread c([&] { control.run(); });
    std::thread t([&] { transfer.run(); });
    std::thread u([&] { udp_io.run(); });
    std::thread d([&] { data.run(); });
    std::thread ac([&] { agent_control.run(); });
    std::thread at([&] { agent_transfer.run(); });
    std::thread client_thread([&] { client.run(); });
    std::vector<std::shared_ptr<TLSChannel>> channels;
    std::vector<tcp::socket> live_sockets;
    std::shared_ptr<RelayAgent> consumer_agent;
    std::shared_ptr<RelayAgent> discovery_agent;
    std::exception_ptr failure;
    try
    {
        auto scenario = [&]() -> asio::awaitable<void> {
            FlowResult warm;
            for (unsigned attempt = 0; attempt < 100; ++attempt)
            {
                warm = co_await nodes[0]->async_open_flow({"a", "b", "c"}, RelayProtocol::Tcp);
                if (warm)
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(bool(warm), "Cluster membership did not converge");
            const auto epoch = warm.epoch;
            co_await nodes[0]->async_close_flow(warm.epoch, warm.id);
            for (const auto &config : configs)
            {
                channels.push_back(co_await connect_control(client_ssl, config));
            }
            // The primary can be outside the path. node.lookup does not need a service.
            channels[0]->send(CtrlMessage(CtrlCommand::NodeLookup, njson{{"request_id", 1}, {"node_id", "a"}}));
            auto location = co_await receive(channels[0], CtrlCommand::NodeLocated);
            require(location.params->at("request_id") == 1 && location.params->at("node_id") == "a" &&
                    location.params->at("address") == configs[1].control.address &&
                    location.params->at("port") == configs[1].control.port, "Directed entry lookup returned wrong control endpoint");
            channels[1]->send(CtrlMessage(CtrlCommand::NodeLookup, njson{{"request_id", 2}, {"node_id", "a"}}));
            location = co_await receive(channels[1], CtrlCommand::NodeLocated);
            require(location.params->at("node_id") == "a", "Local entry lookup failed");
            channels[2]->send(CtrlMessage(CtrlCommand::NodeLookup, njson{{"request_id", 3}, {"node_id", "missing"}}));
            location = co_await receive(channels[2], CtrlCommand::NodeError);
            require(location.params->at("node_id") == "missing" && location.params->at("request_id") == 3,
                    "Missing entry lookup lost request identity");
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
            {
                channels[3]->send(CtrlMessage(CtrlCommand::ServiceRegister,
                    njson{{"request_id", 4}, {"service", "manual-" + std::string(relay_protocol_name(protocol))},
                          {"protocol", relay_protocol_name(protocol)}}));
                co_await receive(channels[3], CtrlCommand::ServiceOk);
            }
            channels[0]->send(CtrlMessage(CtrlCommand::ServiceLookup,
                njson{{"request_id", 5}, {"service", "manual-tcp"}, {"protocol", "tcp"}}));
            location = co_await receive(channels[0], CtrlCommand::ServiceLocated);
            require(location.params->at("node_id") == "c", "Entry lookup changed service ownership");
            // Egress rejection can race the ingress notification; either order must close the same request.
            for (const auto &service : {"missing-service", "manual-tcp"})
            {
                channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                    njson{{"request_id", 7}, {"service", service}, {"protocol", "tls"},
                          {"path", {"a", "b", "c"}}, {"epoch", epoch}, {"budget_ms", 2000}}));
                auto reply = co_await channels[1]->async_receive(3s);
                if (reply.type() == CtrlCommand::RelayOpened)
                {
                    reply = co_await receive(channels[1], CtrlCommand::RelayError);
                }
                require(reply.type() == CtrlCommand::RelayError && reply.params->at("request_id") == 7 &&
                        reply.params->at("stage") == "bind" && reply.params->at("reason") ==
                            (std::string_view(service) == "missing-service" ? "service unavailable" : "service protocol mismatch"),
                        "Egress validation did not roll back the request: " + reply.params->dump());
            }
            // Invalid origin and stale epoch fail before any flow or data endpoint is installed.
            auto invalid = njson{{"request_id", 6}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                {"path", {"a", "b", "c"}}, {"epoch", epoch}};
            channels[2]->send(CtrlMessage(CtrlCommand::RelayOpen, invalid));
            co_await receive(channels[2], CtrlCommand::RelayError);
            invalid["epoch"] = epoch + 1;
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen, invalid));
            co_await receive(channels[1], CtrlCommand::RelayError);
            // A single-node Relay and a multi-node endpoint share the first Node's existing listener.
            auto local_producer = co_await connect_control(client_ssl, configs[1]);
            channels.push_back(local_producer);
            local_producer->send(CtrlMessage(CtrlCommand::ServiceRegister,
                njson{{"request_id", 1}, {"service", "coexist-tcp"}, {"protocol", "tcp"}}));
            co_await receive(local_producer, CtrlCommand::ServiceOk);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 39}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                      {"path", {"a", "b", "c"}}, {"epoch", epoch}, {"budget_ms", 5000}}));
            const auto multi = co_await receive(channels[1], CtrlCommand::RelayOpened);
            co_await receive(channels[3], CtrlCommand::RelayOffer);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 38}, {"service", "coexist-tcp"}, {"protocol", "tcp"}}));
            const auto single = co_await receive(channels[1], CtrlCommand::RelayOpened);
            const auto single_offer = co_await receive(local_producer, CtrlCommand::RelayOffer);
            require(!single.params->contains("flow_id") && single.params->at("data_port") == multi.params->at("data_port") &&
                    single.params->at("uuid") != multi.params->at("uuid"), "Single/multi relay dispatch or ID allocation crossed");
            std::vector<tcp::socket> single_sockets;
            for (int role : {RelayAttach::Consumer, RelayAttach::Producer})
            {
                const auto &p = role == RelayAttach::Consumer ? *single.params : *single_offer.params;
                single_sockets.emplace_back(client);
                co_await single_sockets.back().async_connect({asio::ip::make_address(configs[1].control.address),
                    config::message_data_port(p)}, asio::use_awaitable);
                const auto frame = WireMessage::pack(RelayAttach::to_msg({role,
                    p.at("uuid").get<std::uint64_t>(), p.at("ticket").get<std::uint64_t>()}));
                co_await asio::async_write(single_sockets.back(), asio::buffer(frame), asio::use_awaitable);
            }
            co_await receive(channels[1], CtrlCommand::RelayReady);
            co_await receive(local_producer, CtrlCommand::RelayReady);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayCancel, njson{{"request_id", 39}, {"reason", "coexist cancellation"}}));
            const auto multi_closed = co_await receive(channels[1], CtrlCommand::RelayError);
            require(multi_closed.params->at("request_id") == 39, "Multi cancel reached the single relay");
            co_await receive(channels[3], CtrlCommand::RelayClosed);
            const std::array<std::uint8_t, 3> bytes{5, 6, 7};
            std::array<std::uint8_t, 3> echoed{};
            co_await asio::async_write(single_sockets.front(), asio::buffer(bytes), asio::use_awaitable);
            co_await asio::async_read(single_sockets.back(), asio::buffer(echoed), asio::cancel_after(1s, asio::use_awaitable));
            require(echoed == bytes, "Multi cancellation interrupted single-node forwarding");
            for (auto &socket : single_sockets)
            {
                socket.close();
            }
            for (const auto &peer : {"c", "master"})
            {
                for (int attempt = 0; attempt < 3; ++attempt)
                {
                    const auto link = co_await nodes[0]->async_ensure_link("b", peer, RelayProtocol::Udp);
                    if (link)
                    {
                        break;
                    }
                    require(attempt != 2, "UDP warmup failed");
                }
            }
            for (int attempt = 0; attempt < 3; ++attempt)
            {
                const auto link = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Udp);
                if (link)
                {
                    break;
                }
                require(attempt != 2, "UDP ingress warmup failed");
            }
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls})
            {
                co_await exercise(channels[1], channels[3], client_ssl, configs[1], configs[3], epoch,
                                  10 + static_cast<unsigned>(protocol), protocol, {"a", "b", "c"});
            }
            auto parallel_consumer = co_await connect_control(client_ssl, configs[1]);
            auto parallel_producer = co_await connect_control(client_ssl, configs[3]);
            channels.push_back(parallel_consumer);
            channels.push_back(parallel_producer);
            parallel_producer->send(CtrlMessage(CtrlCommand::ServiceRegister,
                njson{{"request_id", 1}, {"service", "parallel-udp"}, {"protocol", "udp"}}));
            co_await receive(parallel_producer, CtrlCommand::ServiceOk);
            co_await await_transfers(
                exercise(channels[1], channels[3], client_ssl, configs[1], configs[3], epoch, 12,
                    RelayProtocol::Udp, {"a", "b", "c"}),
                exercise(parallel_consumer, parallel_producer, client_ssl, configs[1], configs[3], epoch, 13,
                    RelayProtocol::Udp, {"a", "b", "c"}, false, "parallel-udp"));
            channels[3]->send(CtrlMessage(CtrlCommand::ServerTraffic, njson{{"request_id", 90}}));
            const auto accounting = co_await receive(channels[3], CtrlCommand::ServerTrafficReported);
            for (const auto &service : accounting.params->at("services"))
            {
                const auto protocol = service.at("protocol").get<std::string>();
                const auto tx = protocol == "udp" ? 4099u : protocol == "tcp" ? 8196u : 8193u;
                const auto rx = protocol == "udp" ? 4099u : protocol == "tcp" ? 6u : 3u;
                require(service.at("tx_bytes") == tx && service.at("rx_bytes") == rx && service.at("accessors").empty(),
                        "Multi-hop service accounting duplicated bytes or retained a closed accessor: " + service.dump());
            }
            co_await exercise(channels[1], channels[3], client_ssl, configs[1], configs[3], epoch, 20,
                              RelayProtocol::Tcp, {"a", "b", "c"}, true);
            co_await exercise(channels[1], channels[3], client_ssl, configs[1], configs[3], epoch, 21,
                              RelayProtocol::Udp, {"a", "b", "c"}, true);
            // Consistent duplicates reuse the endpoint; conflicting parameters do not create another flow.
            auto duplicate = njson{{"request_id", 40}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                {"path", {"a", "b", "c"}}, {"epoch", epoch}, {"budget_ms", 5000}};
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen, duplicate));
            const auto original = co_await receive(channels[1], CtrlCommand::RelayOpened);
            co_await receive(channels[3], CtrlCommand::RelayOffer);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen, duplicate));
            const auto replay = co_await receive(channels[1], CtrlCommand::RelayOpened);
            require(replay.params->at("uuid") == original.params->at("uuid") &&
                    replay.params->at("flow_id") == original.params->at("flow_id"), "Duplicate request installed another endpoint");
            duplicate["path"] = {"a", "master", "c"};
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen, duplicate));
            const auto conflict = co_await receive(channels[1], CtrlCommand::RelayError);
            require(conflict.params->at("reason") == "relay request conflicts with existing request", "Conflicting duplicate was accepted");
            channels[1]->send(CtrlMessage(CtrlCommand::RelayCancel,
                njson{{"request_id", 40}, {"reason", "test cancellation"}}));
            const auto cancelled = co_await receive(channels[1], CtrlCommand::RelayError);
            require(cancelled.params->at("reason") == "test cancellation", "Cancellation reason lost");
            co_await receive(channels[3], CtrlCommand::RelayClosed);
            // The master can be the first or an intermediate Node.
            co_await exercise(channels[0], channels[3], client_ssl, configs[0], configs[3], epoch, 41,
                              RelayProtocol::Tcp, {"master", "b", "c"});
            co_await exercise(channels[1], channels[3], client_ssl, configs[1], configs[3], epoch, 42,
                              RelayProtocol::Tcp, {"a", "master", "c"});
            // Forked flows retain their shared link when one consumer disconnects.
            channels[4]->send(CtrlMessage(CtrlCommand::ServiceRegister,
                njson{{"request_id", 1}, {"service", "fork-tcp"}, {"protocol", "tcp"}}));
            co_await receive(channels[4], CtrlCommand::ServiceOk);
            auto other_consumer = co_await connect_control(client_ssl, configs[1]);
            channels.push_back(other_consumer);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 43}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                      {"path", {"a", "b", "c"}}, {"epoch", epoch}, {"budget_ms", 5000}}));
            const auto fork1 = co_await receive(channels[1], CtrlCommand::RelayOpened);
            const auto fork1_offer = co_await receive(channels[3], CtrlCommand::RelayOffer);
            other_consumer->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 43}, {"service", "fork-tcp"}, {"protocol", "tcp"},
                      {"path", {"a", "b", "d"}}, {"epoch", epoch}, {"budget_ms", 5000}}));
            const auto fork2 = co_await receive(other_consumer, CtrlCommand::RelayOpened);
            const auto fork2_offer = co_await receive(channels[4], CtrlCommand::RelayOffer);
            require(fork1.params->at("flow_id") != fork2.params->at("flow_id") &&
                    fork1.params->at("uuid") != fork2.params->at("uuid"), "Sessions or forked flows share identity");
            const auto shared = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Tcp);
            require(bool(shared), "Shared link not found");
            auto first_consumer = co_await attach_tcp(*fork1.params, configs[1], RelayAttach::Consumer);
            auto first_producer = co_await attach_tcp(*fork1_offer.params, configs[3], RelayAttach::Producer);
            auto second_consumer = co_await attach_tcp(*fork2.params, configs[1], RelayAttach::Consumer);
            auto second_producer = co_await attach_tcp(*fork2_offer.params, configs[4], RelayAttach::Producer);
            co_await receive(channels[1], CtrlCommand::RelayReady);
            co_await receive(channels[3], CtrlCommand::RelayReady);
            co_await receive(other_consumer, CtrlCommand::RelayReady);
            co_await receive(channels[4], CtrlCommand::RelayReady);
            co_await asio::async_write(second_consumer, asio::buffer(bytes), asio::use_awaitable);
            co_await asio::async_read(second_producer, asio::buffer(echoed), asio::cancel_after(1s, asio::use_awaitable));
            require(echoed == bytes, "Forked active flow changed its payload");
            co_await other_consumer->async_disconnect();
            const auto lost_consumer = co_await receive(channels[4], CtrlCommand::RelayClosed);
            require(lost_consumer.params->at("reason") == "consumer control disconnected", "Control loss did not close endpoint");
            const auto still_shared = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Tcp);
            require(still_shared.id == shared.id, "Closing a flow destroyed a shared NodeLink");
            co_await asio::async_write(first_consumer, asio::buffer(bytes), asio::use_awaitable);
            co_await asio::async_read(first_producer, asio::buffer(echoed), asio::cancel_after(1s, asio::use_awaitable));
            require(echoed == bytes, "Consumer disconnect closed a different session's active flow");
            // A NodeLink failure terminates active socket pumps immediately.
            co_await nodes[0]->async_close_link(shared.id);
            const auto broken = co_await receive(channels[1], CtrlCommand::RelayClosed);
            require(broken.params->at("request_id") == 43 && broken.params->at("reason") != "relay establishment timed out",
                    "Broken NodeLink left the business transaction waiting");
            co_await receive(channels[3], CtrlCommand::RelayClosed);
            for (const bool producer_failure : {false, true})
            {
                channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                    njson{{"request_id", producer_failure ? 55 : 54}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                          {"epoch", epoch}, {"path", {"a", "b", "c"}}, {"budget_ms", 3000}}));
                const auto reset_opened = co_await receive(channels[1], CtrlCommand::RelayOpened);
                const auto reset_offer = co_await receive(channels[3], CtrlCommand::RelayOffer);
                auto reset_consumer = co_await attach_tcp(*reset_opened.params, configs[1], RelayAttach::Consumer);
                auto reset_producer = co_await attach_tcp(*reset_offer.params, configs[3], RelayAttach::Producer);
                co_await receive(channels[1], CtrlCommand::RelayReady);
                co_await receive(channels[3], CtrlCommand::RelayReady);
                auto &failed_socket = producer_failure ? reset_producer : reset_consumer;
                failed_socket.set_option(asio::socket_base::linger(true, 0));
                failed_socket.close();
                const auto reset = co_await receive(channels[1], CtrlCommand::RelayClosed);
                require(reset.params->at("reason").get<std::string>().find("reset") != std::string::npos,
                        "Socket failure did not preserve its RESET reason");
                co_await receive(channels[3], CtrlCommand::RelayClosed);
            }
            // Real producer Agent forwards service data over all three protocols after readiness.
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
            {
                channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                    njson{{"request_id", 30}, {"service", "auto-" + std::string(relay_protocol_name(protocol))},
                          {"protocol", relay_protocol_name(protocol)}, {"epoch", epoch}, {"path", {"a", "b", "master"}},
                          {"budget_ms", 700}}));
                const auto opened = co_await receive(channels[1], CtrlCommand::RelayOpened);
                const auto &p = *opened.params;
                const auto frame = WireMessage::pack(RelayAttach::to_msg({RelayAttach::Consumer,
                    p.at("uuid").get<std::uint64_t>(), p.at("ticket").get<std::uint64_t>()}));
                tcp::socket socket(client);
                std::optional<TLSStream> tls;
                udp::socket datagram(client);
                std::optional<tcp::socket> app;
                if (protocol == RelayProtocol::Udp)
                {
                    datagram.open(udp::v4());
                    datagram.connect({asio::ip::make_address(configs[1].control.address), config::message_data_port(p)});
                    co_await datagram.async_send(asio::buffer(frame), asio::use_awaitable);
                }
                else
                {
                    co_await socket.async_connect({asio::ip::make_address(configs[1].control.address), config::message_data_port(p)}, asio::use_awaitable);
                    if (protocol == RelayProtocol::Tls)
                    {
                        tls.emplace(std::move(socket), client_ssl);
                        configure_tls_client(*tls, "localhost");
                        co_await tls->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
                        co_await asio::async_write(*tls, asio::buffer(frame), asio::use_awaitable);
                    }
                    else
                    {
                        co_await asio::async_write(socket, asio::buffer(frame), asio::use_awaitable);
                    }
                    app.emplace(co_await target.async_accept(asio::cancel_after(1s, asio::use_awaitable)));
                    const std::array<std::uint8_t, 3> bytes{1, 2, 3};
                    co_await asio::async_write(*app, asio::buffer(bytes), asio::use_awaitable);
                }
                co_await receive(channels[1], CtrlCommand::RelayReady);
                if (protocol == RelayProtocol::Udp)
                {
                    const auto header = DatagramHeader::encode(p.at("session_id").get<std::uint64_t>());
                    const std::array<std::uint8_t, 3> bytes{3, 4, 5};
                    const std::array buffers{asio::buffer(header), asio::buffer(bytes)};
                    co_await datagram.async_send(buffers, asio::use_awaitable);
                    udp::endpoint sender;
                    std::array<std::uint8_t, 32> data;
                    const auto n = co_await udp_target.async_receive_from(asio::buffer(data), sender,
                        asio::cancel_after(1s, asio::use_awaitable));
                    require(n == bytes.size() && std::equal(bytes.begin(), bytes.end(), data.begin()),
                            "Real producer Agent did not deliver UDP to its service");
                    co_await udp_target.async_send_to(asio::buffer(bytes), sender, asio::use_awaitable);
                    const auto returned = co_await datagram.async_receive(asio::buffer(data),
                        asio::cancel_after(1s, asio::use_awaitable));
                    require(returned == header.size() + bytes.size(), "Real producer UDP return path failed");
                }
                else
                {
                    std::array<std::uint8_t, 3> data;
                    if (tls)
                    {
                        co_await asio::async_read(*tls, asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
                    }
                    else
                    {
                        co_await asio::async_read(socket, asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
                    }
                    require(data == std::array<std::uint8_t, 3>{1, 2, 3}, "Real Agent business return payload changed");
                }
                channels[1]->send(CtrlMessage(CtrlCommand::RelayCancel, njson{{"request_id", 30}}));
                co_await receive(channels[1], CtrlCommand::RelayClosed);
            }
            // Full application -> consumer Agent -> three Nodes -> producer Agent -> service.
            // The consumer's primary d is outside a -> b -> master.
            AgentConfig consumer_config;
            consumer_config.host = configs[4].control.address;
            consumer_config.port = configs[4].control.port;
            consumer_config.server_name = "localhost";
            consumer_config.relay_open_timeout = 5s;
            consumer_config.channel.disconnect_timeout = 100ms;
            std::vector<std::uint16_t> ports;
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
            {
                const auto port = protocol == RelayProtocol::Udp ? udp_port(client) : tcp_port(client);
                ports.push_back(port);
                consumer_config.forwards.push_back({"auto-" + std::string(relay_protocol_name(protocol)),
                    "127.0.0.1", port, protocol});
            }
            // A TCP-only discovery Agent has no proactive business instance.
            auto discovery_config = consumer_config;
            discovery_config.forwards.resize(1);
            discovery_config.forwards.front().listen_port = tcp_port(client);
            std::vector<std::uint16_t> self_ports;
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls})
            {
                const auto port = tcp_port(client);
                const auto name = "self-" + std::string(relay_protocol_name(protocol));
                self_ports.push_back(port);
                discovery_config.services.push_back({name, "127.0.0.1", target.local_endpoint().port(), protocol});
                discovery_config.forwards.push_back({name, "127.0.0.1", port, protocol});
            }
            discovery_agent = std::make_shared<RelayAgent>(agent_control, agent_transfer, client_ssl, discovery_config);
            discovery_agent->start();
            co_await asio::co_spawn(agent_control, RelayAgentTestAccess::verify_entry_lifetime(*discovery_agent, epoch), asio::use_awaitable);
            // Both Single roles share the same Agent control and Node UUID.
            for (const auto port : self_ports)
            {
                tcp::socket app(client);
                co_await app.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
                auto service_socket = co_await target.async_accept(asio::cancel_after(2s, asio::use_awaitable));
                const std::array<std::uint8_t, 3> bytes{8, 2, 1};
                std::array<std::uint8_t, 3> received;
                co_await asio::async_write(app, asio::buffer(bytes), asio::use_awaitable);
                co_await asio::async_read(service_socket, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
                require(received == bytes, "Same-Agent Single request did not pass readiness");
                co_await asio::async_write(service_socket, asio::buffer(bytes), asio::use_awaitable);
                co_await asio::async_read(app, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
                require(received == bytes, "Same-Agent Single response did not pass readiness");
            }
            co_await discovery_agent->async_stop();
            consumer_agent = std::make_shared<RelayAgent>(agent_control, agent_transfer, client_ssl, consumer_config);
            consumer_agent->start();
            std::size_t protocol_index = 0;
            for (const auto protocol : {RelayProtocol::Tcp, RelayProtocol::Tls, RelayProtocol::Udp})
            {
                const ServiceKey service{"auto-" + std::string(relay_protocol_name(protocol)), protocol};
                co_await asio::co_spawn(agent_control, RelayAgentTestAccess::select_cached_path(
                    *consumer_agent, service, epoch, {"a", "b", "master"}), asio::use_awaitable);
                if (protocol == RelayProtocol::Udp)
                {
                    udp::socket app(client, udp::v4());
                    app.connect({asio::ip::address_v4::loopback(), ports[protocol_index]});
                    const std::array<std::uint8_t, 4> bytes{7, 6, 5, 4};
                    std::array<std::uint8_t, 32> data;
                    udp::endpoint sender;
                    bool delivered = false;
                    for (unsigned attempt = 0; attempt < 40; ++attempt)
                    {
                        co_await app.async_send(asio::buffer(bytes), asio::use_awaitable);
                        const auto [error, size] = co_await udp_target.async_receive_from(asio::buffer(data), sender,
                            asio::cancel_after(100ms, use_nothrow_awaitable));
                        if (!error)
                        {
                            require(size == bytes.size() && std::equal(bytes.begin(), bytes.end(), data.begin()),
                                    "Consumer Agent UDP payload changed");
                            delivered = true;
                            break;
                        }
                    }
                    require(delivered, "Consumer Agent UDP path did not become ready");
                    co_await udp_target.async_send_to(asio::buffer(bytes), sender, asio::use_awaitable);
                    const auto n = co_await app.async_receive(asio::buffer(data), asio::cancel_after(1s, asio::use_awaitable));
                    require(n == bytes.size() && std::equal(bytes.begin(), bytes.end(), data.begin()),
                            "Consumer Agent UDP reverse path failed");
                    // Both relay branches must enforce the listener's original source-address binding.
                    udp::socket stranger(client, {asio::ip::make_address("127.0.0.2"), 0});
                    stranger.connect(app.remote_endpoint());
                    co_await stranger.async_send(asio::buffer(bytes), asio::use_awaitable);
                    const auto [source_error, source_size] = co_await udp_target.async_receive_from(
                        asio::buffer(data), sender, asio::cancel_after(150ms, use_nothrow_awaitable));
                    require(source_error == asio::error::operation_aborted && source_size == 0,
                            "Consumer Agent forwarded a datagram from a different local source address");
                    udp::socket replacement(client, udp::v4());
                    replacement.connect(app.remote_endpoint());
                    co_await replacement.async_send(asio::buffer(bytes), asio::use_awaitable);
                    const auto forwarded = co_await udp_target.async_receive_from(asio::buffer(data), sender,
                        asio::cancel_after(1s, asio::use_awaitable));
                    require(forwarded == bytes.size() && std::equal(bytes.begin(), bytes.end(), data.begin()),
                            "Consumer Agent rejected the original source address with a new port");
                    co_await udp_target.async_send_to(asio::buffer(bytes), sender, asio::use_awaitable);
                    const auto returned = co_await replacement.async_receive(asio::buffer(data),
                        asio::cancel_after(1s, asio::use_awaitable));
                    require(returned == bytes.size() && std::equal(bytes.begin(), bytes.end(), data.begin()),
                            "Consumer Agent did not update the local return endpoint");
                }
                else
                {
                    tcp::socket app(client);
                    co_await app.async_connect({asio::ip::address_v4::loopback(), ports[protocol_index]}, asio::use_awaitable);
                    auto service_socket = co_await target.async_accept(asio::cancel_after(3s, asio::use_awaitable));
                    const BytesBuf bytes(8193, 0x5a);
                    co_await asio::async_write(app, asio::buffer(bytes), asio::use_awaitable);
                    app.shutdown(tcp::socket::shutdown_send);
                    BytesBuf data(bytes.size());
                    co_await asio::async_read(service_socket, asio::buffer(data), asio::cancel_after(2s, asio::use_awaitable));
                    require(data == bytes, "Consumer Agent stream forward payload changed");
                    std::array<std::uint8_t, 1> byte;
                    const auto [eof, n] = co_await service_socket.async_read_some(asio::buffer(byte),
                        asio::cancel_after(1s, use_nothrow_awaitable));
                    require(eof == asio::error::eof, "Application FIN did not reach the service");
                    co_await asio::async_write(service_socket, asio::buffer(bytes), asio::use_awaitable);
                    service_socket.shutdown(tcp::socket::shutdown_send);
                    co_await asio::async_read(app, asio::buffer(data), asio::cancel_after(2s, asio::use_awaitable));
                    require(data == bytes, "Consumer Agent stream reverse payload changed after FIN");
                    const auto [final_eof, final_size] = co_await app.async_read_some(asio::buffer(byte),
                        asio::cancel_after(1s, use_nothrow_awaitable));
                    require(final_eof == asio::error::eof, "Service FIN did not reach the application");
                }
                ++protocol_index;
            }
            co_await consumer_agent->async_stop();
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 50}, {"service", "auto-bad"}, {"protocol", "tcp"},
                      {"epoch", epoch}, {"path", {"a", "b", "master"}}, {"budget_ms", 3000}}));
            co_await receive(channels[1], CtrlCommand::RelayOpened);
            const auto rejected = co_await receive(channels[1], CtrlCommand::RelayError);
            require(rejected.params->at("stage") == "attach" &&
                    rejected.params->at("reason").get<std::string>().find("refused") != std::string::npos,
                    "Producer target failure did not preserve the original error");
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 51}, {"service", "auto-tcp"}, {"protocol", "tcp"},
                      {"epoch", epoch}, {"path", {"a", "b", "master"}}, {"budget_ms", 5000}}));
            co_await receive(channels[1], CtrlCommand::RelayOpened);
            co_await agent->async_stop();
            const auto producer_lost = co_await receive(channels[1], CtrlCommand::RelayError);
            require(producer_lost.params->at("reason").get<std::string>().find("control disconnected") != std::string::npos,
                    "Producer control loss did not wake the consumer");
            // Leave a real pending transaction for Node::stop() to cancel and drain.
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 52}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                      {"epoch", epoch}, {"path", {"a", "b", "c"}}, {"budget_ms", 5000}}));
            co_await receive(channels[1], CtrlCommand::RelayOpened);
            co_await receive(channels[3], CtrlCommand::RelayOffer);
            channels[1]->send(CtrlMessage(CtrlCommand::RelayOpen,
                njson{{"request_id", 53}, {"service", "manual-tcp"}, {"protocol", "tcp"},
                      {"epoch", epoch}, {"path", {"a", "b", "c"}}, {"budget_ms", 5000}}));
            const auto live = co_await receive(channels[1], CtrlCommand::RelayOpened);
            const auto live_offer = co_await receive(channels[3], CtrlCommand::RelayOffer);
            live_sockets.push_back(co_await attach_tcp(*live.params, configs[1], RelayAttach::Consumer));
            live_sockets.push_back(co_await attach_tcp(*live_offer.params, configs[3], RelayAttach::Producer));
            co_await receive(channels[1], CtrlCommand::RelayReady);
            co_await receive(channels[3], CtrlCommand::RelayReady);
            co_await agent->async_stop();
            if (discovery_agent)
            {
                co_await discovery_agent->async_stop();
            }
            if (consumer_agent)
            {
                co_await consumer_agent->async_stop();
            }
        };
        auto future = asio::co_spawn(client, scenario(), asio::use_future);
        future.get();
    }
    catch (...)
    {
        failure = std::current_exception();
        auto cleanup = [&]() -> asio::awaitable<void> {
            for (auto &channel : channels)
            {
                co_await channel->async_disconnect();
            }
            co_await agent->async_stop();
            if (discovery_agent)
            {
                co_await discovery_agent->async_stop();
            }
            if (consumer_agent)
            {
                co_await consumer_agent->async_stop();
            }
        };
        auto stopped = asio::co_spawn(client, cleanup(), asio::use_future);
        stopped.get();
    }
    for (auto &node : nodes)
    {
        node->stop();
    }
    cw.reset();
    tw.reset();
    uw.reset();
    dw.reset();
    aw.reset();
    atw.reset();
    client_work.reset();
    client.stop();
    client_thread.join();
    control.stop();
    transfer.stop();
    udp_io.stop();
    data.stop();
    agent_control.stop();
    agent_transfer.stop();
    c.join();
    t.join();
    u.join();
    d.join();
    ac.join();
    at.join();
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}
}

int main()
{
    try
    {
        integration();
        std::cout << "[PASS] Routed relay control, TCP/TLS/UDP business transfer and half-close\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
