#include "frame_io.h"
#include "node_fixture.h"
#include <future>
#include <iostream>
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

void require(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
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
asio::awaitable<void> pause(std::chrono::milliseconds delay)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(delay);
    co_await timer.async_wait(asio::use_awaitable);
}

void integration()
{
    asio::io_context control(1);
    asio::io_context transfer(1);
    asio::io_context datagram(1);
    asio::io_context data(1);
    auto work = asio::make_work_guard(control);
    auto tcp_work = asio::make_work_guard(transfer);
    auto udp_work = asio::make_work_guard(datagram);
    auto data_work = asio::make_work_guard(data);
    asio::ssl::context ssl(asio::ssl::context::tls_server);
    auto settings = make_test_node_config();
    ssl.use_certificate_chain_file(settings.certificate_chain.string());
    ssl.use_private_key_file(settings.private_key.string(), asio::ssl::context::pem);
    ssl.load_verify_file(settings.server_ca_file.string());
    const auto cp = tcp_port(control);
    const auto tp = tcp_port(data);
    const auto up = udp_port(data);
    std::vector<std::shared_ptr<RelayNode>> nodes;
    std::vector<NodeConfig> configurations;
    for (int i = 0; i < 3; ++i)
    {
        auto config = make_test_node_config();
        const auto address = "127.0.0." + std::to_string(i + 1);
        config.control.address = config.control.advertise_address = config.tcp.address = config.tls.address =
            config.datagram.address = address;
        config.cluster = {i == 0 ? ClusterConfig::Role::Master : ClusterConfig::Role::Slave,
                          i == 0   ? "master"
                          : i == 1 ? "a"
                                   : "b",
                          "127.0.0.1",
                          cp,
                          tp,
                          up};
        config.channel.disconnect_timeout = 100ms;
        auto node = std::make_shared<RelayNode>(control, transfer, datagram, data, ssl, config);
        node->start();
        nodes.push_back(node);
        configurations.push_back(config);
    }
    std::thread c([&] { control.run(); });
    std::thread t([&] { transfer.run(); });
    std::thread u([&] { datagram.run(); });
    std::thread d([&] { data.run(); });
    std::exception_ptr failure;
    std::uint64_t live_id = 0;
    try
    {
        auto scenario = [&]() -> asio::awaitable<void> {
            LinkResult warm;
            for (int tries = 0; tries < 100; ++tries)
            {
                warm = co_await test_node::ensure_link(nodes[0], "a", "b", RelayProtocol::Tcp);
                if (warm)
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(bool(warm), "TCP slave pair did not become Ready");
            live_id = warm.id;
            // Business transfer executors may be busy; node data has its own domain.
            asio::post(transfer, [] { std::this_thread::sleep_for(1s); });
            asio::post(datagram, [] { std::this_thread::sleep_for(1s); });
            auto reversed = co_await test_node::ensure_link(nodes[0], "b", "a", RelayProtocol::Tcp);
            require(reversed.id == warm.id, "Ready normalized node pair was not reused");
            auto denied = co_await test_node::ensure_link(nodes[1], "a", "b", RelayProtocol::Tcp);
            require(!denied && denied.stage == "ensure", "slave was allowed to coordinate");
            for (auto transport : {RelayProtocol::Tcp, RelayProtocol::Udp})
            {
                // Concurrent calls must share a single attempt and NodeLink ID.
                asio::experimental::channel<void(asio::error_code, LinkResult)> results(control, 2);
                auto call = [&]() -> asio::awaitable<void> {
                    auto link = co_await test_node::ensure_link(nodes[0], "master", "a", transport);
                    results.try_send(asio::error_code{}, std::move(link));
                };
                asio::co_spawn(control, call(), asio::detached);
                asio::co_spawn(control, call(), asio::detached);
                auto first = co_await results.async_receive(asio::use_awaitable);
                auto second = co_await results.async_receive(asio::use_awaitable);
                require(bool(first) && first.id == second.id, "concurrent ensure did not coalesce");
                std::array<FlowResult, 2> flows;
                for (auto &flow : flows)
                {
                    flow = co_await test_node::open_flow(nodes[0], {"master", "a"}, transport);
                    require(bool(flow), "business flow did not become Ready");
                }
                for (std::uint64_t i = 1; i <= 12; ++i)
                {
                    const auto &flow = flows[i % flows.size()];
                    auto forward = co_await test_node::send_flow(nodes[0],
                        {flow.epoch, flow.id, false, LnkFrType::Data, {0, 255, std::uint8_t(i)}, {}});
                    auto reverse = co_await test_node::send_flow(nodes[1],
                        {flow.epoch, flow.id, true, LnkFrType::Data, {std::uint8_t(i), 0}, {}});
                    require(forward == FlowSendStatus::Queued && reverse == FlowSendStatus::Queued,
                            "business flow send failed");
                }
                for (std::uint64_t i = 1; i <= 12; ++i)
                {
                    const auto &flow = flows[i % flows.size()];
                    auto from_master = co_await test_node::receive_flow(nodes[1], flow.epoch, flow.id);
                    auto from_slave = co_await test_node::receive_flow(nodes[0], flow.epoch, flow.id);
                    require(from_master.flow_id == flow.id && !from_master.reverse &&
                                from_master.payload == BytesBuf({0, 255, std::uint8_t(i)}),
                            "business flow data mixed on shared channel");
                    require(from_slave.flow_id == flow.id && from_slave.reverse &&
                                from_slave.payload == BytesBuf({std::uint8_t(i), 0}),
                            "reverse business flow data mixed");
                }
                for (const auto &flow : flows)
                {
                    co_await test_node::close_flow(nodes[0], flow.epoch, flow.id);
                }
                co_await test_node::close_link(nodes[0], first.id);
                auto fresh = co_await test_node::ensure_link(nodes[0], "master", "a", transport);
                require(bool(fresh) && fresh.id != first.id, "outer ensure failed to create fresh link after close");
            }
            // Hold links past one keepalive interval and verify they still reuse.
            co_await pause(5300ms);
            auto alive = co_await test_node::ensure_link(nodes[0], "a", "b", RelayProtocol::Tcp);
            require(bool(alive) && alive.id == live_id, "TCP keepalive lost a healthy channel");
        };
        asio::co_spawn(control, scenario(), asio::use_future).get();
        nodes[2]->stop();
        auto offline = [&]() -> asio::awaitable<void> {
            for (int tries = 0; tries < 100; ++tries)
            {
                auto result = co_await test_node::ensure_link(nodes[0], "a", "b", RelayProtocol::Tcp);
                if (!result)
                {
                    co_return;
                }
                co_await pause(20ms);
            }
            throw std::runtime_error("offline member retained a Ready link");
        };
        asio::co_spawn(control, offline(), asio::use_future).get();
        nodes[0]->stop();
        nodes[0] = std::make_shared<RelayNode>(control, transfer, datagram, data, ssl, configurations[0]);
        nodes[0]->start();
        auto new_epoch = [&]() -> asio::awaitable<void> {
            LinkResult result;
            for (int tries = 0; tries < 350; ++tries)
            {
                result = co_await test_node::ensure_link(nodes[0], "master", "a", RelayProtocol::Tcp);
                if (result)
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(bool(result), "master restart did not establish link in fresh epoch");
            // Port mismatch is a prepare error; no data connect should be attempted.
            auto bad = configurations[2];
            bad.cluster.tcp_port = tcp_port(data);
            nodes[2] = std::make_shared<RelayNode>(control, transfer, datagram, data, ssl, bad);
            nodes[2]->start();
            LinkResult mismatch;
            for (int tries = 0; tries < 100; ++tries)
            {
                mismatch = co_await test_node::ensure_link(nodes[0], "master", "b", RelayProtocol::Tcp);
                if (mismatch.stage == "prepare")
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(!mismatch && mismatch.stage == "prepare", "different data ports were accepted");
            auto retry = co_await test_node::ensure_link(nodes[0], "master", "b", RelayProtocol::Tcp);
            require(!retry && retry.id != mismatch.id, "failed ensure was retained instead of permitting outer retry");
        };
        asio::co_spawn(control, new_epoch(), asio::use_future).get();
        auto in_flight =
            asio::co_spawn(control, test_node::ensure_link(nodes[0], "master", "a", RelayProtocol::Udp), asio::use_future);
        nodes[0]->stop();
        const auto stopped_result = in_flight.get();
        require(bool(stopped_result) || stopped_result.stage == "stop" || stopped_result.stage == "ensure",
                "stop left an in-flight ensure unresolved");
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        (*it)->stop();
    }
    work.reset();
    tcp_work.reset();
    udp_work.reset();
    data_work.reset();
    std::cout << "joining executors" << std::endl;
    c.join();
    t.join();
    u.join();
    d.join();
    std::cout << "integration completed" << std::endl;
    require(control.stopped() && transfer.stopped() && datagram.stopped() && data.stopped(),
            "executor retained tasks after stop");
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}

void data_failures()
{
    asio::io_context io(1);
    std::vector<CtrlMessage> events;
    const auto tp = tcp_port(io);
    const auto up = udp_port(io);
    auto module = std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(), "a", "127.0.0.1", tp, "127.0.0.1", up);
    module->start();
    module->activate();
    auto observe = [&]() -> asio::awaitable<void> {
        try
        {
            for (;;)
            {
                events.push_back(co_await module->receive_event());
            }
        }
        catch (const asio::system_error &e)
        {
            if (e.code() != asio::experimental::error::channel_cancelled &&
                e.code() != asio::experimental::error::channel_closed)
            {
                throw;
            }
        }
    };
    auto observed = asio::co_spawn(io, observe(), asio::use_future);
    auto scenario = [&]() -> asio::awaitable<void> {
        std::exception_ptr failure;
        try
        {
            // Let the empty monitor enter its indefinite wait before the first prepare.
            co_await pause(20ms);
            auto parameters = [&](std::uint64_t id, std::string transport, std::string address) {
                return njson{{"id", id},
                             {"epoch", 7},
                             {"token", "test-token"},
                             {"left", "a"},
                             {"right", "b"},
                             {"peer", "b"},
                             {"peer_address", address},
                             {"transport", transport},
                             {"tcp_port", tcp_port(io)},
                             {"udp_port", up}};
            };
            auto until_error = [&](std::uint64_t id) -> asio::awaitable<void> {
                for (int i = 0; i < 120; ++i)
                {
                    if (std::ranges::any_of(events, [id](const auto &m) {
                            return m.type() == CtrlCommand::LinkError && m.params->at("id") == id;
                        }))
                    {
                        co_return;
                    }
                    co_await pause(100ms);
                }
                throw std::runtime_error("missing link failure");
            };
            std::cout << "testing refusal" << std::endl;
            auto refused = parameters(1, "tcp", "127.0.0.1");
            module->prepare(refused);
            module->connect(1);
            module->connect(1);
            co_await until_error(1);
            std::cout << "testing DNS" << std::endl;
            auto bad_dns = parameters(2, "tcp", "invalid host name !");
            module->prepare(bad_dns);
            module->connect(2);
            co_await until_error(2);
            require(events.back().params->at("stage") == "resolve", "DNS failure phase lost");

            // Close before the independently spawned UDP resolver has started.
            auto cancelled = parameters(11, "udp", "invalid host name !");
            module->prepare(cancelled);
            module->close(11);
            co_await pause(20ms);
            require(std::ranges::none_of(events,
                                         [](const auto &m) {
                                             return m.type() == CtrlCommand::LinkError && m.params->at("id") == 11;
                                         }),
                    "closed UDP preparation resumed resolving");

            // Receive the bootstrap and deliberately drop its acknowledgement. Count
            // packets to prove bootstrap is sent once, including repeated connect calls.
            std::cout << "testing missing UDP ACK" << std::endl;
            udp::socket peer(io, {asio::ip::make_address("127.0.0.2"), up});
            auto missing_ack = parameters(3, "udp", "127.0.0.2");
            module->prepare(missing_ack);
            co_await pause(100ms);
            module->connect(3);
            module->connect(3);
            std::array<std::uint8_t, 8192> bytes{};
            udp::endpoint source;
            auto size = co_await peer.async_receive_from(asio::buffer(bytes), source, asio::use_awaitable);
            require(LnkFrameHeader::decode(std::span(bytes).subspan<8, LnkFrameHeader::length>()).kind ==
                            LnkFrType::Attach &&
                        decode_ctrl_datagram(
                            std::span(bytes).subspan(8 + LnkFrameHeader::length, size - 8 - LnkFrameHeader::length))
                                .type() == CtrlCommand::LinkAttach,
                    "missing UDP bootstrap");
            co_await until_error(3);
            require(peer.available() == 0, "UDP bootstrap retried");
            std::cout << "testing invalid identity" << std::endl;
            auto wrong = missing_ack;
            wrong["id"] = std::uint64_t(4);
            module->prepare(wrong);
            co_await pause(100ms);
            module->connect(4);
            size = co_await peer.async_receive_from(asio::buffer(bytes), source, asio::use_awaitable);
            auto attach = decode_ctrl_datagram(
                std::span(bytes).subspan(8 + LnkFrameHeader::length, size - 8 - LnkFrameHeader::length));
            auto send_peer = [&](CtrlMessage message, std::uint64_t id,
                                 int length_adjustment = 0) -> asio::awaitable<void> {
                const auto kind = message.type() == CtrlCommand::LinkAttach ? LnkFrType::Attach : LnkFrType::Attached;
                const auto epoch = message.params->at("epoch").get<std::uint64_t>();
                auto packet = WireMessage::pack(std::move(message));
                auto header = DatagramHeader::encode(id);
                auto frame_header =
                    LnkFrameHeader{kind, false, static_cast<std::uint32_t>(packet.size() + length_adjustment), epoch}
                        .encode();
                const std::array<asio::const_buffer, 3> buffers{asio::buffer(header), asio::buffer(frame_header),
                                                                asio::buffer(packet)};
                co_await peer.async_send_to(buffers, source, asio::use_awaitable);
            };
            auto p = *attach.params;
            p["node"] = "b";
            p["token"] = "wrong";
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4);
            p["token"] = "test-token";
            p["id"] = 3;
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4);
            p["id"] = 4;
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4, 1);
            p["data_version"] = 0;
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4);
            p["data_version"] = LnkFrameHeader::version;
            co_await pause(100ms);
            require(peer.available() == 0, "wrong credential, NodeLink ID, version or UDP length was accepted");
            p["id"] = 4;
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttached, p), 4);
            co_await pause(20ms);
            require(std::ranges::none_of(events,
                                         [](const auto &m) {
                                             return m.type() == CtrlCommand::LinkReady && m.params->at("id") == 4;
                                         }),
                    "UDP acknowledgement alone made the link Ready");
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4);
            size = co_await peer.async_receive_from(asio::buffer(bytes), source, asio::use_awaitable);
            co_await pause(100ms);
            require(std::ranges::any_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkReady && m.params->at("id") == 4; }),
                    "fresh outer UDP attempt did not become Ready");
            njson udp_flow{{"epoch", std::uint64_t(7)},
                           {"flow_id", std::uint64_t(24)},
                           {"request_id", std::uint64_t(24)},
                           {"path", std::vector<std::string>{"b", "a"}},
                           {"links", std::vector<std::uint64_t>{4}},
                           {"transport", "udp"},
                           {"ttl_ms", 10000U}};
            module->prepare_flow(udp_flow);
            module->commit_flow(24);
            const auto udp_link_header = DatagramHeader::encode(4);
            const auto large_header =
                LnkFrameHeader{LnkFrType::Data, false, LnkFrameHeader::maximum_payload, 7, 24}.encode();
            BytesBuf oversized(LnkFrameHeader::maximum_payload + 1, 0xee);
            for (int padding : {1, 1024})
            {
                oversized.resize(LnkFrameHeader::maximum_payload + padding, 0xee);
                const std::array<asio::const_buffer, 3> buffers{
                    asio::buffer(udp_link_header), asio::buffer(large_header), asio::buffer(oversized)};
                co_await peer.async_send_to(buffers, source, asio::use_awaitable);
            }
            const auto valid_header = LnkFrameHeader{LnkFrType::Data, false, 1, 7, 24}.encode();
            const std::array<std::uint8_t, 1> valid_payload{42};
            const std::array<asio::const_buffer, 3> valid_buffers{
                asio::buffer(udp_link_header), asio::buffer(valid_header), asio::buffer(valid_payload)};
            co_await peer.async_send_to(valid_buffers, source, asio::use_awaitable);
            auto udp_received = co_await module->receive_flow(7, 24);
            require(udp_received.payload == BytesBuf{42}, "truncated oversized UDP frame entered the flow");
            module->close_flow(24);
            // Delay Pong 1 until Ping 2 has been sent. An older, unacknowledged Ping
            // still proves liveness; duplicate and unsent sequences must not renew it.
            auto receive_ping = [&]() -> asio::awaitable<LnkFrameHeader> {
                const auto received = co_await peer.async_receive_from(
                    asio::buffer(bytes), source, asio::cancel_after(6s, asio::use_awaitable));
                require(received == DatagramHeader::length + LnkFrameHeader::length &&
                            DatagramHeader::decode(std::span(bytes).first<DatagramHeader::length>()) == 4,
                        "unexpected heartbeat datagram");
                const auto header = LnkFrameHeader::decode(
                    std::span(bytes).subspan<DatagramHeader::length, LnkFrameHeader::length>());
                require(header.kind == LnkFrType::Ping, "missing UDP Ping");
                co_return header;
            };
            const auto first_ping = co_await receive_ping();
            const auto second_ping = co_await receive_ping();
            require(second_ping.sequence > first_ping.sequence, "heartbeat sequence did not advance");
            auto send_pong = [&](std::uint64_t sequence) -> asio::awaitable<void> {
                const auto link_header = DatagramHeader::encode(4);
                const auto header = LnkFrameHeader{LnkFrType::Pong, false, 0, 7, 0, sequence}.encode();
                const std::array<asio::const_buffer, 2> buffers{asio::buffer(link_header), asio::buffer(header)};
                co_await peer.async_send_to(buffers, source, asio::use_awaitable);
            };
            co_await send_pong(first_ping.sequence);
            co_await pause(10500ms);
            require(std::ranges::none_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkError && m.params->at("id") == 4; }),
                    "delayed Pong for an earlier Ping did not keep the link alive");
            co_await send_pong(first_ping.sequence);
            co_await send_pong(second_ping.sequence + 100);
            co_await pause(10s);
            require(std::ranges::any_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkError && m.params->at("id") == 4; }),
                    "duplicate or unsent Pong renewed the link, or keepalive expiration was missed");
            module->close(4);

            // Queued datagrams must neither retain a closed link nor target a replacement epoch.
            while (peer.available())
            {
                peer.receive_from(asio::buffer(bytes), source);
            }
            for (std::uint64_t id : {9, 10})
            {
                auto queued_link = parameters(id, "udp", "127.0.0.2");
                module->prepare(queued_link);
                co_await pause(100ms);
                module->connect(id);
                size = co_await peer.async_receive_from(
                    asio::buffer(bytes), source, asio::cancel_after(1s, asio::use_awaitable));
                auto identity = *decode_ctrl_datagram(
                                     std::span(bytes).subspan(DatagramHeader::length + LnkFrameHeader::length,
                                                              size - DatagramHeader::length - LnkFrameHeader::length))
                                     .params;
                identity["node"] = "b";
                co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, identity), id);
                co_await send_peer(CtrlMessage(CtrlCommand::LinkAttached, identity), id);
                co_await peer.async_receive_from(asio::buffer(bytes), source,
                                                 asio::cancel_after(1s, asio::use_awaitable));
                co_await pause(5ms);
                auto queued_flow = udp_flow;
                queued_flow["flow_id"] = id + 20;
                queued_flow["request_id"] = id + 20;
                queued_flow["links"] = std::vector<std::uint64_t>{id};
                module->prepare_flow(queued_flow);
                module->commit_flow(id + 20);
                // No yield between queueing and close: even a directly handed-off send item is still pending.
                for (int i = 0; i < 3; ++i)
                {
                    require(module->send_flow({7, id + 20, true, LnkFrType::Data, {43}, {}}) == FlowSendStatus::Queued,
                            "could not queue UDP frame before close");
                }
                module->close(id);
                if (id == 10)
                {
                    queued_link["epoch"] = 8;
                    module->prepare(queued_link);
                }
                co_await pause(20ms);
                require(peer.available() == 0, "queued UDP frame was sent after its link closed");
                require(std::ranges::none_of(events,
                                             [id](const auto &message) {
                                                 return message.type() == CtrlCommand::LinkError &&
                                                        message.params->at("id") == id &&
                                                        message.params->at("epoch") == 8;
                                             }),
                        "stale queued UDP frame failed the replacement epoch");
                module->close(id);
            }

            // A peer can process its connect command before the local command arrives.
            auto early_peer = parameters(12, "udp", "127.0.0.2");
            module->prepare(early_peer);
            co_await pause(100ms);
            source = {asio::ip::address_v4::loopback(), up};
            njson early_identity{{"id", 12},
                                 {"epoch", 7},
                                 {"node", "b"},
                                 {"token", "test-token"},
                                 {"data_version", LnkFrameHeader::version}};
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, early_identity), 12);
            size = co_await peer.async_receive_from(asio::buffer(bytes), source,
                                                    asio::cancel_after(1s, asio::use_awaitable));
            require(LnkFrameHeader::decode(std::span(bytes).subspan<8, LnkFrameHeader::length>()).kind ==
                        LnkFrType::Attached,
                    "peer Attach before local connect was not acknowledged");
            module->connect(12);
            module->connect(12);
            size = co_await peer.async_receive_from(asio::buffer(bytes), source,
                                                    asio::cancel_after(1s, asio::use_awaitable));
            require(LnkFrameHeader::decode(std::span(bytes).subspan<8, LnkFrameHeader::length>()).kind ==
                        LnkFrType::Attach,
                    "early peer Attach prevented local bootstrap");
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttached, early_identity), 12);
            co_await pause(20ms);
            require(std::ranges::count_if(events,
                                          [](const auto &m) {
                                              return m.type() == CtrlCommand::LinkReady && m.params->at("id") == 12;
                                          }) == 1 && peer.available() == 0,
                    "early peer Attach lost progress or duplicate connect resent bootstrap");
            module->close(12);

            auto inbound = parameters(6, "tcp", "127.0.0.1");
            inbound["left"] = "0";
            inbound["right"] = "a";
            inbound["peer"] = "0";
            module->prepare(inbound);
            auto handshake = [&](std::uint64_t id, std::string token,
                                 int version = LnkFrameHeader::version) -> asio::awaitable<void> {
                tcp::socket socket(io);
                co_await socket.async_connect({asio::ip::address_v4::loopback(), tp}, asio::use_awaitable);
                auto bytes = WireMessage::pack(CtrlMessage(
                    CtrlCommand::LinkAttach,
                    njson{{"id", id}, {"epoch", 7}, {"node", "0"}, {"token", token}, {"data_version", version}}));
                co_await asio::async_write(socket, asio::buffer(bytes), asio::use_awaitable);
                if (id == 6 && token == "test-token" && version == LnkFrameHeader::version)
                {
                    auto response = co_await read_ctrl_frame(socket, std::chrono::steady_clock::now() + 1s);
                    require(response.type() == CtrlCommand::LinkAttached,
                            "valid TCP attach was rejected after invalid attach");
                }
            };
            co_await handshake(6, "bad-token");
            co_await handshake(5, "test-token");
            co_await handshake(6, "test-token", 0);
            co_await handshake(6, "test-token");
            co_await until_error(6);
            require(std::ranges::count_if(events,
                                          [](const auto &m) {
                                              return m.type() == CtrlCommand::LinkError && m.params->at("id") == 1;
                                          }) == 1,
                    "connection refusal automatically retried");
            // The refused target becomes available only for a fresh outer attempt.
            tcp::acceptor target(io, {asio::ip::address_v4::loopback(), refused.at("tcp_port").get<std::uint16_t>()});
            module->connect(1);
            auto fresh_tcp = refused;
            fresh_tcp["id"] = std::uint64_t(8);
            module->prepare(fresh_tcp);
            module->connect(8);
            auto socket = co_await target.async_accept(asio::use_awaitable);
            auto first_frame = co_await read_ctrl_frame(socket, std::chrono::steady_clock::now() + 1s);
            require(first_frame.params->at("id") == 8, "TCP failure was retried instead of fresh attempt");
            auto confirmed = *first_frame.params;
            confirmed["node"] = "b";
            auto ack = WireMessage::pack(CtrlMessage(CtrlCommand::LinkAttached, confirmed));
            co_await asio::async_write(socket, asio::buffer(ack), asio::use_awaitable);
            co_await pause(100ms);
            require(std::ranges::any_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkReady && m.params->at("id") == 8; }),
                    "TCP outer retry failed");
            BytesBuf payload(LnkFrameHeader::maximum_payload);
            for (std::size_t i = 0; i < payload.size(); ++i)
            {
                payload[i] = static_cast<std::uint8_t>(i);
            }
            njson flow{{"epoch", std::uint64_t(7)},
                       {"flow_id", std::uint64_t(21)},
                       {"request_id", std::uint64_t(21)},
                       {"master", "a"},
                       {"path", std::vector<std::string>{"b", "a"}},
                       {"addresses", std::vector<std::string>{"127.0.0.1", "127.0.0.1"}},
                       {"links", std::vector<std::uint64_t>{8}},
                       {"transport", "tcp"},
                       {"ttl_ms", unsigned(10000)}};
            module->prepare_flow(flow);
            module->commit_flow(21);
            auto send = [&](std::uint64_t epoch, std::uint64_t id, bool reverse,
                            BytesBuf body) -> asio::awaitable<void> {
                const auto header =
                    LnkFrameHeader{LnkFrType::Data, reverse, static_cast<std::uint32_t>(body.size()), epoch, id}
                        .encode();
                const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(body)};
                co_await asio::async_write(socket, buffers, asio::use_awaitable);
            };
            // Exercise the actual receive boundary with a real peer, without production injection hooks.
            co_await send(7, 21, true, {99});
            co_await send(7, 999, false, {98});
            co_await send(6, 21, false, {97});
            co_await send(7, 21, false, payload);
            auto received = co_await module->receive_flow(7, 21);
            require(received.flow_id == 21 && !received.reverse && received.payload == payload,
                    "wrong direction, unknown identity or stale epoch entered the business flow");
            module->close_flow(21);
            co_await send(7, 21, false, {96});
            module->prepare_flow(flow);
            module->commit_flow(21);
            require(module->send_flow({7, 21, true, LnkFrType::Data, {1}, {}}) == FlowSendStatus::Closed,
                    "retired identity was revived");
            flow["flow_id"] = flow["request_id"] = std::uint64_t(22);
            module->prepare_flow(flow);
            module->commit_flow(22);
            co_await send(7, 22, false, {42});
            received = co_await module->receive_flow(7, 22);
            require(received.flow_id == 22 && received.payload == BytesBuf{42}, "late data reached a new flow");
            auto malformed = LnkFrameHeader{LnkFrType::Data, false, 0, 7, 1}.encode();
            malformed[4] = 0xff;
            co_await asio::async_write(socket, asio::buffer(malformed), asio::use_awaitable);
            co_await until_error(8);
            require(module->send_flow({7, 22, true, LnkFrType::Data, {1}, {}}) == FlowSendStatus::Closed,
                    "malformed transport left its business flow alive");
            // Stop with a resolver/bootstrap task still in flight; Flow receiver cleanup is tested separately.
            auto pending = parameters(5, "udp", "127.0.0.2");
            module->prepare(pending);
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        std::cout << "stopping data module" << std::endl;
        co_await module->stop();
        std::cout << "data module stopped" << std::endl;
        if (failure)
        {
            std::rethrow_exception(failure);
        }
    };
    auto result = asio::co_spawn(io, scenario(), asio::use_future);
    io.run();
    result.get();
    observed.get();
    tcp::acceptor rebound(io, {asio::ip::address_v4::loopback(), tp});
    udp::socket rebound_udp(io, {asio::ip::address_v4::loopback(), up});
    require(io.stopped(), "data shutdown left tasks");
    // UDP bind failure must roll back the TCP listener opened immediately before it.
    const auto rollback_port = tcp_port(io);
    auto failed =
        std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(), "rollback", "127.0.0.1", rollback_port,
                                     "127.0.0.1", up);
    bool rejected = false;
    try
    {
        failed->start();
    }
    catch (const asio::system_error &)
    {
        rejected = true;
    }
    require(rejected, "conflicting UDP listener was accepted");
    tcp::acceptor rollback_probe(io, {asio::ip::address_v4::loopback(), rollback_port});
}

// A delayed activation must not revive data tasks after shutdown has completed.
void delayed_activation()
{
    asio::io_context io(1);
    const auto tp = tcp_port(io);
    const auto up = udp_port(io);
    auto module = std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(), "delayed", "127.0.0.1", tp,
                                             "127.0.0.1", up);
    module->start();
    auto stopped = asio::co_spawn(io, module->stop(), asio::use_future);
    io.run();
    stopped.get();
    io.restart();
    module->activate();
    io.run();
    require(io.stopped(), "late activation retained tasks after shutdown");
    tcp::acceptor rebound(io, {asio::ip::address_v4::loopback(), tp});
    udp::socket rebound_udp(io, {asio::ip::address_v4::loopback(), up});
}

void concurrent_stop()
{
    asio::io_context io(1);
    const auto tp = tcp_port(io);
    const auto up = udp_port(io);
    auto module = std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(), "stopping", "127.0.0.1", tp,
                                             "127.0.0.1", up);
    module->start();
    module->activate();
    module->activate();
    asio::cancellation_signal cancellation;
    auto stop_with_cancellation = [&]() -> asio::awaitable<void> {
        // Queue cancellation before stop queues the completion of its pending data operations.
        asio::post(io, [&] { cancellation.emit(asio::cancellation_type::all); });
        co_await module->stop();
    };
    auto cancelled = asio::co_spawn(io, stop_with_cancellation(),
                                    asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    auto concurrent = asio::co_spawn(io, module->stop(), asio::use_future);
    io.run();
    cancelled.get();
    concurrent.get();
    require(io.stopped(), "concurrent or cancelled stop retained data tasks");

    io.restart();
    auto repeated = asio::co_spawn(io, module->stop(), asio::use_future);
    io.run();
    repeated.get();
    tcp::acceptor rebound(io, {asio::ip::address_v4::loopback(), tp});
    udp::socket rebound_udp(io, {asio::ip::address_v4::loopback(), up});
}

void notification_overflow()
{
    asio::io_context io(1);
    auto module = std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(), "a", "127.0.0.1", 1, "127.0.0.1", 1);
    // Every rejected prepare emits one control notification without allocating a live flow.
    for (std::uint64_t id = 1; id <= 4097; ++id)
    {
        module->prepare_flow(njson{{"epoch", std::uint64_t(7)},
                                   {"flow_id", id},
                                   {"request_id", id},
                                   {"ttl_ms", 1000U},
                                   {"path", std::vector<std::string>{"a", "b"}},
                                   {"links", std::vector<std::uint64_t>{1}},
                                   {"transport", "tcp"}});
    }
    auto scenario = [&]() -> asio::awaitable<void> {
        bool rejected = false;
        try
        {
            static_cast<void>(co_await module->receive_event());
        }
        catch (const std::runtime_error &e)
        {
            rejected = std::string(e.what()) == "Node control event queue capacity exceeded";
        }
        require(rejected, "control notification overload was silently discarded");
        co_await module->stop();
    };
    auto result = asio::co_spawn(io, scenario(), asio::use_future);
    io.run();
    result.get();
    require(io.stopped(), "notification overflow retained an operation");
}

int main()
{
    try
    {
        integration();
        data_failures();
        delayed_activation();
        concurrent_stop();
        notification_overflow();
        std::cout << "[PASS] shared TCP/UDP node links\n";
        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
