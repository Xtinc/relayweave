#include "frame_io.h"
#include "node_test_config.h"
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
                warm = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Tcp);
                if (warm)
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(bool(warm), "TCP slave pair did not become Ready");
            live_id = warm.id;
            auto slave_pair_status = co_await nodes[0]->async_link_status(warm.id);
            require(bool(slave_pair_status.result) && slave_pair_status.llink == "a" && slave_pair_status.rlink == "b" &&
                        slave_pair_status.lready && slave_pair_status.rready,
                    "slave status replies did not reach master");
            auto denied_status = co_await nodes[1]->async_link_status(warm.id);
            require(!denied_status.result, "slave was allowed to query node link status");
            // Business transfer executors may be busy; node data has its own domain.
            asio::post(transfer, [] { std::this_thread::sleep_for(1s); });
            asio::post(datagram, [] { std::this_thread::sleep_for(1s); });
            auto reversed = co_await nodes[0]->async_ensure_link("b", "a", RelayProtocol::Tcp);
            require(reversed.id == warm.id, "Ready normalized node pair was not reused");
            auto denied = co_await nodes[1]->async_ensure_link("a", "b", RelayProtocol::Tcp);
            require(!denied && denied.stage == "ensure", "slave was allowed to coordinate");
            for (auto transport : {RelayProtocol::Tcp, RelayProtocol::Udp})
            {
                // Concurrent calls must share a single attempt and NodeLink ID.
                asio::experimental::channel<void(asio::error_code, LinkResult)> results(control, 2);
                auto call = [&]() -> asio::awaitable<void> {
                    auto link = co_await nodes[0]->async_ensure_link("master", "a", transport);
                    results.try_send(asio::error_code{}, std::move(link));
                };
                asio::co_spawn(control, call(), asio::detached);
                asio::co_spawn(control, call(), asio::detached);
                auto first = co_await results.async_receive(asio::use_awaitable);
                auto second = co_await results.async_receive(asio::use_awaitable);
                require(bool(first) && first.id == second.id, "concurrent ensure did not coalesce");
                asio::experimental::channel<void(asio::error_code, LinkStatus)> statuses(control, 2);
                auto query = [&]() -> asio::awaitable<void> {
                    auto status = co_await nodes[0]->async_link_status(first.id);
                    statuses.try_send(asio::error_code{}, std::move(status));
                };
                asio::co_spawn(control, query(), asio::detached);
                asio::co_spawn(control, query(), asio::detached);
                for (int i = 0; i < 2; ++i)
                {
                    auto status = co_await statuses.async_receive(asio::use_awaitable);
                    require(bool(status.result) && status.result.id == first.id && status.llink == "a" &&
                                status.rlink == "master" && status.lready && status.rready,
                            "concurrent TCP/UDP status queries failed for master endpoint");
                }
                // Cancelling one caller must leave the shared status operation alive for the other.
                asio::cancellation_signal cancellation;
                const auto completed_status = [&](std::exception_ptr error, LinkStatus status) {
                    if (error)
                    {
                        status.result = {first.id, "status", exception_description(error)};
                    }
                    statuses.try_send(asio::error_code{}, std::move(status));
                };
                asio::co_spawn(control, nodes[0]->async_link_status(first.id),
                               asio::bind_cancellation_slot(cancellation.slot(), completed_status));
                asio::co_spawn(control, nodes[0]->async_link_status(first.id), completed_status);
                co_await asio::post(control, asio::use_awaitable);
                cancellation.emit(asio::cancellation_type::all);
                auto cancelled_status = co_await statuses.async_receive(asio::use_awaitable);
                auto surviving_status = co_await statuses.async_receive(asio::use_awaitable);
                if (cancelled_status.result.reason.empty())
                {
                    std::swap(cancelled_status, surviving_status);
                }
                require(!cancelled_status.result.reason.empty() && bool(surviving_status.result) &&
                            surviving_status.complete() && surviving_status.lready && surviving_status.rready,
                        "status cancellation affected another caller");
                std::array<FlowResult, 2> flows;
                for (auto &flow : flows)
                {
                    flow = co_await nodes[0]->async_open_flow({"master", "a"}, transport);
                    require(bool(flow), "business flow did not become Ready");
                }
                for (std::uint64_t i = 1; i <= 12; ++i)
                {
                    const auto &flow = flows[i % flows.size()];
                    auto forward = co_await nodes[0]->async_send_flow(
                        {flow.epoch, flow.id, false, LnkFrType::Data, {0, 255, std::uint8_t(i)}, {}});
                    auto reverse = co_await nodes[1]->async_send_flow(
                        {flow.epoch, flow.id, true, LnkFrType::Data, {std::uint8_t(i), 0}, {}});
                    require(forward.status == FlowSendStatus::Queued && reverse.status == FlowSendStatus::Queued,
                            "business flow send failed");
                }
                for (std::uint64_t i = 1; i <= 12; ++i)
                {
                    const auto &flow = flows[i % flows.size()];
                    auto from_master = co_await nodes[1]->async_receive_flow(flow.epoch, flow.id);
                    auto from_slave = co_await nodes[0]->async_receive_flow(flow.epoch, flow.id);
                    require(from_master.flow_id == flow.id && !from_master.reverse &&
                                from_master.payload == BytesBuf({0, 255, std::uint8_t(i)}),
                            "business flow data mixed on shared channel");
                    require(from_slave.flow_id == flow.id && from_slave.reverse &&
                                from_slave.payload == BytesBuf({std::uint8_t(i), 0}),
                            "reverse business flow data mixed");
                }
                for (const auto &flow : flows)
                {
                    co_await nodes[0]->async_close_flow(flow.epoch, flow.id);
                }
                asio::co_spawn(control, query(), asio::detached);
                co_await nodes[0]->async_close_link(first.id);
                auto closing_status = co_await statuses.async_receive(asio::use_awaitable);
                require(!closing_status.result, "close left an in-flight status unresolved");
                auto closed_status = co_await nodes[0]->async_link_status(first.id);
                require(!closed_status.result && closed_status.llink.empty() && closed_status.rlink.empty(),
                        "closed link returned cached status");
                auto fresh = co_await nodes[0]->async_ensure_link("master", "a", transport);
                require(bool(fresh) && fresh.id != first.id, "outer ensure failed to create fresh link after close");
            }
            // Hold links past one keepalive interval and verify they still reuse.
            co_await pause(5300ms);
            auto alive = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Tcp);
            require(bool(alive) && alive.id == live_id, "TCP keepalive lost a healthy channel");
        };
        asio::co_spawn(control, scenario(), asio::use_future).get();
        nodes[2]->stop();
        auto offline = [&]() -> asio::awaitable<void> {
            for (int tries = 0; tries < 100; ++tries)
            {
                auto result = co_await nodes[0]->async_ensure_link("a", "b", RelayProtocol::Tcp);
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
                result = co_await nodes[0]->async_ensure_link("master", "a", RelayProtocol::Tcp);
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
                mismatch = co_await nodes[0]->async_ensure_link("master", "b", RelayProtocol::Tcp);
                if (mismatch.stage == "prepare")
                {
                    break;
                }
                co_await pause(20ms);
            }
            require(!mismatch && mismatch.stage == "prepare", "different data ports were accepted");
            auto retry = co_await nodes[0]->async_ensure_link("master", "b", RelayProtocol::Tcp);
            require(!retry && retry.id != mismatch.id, "failed ensure was retained instead of permitting outer retry");
        };
        asio::co_spawn(control, new_epoch(), asio::use_future).get();
        auto in_flight =
            asio::co_spawn(control, nodes[0]->async_ensure_link("master", "a", RelayProtocol::Udp), asio::use_future);
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
    auto module = std::make_shared<LnkChannel>(io.get_executor(), "a", "127.0.0.1", tp, "127.0.0.1", up);
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
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttach, p), 4);
            co_await send_peer(CtrlMessage(CtrlCommand::LinkAttached, p), 4);
            size = co_await peer.async_receive_from(asio::buffer(bytes), source, asio::use_awaitable);
            co_await pause(100ms);
            require(std::ranges::any_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkReady && m.params->at("id") == 4; }),
                    "fresh outer UDP attempt did not become Ready");
            // Deliberately stop responding: Ready links must expire, without reconnecting.
            co_await pause(20500ms);
            require(std::ranges::any_of(
                        events,
                        [](const auto &m) { return m.type() == CtrlCommand::LinkError && m.params->at("id") == 4; }),
                    "missing Ready keepalive expiration");
            module->close(4);

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
            require(module->send_flow({7, 21, true, LnkFrType::Data, {1}, {}}).status == FlowSendStatus::Closed,
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
            require(module->send_flow({7, 22, true, LnkFrType::Data, {1}, {}}).status == FlowSendStatus::Closed,
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
        std::make_shared<LnkChannel>(io.get_executor(), "rollback", "127.0.0.1", rollback_port, "127.0.0.1", up);
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
    auto module = std::make_shared<LnkChannel>(io.get_executor(), "delayed", "127.0.0.1", tp, "127.0.0.1", up);
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

void notification_overflow()
{
    asio::io_context io(1);
    auto module = std::make_shared<LnkChannel>(io.get_executor(), "a", "127.0.0.1", 1, "127.0.0.1", 1);
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
