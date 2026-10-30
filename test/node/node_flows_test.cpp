#include "node_fixture.h"
TEST_MEMBER(NodeMetrics, bandwidth_at_)
TEST_MEMBER(RelayNode, topology_)
TEST_MEMBER(NodeLinkMgr, create_flow)
TEST_MEMBER(NodeLinkMgr, send_all)
TEST_MEMBER(NodeLinkMgr, finish_flow)
TEST_MEMBER(NodeLinkMgr, flow_requests_)
TEST_MEMBER(NodeLinkMgr, closing_flows_)
TEST_MEMBER(NodeLinkMgr, flow_endpoints_)
TEST_MEMBER(NodeLinkMgr, running_)
TEST_MEMBER(NodeLinkMgr, receive_events)
TEST_MEMBER(ClusterMgr, running_)
TEST_MEMBER(ClusterMgr, outbound_channel_)
TEST_MEMBER(FlowProtocol, emit)
TEST_MEMBER(FlowProtocol, events_)
#include <asio/experimental/awaitable_operators.hpp>
#include <future>
#include <iostream>
using namespace std::chrono_literals;
void require_flow(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}
asio::awaitable<void> delay_flow(std::chrono::milliseconds ms)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(ms);
    co_await timer.async_wait(asio::use_awaitable);
}
std::uint16_t flow_tcp_port(asio::io_context &io)
{
    asio::ip::tcp::acceptor socket(io, {asio::ip::address_v4::loopback(), 0});
    return socket.local_endpoint().port();
}
std::uint16_t flow_udp_port(asio::io_context &io)
{
    asio::ip::udp::socket socket(io, {asio::ip::address_v4::loopback(), 0});
    return socket.local_endpoint().port();
}
/** @brief Exercise transaction identity and real control serialization without opening data sockets. */
static void verify_flow_transactions()
{
    asio::io_context control(1);
    asio::io_context transfer(1);
    asio::io_context udp(1);
    asio::io_context data(1);
    asio::ssl::context ssl(asio::ssl::context::tls_server);
    auto config = make_test_node_config();
    config.control.address = config.tcp.address = config.tls.address = config.datagram.address = "127.0.0.1";
    auto node = std::make_shared<RelayNode>(control, transfer, udp, data, ssl, config);
    auto &manager = test_node::links(node);
    auto &cluster = test_node::cluster(node);
    auto &topology = *test_access::RelayNode_topology_(*node);
    std::vector<std::string> names{config.cluster.node_id};
    njson members = njson::array({{{"node_id", names.front()}, {"address", "127.0.0.1"}}});
    for (int i = 1; i < 8; ++i)
    {
        names.push_back("node-" + std::to_string(i));
        members.push_back({{"node_id", names.back()}, {"address", "127.0.0." + std::to_string(i + 1)}});
    }
    topology.update_members({{"source", names.front()}, {"master", names.front()},
                             {"epoch", 0x2122232425262728ULL}, {"version", 1U}, {"members", members}});
    test_access::ClusterMgr_running_(cluster) = true;
    test_access::NodeLinkMgr_running_(manager) = true;
    auto receiver = asio::co_spawn(control, test_access::NodeLinkMgr_receive_events(manager), asio::use_future);
    const auto poll = [&] { control.restart(); control.poll(); };
    const auto take = [&] {
        std::vector<CtrlMessage> messages;
        while (test_access::ClusterMgr_outbound_channel_(cluster).try_receive(
            [&](asio::error_code error, CtrlMessage message) {
                require_flow(!error, "control capture failed");
                messages.push_back(std::move(message));
            }))
        {
        }
        return messages;
    };
    const auto compact = [&](const CtrlMessage &message) {
        const auto &p = *message.params;
        for (const auto &[key, value] : p.items())
        {
            require_flow(key == "master" || key == "epoch" || key == "flow_id" || key == "request_id" ||
                             key == "stage" || key == "reason" || key == "target",
                         "Flow phase retained redundant path or endpoint fields");
        }
        for (const auto *field : {"master", "epoch", "flow_id", "request_id"})
        {
            require_flow(p.contains(field), "compact Flow message lost transaction identity");
        }
    };
    poll();
    for (const std::size_t count : {2U, 3U, 8U})
    {
        const std::vector<std::string> path(names.begin(), names.begin() + count);
        auto request = test_access::NodeLinkMgr_create_flow(manager, path, RelayProtocol::Tcp,
                                                            0x0102030405060708ULL + count);
        request->params.request_id = 0x1112131415161718ULL;
        for (std::size_t i = 0; i < request->params.links.size(); ++i)
        {
            request->params.links[i] = 0x3132333435363738ULL + i;
        }
        request->result.stage = "prepare";
        auto full = request->params.to_params();
        full["ttl_ms"] = 10000U;
        full["source"] = names.front();
        const auto measure = [&](const CtrlMessage &message) {
            auto after = *message.params;
            after.erase("target");
            after["source"] = names.front();
            auto before = after;
            if (message.type() != CtrlCommand::FlowPrepare)
            {
                for (const auto *field : {"path", "addresses", "links", "transport"})
                {
                    before[field] = full.at(field);
                }
                if (message.type() != CtrlCommand::FlowCommit && message.type() != CtrlCommand::FlowClose)
                {
                    before["ttl_ms"] = full.at("ttl_ms");
                }
                if (message.type() == CtrlCommand::FlowClosed)
                {
                    before["committed"] = true;
                }
            }
            const auto old_size = WireMessage::pack(CtrlMessage(message.type(), std::move(before))).size() -
                                  WireMessage::header_length;
            const auto new_size = WireMessage::pack(CtrlMessage(message.type(), std::move(after))).size() -
                                  WireMessage::header_length;
            require_flow(message.type() == CtrlCommand::FlowPrepare ? new_size == old_size : new_size < old_size,
                         "Flow payload size did not match phase simplification");
            std::cout << "[WIRE] nodes=" << count << " command=" << message.command
                      << " old=" << old_size << " new=" << new_size << '\n';
        };
        const auto acknowledge = [&](CtrlCommand command, std::string source) {
            auto p = request->params.identity_params();
            p["source"] = std::move(source);
            manager.handle(command, CtrlMessage(command, std::move(p)));
        };
        require_flow(test_access::NodeLinkMgr_send_all(manager, *request, CtrlCommand::FlowPrepare),
                     "Prepare submission failed");
        auto messages = take();
        require_flow(messages.size() == count && messages.front().params->at("path") == path,
                     "Prepare lost its full path or recipients");
        measure(messages.front());
        for (const auto *field : {"master", "epoch", "flow_id", "request_id", "source"})
        {
            auto wrong = request->params.identity_params();
            wrong["source"] = names.front();
            wrong[field] = std::string_view(field) == "source" || std::string_view(field) == "master"
                               ? njson("outsider") : njson(std::uint64_t{1});
            manager.handle(CtrlCommand::FlowPrepared, CtrlMessage(CtrlCommand::FlowPrepared, std::move(wrong)));
            require_flow(request->prepared == 0, "wrong identity or source advanced Prepare");
        }
        acknowledge(CtrlCommand::FlowCommitted, names.front());
        require_flow(request->committed == 0, "Commit confirmation advanced before Prepare");
        manager.handle(CtrlCommand::FlowPrepare, CtrlMessage(CtrlCommand::FlowPrepare, full));
        auto duplicate = full;
        duplicate["ttl_ms"] = 5000U;
        manager.handle(CtrlCommand::FlowPrepare, CtrlMessage(CtrlCommand::FlowPrepare, duplicate));
        require_flow(test_access::NodeLinkMgr_flow_endpoints_(manager).size() == 1,
                     "duplicate Prepare allocated a second endpoint");
        duplicate["request_id"] = 2U;
        manager.handle(CtrlCommand::FlowPrepare, CtrlMessage(CtrlCommand::FlowPrepare, duplicate));
        messages = take();
        require_flow(messages.size() == 1 && messages.front().type() == CtrlCommand::FlowError &&
                         messages.front().params->at("reason") == "conflicting flow prepare",
                     "conflicting Prepare replaced authorization or lost its original error");
        compact(messages.front());
        measure(messages.front());
        const auto notification = [&](lnk::EventKind kind, std::string stage) {
            test_access::FlowProtocol_emit(manager.protocol(),
                lnk::ProtocolEvent{kind, request->params.epoch, request->params.id, std::move(stage), ""});
            poll();
            auto replies = take();
            require_flow(replies.size() == 1, "data notification did not produce one acknowledgement");
            compact(replies.front());
            measure(replies.front());
        };
        notification(lnk::EventKind::FlowPrepared, "prepare");
        acknowledge(CtrlCommand::FlowPrepared, names.front());
        acknowledge(CtrlCommand::FlowPrepared, names.front());
        require_flow(request->prepared == 1, "duplicate acknowledgement advanced another node");
        for (std::size_t i = 1; i < count; ++i)
        {
            acknowledge(CtrlCommand::FlowPrepared, path[i]);
        }
        messages = take();
        require_flow(messages.size() == count && messages.front().type() == CtrlCommand::FlowCommit,
                     "complete Prepare did not submit Commit to the path");
        for (const auto &message : messages)
        {
            compact(message);
        }
        measure(messages.front());
        acknowledge(CtrlCommand::FlowPrepared, names.front());
        require_flow(take().empty(), "late Prepare acknowledgement repeated Commit");
        notification(lnk::EventKind::FlowCommitted, "commit");
        for (const auto &source : path)
        {
            acknowledge(CtrlCommand::FlowCommitted, source);
        }
        require_flow(request->completed && bool(request->result), "compact Commit did not finish establishment");
        test_access::NodeLinkMgr_finish_flow(manager, request, "close", "closed by caller");
        messages = take();
        require_flow(messages.size() == count && messages.front().type() == CtrlCommand::FlowClose,
                     "Close did not cover the original path");
        for (const auto &message : messages)
        {
            compact(message);
        }
        measure(messages.front());
        acknowledge(CtrlCommand::FlowClosed, "outsider");
        auto wrong = request->params.identity_params();
        wrong["request_id"] = 2U;
        wrong["source"] = names.front();
        manager.handle(CtrlCommand::FlowClosed, CtrlMessage(CtrlCommand::FlowClosed, wrong));
        require_flow(request->closed == 0 && !request->released, "wrong Close acknowledgement released the path");
        notification(lnk::EventKind::FlowClosed, "close");
        for (const auto &source : path)
        {
            acknowledge(CtrlCommand::FlowClosed, source);
        }
        require_flow(request->released && test_access::NodeLinkMgr_closing_flows_(manager).empty(),
                     "compact Closed did not release coordination state");
        auto identity = request->params.identity_params();
        identity["source"] = names.front();
        manager.handle(CtrlCommand::FlowCommit, CtrlMessage(CtrlCommand::FlowCommit, identity));
        require_flow(take().empty(), "unknown Commit produced a reply");
        manager.handle(CtrlCommand::FlowClose, CtrlMessage(CtrlCommand::FlowClose, identity));
        messages = take();
        require_flow(messages.size() == 1 && messages.front().type() == CtrlCommand::FlowClosed &&
                         test_access::NodeLinkMgr_flow_endpoints_(manager).empty(),
                     "unknown Close failed to acknowledge or created endpoint state");
        compact(messages.front());
        identity.erase("request_id");
        manager.handle(CtrlCommand::FlowClose, CtrlMessage(CtrlCommand::FlowClose, identity));
        require_flow(take().empty(), "Close without transaction identity was acknowledged");
        acknowledge(CtrlCommand::FlowPrepared, names.front());
        acknowledge(CtrlCommand::FlowCommitted, names.front());
        require_flow(test_access::NodeLinkMgr_flow_requests_(manager).empty() && take().empty(),
                     "late acknowledgement revived a released transaction");
    }
    test_access::NodeLinkMgr_running_(manager) = false;
    data.poll();
    test_access::FlowProtocol_events_(manager.protocol()).close();
    poll();
    receiver.get();
    test_access::ClusterMgr_running_(cluster) = false;
}

void flow_integration()
{
    asio::io_context control(1);
    asio::io_context transfer(1);
    asio::io_context udp(1);
    asio::io_context data(1);
    auto cw = asio::make_work_guard(control);
    auto tw = asio::make_work_guard(transfer);
    auto uw = asio::make_work_guard(udp);
    auto dw = asio::make_work_guard(data);
    asio::ssl::context ssl(asio::ssl::context::tls_server);
    auto settings = make_test_node_config();
    ssl.use_certificate_chain_file(settings.certificate_chain.string());
    ssl.use_private_key_file(settings.private_key.string(), asio::ssl::context::pem);
    ssl.load_verify_file(settings.server_ca_file.string());
    const auto cp = flow_tcp_port(control);
    const auto tp = flow_tcp_port(data);
    const auto up = flow_udp_port(data);
    const std::vector<std::string> names{"master", "a", "b", "c", "d", "e", "f", "g"};
    std::vector<NodeConfig> configs;
    std::vector<std::shared_ptr<RelayNode>> nodes;
    for (std::size_t i = 0; i < names.size(); ++i)
    {
        auto config = make_test_node_config();
        const auto address = "127.0.0." + std::to_string(i + 1);
        config.control.address = config.control.advertise_address = config.tcp.address = config.tls.address =
            config.datagram.address = address;
        config.cluster = {
            i == 0 ? ClusterConfig::Role::Master : ClusterConfig::Role::Slave, names[i], "127.0.0.1", cp, tp, up};
        config.channel.disconnect_timeout = 100ms;
        auto node = std::make_shared<RelayNode>(control, transfer, udp, data, ssl, config);
        node->start();
        configs.push_back(config);
        nodes.push_back(node);
    }
    std::thread c([&] { control.run(); });
    std::thread t([&] { transfer.run(); });
    std::thread u([&] { udp.run(); });
    std::thread d([&] { data.run(); });
    std::exception_ptr failure;
    try
    {
        auto scenario = [&]() -> asio::awaitable<void> {
            auto invalid = co_await test_node::open_flow(nodes[0], {"a", "a"}, RelayProtocol::Tcp);
            require_flow(!invalid, "cyclic path accepted");
            invalid = co_await test_node::open_flow(nodes[0], {"a", "b"}, RelayProtocol::Tls);
            require_flow(!invalid, "Node TLS accepted");
            invalid = co_await test_node::open_flow(nodes[1], {"b", "c"}, RelayProtocol::Tcp);
            require_flow(!invalid, "non-ingress requested a flow");
            static_assert(NodeLinkMgr::max_flow_nodes == 8);
            auto oversized_path = names;
            oversized_path.push_back("extra");
            invalid = co_await test_node::open_flow(nodes[0], oversized_path, RelayProtocol::Tcp);
            require_flow(!invalid && invalid.id == 0 && invalid.stage == "open", "nine-node path accepted");
            auto queued = [&](std::size_t node, const FlowResult &s, bool reverse,
                              BytesBuf payload) -> asio::awaitable<void> {
                require_flow(co_await test_node::send_data(nodes[node], s.epoch, s.id, reverse, std::move(payload)),
                             "flow send failed");
            };
            auto receive = [&](std::size_t node, const FlowResult &s,
                               BytesBuf expected) -> asio::awaitable<void> {
                auto data = co_await test_node::receive_flow(nodes[node], s.epoch, s.id);
                require_flow(data == expected, "flow data crossed path/direction");
            };
            auto finish = [&](std::size_t node, const FlowResult &s, bool reverse) -> asio::awaitable<void> {
                const auto stream = std::get<std::shared_ptr<lnk::FlowStream>>(
                    co_await test_node::port(nodes[node], s.epoch, s.id, reverse));
                co_await asio::co_spawn(stream->executor(), stream->finish_send(), asio::use_awaitable);
            };
            FlowResult warm;
            for (int i = 0; i < 100; ++i)
            {
                warm = co_await test_node::open_flow(nodes[0], {"a", "b", "c"}, RelayProtocol::Tcp);
                if (warm)
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(bool(warm), "flow did not become Ready");
            auto identity = test_access::NodeLinkMgr_flow_requests_(test_node::links(nodes[0])).at(warm.id)->params.identity_params();
            identity["source"] = "master";
            for (const auto *field : {"source", "epoch", "request_id"})
            {
                auto wrong = identity;
                wrong[field] = std::string_view(field) == "source" ? njson("c") : njson(std::uint64_t{1});
                test_node::links(nodes[1]).handle(CtrlCommand::FlowClose, CtrlMessage(CtrlCommand::FlowClose, wrong));
            }
            require_flow(co_await test_node::send_data(nodes[1], warm.epoch, warm.id, false, {61}),
                         "wrong compact Close cancelled a healthy Flow");
            require_flow(co_await test_node::receive_flow(nodes[3], warm.epoch, warm.id) == BytesBuf{61},
                         "wrong compact Close changed forwarding authorization");
            co_await test_node::close_flow(nodes[0], warm.epoch, warm.id);
            // Open and cancel can arrive in the same receive batch; late establishment cannot resurrect the Flow.
            const auto cancelled_id = generate_random_id();
            test_node::cluster(nodes[1]).send("master", CtrlMessage("flow.open", njson{{"epoch", warm.epoch},
                {"flow_id", cancelled_id}, {"path", {"a", "b", "c"}}, {"transport", "tcp"}}));
            test_node::cluster(nodes[1]).send("master", CtrlMessage("flow.close.request",
                njson{{"epoch", warm.epoch}, {"flow_id", cancelled_id}}));
            co_await delay_flow(100ms);
            require_flow(co_await test_flow::throws<std::runtime_error>(
                             test_node::port(nodes[1], warm.epoch, cancelled_id, false)),
                         "cancelled remote flow was resurrected");
            FlowResult full_path;
            for (int i = 0; i < 100; ++i)
            {
                full_path = co_await test_node::open_flow(nodes[0], names, RelayProtocol::Tcp);
                if (full_path)
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(bool(full_path), "eight-node path did not become Ready");
            co_await queued(0, full_path, false, {8});
            co_await receive(7, full_path, {8});
            co_await queued(7, full_path, true, {9});
            co_await receive(0, full_path, {9});
            co_await test_node::close_flow(nodes[0], full_path.epoch, full_path.id);
            for (auto transport : {RelayProtocol::Tcp, RelayProtocol::Udp})
            {
                const std::string middle = "b";
                const std::size_t middle_index = 2;
                const std::vector<std::string> path1{"a", middle, "c"};
                const std::vector<std::string> path2{"a", middle, "d"};
                if (transport == RelayProtocol::Udp)
                {
                    // A cold local UDP packet can be filtered before the reciprocal outbound packet.
                    // The caller makes a fresh ensure after a failure; the implementation never retries.
                    for (const auto &peer : {"a", "c", "d"})
                    {
                        LinkResult edge;
                        std::uint64_t failed_id = 0;
                        for (int attempt = 0; attempt < 3; ++attempt)
                        {
                            edge = co_await test_node::ensure_link(nodes[0], peer, middle, transport);
                            require_flow(edge.id != failed_id, "outer UDP attempt reused failed NodeLink identity");
                            if (edge)
                            {
                                break;
                            }
                            failed_id = edge.id;
                        }
                        require_flow(bool(edge), "outer UDP ensure never became Ready");
                    }
                }
                // Ordinary ingress uses the same master Flow transaction and owns its local lifecycle.
                const auto remote = co_await test_node::open_flow(nodes[1], {"a", "b", "c"}, transport);
                require_flow(bool(remote), "ingress Flow request failed");
                require_flow(test_node::cluster(nodes[0]).send({}, CtrlMessage{"invalid.target"}) == ClusterMgr::SendResult::InvalidMessage,
                             "master accepted an invalid target");
                require_flow(test_node::cluster(nodes[1]).broadcast(CtrlMessage{"cluster.invalid"}) == ClusterMgr::SendResult::InvalidMessage,
                             "slave accepted a reserved command");
                require_flow(test_node::cluster(nodes[1]).broadcast(
                                 CtrlMessage{std::string(CtrlMessage::max_command_length + 1, 'x')}) == ClusterMgr::SendResult::InvalidMessage,
                             "slave accepted an unencodable message");
                co_await queued(1, remote, false, {31});
                co_await receive(3, remote, {31});
                co_await queued(3, remote, true, {32});
                co_await receive(1, remote, {32});
                co_await test_node::close_flow(nodes[1], remote.epoch, remote.id);
                require_flow(co_await test_flow::throws<std::runtime_error>(
                                 test_node::port(nodes[1], remote.epoch, remote.id, false)),
                             "remote close retained the local flow");
                asio::experimental::channel<void(asio::error_code, FlowResult)> opened(control, 2);
                auto open = [&](std::vector<std::string> path) -> asio::awaitable<void> {
                    auto s = co_await test_node::open_flow(nodes[0], std::move(path), transport);
                    opened.try_send(asio::error_code{}, std::move(s));
                };
                asio::co_spawn(control, open(path1), asio::detached);
                asio::co_spawn(control, open(path2), asio::detached);
                auto s1 = co_await opened.async_receive(asio::use_awaitable);
                auto s2 = co_await opened.async_receive(asio::use_awaitable);
                if (!s1 || !s2)
                {
                    std::cerr << "Concurrent open: " << s1.stage << ": " << s1.reason << "; " << s2.stage << ": "
                              << s2.reason << '\n';
                }
                require_flow(bool(s1) && bool(s2) && s1.id != s2.id, "concurrent paths failed or share flow identity");
                // Completion order need not match launch order; identify endpoints by an explicit fresh path below.
                co_await test_node::close_flow(nodes[0], s1.epoch, s1.id);
                co_await test_node::close_flow(nodes[0], s2.epoch, s2.id);
                s1 = co_await test_node::open_flow(nodes[0], path1, transport);
                s2 = co_await test_node::open_flow(nodes[0], path2, transport);
                auto shared = co_await test_node::ensure_link(nodes[0], "a", middle, transport);
                require_flow(bool(s1) && bool(s2) && bool(shared), "forked flows failed");
                for (const auto &node : nodes)
                {
                    test_access::NodeMetrics_bandwidth_at_(test_node::metrics(node)) = std::chrono::steady_clock::now();
                    test_node::links(node).protocol().take_forwarded_bytes();
                }
                for (std::uint8_t i = 0; i < 12; ++i)
                {
                    co_await queued(1, s1, false, {1, i, 0, 255});
                    co_await queued(1, s2, false, {2, i, 0, 255});
                    co_await queued(3, s1, true, {3, i});
                    co_await queued(4, s2, true, {4, i});
                }
                for (std::uint8_t i = 0; i < 12; ++i)
                {
                    co_await receive(3, s1, {1, i, 0, 255});
                    co_await receive(4, s2, {2, i, 0, 255});
                    co_await receive(1, s1, {3, i});
                    co_await receive(1, s2, {4, i});
                }
                BytesBuf large(LnkFrameHeader::maximum_payload);
                for (std::size_t i = 0; i < large.size(); ++i)
                {
                    large[i] = static_cast<std::uint8_t>(i);
                }
                co_await queued(1, s1, false, large);
                co_await queued(1, s2, false, large);
                co_await queued(3, s1, true, large);
                co_await queued(4, s2, true, large);
                co_await receive(3, s1, large);
                co_await receive(4, s2, large);
                co_await receive(1, s1, large);
                co_await receive(1, s2, large);
                require_flow(test_node::links(nodes[middle_index]).protocol().take_forwarded_bytes() ==
                                 144 + 4 * large.size(), "Transit bandwidth counted payloads incorrectly");
                require_flow(test_node::links(nodes[1]).protocol().take_forwarded_bytes() ==
                                 48 + 2 * large.size(), "Ingress bandwidth counted outgoing payloads");
                require_flow(test_node::links(nodes[3]).protocol().take_forwarded_bytes() == 48 + large.size() &&
                                 test_node::links(nodes[4]).protocol().take_forwarded_bytes() == 48 + large.size(),
                             "Tail bandwidth did not count only incoming payloads");
                co_await queued(1, s1, false, {});
                co_await receive(3, s1, {});
                require_flow(co_await test_flow::throws<std::invalid_argument>(
                                 test_node::port(nodes[middle_index], s1.epoch, s1.id, false)),
                             "middle Node bound an endpoint");
                require_flow(co_await test_flow::throws<std::runtime_error>(
                                 test_node::port(nodes[1], s1.epoch + 1, s1.id, false)),
                             "old epoch accepted");
                if (transport == RelayProtocol::Tcp)
                {
                    co_await queued(1, s1, false, {9});
                    co_await finish(1, s1, false);
                    co_await receive(3, s1, {9});
                    auto fin = co_await test_node::receive_flow(nodes[3], s1.epoch, s1.id);
                    require_flow(!fin, "FIN did not follow DATA");
                    require_flow(co_await test_flow::throws<std::runtime_error>(
                                     test_node::send_data(nodes[1], s1.epoch, s1.id, false, {7})),
                                 "DATA after FIN accepted");
                    require_flow(co_await test_flow::throws<std::runtime_error>(finish(1, s1, false)),
                                 "duplicate FIN accepted");
                    co_await queued(3, s1, true, {8});
                    co_await receive(1, s1, {8});
                }
                else
                {
                    // Datagram ports expose only packet send/receive, with no FIN or RESET operation.
                    co_await queued(1, s1, false, {8});
                    co_await receive(3, s1, {8});
                }
                co_await test_node::close_flow(nodes[0], s1.epoch, s1.id);
                co_await test_node::close_flow(nodes[0], s1.epoch, s1.id);
                co_await queued(1, s2, false, {10});
                co_await receive(4, s2, {10});
                auto reused = co_await test_node::ensure_link(nodes[0], middle, "a", transport);
                require_flow(reused.id == shared.id, "closing flow destroyed shared NodeLink");
                // Node ID order reverses the forward path on every edge.
                // Keep the initializer-list temporary out of co_await to avoid a GCC 14 ICE.
                const std::vector<std::string> reversed_path{"c", middle, "a"};
                auto reversed = co_await test_node::open_flow(nodes[0], reversed_path, transport);
                require_flow(bool(reversed), "reverse path failed");
                co_await queued(3, reversed, false, {11});
                co_await receive(1, reversed, {11});
                co_await test_node::close_flow(nodes[0], reversed.epoch, reversed.id);
                // A slow endpoint must preserve its Flow and reverse direction.
                auto crowded = co_await test_node::open_flow(nodes[0], path1, transport);
                const auto congested_frames = transport == RelayProtocol::Tcp ? 136 : 24;
                for (int i = 0; i < congested_frames; ++i)
                {
                    auto result = co_await test_node::send_data(nodes[1], crowded.epoch, crowded.id, false, {12});
                    if (!result)
                    {
                        break;
                    }
                    co_await delay_flow(5ms);
                }
                co_await delay_flow(100ms);
                auto blocked =
                    co_await test_node::send_data(nodes[3], crowded.epoch, crowded.id, true, {13});
                require_flow(blocked, "slow endpoint closed the reverse direction");
                co_await receive(1, crowded, {13});
                co_await test_node::close_flow(nodes[0], crowded.epoch, crowded.id);
                co_await queued(1, s2, false, {14});
                co_await receive(4, s2, {14});
                reused = co_await test_node::ensure_link(nodes[0], "a", middle, transport);
                require_flow(reused.id == shared.id, "flow congestion closed shared NodeLink");
                if (transport == RelayProtocol::Tcp)
                {
                    auto reset = co_await test_node::open_flow(nodes[0], path1, transport);
                    auto receive_reset = [&]() -> asio::awaitable<void> {
                        std::string reason;
                        try
                        {
                            static_cast<void>(co_await test_node::receive_flow(nodes[3], reset.epoch, reset.id));
                        }
                        catch (const std::exception &e)
                        {
                            reason = e.what();
                        }
                        require_flow(reason == "flow reset", ("RESET receiver failed with: " + reason).c_str());
                    };
                    // Start the receiver first on the shared data executor, before RESET can remove the Flow.
                    using namespace asio::experimental::awaitable_operators;
                    co_await asio::co_spawn(data,
                        receive_reset() && test_flow::reset(test_node::links(nodes[1]).protocol().shared_from_this(),
                                                           reset.epoch, reset.id, false), asio::use_awaitable);
                }
                co_await test_node::close_flow(nodes[0], s2.epoch, s2.id);
            }
            for (auto path : {std::vector<std::string>{"master", "a", "c"}, {"a", "master", "c"}, {"a", "b", "master"}})
            {
                auto s = co_await test_node::open_flow(nodes[0], path, RelayProtocol::Tcp);
                require_flow(bool(s), "master on path failed");
                const auto left = std::size_t(std::ranges::find(names, path.front()) - names.begin());
                const auto right = std::size_t(std::ranges::find(names, path.back()) - names.begin());
                co_await queued(left, s, false, {15});
                co_await receive(right, s, {15});
                co_await queued(right, s, true, {16});
                co_await receive(left, s, {16});
                co_await test_node::close_flow(nodes[0], s.epoch, s.id);
            }
            auto healthy = co_await test_node::open_flow(nodes[0], {"a", "b", "d"}, RelayProtocol::Tcp);
            co_await delay_flow(21500ms); // Idle Flows remain active while control and NodeLinks are healthy.
            co_await queued(1, healthy, false, {17});
            co_await receive(4, healthy, {17});
            auto broken = co_await test_node::open_flow(nodes[0], {"a", "b", "c"}, RelayProtocol::Tcp);
            auto edge = co_await test_node::ensure_link(nodes[0], "b", "c", RelayProtocol::Tcp);
            co_await test_node::close_link(nodes[0], edge.id);
            co_await delay_flow(200ms);
            bool failed = false;
            try
            {
                static_cast<void>(co_await test_node::receive_flow(nodes[3], broken.epoch, broken.id));
            }
            catch (const std::exception &)
            {
                failed = true;
            }
            require_flow(failed, "broken NodeLink left a flow active");
            co_await queued(1, healthy, false, {18});
            co_await receive(4, healthy, {18});
            co_await test_node::close_flow(nodes[0], healthy.epoch, healthy.id);
        };
        asio::co_spawn(control, scenario(), asio::use_future).get();
        // Restart a path Node at inconsistent ports: return one prepare failure and allow outer retry.
        nodes[3]->stop();
        auto bad = configs[3];
        bad.cluster.tcp_port = flow_tcp_port(data);
        nodes[3] = std::make_shared<RelayNode>(control, transfer, udp, data, ssl, bad);
        nodes[3]->start();
        auto failures = [&]() -> asio::awaitable<void> {
            FlowResult result;
            for (int i = 0; i < 100; ++i)
            {
                result = co_await test_node::open_flow(nodes[0], {"a", "b", "c"}, RelayProtocol::Tcp);
                if (result.stage == "links")
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(!result && result.stage == "links", "different data ports accepted in flow");
            auto again = co_await test_node::open_flow(nodes[0], {"a", "b", "c"}, RelayProtocol::Tcp);
            require_flow(!again && again.id != result.id, "outer retry retained failed flow identity");
        };
        asio::co_spawn(control, failures(), asio::use_future).get();
        auto live = asio::co_spawn(control, test_node::open_flow(nodes[0], {"a", "b", "d"}, RelayProtocol::Tcp),
                                   asio::use_future).get();
        require_flow(bool(live), "shutdown fixture did not become Ready");
        auto receiver = asio::co_spawn(control, test_node::receive_flow(nodes[1], live.epoch, live.id), asio::use_future);
        auto pending =
            asio::co_spawn(control, test_node::open_flow(nodes[0], {"a", "b", "d"}, RelayProtocol::Udp), asio::use_future);
        asio::post(control, asio::use_future([&] {
            auto &cluster = test_node::cluster(nodes[0]);
            auto result = ClusterMgr::SendResult::Submitted;
            for (std::size_t i = 0; i < configs[0].channel.max_queued_messages + 2 &&
                                    result == ClusterMgr::SendResult::Submitted; ++i)
            {
                result = cluster.broadcast(CtrlMessage{"overflow.notice"});
            }
            require_flow(result == ClusterMgr::SendResult::QueueFull, "master overflow did not fail control");
        })).get();
        require_flow(receiver.wait_for(3s) == std::future_status::ready,
                     "master control failure retained a remote receiver before owner shutdown");
        require_flow(!nodes[0]->stop(), "master control failure was reported as a successful shutdown");
        const auto stopped = pending.get();
        require_flow(bool(stopped) || !stopped.reason.empty(), "stop left open unresolved");
        bool invalidated = false;
        try
        {
            static_cast<void>(receiver.get());
        }
        catch (const std::exception &)
        {
            invalidated = true;
        }
        require_flow(invalidated, "master control failure left a remote Flow active");
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        (*it)->stop();
    }
    cw.reset();
    tw.reset();
    uw.reset();
    dw.reset();
    c.join();
    t.join();
    u.join();
    d.join();
    require_flow(control.stopped() && data.stopped() && transfer.stopped() && udp.stopped(),
                 "Node shutdown retained tasks");
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}
int main()
{
    try
    {
        verify_flow_transactions();
        flow_integration();
        std::cout << "[PASS] NodeLinkMgr flows and TCP/UDP hop forwarding\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
