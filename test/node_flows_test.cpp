#include "node_test_config.h"
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
            auto invalid = co_await nodes[0]->async_open_flow({"a", "a"}, RelayProtocol::Tcp);
            require_flow(!invalid, "cyclic path accepted");
            invalid = co_await nodes[0]->async_open_flow({"a", "b"}, RelayProtocol::Tls);
            require_flow(!invalid, "Node TLS accepted");
            invalid = co_await nodes[1]->async_open_flow({"a", "b"}, RelayProtocol::Tcp);
            require_flow(!invalid, "slave coordinated flow");
            static_assert(NodeLinkMgr::max_flow_nodes == 8);
            auto oversized_path = names;
            oversized_path.push_back("extra");
            invalid = co_await nodes[0]->async_open_flow(oversized_path, RelayProtocol::Tcp);
            require_flow(!invalid && invalid.id == 0 && invalid.stage == "open", "nine-node path accepted");
            auto queued = [&](std::size_t node, const FlowResult &s, bool reverse, BytesBuf payload,
                              LnkFrType kind = LnkFrType::Data) -> asio::awaitable<void> {
                auto result =
                    co_await nodes[node]->async_send_flow({s.epoch, s.id, reverse, kind, std::move(payload), {}});
                require_flow(result.status == FlowSendStatus::Queued, "flow send failed");
            };
            auto receive = [&](std::size_t node, const FlowResult &s, bool reverse,
                               BytesBuf expected) -> asio::awaitable<void> {
                auto f = co_await nodes[node]->async_receive_flow(s.epoch, s.id);
                require_flow(f.epoch == s.epoch && f.flow_id == s.id && f.reverse == reverse &&
                                 f.kind == LnkFrType::Data && f.payload == expected,
                             "flow data crossed path/direction");
            };
            FlowResult warm;
            for (int i = 0; i < 100; ++i)
            {
                warm = co_await nodes[0]->async_open_flow({"a", "b", "c"}, RelayProtocol::Tcp);
                if (warm)
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(bool(warm), "flow did not become Ready");
            co_await nodes[0]->async_close_flow(warm.epoch, warm.id);
            FlowResult full_path;
            for (int i = 0; i < 100; ++i)
            {
                full_path = co_await nodes[0]->async_open_flow(names, RelayProtocol::Tcp);
                if (full_path)
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(bool(full_path), "eight-node path did not become Ready");
            co_await queued(0, full_path, false, {8});
            co_await receive(7, full_path, false, {8});
            co_await queued(7, full_path, true, {9});
            co_await receive(0, full_path, true, {9});
            co_await nodes[0]->async_close_flow(full_path.epoch, full_path.id);
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
                            edge = co_await nodes[0]->async_ensure_link(peer, middle, transport);
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
                asio::experimental::channel<void(asio::error_code, FlowResult)> opened(control, 2);
                auto open = [&](std::vector<std::string> path) -> asio::awaitable<void> {
                    auto s = co_await nodes[0]->async_open_flow(std::move(path), transport);
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
                co_await nodes[0]->async_close_flow(s1.epoch, s1.id);
                co_await nodes[0]->async_close_flow(s2.epoch, s2.id);
                s1 = co_await nodes[0]->async_open_flow(path1, transport);
                s2 = co_await nodes[0]->async_open_flow(path2, transport);
                auto shared = co_await nodes[0]->async_ensure_link("a", middle, transport);
                require_flow(bool(s1) && bool(s2) && bool(shared), "forked flows failed");
                for (std::uint8_t i = 0; i < 12; ++i)
                {
                    co_await queued(1, s1, false, {1, i, 0, 255});
                    co_await queued(1, s2, false, {2, i, 0, 255});
                    co_await queued(3, s1, true, {3, i});
                    co_await queued(4, s2, true, {4, i});
                }
                for (std::uint8_t i = 0; i < 12; ++i)
                {
                    co_await receive(3, s1, false, {1, i, 0, 255});
                    co_await receive(4, s2, false, {2, i, 0, 255});
                    co_await receive(1, s1, true, {3, i});
                    co_await receive(1, s2, true, {4, i});
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
                co_await receive(3, s1, false, large);
                co_await receive(4, s2, false, large);
                co_await receive(1, s1, true, large);
                co_await receive(1, s2, true, large);
                co_await queued(1, s1, false, {});
                co_await receive(3, s1, false, {});
                auto wrong = co_await nodes[middle_index]->async_send_flow(
                    {s1.epoch, s1.id, false, LnkFrType::Data, {5}, {}});
                require_flow(wrong.status == FlowSendStatus::Invalid && wrong.unsent.payload == BytesBuf{5},
                             "middle Node injected data");
                wrong = co_await nodes[1]->async_send_flow({s1.epoch + 1, s1.id, false, LnkFrType::Data, {6}, {}});
                require_flow(wrong.status == FlowSendStatus::Closed && wrong.unsent.payload == BytesBuf{6},
                             "old epoch accepted");
                if (transport == RelayProtocol::Tcp)
                {
                    co_await queued(1, s1, false, {9});
                    co_await queued(1, s1, false, {}, LnkFrType::Fin);
                    co_await receive(3, s1, false, {9});
                    auto fin = co_await nodes[3]->async_receive_flow(s1.epoch, s1.id);
                    require_flow(fin.kind == LnkFrType::Fin, "FIN did not follow DATA");
                    wrong = co_await nodes[1]->async_send_flow({s1.epoch, s1.id, false, LnkFrType::Data, {7}, {}});
                    require_flow(wrong.status == FlowSendStatus::Invalid, "DATA after FIN accepted");
                    co_await queued(3, s1, true, {8});
                    co_await receive(1, s1, true, {8});
                }
                co_await nodes[0]->async_close_flow(s1.epoch, s1.id);
                co_await nodes[0]->async_close_flow(s1.epoch, s1.id);
                co_await queued(1, s2, false, {10});
                co_await receive(4, s2, false, {10});
                auto reused = co_await nodes[0]->async_ensure_link(middle, "a", transport);
                require_flow(reused.id == shared.id, "closing flow destroyed shared NodeLink");
                // Node ID order reverses the forward path on every edge.
                auto reversed = co_await nodes[0]->async_open_flow({"c", middle, "a"}, transport);
                require_flow(bool(reversed), "reverse path failed");
                co_await queued(3, reversed, false, {11});
                co_await receive(1, reversed, false, {11});
                co_await nodes[0]->async_close_flow(reversed.epoch, reversed.id);
                // A slow endpoint reaches its bounded queue; only that flow fails.
                auto crowded = co_await nodes[0]->async_open_flow(path1, transport);
                for (int i = 0; i < 24; ++i)
                {
                    auto result = co_await nodes[1]->async_send_flow(
                        {crowded.epoch, crowded.id, false, LnkFrType::Data, {12}, {}});
                    if (result.status == FlowSendStatus::Closed || result.status == FlowSendStatus::WouldBlock)
                    {
                        break;
                    }
                    co_await delay_flow(5ms);
                }
                co_await delay_flow(100ms);
                auto blocked =
                    co_await nodes[3]->async_send_flow({crowded.epoch, crowded.id, true, LnkFrType::Data, {13}, {}});
                require_flow(blocked.status == FlowSendStatus::Closed, "full endpoint queue did not fail flow");
                co_await queued(1, s2, false, {14});
                co_await receive(4, s2, false, {14});
                reused = co_await nodes[0]->async_ensure_link("a", middle, transport);
                require_flow(reused.id == shared.id, "flow congestion closed shared NodeLink");
                if (transport == RelayProtocol::Tcp)
                {
                    auto reset = co_await nodes[0]->async_open_flow(path1, transport);
                    co_await queued(1, reset, false, {}, LnkFrType::Reset);
                    bool failed = false;
                    try
                    {
                        static_cast<void>(co_await nodes[3]->async_receive_flow(reset.epoch, reset.id));
                    }
                    catch (const std::exception &e)
                    {
                        failed = std::string(e.what()) == "flow reset";
                    }
                    require_flow(failed, "RESET did not preserve failure reason for the receiver");
                }
                co_await nodes[0]->async_close_flow(s2.epoch, s2.id);
            }
            // Master as first, middle and last; extra direct edges use the same Node listeners.
            for (auto path : {std::vector<std::string>{"master", "a", "c"}, {"a", "master", "c"}, {"a", "b", "master"}})
            {
                auto s = co_await nodes[0]->async_open_flow(path, RelayProtocol::Tcp);
                require_flow(bool(s), "master on path failed");
                const auto left = std::size_t(std::ranges::find(names, path.front()) - names.begin());
                const auto right = std::size_t(std::ranges::find(names, path.back()) - names.begin());
                co_await queued(left, s, false, {15});
                co_await receive(right, s, false, {15});
                co_await queued(right, s, true, {16});
                co_await receive(left, s, true, {16});
                co_await nodes[0]->async_close_flow(s.epoch, s.id);
            }
            auto healthy = co_await nodes[0]->async_open_flow({"a", "b", "d"}, RelayProtocol::Tcp);
            co_await delay_flow(21500ms); // Idle Flows remain active while control and NodeLinks are healthy.
            co_await queued(1, healthy, false, {17});
            co_await receive(4, healthy, false, {17});
            auto broken = co_await nodes[0]->async_open_flow({"a", "b", "c"}, RelayProtocol::Tcp);
            auto edge = co_await nodes[0]->async_ensure_link("b", "c", RelayProtocol::Tcp);
            co_await nodes[0]->async_close_link(edge.id);
            co_await delay_flow(200ms);
            bool failed = false;
            try
            {
                static_cast<void>(co_await nodes[3]->async_receive_flow(broken.epoch, broken.id));
            }
            catch (const std::exception &)
            {
                failed = true;
            }
            require_flow(failed, "broken NodeLink left a flow active");
            co_await queued(1, healthy, false, {18});
            co_await receive(4, healthy, false, {18});
            co_await nodes[0]->async_close_flow(healthy.epoch, healthy.id);
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
                result = co_await nodes[0]->async_open_flow({"a", "b", "c"}, RelayProtocol::Tcp);
                if (result.stage == "links")
                {
                    break;
                }
                co_await delay_flow(20ms);
            }
            require_flow(!result && result.stage == "links", "different data ports accepted in flow");
            auto again = co_await nodes[0]->async_open_flow({"a", "b", "c"}, RelayProtocol::Tcp);
            require_flow(!again && again.id != result.id, "outer retry retained failed flow identity");
        };
        asio::co_spawn(control, failures(), asio::use_future).get();
        auto live = asio::co_spawn(control, nodes[0]->async_open_flow({"a", "b", "d"}, RelayProtocol::Tcp),
                                   asio::use_future).get();
        require_flow(bool(live), "shutdown fixture did not become Ready");
        auto receiver = asio::co_spawn(control, nodes[1]->async_receive_flow(live.epoch, live.id), asio::use_future);
        auto pending =
            asio::co_spawn(control, nodes[0]->async_open_flow({"a", "b", "d"}, RelayProtocol::Udp), asio::use_future);
        nodes[0]->stop();
        const auto stopped = pending.get();
        require_flow(bool(stopped) || !stopped.reason.empty(), "stop left open unresolved");
        require_flow(receiver.wait_for(3s) == std::future_status::ready, "master stop retained a remote receiver");
        bool invalidated = false;
        try
        {
            static_cast<void>(receiver.get());
        }
        catch (const std::exception &)
        {
            invalidated = true;
        }
        require_flow(invalidated, "master stop left a remote Flow active");
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
        flow_integration();
        std::cout << "[PASS] NodeLinkMgr flows and TCP/UDP hop forwarding\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
