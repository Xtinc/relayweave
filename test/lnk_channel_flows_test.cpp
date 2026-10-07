#include "lnk_channel.h"
#include <future>
#include <iostream>
using namespace std::chrono_literals;
void check_channel(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}
asio::awaitable<void> pause_channel(std::chrono::milliseconds duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(duration);
    co_await timer.async_wait(asio::use_awaitable);
}
int main()
{
    try
    {
        asio::io_context io(1);
        auto guard = asio::make_work_guard(io);
        asio::ip::tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
        auto tp = probe.local_endpoint().port();
        probe.close();
        asio::ip::udp::socket udp_probe(io, {asio::ip::address_v4::loopback(), 0});
        auto up = udp_probe.local_endpoint().port();
        udp_probe.close();
        auto a = std::make_shared<LnkChannel>(io.get_executor(), "a", "127.0.0.1", tp, "127.0.0.1", up);
        auto b = std::make_shared<LnkChannel>(io.get_executor(), "b", "127.0.0.2", tp, "127.0.0.2", up);
        std::vector<CtrlMessage> events;
        std::future<FlowFrame> stopped_receiver;
        auto link_ready = [&] {
            const auto ready = std::ranges::count_if(events, [](const auto &message) {
                return message.type() == CtrlCommand::LinkReady && message.params->at("id") == 101;
            });
            const auto failed = std::ranges::any_of(events, [](const auto &message) {
                return (message.type() == CtrlCommand::LinkError || message.type() == CtrlCommand::LinkClosed) &&
                       message.params->at("id") == 101;
            });
            return ready == 2 && !failed;
        };
        a->start();
        b->start();
        a->activate();
        b->activate();
        auto params = [&](std::uint64_t id, int ttl) {
            return njson{{"epoch", std::uint64_t(7)},
                         {"flow_id", id},
                         {"request_id", id},
                         {"master", "a"},
                         {"path", std::vector<std::string>{"a", "b"}},
                         {"addresses", std::vector<std::string>{"127.0.0.1", "127.0.0.2"}},
                         {"links", std::vector<std::uint64_t>{101}},
                         {"transport", "tcp"},
                         {"ttl_ms", unsigned(ttl)}};
        };
        auto install = [&](std::uint64_t id) {
            auto p = params(id, 10000);
            a->prepare_flow(p);
            b->prepare_flow(p);
            a->commit_flow(id);
            b->commit_flow(id);
        };
        auto scenario = [&]() -> asio::awaitable<void> {
            njson link{{"epoch", std::uint64_t(7)},
                       {"id", std::uint64_t(101)},
                       {"left", "a"},
                       {"right", "b"},
                       {"transport", "tcp"},
                       {"tcp_port", tp},
                       {"udp_port", up},
                       {"token", "test-credential"},
                       {"peer", "b"},
                       {"peer_address", "127.0.0.2"}};
            a->prepare(link);
            link["peer"] = "a";
            link["peer_address"] = "127.0.0.1";
            b->prepare(link);
            a->connect(101);
            b->connect(101);
            for (int i = 0; i < 200 && !link_ready(); ++i)
            {
                co_await pause_channel(5ms);
            }
            check_channel(link_ready(), "standalone NodeLink not Ready");
            auto p = params(201, 1);
            a->prepare_flow(p);
            b->prepare_flow(p);
            co_await pause_channel(150ms);
            a->commit_flow(201);
            b->commit_flow(201);
            check_channel(a->send_flow({7, 201, false, LnkFrType::Data, {1}, {}}).status == FlowSendStatus::Closed,
                          "expired preparation revived");
            a->prepare_flow(p);
            co_await pause_channel(5ms);
            check_channel(events.back().type() == CtrlCommand::FlowError, "retired prepare not rejected");
            install(202);
            check_channel(a->send_flow({7, 202, true, LnkFrType::Data, {99}, {}}).status == FlowSendStatus::Invalid,
                          "wrong injection direction accepted");
            check_channel(a->send_flow({7, 999, false, LnkFrType::Data, {98}, {}}).status == FlowSendStatus::Closed,
                          "unknown flow accepted");
            check_channel(a->send_flow({8, 202, false, LnkFrType::Data, {97}, {}}).status == FlowSendStatus::Closed,
                          "stale epoch accepted");
            for (auto frame : {FlowFrame{7, 202, false, LnkFrType::Data, BytesBuf(4097), {}},
                               FlowFrame{7, 202, false, LnkFrType::Data, {1}, "invalid reason"},
                               FlowFrame{7, 202, false, LnkFrType::Fin, {1}, {}},
                               FlowFrame{7, 202, false, LnkFrType::Reset, {}, std::string(513, 'x')},
                               FlowFrame{7, 202, false, LnkFrType::Ping, {}, {}}})
            {
                const auto payload = frame.payload;
                auto result = a->send_flow(std::move(frame));
                check_channel(result.status == FlowSendStatus::Invalid && result.unsent.payload == payload,
                              "local format validation lost or admitted invalid data");
            }
            check_channel(a->send_flow({7, 202, false, LnkFrType::Data, {2}, {}}).status == FlowSendStatus::Queued,
                          "valid frame not queued");
            auto f = co_await b->receive_flow(7, 202);
            check_channel(f.payload == BytesBuf{2}, "wrong incoming identity delivered");
            a->close_flow(202);
            b->close_flow(202);
            b->prepare_flow(params(202, 10000));
            b->commit_flow(202);
            check_channel(b->send_flow({7, 202, true, LnkFrType::Data, {3}, {}}).status == FlowSendStatus::Closed,
                          "closed flow revived");
            install(203);
            check_channel(a->send_flow({7, 203, false, LnkFrType::Fin, {}, {}}).status == FlowSendStatus::Queued,
                          "FIN rejected");
            f = co_await b->receive_flow(7, 203);
            check_channel(f.kind == LnkFrType::Fin, "FIN lost");
            check_channel(a->send_flow({7, 203, false, LnkFrType::Reset, {}, "reset after FIN"}).status ==
                              FlowSendStatus::Queued,
                          "RESET after FIN rejected");
            co_await pause_channel(20ms);
            check_channel(b->send_flow({7, 203, true, LnkFrType::Data, {4}, {}}).status == FlowSendStatus::Closed,
                          "RESET left reverse direction alive");
            install(204);
            FlowSendResult full;
            // No executor yield: fill exactly the transport admission queue before its writer runs.
            for (int i = 0; i < 110; ++i)
            {
                full = a->send_flow({7, 204, false, LnkFrType::Data, {5}, {}});
                if (full.status != FlowSendStatus::Queued)
                {
                    break;
                }
            }
            check_channel(full.status == FlowSendStatus::WouldBlock && full.unsent.payload == BytesBuf{5},
                          "bounded admission did not return intact payload");
            check_channel(link_ready(), "flow congestion closed physical NodeLink");
            a->close_flow(204);
            b->close_flow(204);
            co_await pause_channel(100ms);
            install(205);
            bool cancelled = false;
            asio::steady_timer received(io);
            received.expires_at(std::chrono::steady_clock::time_point::max());
            asio::co_spawn(io, b->receive_flow(7, 205), [&](std::exception_ptr error, FlowFrame) {
                cancelled = bool(error);
                received.cancel();
            });
            // Committed Flows stay active while the owning control connection and NodeLinks remain valid.
            co_await pause_channel(20500ms);
            check_channel(!cancelled, "idle committed flow expired");
            check_channel(link_ready(), "idle flow closed shared NodeLink");
            a->invalidate_flows("cluster control disconnected");
            b->invalidate_flows("cluster control disconnected");
            if (!cancelled)
            {
                co_await received.async_wait(use_nothrow_awaitable);
            }
            check_channel(cancelled, "control disconnect retained pending receiver");
            check_channel(link_ready(), "control failure closed shared NodeLink");
            a->commit_flow(205);
            b->commit_flow(205);
            check_channel(a->send_flow({7, 205, false, LnkFrType::Data, {6}, {}}).status == FlowSendStatus::Closed,
                          "invalidated flow revived by commit");
            install(206);
            check_channel(a->send_flow({7, 206, false, LnkFrType::Data, {7}, {}}).status == FlowSendStatus::Queued,
                          "fresh identity failed");
            f = co_await b->receive_flow(7, 206);
            check_channel(f.payload == BytesBuf{7}, "fresh identity received old data");
            stopped_receiver = asio::co_spawn(io, a->receive_flow(7, 206), asio::use_future);
        };
        auto observe = [&](std::shared_ptr<LnkChannel> channel) -> asio::awaitable<void> {
            try
            {
                for (;;)
                {
                    events.push_back(co_await channel->receive_event());
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
        auto observed_a = asio::co_spawn(io, observe(a), asio::use_future);
        auto observed_b = asio::co_spawn(io, observe(b), asio::use_future);
        std::thread thread([&] { io.run(); });
        std::exception_ptr failure;
        try
        {
            asio::co_spawn(io, scenario(), asio::use_future).get();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        auto stop = [&]() -> asio::awaitable<void> {
            co_await a->stop();
            co_await b->stop();
        };
        asio::co_spawn(io, stop(), asio::use_future).get();
        guard.reset();
        thread.join();
        observed_a.get();
        observed_b.get();
        bool stopped = false;
        if (failure)
        {
            std::rethrow_exception(failure);
        }
        try
        {
            stopped_receiver.get();
        }
        catch (const std::runtime_error &e)
        {
            stopped = std::string(e.what()).find("node stopping") != std::string::npos;
        }
        check_channel(stopped, "stop did not release the business flow receiver");
        check_channel(io.stopped(), "channel shutdown retained tasks");
        std::cout << "[PASS] forwarding identities, FIN/RESET, bounded admission and control cleanup\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
