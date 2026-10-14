#include "flow_test_io.h"
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

asio::awaitable<FlowFrame> receive_consumed(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch, std::uint64_t id)
{
    co_return co_await test_flow::receive(channel, epoch, id);
}

int main()
{
    try
    {
        asio::io_context io(1);
        auto guard = asio::make_work_guard(io);
        asio::io_context submit_io(1);
        auto submit_guard = asio::make_work_guard(submit_io);
        asio::ip::tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
        auto tp = probe.local_endpoint().port();
        probe.close();
        asio::ip::udp::socket udp_probe(io, {asio::ip::address_v4::loopback(), 0});
        auto up = udp_probe.local_endpoint().port();
        udp_probe.close();
        auto a = std::make_shared<FlowProtocol>(io.get_executor(), io.get_executor(), "a", "127.0.0.1", tp, "127.0.0.1", up);
        auto b = std::make_shared<FlowProtocol>(io.get_executor(), io.get_executor(), "b", "127.0.0.2", tp, "127.0.0.2", up);
        std::vector<CtrlMessage> events;
        std::future<FlowFrame> stopped_receiver;
        FlowFrame retained_frame;
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
        a->links().start();
        b->links().start();
        a->activate();
        a->activate(); // Repeated activation must preserve a single set of data tasks.
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
            test_flow::prepare(a, p);
            test_flow::prepare(b, p);
            a->commit_flow(id);
            b->commit_flow(id);
        };
        auto submit = [&](FlowFrame frame) -> asio::awaitable<FlowSendStatus> {
            check_channel(submit_io.get_executor().running_in_this_thread(), "wrong submission executor");
            const auto status = co_await test_flow::submit(a, std::move(frame));
            check_channel(submit_io.get_executor().running_in_this_thread(), "send resumed on the data executor");
            co_return status;
        };
        auto submit_data = [&](std::uint64_t epoch, std::uint64_t id, bool reverse,
                               std::span<const std::uint8_t> payload) -> asio::awaitable<FlowSendStatus> {
            check_channel(submit_io.get_executor().running_in_this_thread(), "wrong borrowed submission executor");
            const auto status = co_await test_flow::submit_data(a, epoch, id, reverse, payload);
            check_channel(submit_io.get_executor().running_in_this_thread(), "borrowed send resumed on the data executor");
            co_return status;
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
            test_flow::prepare_link(a, link);
            link["peer"] = "a";
            link["peer_address"] = "127.0.0.1";
            test_flow::prepare_link(b, link);
            a->links().connect(101);
            b->links().connect(101);
            for (int i = 0; i < 200 && !link_ready(); ++i)
            {
                co_await pause_channel(5ms);
            }
            check_channel(link_ready(), "standalone NodeLink not Ready");
            auto p = params(201, 1);
            test_flow::prepare(a, p);
            test_flow::prepare(b, p);
            co_await pause_channel(150ms);
            check_channel(std::ranges::count_if(events,
                                               [](const auto &message) {
                                                   return message.type() == CtrlCommand::FlowError &&
                                                          message.params->at("flow_id") == 201 &&
                                                          message.params->at("stage") == "prepare";
                                               }) == 2,
                          "short preparation deadline did not wake the heartbeat monitor");
            a->commit_flow(201);
            b->commit_flow(201);
            check_channel(test_flow::send(a, {7, 201, false, LnkFrType::Data, {1}, {}}) == FlowSendStatus::Closed,
                          "expired preparation revived");
            test_flow::prepare(a, p);
            co_await pause_channel(5ms);
            check_channel(events.back().type() == CtrlCommand::FlowError, "retired prepare not rejected");
            install(202);
            check_channel(test_flow::send(a, {7, 202, true, LnkFrType::Data, {99}, {}}) == FlowSendStatus::Invalid,
                          "wrong injection direction accepted");
            check_channel(test_flow::send(a, {7, 999, false, LnkFrType::Data, {98}, {}}) == FlowSendStatus::Closed,
                          "unknown flow accepted");
            check_channel(test_flow::send(a, {8, 202, false, LnkFrType::Data, {97}, {}}) == FlowSendStatus::Closed,
                          "stale epoch accepted");
            for (auto frame : {FlowFrame{7, 202, false, LnkFrType::Data,
                                         BytesBuf(LnkFrameHeader::maximum_payload + 1), {}},
                               FlowFrame{7, 202, false, LnkFrType::Data, {1}, "invalid reason"},
                               FlowFrame{7, 202, false, LnkFrType::Fin, {1}, {}},
                               FlowFrame{7, 202, false, LnkFrType::Reset, {}, std::string(513, 'x')},
                               FlowFrame{7, 202, false, LnkFrType::Ping, {}, {}}})
            {
                auto result = co_await test_flow::submit(a, std::move(frame));
                check_channel(result == FlowSendStatus::Invalid, "local format validation admitted invalid data");
            }
            const BytesBuf oversized(LnkFrameHeader::maximum_payload + 1, 0xff);
            check_channel(co_await test_flow::submit_data(a, 7, 202, false, oversized) == FlowSendStatus::Invalid,
                          "borrowed submission admitted oversized data");
            check_channel(co_await test_flow::submit_data(a, 7, 202, true, {}) == FlowSendStatus::Invalid,
                          "borrowed submission admitted wrong direction");
            check_channel(co_await test_flow::submit_data(a, 8, 202, false, {}) == FlowSendStatus::Closed,
                          "borrowed submission admitted stale epoch");
            check_channel(co_await test_flow::submit_data(a, 7, 999, false, {}) == FlowSendStatus::Closed,
                          "borrowed submission admitted unknown flow");
            FlowFrame valid{7, 202, false, LnkFrType::Data, {2}, {}};
            check_channel(co_await test_flow::submit(a, std::move(valid)) == FlowSendStatus::Queued,
                          "valid frame not queued");
            check_channel(io.get_executor().running_in_this_thread(), "same-executor send changed executor");
            auto f = co_await receive_consumed(b, 7, 202);
            check_channel(f.payload == BytesBuf{2}, "wrong incoming identity delivered");
            for (auto size : {std::size_t{}, std::size_t{4097}, LnkFrameHeader::maximum_payload})
            {
                const BytesBuf payload(size, 0xa5);
                FlowFrame frame{7, 202, false, LnkFrType::Data, payload, {}};
                const auto status = co_await asio::co_spawn(submit_io, submit(std::move(frame)), asio::use_awaitable);
                check_channel(status == FlowSendStatus::Queued, "cross-executor frame not queued");
                f = co_await receive_consumed(b, 7, 202);
                check_channel(f.payload == payload, "cross-executor payload changed");
                BytesBuf borrowed = payload;
                check_channel(co_await asio::co_spawn(submit_io, submit_data(7, 202, false, borrowed),
                                                      asio::use_awaitable) == FlowSendStatus::Queued,
                              "borrowed cross-executor frame not queued");
                std::ranges::fill(borrowed, 0x5a); // Reuse immediately after submission completes.
                f = co_await receive_consumed(b, 7, 202);
                check_channel(f.payload == payload, "queued frame retained the caller's borrowed buffer");
            }
            FlowFrame stale{8, 202, false, LnkFrType::Data};
            check_channel(co_await asio::co_spawn(submit_io, submit(std::move(stale)), asio::use_awaitable) ==
                              FlowSendStatus::Closed,
                          "cross-executor stale epoch accepted");
            a->close_flow(202);
            b->close_flow(202);
            test_flow::prepare(b, params(202, 10000));
            b->commit_flow(202);
            check_channel(test_flow::send(b, {7, 202, true, LnkFrType::Data, {3}, {}}) == FlowSendStatus::Closed,
                          "closed flow revived");
            install(203);
            check_channel(test_flow::send(a, {7, 203, false, LnkFrType::Fin, {}, {}}) == FlowSendStatus::Queued,
                          "FIN rejected");
            f = co_await receive_consumed(b, 7, 203);
            check_channel(f.kind == LnkFrType::Fin, "FIN lost");
            check_channel(co_await test_flow::submit_data(a, 7, 203, false, {}) == FlowSendStatus::Invalid,
                          "borrowed DATA accepted after FIN");
            check_channel(test_flow::send(a, {7, 203, false, LnkFrType::Reset, {}, "reset after FIN"}) == FlowSendStatus::Queued,
                          "RESET after FIN rejected");
            co_await pause_channel(20ms);
            // FIN ended the receive loop; RESET must still close the Flow and its reverse direction.
            check_channel(std::ranges::any_of(events, [](const auto &event) {
                return event.type() == CtrlCommand::FlowClosed && event.params->at("flow_id") == 203 &&
                       event.params->at("reason") == "reset after FIN";
            }), "RESET after FIN did not preserve close reason");
            check_channel(test_flow::send(b, {7, 203, true, LnkFrType::Data, {4}, {}}) == FlowSendStatus::Closed,
                          "RESET left reverse direction alive");

            // Keep many short responses buffered together, as with a browser opening parallel connections.
            install(300);
            for (std::uint64_t id = 400; id < 448; ++id)
            {
                install(id);
                for (int index = 0; index < 48; ++index)
                {
                    BytesBuf payload(8192, static_cast<std::uint8_t>(id));
                    payload.front() = static_cast<std::uint8_t>(index);
                    check_channel(test_flow::send(a, {7, id, false, LnkFrType::Data, std::move(payload), {}}) ==
                                      FlowSendStatus::Queued, "parallel response burst was rejected at sender");
                }
                check_channel(test_flow::send(a, {7, id, false, LnkFrType::Fin, {}, {}}) == FlowSendStatus::Queued,
                              "parallel response FIN was rejected");
                // The shared FIFO marker proves this response is buffered before submitting the next one.
                check_channel(test_flow::send(a, {7, 300, false, LnkFrType::Data, {1}, {}}) == FlowSendStatus::Queued,
                              "parallel response marker was rejected");
                static_cast<void>(co_await receive_consumed(b, 7, 300));
            }
            for (std::uint64_t id = 400; id < 448; ++id)
            {
                for (int index = 0; index < 48; ++index)
                {
                    auto frame = co_await receive_consumed(b, 7, id);
                    BytesBuf expected(8192, static_cast<std::uint8_t>(id));
                    expected.front() = static_cast<std::uint8_t>(index);
                    check_channel(frame.kind == LnkFrType::Data && frame.payload == expected,
                                  "parallel response burst lost or reordered bytes");
                }
                check_channel((co_await receive_consumed(b, 7, id)).kind == LnkFrType::Fin,
                              "parallel response burst lost FIN");
                check_channel(test_flow::send(b, {7, id, true, LnkFrType::Data, {2}, {}}) == FlowSendStatus::Queued &&
                              test_flow::send(b, {7, id, true, LnkFrType::Fin, {}, {}}) == FlowSendStatus::Queued,
                              "parallel response burst closed the reverse direction");
                check_channel((co_await receive_consumed(a, 7, id)).payload == BytesBuf{2} &&
                              (co_await receive_consumed(a, 7, id)).kind == LnkFrType::Fin,
                              "parallel response burst lost reverse data or FIN");
                a->close_flow(id);
                b->close_flow(id);
            }
            a->close_flow(300);
            b->close_flow(300);
            check_channel(link_ready(), "parallel response bursts closed the shared NodeLink");

            install(207);
            install(208);
            for (int i = 0; i < 128; ++i)
            {
                check_channel(test_flow::send(a, {7, 207, false, LnkFrType::Data, {9}, {}}) == FlowSendStatus::Queued,
                              "could not fill endpoint receive queue");
            }
            // The marker uses the same TCP FIFO; receiving it proves all preceding frames reached the endpoint.
            check_channel(test_flow::send(a, {7, 208, false, LnkFrType::Data, {10}, {}}) == FlowSendStatus::Queued,
                          "full-queue marker rejected");
            static_cast<void>(co_await receive_consumed(b, 7, 208));
            check_channel(test_flow::send(a, {7, 207, false, LnkFrType::Reset, {}, "reset with full receive queue"}) ==
                              FlowSendStatus::Queued,
                          "RESET rejected with full endpoint queue");
            check_channel(test_flow::send(a, {7, 208, false, LnkFrType::Data, {11}, {}}) == FlowSendStatus::Queued,
                          "RESET marker rejected");
            static_cast<void>(co_await receive_consumed(b, 7, 208));
            co_await pause_channel(5ms);
            check_channel(std::ranges::count_if(events,
                                               [](const auto &message) {
                                                   return message.type() == CtrlCommand::FlowError &&
                                                          message.params->at("flow_id") == 207 &&
                                                          message.params->at("reason") == "reset with full receive queue";
                                               }) == 2,
                          "endpoint queue capacity replaced RESET reason");
            check_channel(link_ready(), "RESET closed shared NodeLink");
            a->close_flow(208);
            b->close_flow(208);
            install(204);
            FlowSendStatus full = FlowSendStatus::Queued;
            // No executor yield: fill exactly the transport admission queue before its writer runs.
            for (int i = 0; i < 10000; ++i)
            {
                full = test_flow::send(a, {7, 204, false, LnkFrType::Data, {5}, {}});
                if (full != FlowSendStatus::Queued)
                {
                    break;
                }
            }
            check_channel(full == FlowSendStatus::WouldBlock, "bounded admission did not suspend the frame");
            check_channel(test_flow::send(a, {7, 204, false, LnkFrType::Data, {6}, {}}) == FlowSendStatus::WouldBlock,
                          "capacity exhaustion closed the flow");
            check_channel(link_ready(), "flow congestion closed physical NodeLink");
            a->close_flow(204);
            b->close_flow(204);
            co_await pause_channel(100ms);
            install(205);
            bool cancelled = false;
            asio::steady_timer received(io);
            received.expires_at(std::chrono::steady_clock::time_point::max());
            asio::co_spawn(io, receive_consumed(b, 7, 205), [&](std::exception_ptr error, FlowFrame) {
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
            check_channel(test_flow::send(a, {7, 205, false, LnkFrType::Data, {6}, {}}) == FlowSendStatus::Closed,
                          "invalidated flow revived by commit");
            install(206);
            check_channel(test_flow::send(a, {7, 206, false, LnkFrType::Data, {7}, {}}) == FlowSendStatus::Queued,
                          "fresh identity failed");
            f = co_await receive_consumed(b, 7, 206);
            check_channel(f.payload == BytesBuf{7}, "fresh identity received old data");
            retained_frame = std::move(f);
            check_channel(test_flow::send(a, {7, 206, false, LnkFrType::Data, {8}, {}}) == FlowSendStatus::Queued,
                          "could not queue a pooled frame before shutdown");
            co_await pause_channel(20ms);
            stopped_receiver = asio::co_spawn(io, receive_consumed(a, 7, 206), asio::use_future);
        };
        auto observe = [&](std::shared_ptr<FlowProtocol> channel) -> asio::awaitable<void> {
            try
            {
                for (;;)
                {
                    events.push_back(co_await test_flow::event(channel));
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
        std::thread submit_thread([&] { submit_io.run(); });
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
        submit_guard.reset();
        submit_thread.join();
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
        const std::weak_ptr<FlowProtocol> released_a = a;
        const std::weak_ptr<FlowProtocol> released_b = b;
        a.reset();
        b.reset();
        check_channel(released_a.expired() && released_b.expired(), "shutdown retained a payload pool owner");
        check_channel(retained_frame.payload == BytesBuf{7}, "public payload depended on a destroyed internal pool");
        std::cout << "[PASS] forwarding identities, FIN/RESET, bounded admission and control cleanup\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
