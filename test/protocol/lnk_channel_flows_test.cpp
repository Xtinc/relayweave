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
        std::future<std::optional<BytesBuf>> stopped_receiver;
        std::optional<BytesBuf> retained_payload;
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
        auto submit = [&](BytesBuf payload) -> asio::awaitable<void> {
            check_channel(submit_io.get_executor().running_in_this_thread(), "wrong submission executor");
            check_channel(co_await test_flow::send_data(a, 7, 202, false, std::move(payload)), "stream send failed");
            check_channel(submit_io.get_executor().running_in_this_thread(), "send resumed on the data executor");
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
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(a, 7, 201, false)),
                          "expired preparation revived");
            test_flow::prepare(a, p);
            co_await pause_channel(5ms);
            check_channel(events.back().type() == CtrlCommand::FlowError, "retired prepare not rejected");
            install(202);
            check_channel(co_await test_flow::throws<std::invalid_argument>(test_flow::port(a, 7, 202, true)),
                          "wrong injection direction accepted");
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(a, 7, 999, false)),
                          "unknown flow accepted");
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(a, 8, 202, false)),
                          "stale epoch accepted");
            const BytesBuf oversized(LnkFrameHeader::maximum_payload + 1, 0xff);
            check_channel(co_await test_flow::throws<std::invalid_argument>(
                              test_flow::send_data(a, 7, 202, false, oversized)),
                          "stream admitted oversized data");
            check_channel(co_await test_flow::send_data(a, 7, 202, false, {2}), "valid data not sent");
            check_channel(io.get_executor().running_in_this_thread(), "same-executor send changed executor");
            auto f = co_await test_flow::receive(b, 7, 202);
            check_channel(f == BytesBuf{2}, "wrong incoming identity delivered");
            for (auto size : {std::size_t{}, std::size_t{4097}, LnkFrameHeader::maximum_payload})
            {
                BytesBuf payload(size, 0xa5);
                const auto expected = payload;
                co_await asio::co_spawn(submit_io, submit(payload), asio::use_awaitable);
                std::ranges::fill(payload, 0x5a); // Reuse immediately after submission completes.
                f = co_await test_flow::receive(b, 7, 202);
                check_channel(f == expected, "cross-executor send retained or changed the caller's bytes");
            }
            check_channel(co_await asio::co_spawn(submit_io,
                              test_flow::throws<std::runtime_error>(test_flow::port(a, 8, 202, false)),
                              asio::use_awaitable), "cross-executor stale epoch accepted");
            a->close_flow(202);
            b->close_flow(202);
            test_flow::prepare(b, params(202, 10000));
            b->commit_flow(202);
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(b, 7, 202, true)),
                          "closed flow revived");
            install(203);
            check_channel(test_flow::try_finish(a, 203, false),
                          "FIN rejected");
            f = co_await test_flow::receive(b, 7, 203);
            check_channel(!f, "FIN lost");
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::send_data(a, 7, 203, false, {})),
                          "DATA accepted after FIN");
            co_await test_flow::reset(a, 7, 203, false, "reset after FIN");
            co_await pause_channel(20ms);
            // FIN ended the receive loop; RESET must still close the Flow and its reverse direction.
            check_channel(std::ranges::any_of(events, [](const auto &event) {
                return event.type() == CtrlCommand::FlowClosed && event.params->at("flow_id") == 203 &&
                       event.params->at("reason") == "reset after FIN";
            }), "RESET after FIN did not preserve close reason");
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(b, 7, 203, true)),
                          "RESET left reverse direction alive");

            // Keep many short responses buffered together, as with a browser opening parallel connections.
            for (std::uint64_t id = 400; id < 448; ++id)
            {
                install(id);
                for (std::size_t index = 0; index < test_flow::window_bytes / (8192 + lnk::Frame::buffer_overhead); ++index)
                {
                    BytesBuf payload(8192, static_cast<std::uint8_t>(id));
                    payload.front() = static_cast<std::uint8_t>(index);
                    check_channel(test_flow::try_send_data(a, id, false, std::move(payload)), "parallel response burst was rejected at sender");
                }
                check_channel(test_flow::try_finish(a, id, false),
                              "parallel response FIN was rejected");
            }
            for (std::uint64_t id = 400; id < 448; ++id)
            {
                for (std::size_t index = 0; index < test_flow::window_bytes / (8192 + lnk::Frame::buffer_overhead); ++index)
                {
                    auto frame = co_await test_flow::receive(b, 7, id);
                    BytesBuf expected(8192, static_cast<std::uint8_t>(id));
                    expected.front() = static_cast<std::uint8_t>(index);
                    check_channel(frame == expected,
                                  "parallel response burst lost or reordered bytes");
                }
                check_channel(!(co_await test_flow::receive(b, 7, id)),
                              "parallel response burst lost FIN");
                check_channel(test_flow::try_send_data(b, id, true, {2}) &&
                              test_flow::try_finish(b, id, true),
                              "parallel response burst closed the reverse direction");
                check_channel((co_await test_flow::receive(a, 7, id)) == BytesBuf{2} &&
                              !(co_await test_flow::receive(a, 7, id)),
                              "parallel response burst lost reverse data or FIN");
                a->close_flow(id);
                b->close_flow(id);
            }
            check_channel(link_ready(), "parallel response bursts closed the shared NodeLink");

            install(207);
            install(208);
            for (int i = 0; i < 128; ++i)
            {
                check_channel(test_flow::try_send_data(a, 207, false, {9}),
                              "could not buffer DATA at the endpoint");
            }
            // Another Flow can overtake these frames. Wait for this Flow's actual admission.
            const auto buffered = test_access::FlowProtocol_flows_(*b).at(207);
            const auto expected = 128 * lnk::Frame::charge_for_payload(1);
            const auto receive_deadline = lnk::Clock::now() + 1s;
            while (buffered->stream->directions[0].received_bytes < expected && lnk::Clock::now() < receive_deadline)
            {
                co_await pause_channel(1ms);
            }
            check_channel(buffered->stream->directions[0].received_bytes == expected,
                          "DATA did not reach the endpoint before RESET");
            co_await test_flow::reset(a, 7, 207, false, "reset with buffered receive queue");
            check_channel(test_flow::try_send_data(a, 208, false, {11}),
                          "healthy Flow was rejected after RESET");
            check_channel((co_await test_flow::receive(b, 7, 208)) == BytesBuf{11},
                          "healthy Flow lost data after RESET");
            const auto reset_errors = [&] {
                return std::ranges::count_if(events, [](const auto &message) {
                    return message.type() == CtrlCommand::FlowError && message.params->at("flow_id") == 207 &&
                           message.params->at("reason") == "reset with buffered receive queue";
                });
            };
            const auto reset_deadline = lnk::Clock::now() + 1s;
            while (reset_errors() < 2 && lnk::Clock::now() < reset_deadline)
            {
                co_await pause_channel(1ms);
            }
            check_channel(reset_errors() == 2,
                          "endpoint queue capacity replaced RESET reason");
            check_channel(link_ready(), "RESET closed shared NodeLink");
            a->close_flow(208);
            b->close_flow(208);
            install(204);
            bool full = false;
            // No executor yield: fill exactly the transport admission queue before its writer runs.
            for (int i = 0; i < 10000; ++i)
            {
                full = !test_flow::try_send_data(a, 204, false, {5});
                if (full)
                {
                    break;
                }
            }
            check_channel(full, "bounded admission did not suspend the frame");
            check_channel(!test_flow::try_send_data(a, 204, false, {6}),
                          "capacity exhaustion closed the flow");
            check_channel(link_ready(), "flow congestion closed physical NodeLink");
            a->close_flow(204);
            b->close_flow(204);
            co_await pause_channel(100ms);
            install(205);
            bool cancelled = false;
            asio::steady_timer received(io);
            received.expires_at(std::chrono::steady_clock::time_point::max());
            asio::co_spawn(io, test_flow::receive(b, 7, 205), [&](std::exception_ptr error, std::optional<BytesBuf>) {
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
            check_channel(co_await test_flow::throws<std::runtime_error>(test_flow::port(a, 7, 205, false)),
                          "invalidated flow revived by commit");
            install(206);
            check_channel(test_flow::try_send_data(a, 206, false, {7}),
                          "fresh identity failed");
            f = co_await test_flow::receive(b, 7, 206);
            check_channel(f == BytesBuf{7}, "fresh identity received old data");
            retained_payload = std::move(f);
            check_channel(test_flow::try_send_data(a, 206, false, {8}),
                          "could not queue a pooled frame before shutdown");
            co_await pause_channel(20ms);
            stopped_receiver = asio::co_spawn(io, test_flow::receive(a, 7, 206), asio::use_future);
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
            check_channel(co_await a->stop(), "first data service shutdown failed");
            check_channel(co_await b->stop(), "second data service shutdown failed");
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
        check_channel(retained_payload == BytesBuf{7}, "copied payload changed after protocol shutdown");
        std::cout << "[PASS] forwarding identities, FIN/RESET, bounded admission and control cleanup\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
