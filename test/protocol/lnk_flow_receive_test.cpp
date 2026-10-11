#include "lnk_channel.h"
#include <future>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

static void check(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

// Separate, manually pumped executors make receive/close/cancel ordering deterministic.
struct Fixture
{
    asio::io_context caller{1}, data{1};
    std::shared_ptr<LnkChannel> channel =
        std::make_shared<LnkChannel>(data.get_executor(), caller.get_executor(), "a", "127.0.0.1", 0, "127.0.0.1", 0);

    Fixture()
    {
        asio::post(data, [this] {
            channel->prepare_flow(njson{{"epoch", 7}, {"flow_id", 101}, {"path", std::vector<std::string>{"a"}},
                                        {"links", std::vector<std::uint64_t>{}}, {"transport", "tcp"}, {"ttl_ms", 10000}});
            channel->commit_flow(101);
        });
        pump_data();
    }

    void pump_data()
    {
        data.restart();
        data.poll();
    }

    void pump_caller()
    {
        caller.restart();
        caller.poll();
    }

    std::future<FlowFrame> receive(asio::cancellation_slot slot = {}, std::uint64_t epoch = 7)
    {
        return asio::co_spawn(caller, [this, epoch]() -> asio::awaitable<FlowFrame> {
            check(caller.get_executor().running_in_this_thread(), "receive did not start on caller executor");
            try
            {
                auto frame = co_await channel->receive_flow(epoch, 101);
                check(caller.get_executor().running_in_this_thread(), "receive resumed on data executor");
                co_return frame;
            }
            catch (...)
            {
                check(caller.get_executor().running_in_this_thread(), "receive failure resumed on data executor");
                throw;
            }
        }, asio::bind_cancellation_slot(slot, asio::use_future));
    }

    void send(LnkFrType kind = LnkFrType::Data, bool reverse = false)
    {
        asio::post(data, [this, kind, reverse] {
            check(channel->send_flow({7, 101, reverse, kind, kind == LnkFrType::Data ? BytesBuf{1, 2, 3} : BytesBuf{}}) ==
                      FlowSendStatus::Queued, "could not enqueue frame");
        });
        pump_data();
    }

    std::future<CtrlMessage> receive_event(asio::cancellation_slot slot = {})
    {
        return asio::co_spawn(caller, [this]() -> asio::awaitable<CtrlMessage> {
            try
            {
                auto message = co_await channel->receive_event();
                check(caller.get_executor().running_in_this_thread(), "event resumed on data executor");
                co_return message;
            }
            catch (...)
            {
                check(caller.get_executor().running_in_this_thread(), "event failure resumed on data executor");
                throw;
            }
        }, asio::bind_cancellation_slot(slot, asio::use_future));
    }

    ~Fixture()
    {
        asio::post(data, [channel = std::move(channel)] {
            if (channel)
            {
                channel->close_flow(101);
            }
        });
        pump_data();
        pump_caller();
    }
};

template <typename Result>
static void expect_failure(std::future<Result> &result, const char *reason = nullptr)
{
    check(result.wait_for(0s) == std::future_status::ready, "pending receive was not completed");
    try
    {
        static_cast<void>(result.get());
    }
    catch (const std::exception &error)
    {
        check(!reason || std::string(error.what()) == reason, "receive lost error reason");
        return;
    }
    throw std::runtime_error("receive unexpectedly succeeded");
}

int main()
{
    try
    {
        {
            asio::cancellation_signal cancellation;
            Fixture f;
            for (auto expected : {CtrlCommand::FlowPrepared, CtrlCommand::FlowCommitted})
            {
                auto event = f.receive_event();
                f.pump_caller();
                check(event.wait_for(0s) == std::future_status::ready, "buffered event required data executor");
                check(event.get().type() == expected, "buffered events were reordered");
            }
            auto cancelled = f.receive_event(cancellation.slot());
            f.pump_caller();
            check(cancelled.wait_for(0s) != std::future_status::ready, "empty event receive did not wait");
            cancellation.emit(asio::cancellation_type::all);
            f.pump_caller();
            expect_failure(cancelled);

            auto next = f.receive_event();
            f.pump_caller();
            asio::post(f.data, [&] { f.channel->close_flow(101, "event after cancellation"); });
            f.pump_data();
            check(next.wait_for(0s) != std::future_status::ready, "data executor completed event caller inline");
            f.pump_caller();
            check(next.get().type() == CtrlCommand::FlowClosed, "event cancellation closed the queue");

            auto pending = f.receive_event();
            f.pump_caller();
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump_data();
            f.pump_caller();
            stopped.get();
            expect_failure(pending);
        }
        {
            Fixture f;
            // A failed prepare emits one event without adding a live flow.
            asio::post(f.data, [&] {
                for (std::uint64_t id = 1000; id < 1000 + 4097; ++id)
                {
                    f.channel->prepare_flow(njson{{"epoch", 7}, {"flow_id", id},
                                                  {"path", std::vector<std::string>{"a", "b"}},
                                                  {"links", std::vector<std::uint64_t>{1}},
                                                  {"transport", "tcp"}, {"ttl_ms", 10000}});
                }
            });
            f.pump_data();
            // Overflow takes priority over buffered notifications and remains observable.
            for (int i = 0; i < 2; ++i)
            {
                auto event = f.receive_event();
                f.pump_caller();
                expect_failure(event, "Node control event queue capacity exceeded");
            }
        }
        {
            Fixture f;
            auto caller_work = asio::make_work_guard(f.caller);
            auto data_work = asio::make_work_guard(f.data);
            auto received = asio::co_spawn(f.caller, [&]() -> asio::awaitable<void> {
                for (int i = 0; i < 2; ++i)
                {
                    static_cast<void>(co_await f.channel->receive_event());
                }
                for (std::uint64_t id = 1000; id < 1128; ++id)
                {
                    for (auto type : {CtrlCommand::FlowPrepared, CtrlCommand::FlowCommitted, CtrlCommand::FlowClosed})
                    {
                        const auto event = co_await f.channel->receive_event();
                        check(f.caller.get_executor().running_in_this_thread(),
                              "threaded event resumed on data executor");
                        check(event.type() == type && event.params->at("flow_id") == id,
                              "concurrent notifications were lost or reordered");
                    }
                }
            }, asio::use_future);
            auto produced = asio::co_spawn(f.data, [&]() -> asio::awaitable<void> {
                for (std::uint64_t id = 1000; id < 1128; ++id)
                {
                    f.channel->prepare_flow(njson{{"epoch", 7}, {"flow_id", id},
                                                  {"path", std::vector<std::string>{"a"}},
                                                  {"links", std::vector<std::uint64_t>{}},
                                                  {"transport", "tcp"}, {"ttl_ms", 10000}});
                    f.channel->commit_flow(id);
                    f.channel->close_flow(id);
                }
                co_return;
            }, asio::use_future);
            f.caller.restart();
            f.data.restart();
            std::jthread caller_thread([&] { f.caller.run(); });
            std::jthread data_thread([&] { f.data.run(); });
            ScopeGuard stop_threads([&] {
                f.caller.stop();
                f.data.stop();
            });
            check(produced.wait_for(5s) == std::future_status::ready, "notification producer did not finish");
            produced.get();
            check(received.wait_for(5s) == std::future_status::ready, "notification consumer did not finish");
            received.get();
        }
        {
            Fixture f;
            for (int i = 0; i < 64; ++i)
            {
                f.send(LnkFrType::Data, i % 2 != 0);
                auto result = f.receive();
                f.pump_caller();
                f.pump_data();
                check(result.wait_for(0s) != std::future_status::ready, "data executor completed caller inline");
                f.pump_caller();
                const auto frame = result.get();
                check(frame.payload == BytesBuf{1, 2, 3} && frame.reverse == (i % 2 != 0) &&
                          frame.epoch == 7 && frame.flow_id == 101,
                      "received frame was changed");
            }
            auto fin = f.receive();
            f.pump_caller();
            f.pump_data();
            check(fin.wait_for(0s) != std::future_status::ready, "empty receive did not wait");
            f.send(LnkFrType::Fin);
            f.pump_caller();
            check(fin.get().kind == LnkFrType::Fin, "FIN was lost");
            auto invalid = f.receive({}, 8);
            f.pump_caller();
            f.pump_data();
            f.pump_caller();
            expect_failure(invalid, "flow is not active");
        }
        {
            asio::cancellation_signal cancellation;
            Fixture f;
            auto cancelled = f.receive(cancellation.slot());
            f.pump_caller();
            // Caller cancellation occurs while the data executor has not registered the receive yet.
            cancellation.emit(asio::cancellation_type::all);
            f.pump_caller();
            f.pump_data();
            f.pump_caller();
            expect_failure(cancelled, "flow closed");
            f.send();
            auto next = f.receive();
            f.pump_caller();
            f.pump_data();
            f.pump_caller();
            check(next.get().payload == BytesBuf{1, 2, 3}, "cancellation closed flow or lost buffered data");
        }
        {
            asio::cancellation_signal cancellation;
            Fixture f;
            auto cancelled = f.receive(cancellation.slot());
            auto other = f.receive();
            f.pump_caller();
            f.pump_data();
            cancellation.emit(asio::cancellation_type::all);
            f.pump_caller();
            f.pump_data();
            f.pump_caller();
            expect_failure(cancelled, "flow closed");
            expect_failure(other, "flow closed");
        }
        for (bool cancel_first : {false, true})
        {
            asio::cancellation_signal cancellation;
            Fixture f;
            auto pending = f.receive(cancellation.slot());
            f.pump_caller();
            f.pump_data();
            auto cancel = [&] {
                cancellation.emit(asio::cancellation_type::all);
                f.pump_caller();
            };
            auto close = [&] {
                asio::post(f.data, [&] { f.channel->close_flow(101, "close/cancel race"); });
            };
            if (cancel_first)
            {
                cancel();
                close();
            }
            else
            {
                close();
                cancel();
            }
            f.pump_data();
            f.pump_caller();
            expect_failure(pending);
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump_caller();
            f.pump_data();
            asio::post(f.data, [&] { f.channel->close_flow(101, "closed by owner"); });
            f.pump_data();
            f.pump_caller();
            expect_failure(pending, "closed by owner");
            auto missing = f.receive();
            f.pump_caller();
            f.pump_data();
            f.pump_caller();
            expect_failure(missing, "flow is not active");
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump_caller();
            f.pump_data();
            asio::post(f.data, [&] {
                check(f.channel->send_flow({7, 101, false, LnkFrType::Data, {4, 5, 6}}) == FlowSendStatus::Queued,
                      "close race could not enqueue data");
                f.channel->close_flow(101, "closed during delivery");
            });
            f.pump_data();
            f.pump_caller();
            expect_failure(pending, "closed during delivery");
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump_caller();
            f.pump_data();
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump_data();
            f.pump_caller();
            stopped.get();
            expect_failure(pending);
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump_caller();
            f.pump_data();
            std::weak_ptr<LnkChannel> weak = f.channel;
            f.channel.reset();
            check(!weak.expired(), "pending receive released channel and pool");
            asio::post(f.data, [weak] { weak.lock()->close_flow(101, "last owner closed"); });
            f.pump_data();
            f.pump_caller();
            expect_failure(pending, "last owner closed");
            check(weak.expired(), "completed receive retained channel");
        }
        std::cout << "Flow receive checks passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
