#include "flow_test_io.h"
#include <future>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;
TEST_MEMBER(FlowProtocol, retired_)
TEST_MEMBER(FlowProtocol, monitor_timer_)
TEST_MEMBER(LnkChannel, acceptor_)
TEST_MEMBER(LnkChannel, udp_socket_)
TEST_MEMBER(LnkChannel, udp_writes_)
TEST_MEMBER(FlowStream, send_queue_)
TEST_MEMBER(FlowStream, receive_completed_)

static void check(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

static bool matches(const lnk::PooledBuffer &buffer, const BytesBuf &expected)
{
    return std::ranges::equal(buffer.bytes(), expected);
}

struct Fixture
{
    asio::io_context caller{1};
    asio::io_context data{1};
    std::shared_ptr<FlowProtocol> channel = std::make_shared<FlowProtocol>(
        data.get_executor(), caller.get_executor(), "a", "127.0.0.1", 0, "127.0.0.1", 0);
    std::shared_ptr<lnk::FlowStream> port;

    void pump()
    {
        for (int i = 0; i < 64; ++i)
        {
            caller.restart();
            const auto a = caller.poll();
            data.restart();
            const auto b = data.poll();
            if (!a && !b)
            {
                return;
            }
        }
        throw std::runtime_error("fixture did not quiesce");
    }
    Fixture()
    {
        asio::post(data, [this] {
            test_flow::prepare(channel, njson{{"epoch", 7}, {"flow_id", 101}, {"path", std::vector<std::string>{"a"}},
                                        {"links", std::vector<std::uint64_t>{}}, {"transport", "tcp"}, {"ttl_ms", 10000}});
            channel->commit_flow(101);
        });
        pump();
        auto bound = bind_stream();
        pump();
        port = bound.get();
    }
    std::future<std::shared_ptr<lnk::FlowStream>> bind_stream(std::uint64_t epoch = 7)
    {
        return asio::co_spawn(data,
            [channel = channel, epoch, transfer = caller.get_executor()]() -> asio::awaitable<std::shared_ptr<lnk::FlowStream>> {
                co_return channel->bind_stream_flow(epoch, 101, false, transfer);
            }, asio::use_future);
    }
    void activate_links()
    {
        asio::post(data, [this] {
            auto &links = channel->links();
            auto &acceptor = test_access::LnkChannel_acceptor_(links);
            acceptor.open(asio::ip::tcp::v4());
            acceptor.bind({asio::ip::address_v4::loopback(), 0});
            acceptor.listen();
            auto &socket = test_access::LnkChannel_udp_socket_(links);
            socket.open(asio::ip::udp::v4());
            socket.bind({asio::ip::address_v4::loopback(), 0});
            channel->activate();
        });
        pump();
    }
    std::future<std::optional<lnk::PooledBuffer>> receive(asio::cancellation_slot slot = {})
    {
        return asio::co_spawn(caller, [port = port, this]() -> asio::awaitable<std::optional<lnk::PooledBuffer>> {
            try
            {
                auto frame = co_await port->receive();
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
    void send(std::uint8_t value = 1)
    {
        asio::post(data, [this, value] {
            check(test_flow::try_send_data(channel, 101, false, {value, 2, 3}),
                  "could not enqueue fixture data");
        });
        pump();
    }
    void complete()
    {
        asio::post(caller, [port = port] { port->complete_receive(); });
        pump();
    }
    void close(std::string reason)
    {
        asio::post(data, [channel = channel, reason = std::move(reason)] { channel->close_flow(101, reason); });
        pump();
    }
    ~Fixture()
    {
        if (channel)
        {
            auto stopped = asio::co_spawn(data, channel->stop(), asio::use_future);
            pump();
            stopped.get();
        }
    }
};

template <typename Result>
static void expect_failure(std::future<Result> &result, const char *reason = nullptr)
{
    check(result.wait_for(0s) == std::future_status::ready, "pending operation did not complete");
    try
    {
        static_cast<void>(result.get());
    }
    catch (const std::exception &error)
    {
        check(!reason || std::string(error.what()) == reason, "failure lost its reason");
        return;
    }
    throw std::runtime_error("operation unexpectedly succeeded");
}

int main()
{
    try
    {
        for (int failure = 0; failure < 3; ++failure)
        {
            Fixture f;
            f.activate_links();
            auto pending = f.receive();
            f.pump();
            asio::post(f.data, [&f] {
                lnk::FlowSpec spec;
                spec.id = 102;
                spec.epoch = 7;
                spec.prepare_timeout = 0ms;
                f.channel->prepare_flow(std::move(spec));
            });
            f.pump();
            check(!test_access::FlowProtocol_flows_(*f.channel).contains(102), "prepare timeout did not expire Flow");
            check(test_access::FlowProtocol_flows_(*f.channel).contains(101), "prepare timeout stopped healthy Flow");
            // A deadline change aborts the old timer wait without cancelling the monitor coroutine.
            asio::post(f.data, [&f] {
                test_access::FlowProtocol_monitor_timer_(*f.channel).expires_at(lnk::Clock::now());
            });
            f.pump();
            check(test_access::FlowProtocol_flows_(*f.channel).contains(101), "deadline update stopped the service");
            check(pending.wait_for(0s) != std::future_status::ready, "deadline update stopped endpoint reception");

            asio::post(f.data, [&f, failure] {
                auto &links = f.channel->links();
                if (failure == 0)
                {
                    test_access::LnkChannel_acceptor_(links).cancel();
                }
                else if (failure == 1)
                {
                    test_access::LnkChannel_udp_socket_(links).cancel();
                }
                else
                {
                    test_access::LnkChannel_udp_writes_(links).close();
                }
            });
            f.pump();
            expect_failure(pending);
            check(test_access::FlowProtocol_flows_(*f.channel).empty(), "shared loop exit left an active Flow");
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump();
            check(stopped.wait_for(0s) == std::future_status::ready, "shared loop cancellation did not drain monitor");
            stopped.get();
        }
        {
            Fixture f;
            f.activate_links();
            // Queue an unauthenticated connection while stop races with accept completion.
            asio::ip::tcp::socket incoming(f.caller);
            incoming.connect(test_access::LnkChannel_acceptor_(f.channel->links()).local_endpoint());
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump();
            check(stopped.wait_for(0s) == std::future_status::ready, "late accept left a bootstrap task after stop");
            stopped.get();
        }
        {
            Fixture f;
            auto oversized = asio::co_spawn(f.caller,
                f.port->send(lnk::PooledBuffer(test_access::FlowProtocol_payload_pool_(*f.channel),
                                             lnk::FlowStream::maximum_chunk_size + 1)), asio::use_future);
            f.pump();
            expect_failure(oversized, "invalid stream chunk size");

            auto sent = asio::co_spawn(f.caller, f.port->send({}), asio::use_future);
            f.pump();
            sent.get();
            auto data = f.receive();
            f.pump();
            const auto empty = data.get();
            check(empty && empty->size() == 0, "empty DATA became EOF");
            f.complete();

            auto finished = asio::co_spawn(f.caller, f.port->finish_send(), asio::use_future);
            f.pump();
            finished.get();
            auto eof = f.receive();
            f.pump();
            check(!eof.get(), "FIN did not become EOF");
            auto after_fin = asio::co_spawn(f.caller, f.port->send(f.port->copy_payload(std::array<std::uint8_t, 1>{1})), asio::use_future);
            f.pump();
            expect_failure(after_fin, "stream sending has finished");
        }
        {
            Fixture f;
            auto incoming = f.receive();
            asio::post(f.data, [&f] {
                check(test_flow::try_finish(f.channel, 101, true),
                      "could not finish only the incoming direction");
            });
            f.pump();
            check(!incoming.get(), "incoming FIN did not reach the endpoint");
            check(test_access::FlowProtocol_flows_(*f.channel).contains(101), "incoming FIN cancelled the sender");
            asio::post(f.caller, [port = f.port] { test_access::FlowStream_send_queue_(*port).close(); });
            f.pump();
            check(test_access::FlowProtocol_flows_(*f.channel).empty(), "sender failure did not close Flow");
            // A normal completion precedes the error: preserve the error stage, not order[0].
            for (int i = 0; i < 3; ++i)
            {
                auto event = asio::co_spawn(f.caller, f.channel->receive_event(), asio::use_future);
                f.pump();
                const auto result = event.get();
                if (i == 2)
                {
                    check(result.kind == lnk::EventKind::FlowError && result.stage == "send",
                          "normal FIN hid the later sender error");
                }
            }
        }
        {
            Fixture f;
            const BytesBuf chunk(32768, 1);
            asio::post(f.data, [&f, &chunk] {
                for (std::size_t i = 0; i < test_flow::window_bytes / (chunk.size() + lnk::Frame::buffer_overhead); ++i)
                {
                    check(test_flow::try_send_data(f.channel, 101, false, chunk),
                          "could not fill Flow for peer cancellation");
                }
            });
            f.pump();
            auto handed_off = asio::co_spawn(f.caller, f.port->send(f.port->copy_payload(chunk)), asio::use_future);
            f.pump();
            handed_off.get(); // The sender now waits for window space on the data executor.
            auto pending = asio::co_spawn(f.caller, f.port->send(f.port->copy_payload(chunk)), asio::use_future);
            f.pump();
            check(pending.wait_for(0s) != std::future_status::ready, "full Flow did not block next handoff");
            asio::post(f.caller, [port = f.port] { test_access::FlowStream_receive_completed_(*port).close(); });
            f.pump();
            expect_failure(pending);
            check(test_access::FlowProtocol_flows_(*f.channel).empty(), "receiver failure did not join credit waiter");
            const std::weak_ptr<FlowProtocol> owner = f.channel;
            f.channel.reset();
            check(owner.expired(), "joined child loops retained their protocol owner");
        }
        {
            Fixture f;
            auto duplicate = f.bind_stream();
            auto stale = f.bind_stream(8);
            auto wrong_transport = asio::co_spawn(f.data,
                [channel = f.channel, transfer = f.caller.get_executor()]() -> asio::awaitable<std::shared_ptr<lnk::FlowDatagram>> {
                    co_return channel->bind_datagram_flow(7, 101, false, transfer);
                }, asio::use_future);
            f.pump();
            expect_failure(duplicate, "invalid or duplicate flow endpoint binding");
            expect_failure(stale, "flow is not active");
            expect_failure(wrong_transport, "invalid or duplicate flow endpoint binding");
        }
        {
            Fixture f;
            auto payload = f.port->copy_payload(std::array<std::uint8_t, 3>{8, 9, 10});
            const auto storage = payload.data();
            auto sent = asio::co_spawn(f.caller, f.port->send(std::move(payload)), asio::use_future);
            f.pump();
            sent.get();
            auto received = f.receive();
            f.pump();
            auto delivery = received.get();
            check(delivery && delivery->data() == storage, "stream endpoint copied its pooled payload");
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump();
            stopped.get();
            const std::weak_ptr<FlowProtocol> owner = f.channel;
            f.channel.reset();
            f.port.reset();
            check(owner.expired(), "exported payload retained the protocol instead of its pool");
            check(matches(*delivery, {8, 9, 10}), "exported payload outlived its pool storage");
        }
        {
            Fixture f;
            f.send(1);
            f.send(4);
            const auto held = test_access::flow_buffered_bytes(*f.channel);
            auto first = f.receive();
            f.pump();
            check(matches(first.get().value(), {1, 2, 3}), "first payload changed");
            auto second = f.receive();
            f.pump();
            check(second.wait_for(0s) != std::future_status::ready, "delivery overtook unconfirmed socket write");
            check(test_access::flow_buffered_bytes(*f.channel) == held, "receive returned TCP credit");
            f.complete();
            check(second.wait_for(0s) == std::future_status::ready, "completion did not unlock next delivery");
            check(matches(second.get().value(), {4, 2, 3}), "second payload changed");
            f.complete();
            check(test_access::flow_buffered_bytes(*f.channel) == 0, "confirmed deliveries retained charge");
        }
        for (const bool before_dispatch : {false, true})
        {
            Fixture f;
            asio::cancellation_signal cancellation;
            auto pending = f.receive(cancellation.slot());
            if (!before_dispatch)
            {
                f.pump();
            }
            cancellation.emit(asio::cancellation_type::all);
            f.pump();
            expect_failure(pending);
            check(test_access::FlowProtocol_flows_(*f.channel).contains(101), "individual receive cancellation closed Flow");
            f.send();
            auto next = f.receive();
            f.pump();
            check(matches(next.get().value(), {1, 2, 3}), "receive cancellation poisoned channel");
            f.complete();
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump();
            f.close("closed by owner");
            expect_failure(pending, "closed by owner");
            f.complete(); // A late completion after close must not touch a new Flow.
            auto closed = f.receive();
            f.pump();
            expect_failure(closed, "closed by owner");
        }
        {
            Fixture f;
            const auto old = f.port;
            f.close("old instance closed");
            asio::post(f.data, [&f] {
                // Advance the retirement boundary without spending ten seconds on the test clock.
                test_access::FlowProtocol_retired_(*f.channel).erase(101);
                test_flow::prepare(f.channel, njson{{"epoch", 7}, {"flow_id", 101}, {"path", std::vector<std::string>{"a"}},
                                             {"links", std::vector<std::uint64_t>{}}, {"transport", "tcp"}, {"ttl_ms", 10000}});
                f.channel->commit_flow(101);
            });
            f.pump();
            auto bound = f.bind_stream();
            f.pump();
            f.port = bound.get();
            asio::post(f.caller, [old] {
                old->complete_receive();
                old->reset("late reset from old identity");
                old->close("late close");
            });
            f.pump();
            check(test_access::FlowProtocol_flows_(*f.channel).at(101)->stream->port == f.port,
                  "late port operation changed reused Flow identity");
            f.send(9);
            auto frame = f.receive();
            f.pump();
            check(matches(frame.get().value(), {9, 2, 3}), "reused Flow did not survive stale port operations");
            f.complete();
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump();
            f.close({});
            expect_failure(pending, "flow closed");
        }
        {
            Fixture f;
            const auto old = f.port;
            f.close("old TCP instance closed");
            asio::post(f.data, [&f] {
                test_access::FlowProtocol_retired_(*f.channel).erase(101);
                lnk::FlowSpec spec;
                spec.id = 101;
                spec.epoch = 7;
                spec.transport = RelayProtocol::Udp;
                f.channel->prepare_flow(std::move(spec));
                f.channel->commit_flow(101);
            });
            auto bound = asio::co_spawn(f.data,
                [channel = f.channel, transfer = f.caller.get_executor()]() -> asio::awaitable<std::shared_ptr<lnk::FlowDatagram>> {
                    co_return channel->bind_datagram_flow(7, 101, false, transfer);
                }, asio::use_future);
            f.pump();
            const auto datagram = bound.get();
            asio::post(f.caller, [old] {
                old->complete_receive();
                old->reset("late reset after protocol change");
            });
            // Exercise late close directly: the old port itself has already been closed.
            asio::post(f.data, [channel = f.channel, old] {
                test_access::FlowProtocol_stream_(*channel).close_port(old, "late close after protocol change");
            });
            f.pump();
            const auto flow = test_access::FlowProtocol_flows_(*f.channel).at(101);
            check(flow->transport == RelayProtocol::Udp && flow->datagram.port == datagram,
                  "old TCP port changed reused UDP Flow");
            auto received = asio::co_spawn(f.caller, datagram->receive(), asio::use_future);
            asio::post(f.caller, [datagram] { check(datagram->try_send(std::array<std::uint8_t, 1>{7}), "reused UDP Flow rejected packet"); });
            f.pump();
            check(received.wait_for(0s) == std::future_status::ready, "reused UDP reception stayed pending");
            check(matches(received.get(), {7}), "reused UDP Flow did not survive old TCP operations");
            auto pending = asio::co_spawn(f.caller, datagram->receive(), asio::use_future);
            f.pump();
            f.close("UDP instance closed");
            expect_failure(pending, "UDP instance closed");
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump();
            auto stopped = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            f.pump();
            check(stopped.wait_for(0s) == std::future_status::ready, "stop did not drain endpoint tasks");
            stopped.get();
            expect_failure(pending, "node stopping");
        }
        {
            Fixture f;
            auto pending = f.receive();
            f.pump();
            std::weak_ptr<FlowProtocol> weak = f.channel;
            f.channel.reset();
            check(!weak.expired(), "active data tasks released their owner");
            asio::post(f.caller, [port = f.port] { port->close("last owner closed"); });
            f.pump();
            expect_failure(pending, "last owner closed");
            check(weak.expired(), "closed endpoint tasks retained channel and pool");
        }
        {
            Fixture f;
            // Native completion must return to the socket executor with real threads, too.
            auto caller_work = asio::make_work_guard(f.caller);
            auto data_work = asio::make_work_guard(f.data);
            f.caller.restart();
            f.data.restart();
            std::thread caller_thread([&] { f.caller.run(); });
            std::thread data_thread([&] { f.data.run(); });
            ScopeGuard stop_threads([&] {
                caller_work.reset();
                data_work.reset();
                f.caller.stop();
                f.data.stop();
                caller_thread.join();
                data_thread.join();
            });
            auto pending = f.receive();
            asio::post(f.data, [&f] {
                check(test_flow::try_send_data(f.channel, 101, false, {5, 6, 7}),
                      "threaded fixture could not send");
            });
            check(pending.wait_for(1s) == std::future_status::ready, "threaded receive remained suspended");
            check(matches(pending.get().value(), {5, 6, 7}), "threaded payload changed");
            auto close = asio::co_spawn(f.caller, [port = f.port]() -> asio::awaitable<void> {
                port->complete_receive();
                port->close("threaded close");
                co_return;
            }, asio::use_future);
            close.get();
            auto stop = asio::co_spawn(f.data, f.channel->stop(), asio::use_future);
            check(stop.wait_for(1s) == std::future_status::ready, "threaded shutdown did not drain");
            stop.get();
        }
        std::cout << "Flow port receive checks passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
