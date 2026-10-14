#include "flow_test_io.h"
#include "frame_io.h"
#include "flow_test_state.h"
#include <future>
#include <iostream>

using namespace std::chrono_literals;
TEST_MEMBER(FlowProtocol, reserved_bytes_)
TEST_MEMBER(LnkChannel, connections_)
using PayloadPool = lnk::PayloadPool;
using lnk::PooledBuffer;
TEST_MEMBER(PayloadPool, live_)
TEST_MEMBER(PayloadPool, retained_)

static void check(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

struct LocalFixture
{
    asio::io_context caller{1};
    asio::io_context data{1};
    std::shared_ptr<FlowProtocol> channel = std::make_shared<FlowProtocol>(
        data.get_executor(), caller.get_executor(), "local", "127.0.0.1", 0, "127.0.0.1", 0);
    std::map<std::uint64_t, test_flow::Port> ports;

    void pump()
    {
        for (int i = 0; i < 64; ++i)
        {
            caller.restart();
            const auto called = caller.poll();
            data.restart();
            const auto processed = data.poll();
            if (!called && !processed)
            {
                return;
            }
        }
        throw std::runtime_error("fixture did not quiesce");
    }
    void prepare(std::uint64_t id, RelayProtocol transport = RelayProtocol::Tcp)
    {
        asio::post(data, [this, id, transport] {
            test_flow::prepare(channel, njson{{"epoch", 7}, {"flow_id", id}, {"path", std::vector<std::string>{"local"}},
                                        {"links", std::vector<std::uint64_t>{}}, {"transport", relay_protocol_name(transport)},
                                        {"ttl_ms", 10000}});
            channel->commit_flow(id);
        });
        pump();
    }
    test_flow::Port &port(std::uint64_t id)
    {
        if (!ports.contains(id))
        {
            auto result = asio::co_spawn(caller, test_flow::port(channel, 7, id), asio::use_future);
            pump();
            ports.emplace(id, result.get());
        }
        return ports.at(id);
    }
    FlowSendStatus send(std::uint64_t id, const BytesBuf &payload)
    {
        auto status = FlowSendStatus::Invalid;
        asio::post(data, [&] { status = test_flow::send(channel, {7, id, false, LnkFrType::Data, payload}); });
        pump();
        return status;
    }
    std::future<void> submit(std::uint64_t id, BytesBuf payload, asio::cancellation_slot slot = {}, bool run = true)
    {
        const auto stream = std::get<std::shared_ptr<lnk::FlowStream>>(port(id));
        auto result = asio::co_spawn(caller, stream->send(stream->copy_payload(payload)),
                                    asio::bind_cancellation_slot(slot, asio::use_future));
        if (run)
        {
            pump();
        }
        return result;
    }
    FlowFrame receive(std::uint64_t id)
    {
        static_cast<void>(port(id));
        auto result = asio::co_spawn(caller, test_flow::receive(channel, 7, id, false), asio::use_future);
        pump();
        check(result.wait_for(0s) == std::future_status::ready, "buffered receive did not complete");
        return result.get();
    }
    void complete(std::uint64_t id)
    {
        const auto stream = std::get<std::shared_ptr<lnk::FlowStream>>(port(id));
        asio::post(caller, [stream] { stream->complete_receive(); });
        pump();
    }
    void close(std::uint64_t id)
    {
        asio::post(data, [this, id] { channel->close_flow(id); });
        pump();
    }
    ~LocalFixture()
    {
        auto stopped = asio::co_spawn(data, channel->stop(), asio::use_future);
        pump();
        stopped.get();
    }
};

static void local_tests()
{
    const auto reservation = 2 * (test_flow::window_bytes + lnk::Frame::buffer_overhead);
    const BytesBuf payload(32768, 0x5a);
    {
        LocalFixture fixture;
        fixture.prepare(1);
        for (int index = 0; index < 15; ++index)
        {
            check(fixture.send(1, payload) == FlowSendStatus::Queued, "could not fill window");
        }
        auto staged = fixture.submit(1, payload);
        check(staged.wait_for(0s) == std::future_status::ready, "handoff did not take ownership of staged frame");
        staged.get();
        auto pending = fixture.submit(1, payload);
        check(pending.wait_for(0s) != std::future_status::ready, "next handoff ignored full window");
        const auto held = test_access::flow_buffered_bytes(*fixture.channel);
        auto frame = fixture.receive(1);
        check(frame.payload == payload, "queued payload changed");
        check(test_access::flow_buffered_bytes(*fixture.channel) == held, "dequeue prematurely returned credit");
        frame = {};
        fixture.complete(1);
        check(pending.wait_for(0s) == std::future_status::ready, "write completion did not resume handoff");
        pending.get();
        for (int index = 0; index < 16; ++index)
        {
            frame = fixture.receive(1);
            check(frame.payload == payload, "resumed stream lost bytes");
            frame = {};
            fixture.complete(1);
        }
        check(test_access::flow_buffered_bytes(*fixture.channel) == 0, "completed stream retained charge");
    }
    for (const bool before_dispatch : {false, true})
    {
        LocalFixture fixture;
        fixture.prepare(2);
        for (int index = 0; index < 15; ++index)
        {
            check(fixture.send(2, payload) == FlowSendStatus::Queued, "could not fill cancellation window");
        }
        auto staged = fixture.submit(2, BytesBuf(32768, 0x6a));
        staged.get();
        asio::cancellation_signal cancellation;
        auto pending = fixture.submit(2, BytesBuf(32768, 0x7b), cancellation.slot(), !before_dispatch);
        cancellation.emit(asio::cancellation_type::all);
        fixture.pump();
        check(pending.wait_for(0s) == std::future_status::ready, "cancelled sender remained suspended");
        bool cancelled = false;
        try
        {
            pending.get();
        }
        catch (const asio::system_error &error)
        {
            cancelled = error.code() == asio::error::operation_aborted;
        }
        check(cancelled, "submission cancellation lost its cause");
        for (int index = 0; index < 16; ++index)
        {
            auto frame = fixture.receive(2);
            check(frame.payload.front() == (index == 15 ? 0x6a : 0x5a), "cancelled frame reached the stream");
            frame = {};
            fixture.complete(2);
        }
        auto next = fixture.submit(2, payload);
        check(next.wait_for(0s) == std::future_status::ready, "cancellation poisoned subsequent handoff");
        next.get();
        fixture.close(2);
        fixture.complete(2); // Late socket completion is harmless after Flow closure.
    }
    {
        LocalFixture fixture;
        fixture.prepare(3);
        for (int index = 0; index < 15; ++index)
        {
            check(fixture.send(3, payload) == FlowSendStatus::Queued, "could not fill closing window");
        }
        fixture.submit(3, payload).get();
        auto pending = fixture.submit(3, payload);
        fixture.close(3);
        check(pending.wait_for(0s) == std::future_status::ready, "close did not wake blocked channel sender");
        bool failed = false;
        try
        {
            pending.get();
        }
        catch (const std::exception &)
        {
            failed = true;
        }
        check(failed, "closed handoff reported success");
        check(test_access::flow_buffered_bytes(*fixture.channel) == 0, "close retained window occupancy");
    }
    {
        LocalFixture fixture;
        const auto capacity = test_flow::buffer_budget_bytes / reservation;
        for (std::uint64_t id = 10; id < 10 + capacity; ++id)
        {
            fixture.prepare(id);
        }
        fixture.prepare(1000);
        check(test_access::FlowProtocol_flows_(*fixture.channel).size() == capacity, "prepare overcommitted reservations");
        check(fixture.send(10, payload) == FlowSendStatus::Queued, "budget test could not send");
        auto frame = fixture.receive(10);
        fixture.close(10);
        check(test_access::FlowProtocol_reserved_bytes_(*fixture.channel) == (capacity - 1) * reservation,
              "closing delivered data retained reservation");
        fixture.prepare(1001);
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(1001), "closed reservation was not reusable");
        frame = {};
        fixture.complete(10);
        check(test_access::FlowProtocol_reserved_bytes_(*fixture.channel) == capacity * reservation,
              "late completion changed reservation accounting");
    }
    {
        LocalFixture fixture;
        for (std::uint64_t id = 100; id < 200; ++id)
        {
            fixture.prepare(id);
        }
        check(test_access::FlowProtocol_flows_(*fixture.channel).size() == 100, "defaults cannot admit 100 TCP Flows");
    }
    {
        LocalFixture fixture;
        fixture.prepare(200, RelayProtocol::Udp);
        for (int index = 0; index < 16; ++index)
        {
            check(fixture.send(200, {9}) == FlowSendStatus::Queued, "UDP burst was rejected early");
        }
        check(fixture.send(200, {10}) == FlowSendStatus::Dropped, "full UDP queue did not drop packet");
        check(fixture.channel->take_dropped_datagrams() == 1, "UDP overload was not counted");
        check(!test_access::FlowProtocol_flows_(*fixture.channel).at(200)->stream, "UDP created stream flow state");
        auto frame = fixture.receive(200);
        check(frame.payload == BytesBuf{9}, "UDP payload changed");
        check(fixture.send(200, {11}) == FlowSendStatus::Queued, "UDP dequeue did not free capacity");
        check(fixture.receive(200).payload == BytesBuf{9}, "UDP incorrectly requires consumption confirmation");
        const auto port = std::get<std::shared_ptr<lnk::FlowDatagram>>(fixture.port(200));
        int accepted = 0;
        asio::post(fixture.caller, [&] {
            for (int i = 0; i < 18; ++i)
            {
                accepted += port->try_send(std::array<std::uint8_t, 1>{42});
            }
        });
        fixture.pump();
        // One packet is handed to the waiting consumer; the channel itself holds exactly 16 more.
        check(accepted == 17, "UDP cross-domain queue did not enforce fixed capacity");
        check(fixture.channel->take_dropped_datagrams() >= 1, "UDP channel rejection was not counted");
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(200), "UDP overload closed healthy Flow");
    }
    {
        LocalFixture fixture;
        test_access::FlowProtocol_payload_pool_(*fixture.channel) = std::make_shared<lnk::PayloadPool>(256);
        fixture.prepare(250, RelayProtocol::Udp);
        const auto port = std::get<std::shared_ptr<lnk::FlowDatagram>>(fixture.port(250));
        asio::post(fixture.caller, [port] {
            const std::array<std::uint8_t, 257> payload{};
            check(!port->try_send(payload), "UDP pool exhaustion did not drop the packet");
        });
        fixture.pump();
        check(fixture.channel->take_dropped_datagrams() == 1, "UDP pool exhaustion was not counted");
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(250), "UDP pool exhaustion closed Flow");
        asio::post(fixture.caller, [port] {
            port->close();
            check(!port->try_send(std::array<std::uint8_t, 1>{1}), "closed UDP port accepted packet");
        });
        fixture.pump();
    }
    {
        LocalFixture fixture;
        test_access::FlowProtocol_payload_pool_(*fixture.channel) = std::make_shared<lnk::PayloadPool>(256);
        fixture.prepare(251);
        check(fixture.send(251, {1}) == FlowSendStatus::Queued, "could not fill reset test pool");
        const auto port = std::get<std::shared_ptr<lnk::FlowStream>>(fixture.port(251));
        asio::post(fixture.caller, [port] { port->reset("pool full"); });
        fixture.pump();
        check(!test_access::FlowProtocol_flows_(*fixture.channel).contains(251), "full pool prevented Flow reset");
    }
    {
        lnk::PayloadPool pool(4096);
        for (int round = 0; round < 64; ++round)
        {
            {
                PooledBuffer first(pool, 2048);
                PooledBuffer second(pool, 2048);
                check(test_access::PayloadPool_live_(pool) == 4096, "pool did not charge rounded blocks");
            }
            {
                PooledBuffer small(pool, 255);
                bool rejected = false;
                try
                {
                    PooledBuffer large(pool, 3072);
                }
                catch (const std::bad_alloc &)
                {
                    rejected = true;
                }
                check(rejected, "pool exceeded hard live budget");
            }
            {
                PooledBuffer large(pool, 3072);
                check(test_access::PayloadPool_live_(pool) == 4096 && test_access::PayloadPool_retained_(pool) <= 4096, "pool failed to reclaim idle blocks");
            }
            check(test_access::PayloadPool_live_(pool) == 0 && test_access::PayloadPool_retained_(pool) <= 4096, "pool retained unbounded storage");
        }
    }
}

static asio::awaitable<void> pause(std::chrono::milliseconds duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(duration);
    co_await timer.async_wait(asio::use_awaitable);
}

static asio::awaitable<lnk::Frame> read_frame(asio::ip::tcp::socket &socket)
{
    LnkFrameHeader::Buffer bytes;
    co_await asio::async_read(socket, asio::buffer(bytes), asio::use_awaitable);
    const auto header = LnkFrameHeader::decode(bytes);
    // Test peer owns ordinary storage; use a resource that outlives the returned test frame.
    lnk::Frame frame{header, PooledBuffer(*std::pmr::new_delete_resource(), header.body_length)};
    if (header.body_length)
    {
        co_await asio::async_read(socket, asio::buffer(frame.payload.data(), frame.payload.size()), asio::use_awaitable);
    }
    co_return frame;
}

static void wire_tests()
{
    asio::io_context io(1);
    asio::ip::tcp::acceptor peer(io, {asio::ip::address_v4::loopback(), 0});
    asio::ip::tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
    const auto port = probe.local_endpoint().port();
    probe.close();
    asio::ip::udp::socket udp_probe(io, {asio::ip::address_v4::loopback(), 0});
    const auto udp_port = udp_probe.local_endpoint().port();
    udp_probe.close();
    auto channel = std::make_shared<FlowProtocol>(io.get_executor(), io.get_executor(), "local",
                                               "127.0.0.1", port, "127.0.0.1", udp_port);
    channel->links().start();
    channel->activate();
    auto scenario = [&]() -> asio::awaitable<void> {
        test_flow::prepare_link(channel, njson{{"epoch", 7}, {"id", 1}, {"left", "local"}, {"right", "peer"},
                               {"transport", "tcp"}, {"tcp_port", peer.local_endpoint().port()},
                               {"udp_port", udp_port}, {"token", "credit-test"}, {"peer", "peer"},
                               {"peer_address", "127.0.0.1"}});
        channel->links().connect(1);
        auto socket = co_await peer.async_accept(asio::use_awaitable);
        const auto attach = co_await read_ctrl_frame(socket, lnk::Clock::now() + 1s);
        auto values = *attach.params;
        values["node"] = "peer";
        const auto response = WireMessage::pack(CtrlMessage(CtrlCommand::LinkAttached, values));
        co_await asio::async_write(socket, asio::buffer(response), asio::use_awaitable);
        while ((co_await test_flow::event(channel)).type() != CtrlCommand::LinkReady)
        {
        }
        auto install = [&](std::uint64_t id) {
            test_flow::prepare(channel, njson{{"epoch", 7}, {"flow_id", id},
                                        {"path", std::vector<std::string>{"local", "peer"}},
                                        {"links", std::vector<std::uint64_t>{1}}, {"transport", "tcp"}, {"ttl_ms", 10000}});
            channel->commit_flow(id);
        };
        auto credit = [&](std::uint64_t id, std::uint64_t limit) -> asio::awaitable<void> {
            const auto bytes = LnkFrameHeader{LnkFrType::Credit, false, 0, 7, id, limit}.encode();
            co_await asio::async_write(socket, asio::buffer(bytes), asio::use_awaitable);
        };
        auto next_data = [&]() -> asio::awaitable<lnk::Frame> {
            for (;;)
            {
                auto frame = co_await read_frame(socket);
                if (frame.header.kind == LnkFrType::Data)
                {
                    co_return frame;
                }
            }
        };
        install(1);
        install(2);
        const auto window = test_flow::window_bytes;
        const auto used = 32768 + lnk::Frame::buffer_overhead;
        constexpr int count = 15;
        for (int index = 0; index < count; ++index)
        {
            check(test_flow::send(channel, {7, 1, false, LnkFrType::Data, BytesBuf(32768, static_cast<std::uint8_t>(index))}) ==
                      FlowSendStatus::Queued, "local queue rejected a peer-credit burst");
        }
        check(test_flow::send(channel, {7, 2, false, LnkFrType::Data, {99}}) == FlowSendStatus::Queued,
              "second Flow could not queue");
        co_await credit(1, window);
        for (int index = 0; index < count; ++index)
        {
            auto frame = co_await next_data();
            check(frame.header.id == 1 && frame.payload.size() == 32768 && frame.payload.data()[0] == index,
                  "wire FIFO changed payload order");
        }
        co_await pause(1ms); // Let the physical writer release local capacity; peer credit stays exhausted.
        for (int index = count; index < count + 3; ++index)
        {
            check(test_flow::send(channel, {7, 1, false, LnkFrType::Data, BytesBuf(32768, static_cast<std::uint8_t>(index))}) ==
                      FlowSendStatus::Queued, "local writer did not release capacity");
        }
        co_await credit(1, window); // Duplicate absolute limit cannot replenish the window.
        co_await credit(1, window - 1); // Old limit cannot replenish it either.
        co_await credit(2, window);
        auto small = co_await next_data();
        check(small.header.id == 2 && small.payload.size() == 1 && small.payload.data()[0] == 99,
              "credit-starved Flow blocked another Flow on shared FIFO");
        auto pending = asio::co_spawn(io, next_data(), asio::use_future);
        co_await pause(120ms);
        check(pending.wait_for(0s) != std::future_status::ready, "duplicate credit let sender overrun peer window");
        co_await credit(1, window + 3 * used);
        while (pending.wait_for(0s) != std::future_status::ready)
        {
            co_await pause(1ms);
        }
        for (int index = count; index < count + 3; ++index)
        {
            auto frame = index == count ? pending.get() : co_await next_data();
            check(frame.header.id == 1 && frame.payload.data()[0] == index, "delayed credit lost pending data");
        }
        install(3);
        co_await credit(3, window);
        co_await credit(3, window + 1);
        bool failed = false;
        while (!failed)
        {
            const auto event = co_await test_flow::event(channel);
            failed = event.type() == CtrlCommand::FlowError && event.params->at("flow_id") == 3;
        }
        install(4);
        const BytesBuf excessive(LnkFrameHeader::maximum_payload, 0x44);
        const auto incoming = LnkFrameHeader{LnkFrType::Data, true,
            static_cast<std::uint32_t>(excessive.size()), 7, 4}.encode();
        for (int index = 0; index < 16; ++index)
        {
            const std::array<asio::const_buffer, 2> buffers{asio::buffer(incoming), asio::buffer(excessive)};
            co_await asio::async_write(socket, buffers, asio::use_awaitable);
        }
        failed = false;
        while (!failed)
        {
            const auto event = co_await test_flow::event(channel);
            failed = event.type() == CtrlCommand::FlowError && event.params->at("flow_id") == 4;
        }
        check(!test_access::FlowProtocol_flows_(*channel).contains(4), "over-window DATA remained active");
        check(test_flow::send(channel, {7, 2, false, LnkFrType::Data, {100}}) == FlowSendStatus::Queued,
              "invalid credit closed shared Link or healthy Flow");
        check((co_await next_data()).header.id == 2, "healthy Flow could not progress after invalid credit");
        auto link = test_access::LnkChannel_connections_(channel->links()).at(1);
        const std::weak_ptr<lnk::NodeLink> owner = link;
        link->writable.close(); // Fail the writer while its sibling waits for the next header.
        link.reset();
        for (;;)
        {
            const auto event = co_await test_flow::event(channel);
            if (event.type() == CtrlCommand::LinkError)
            {
                check(event.params->at("reason").get<std::string>().find("Channel closed") != std::string::npos,
                      "TCP group replaced writer failure with reader cancellation");
                break;
            }
        }
        co_await channel->stop();
        check(owner.expired(), "TCP child loops retained their removed Link after stop");
        check(test_access::FlowProtocol_flows_(*channel).empty(), "TCP writer failure left dependent Flows");
    };
    auto run = [&]() -> asio::awaitable<void> {
        std::exception_ptr failure;
        try
        {
            co_await scenario();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        co_await channel->stop();
        if (failure)
        {
            std::rethrow_exception(failure);
        }
    };
    auto result = asio::co_spawn(io, run(), asio::use_future);
    io.run();
    result.get();
}

static void multihop_tests()
{
    asio::io_context io(1);
    asio::ip::tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
    const auto port = probe.local_endpoint().port();
    probe.close();
    asio::ip::udp::socket udp_probe(io, {asio::ip::address_v4::loopback(), 0});
    const auto udp_port = udp_probe.local_endpoint().port();
    udp_probe.close();
    const auto window = test_flow::window_bytes;
    std::array<std::shared_ptr<FlowProtocol>, 3> nodes;
    const std::vector<std::string> names{"a", "b", "c"};
    for (std::size_t index = 0; index < nodes.size(); ++index)
    {
        const auto address = "127.0.0." + std::to_string(index + 1);
        nodes[index] = std::make_shared<FlowProtocol>(io.get_executor(), io.get_executor(), names[index],
                                                   address, port, address, udp_port);
        nodes[index]->links().start();
        nodes[index]->activate();
    }
    auto scenario = [&]() -> asio::awaitable<void> {
        for (std::size_t index = 0; index < 2; ++index)
        {
            njson link{{"epoch", 7}, {"id", index + 1}, {"left", names[index]}, {"right", names[index + 1]},
                       {"transport", "tcp"}, {"tcp_port", port}, {"udp_port", udp_port}, {"token", "three-hop"},
                       {"peer", names[index + 1]}, {"peer_address", "127.0.0." + std::to_string(index + 2)}};
            test_flow::prepare_link(nodes[index], link);
            link["peer"] = names[index];
            link["peer_address"] = "127.0.0." + std::to_string(index + 1);
            test_flow::prepare_link(nodes[index + 1], link);
            nodes[index]->links().connect(index + 1);
            nodes[index + 1]->links().connect(index + 1);
        }
        const auto deadline = lnk::Clock::now() + 5s;
        for (;;)
        {
            std::size_t ready = 0;
            for (const auto &node : nodes)
            {
                for (const auto &[id, link] : test_access::LnkChannel_connections_(node->links()))
                {
                    ready += link->phase == lnk::NodeLink::Phase::Ready;
                }
            }
            if (ready == 4)
            {
                break;
            }
            check(lnk::Clock::now() < deadline, "three-hop Links did not become Ready");
            co_await pause(1ms);
        }
        for (std::uint64_t id : {10, 11})
        {
            const njson flow{{"epoch", 7}, {"flow_id", id}, {"path", names},
                             {"links", std::vector<std::uint64_t>{1, 2}}, {"transport", "tcp"}, {"ttl_ms", 10000}};
            for (const auto &node : nodes)
            {
                test_flow::prepare(node, flow);
                node->commit_flow(id);
            }
        }
        auto send = [&]() -> asio::awaitable<void> {
            for (int index = 0; index < 64; ++index)
            {
                const BytesBuf payload(32768, static_cast<std::uint8_t>(index));
                check(co_await test_flow::submit_data(nodes.front(), 7, 10, false, payload) == FlowSendStatus::Queued,
                      "three-hop backpressure closed sender");
            }
            check(co_await test_flow::submit(nodes.front(), {7, 10, false, LnkFrType::Fin}) == FlowSendStatus::Queued,
                  "three-hop FIN was rejected");
        };
        auto sent = asio::co_spawn(io, send(), asio::use_future);
        auto first = co_await test_flow::receive(nodes.back(), 7, 10, false);
        check(first.payload.size() == 32768 && first.payload.front() == 0, "first three-hop payload changed");
        co_await pause(120ms); // End socket would still be writing this delivered frame.
        check(sent.wait_for(0s) != std::future_status::ready, "three-hop sender ignored stalled consumer");
        for (const auto &node : nodes)
        {
            check(test_access::flow_buffered_bytes(*node) <= 4 * (window + lnk::Frame::buffer_overhead),
                  "three-hop queue grew beyond its reservations");
        }
        check(co_await test_flow::submit(nodes.front(), {7, 11, false, LnkFrType::Data, {99}}) == FlowSendStatus::Queued,
              "healthy three-hop Flow could not send");
        auto small = co_await test_flow::receive(nodes.back(), 7, 11, false);
        check(small.payload == BytesBuf{99}, "stalled Flow blocked healthy three-hop Flow");
        small = {};
        std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 11))->complete_receive();
        first = {};
        std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 10))->complete_receive();
        for (int index = 1; index < 64; ++index)
        {
            auto frame = co_await test_flow::receive(nodes.back(), 7, 10, false);
            check(frame.payload == BytesBuf(32768, static_cast<std::uint8_t>(index)),
                  "resumed three-hop stream lost or reordered bytes");
            frame = {};
            std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 10))->complete_receive();
        }
        auto fin = co_await test_flow::receive(nodes.back(), 7, 10, false);
        check(fin.kind == LnkFrType::Fin, "three-hop FIN overtook or lost DATA");
        check(sent.wait_for(0s) == std::future_status::ready, "drained sender did not complete");
        sent.get();
        for (const auto &node : nodes)
        {
            check(test_access::flow_buffered_bytes(*node) == 0, "drained three-hop stream retained charge");
        }
    };
    auto run = [&]() -> asio::awaitable<void> {
        std::exception_ptr failure;
        try
        {
            co_await scenario();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        for (const auto &node : nodes)
        {
            co_await node->stop();
        }
        if (failure)
        {
            std::rethrow_exception(failure);
        }
    };
    auto result = asio::co_spawn(io, run(), asio::use_future);
    io.run();
    result.get();
}

int main()
{
    try
    {
        local_tests();
        wire_tests();
        multihop_tests();
        std::cout << "Flow control checks passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
