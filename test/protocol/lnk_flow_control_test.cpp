#include "failing_payload_pool.h"
#include "flow_test_io.h"
#include "frame_io.h"
#include "flow_test_state.h"
#include <future>
#include <iostream>
#include <fstream>
#ifdef __linux__
#include <unistd.h>
#endif

using namespace std::chrono_literals;
TEST_MEMBER(FlowProtocol, retired_)
TEST_MEMBER(FlowProtocol, credit_deadline_)
TEST_MEMBER(FlowProtocol, poll)
TEST_MEMBER(LnkChannel, connections_)
using lnk::PooledBuffer;

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
    bool send(std::uint64_t id, const BytesBuf &payload)
    {
        bool accepted = false;
        asio::post(data, [&] { accepted = test_flow::try_send_data(channel, id, false, payload); });
        pump();
        return accepted;
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
    std::optional<BytesBuf> receive(std::uint64_t id)
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

static void stream_admission_tests()
{
    const BytesBuf payload(32768, 0x5a);
    const auto frames_per_window = test_flow::window_bytes / (payload.size() + lnk::Frame::buffer_overhead);
    {
        LocalFixture fixture;
        fixture.prepare(1);
        for (std::size_t index = 0; index < frames_per_window; ++index)
        {
            check(fixture.send(1, payload), "could not fill window");
        }
        auto staged = fixture.submit(1, payload);
        check(staged.wait_for(0s) == std::future_status::ready, "handoff did not take ownership of staged frame");
        staged.get();
        auto pending = fixture.submit(1, payload);
        check(pending.wait_for(0s) != std::future_status::ready, "next handoff ignored full window");
        const auto held = test_access::flow_buffered_bytes(*fixture.channel);
        auto frame = fixture.receive(1);
        check(frame == payload, "queued payload changed");
        check(test_access::flow_buffered_bytes(*fixture.channel) == held, "dequeue prematurely returned credit");
        frame = {};
        fixture.complete(1);
        check(pending.wait_for(0s) == std::future_status::ready, "write completion did not resume handoff");
        pending.get();
        for (std::size_t index = 0; index < frames_per_window + 1; ++index)
        {
            frame = fixture.receive(1);
            check(frame == payload, "resumed stream lost bytes");
            frame = {};
            fixture.complete(1);
        }
        check(test_access::flow_buffered_bytes(*fixture.channel) == 0, "completed stream retained charge");
    }
    for (const bool before_dispatch : {false, true})
    {
        LocalFixture fixture;
        fixture.prepare(2);
        for (std::size_t index = 0; index < frames_per_window; ++index)
        {
            check(fixture.send(2, payload), "could not fill cancellation window");
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
        for (std::size_t index = 0; index < frames_per_window + 1; ++index)
        {
            auto frame = fixture.receive(2);
            check(frame && !frame->empty() && frame->front() == (index == frames_per_window ? 0x6a : 0x5a), "cancelled frame reached the stream");
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
        for (std::size_t index = 0; index < frames_per_window; ++index)
        {
            check(fixture.send(3, payload), "could not fill closing window");
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
        auto pool = std::make_shared<FailingPayloadPool>();
        test_access::FlowProtocol_payload_pool_(*fixture.channel) = pool;
        fixture.prepare(251);
        check(fixture.send(251, {1}), "could not fill reset allocation fixture");
        pool->fail_allocations = true;
        const auto port = std::get<std::shared_ptr<lnk::FlowStream>>(fixture.port(251));
        asio::post(fixture.caller, [port] { port->reset("allocation failed"); });
        fixture.pump();
        check(!test_access::FlowProtocol_flows_(*fixture.channel).contains(251), "allocation failure prevented Flow reset");
    }
}

static void flow_capacity_tests()
{
    static_assert(lnk::max_flow_count == 1000);
    for (const int mode : {0, 1, 2})
    {
        LocalFixture fixture;
        for (std::uint64_t id = 1; id <= 1000; ++id)
        {
            fixture.prepare(id, mode == 1 || (mode == 2 && id % 2) ? RelayProtocol::Udp : RelayProtocol::Tcp);
        }
        check(test_access::FlowProtocol_flows_(*fixture.channel).size() == 1000,
              "Flow admission still depends on a global byte budget");
        fixture.prepare(1001);
        check(!test_access::FlowProtocol_flows_(*fixture.channel).contains(1001), "Flow count limit was exceeded");
        fixture.close(1);
        fixture.prepare(1001);
        fixture.prepare(1);
        check(test_access::FlowProtocol_flows_(*fixture.channel).size() == 999 &&
                  !test_access::FlowProtocol_flows_(*fixture.channel).contains(1),
              "retired identities stopped consuming capacity or were revived early");
        asio::post(fixture.data, [&] {
            test_access::FlowProtocol_poll(*fixture.channel, lnk::Clock::now() + 11s);
        });
        fixture.pump();
        check(test_access::FlowProtocol_retired_(*fixture.channel).empty(), "retired identity did not expire");
        fixture.prepare(1001);
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(1001),
              "expired identity did not restore count capacity");
    }
}

static void flow_churn_tests()
{
    LocalFixture fixture;
    auto rss_bytes = []() -> std::size_t {
#ifdef __linux__
        std::ifstream stat("/proc/self/statm");
        std::size_t total = 0, resident = 0;
        stat >> total >> resident;
        return resident * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#else
        return 0;
#endif
    };
    std::size_t first_rss = 0, last_rss = 0, peak_rss = 0;
    for (std::uint64_t round = 0; round < 40; ++round)
    {
        for (std::uint64_t slot = 0; slot < 8; ++slot)
            fixture.prepare(2000 + round * 8 + slot, slot % 2 ? RelayProtocol::Udp : RelayProtocol::Tcp);
        check(test_access::FlowProtocol_flows_(*fixture.channel).size() == 8, "churn changed fixed concurrency");
        for (std::uint64_t slot = 0; slot < 8; ++slot)
        {
            const auto id = 2000 + round * 8 + slot;
            const BytesBuf payload(slot % 2 ? LnkFrameHeader::maximum_payload : 32768, 0x5a);
            check(fixture.send(id, payload), "churn could not enqueue payload");
            check(fixture.receive(id) == payload, "churn corrupted payload");
            if (slot % 2 == 0) fixture.complete(id);
            fixture.close(id);
            fixture.ports.erase(id);
        }
        check(test_access::FlowProtocol_flows_(*fixture.channel).empty(), "churn retained active Flows");
        last_rss = rss_bytes();
        if (!round) first_rss = last_rss;
        peak_rss = std::max(peak_rss, last_rss);
    }
    std::cout << "[PASS] 40 mixed Flow churn rounds, concurrency=8; RSS first/last/peak bytes="
              << first_rss << '/' << last_rss << '/' << peak_rss << '\n';
}

static void datagram_admission_tests()
{
    {
        LocalFixture fixture;
        fixture.prepare(200, RelayProtocol::Udp);
        for (int index = 0; index < 16; ++index)
        {
            check(fixture.send(200, {9}), "UDP burst was rejected early");
        }
        check(!fixture.send(200, {10}), "full UDP queue did not drop packet");
        check(!test_access::FlowProtocol_flows_(*fixture.channel).at(200)->stream, "UDP created stream flow state");
        auto frame = fixture.receive(200);
        check(frame == BytesBuf{9}, "UDP payload changed");
        check(fixture.send(200, {11}), "UDP dequeue did not free capacity");
        check(fixture.receive(200) == BytesBuf{9}, "UDP incorrectly requires consumption confirmation");
        for (int index = 0; index < 14; ++index)
        {
            check(fixture.receive(200) == BytesBuf{9}, "UDP overload changed queued packets");
        }
        check(fixture.receive(200) == BytesBuf{11}, "UDP overload admitted the rejected packet or lost recovery data");
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
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(200), "UDP overload closed healthy Flow");
    }
    {
        LocalFixture fixture;
        auto pool = std::make_shared<FailingPayloadPool>();
        test_access::FlowProtocol_payload_pool_(*fixture.channel) = pool;
        fixture.prepare(250, RelayProtocol::Udp);
        pool->fail_allocations = true;
        const auto port = std::get<std::shared_ptr<lnk::FlowDatagram>>(fixture.port(250));
        asio::post(fixture.caller, [port] {
            const std::array<std::uint8_t, 257> payload{};
            check(!port->try_send(payload), "UDP allocation failure did not drop the packet");
        });
        fixture.pump();
        check(test_access::FlowProtocol_flows_(*fixture.channel).contains(250), "UDP allocation failure closed Flow");
        pool->fail_allocations = false;
        asio::post(fixture.caller, [port] {
            check(port->try_send(std::array<std::uint8_t, 1>{42}), "UDP did not recover after allocation failure");
        });
        fixture.pump();
        check(fixture.receive(250) == BytesBuf{42}, "UDP allocation failure changed the next packet");
        asio::post(fixture.caller, [port] {
            port->close();
            check(!port->try_send(std::array<std::uint8_t, 1>{1}), "closed UDP port accepted packet");
        });
        fixture.pump();
    }

}

namespace credit_tests
{
using Clock = lnk::Clock;
constexpr auto expected_batch_interval = 20ms; // Fixed policy expectation, independent of production constants.
constexpr std::size_t chunk_size = 32768;
constexpr std::size_t chunk_charge = chunk_size + lnk::Frame::buffer_overhead;
constexpr auto threshold_frames = (test_flow::window_bytes / 4 + chunk_charge - 1) / chunk_charge;

// A real endpoint and its owned tasks, with a synthetic upstream Link whose outgoing
// CREDIT map we can inspect before a physical writer consumes it. No monitor runs:
// poll is driven at exact deadline boundaries, so tests never rely on scheduler timing.
struct CreditFixture
{
    asio::io_context io{1};
    std::shared_ptr<FlowProtocol> protocol = std::make_shared<FlowProtocol>(
        io.get_executor(), io.get_executor(), "sink", "127.0.0.1", 0, "127.0.0.1", 0);
    std::shared_ptr<lnk::NodeLink> upstream;
    std::shared_ptr<lnk::FlowState> flow;
    std::shared_ptr<lnk::FlowStream> endpoint;

    void pump()
    {
        io.restart();
        io.poll();
    }
    decltype(auto) direction()
    {
        return (flow->stream->directions[0]);
    }
    explicit CreditFixture(bool bind = true)
    {
        upstream = std::make_shared<lnk::NodeLink>(io.get_executor(),
            lnk::LinkSpec{1, 7, RelayProtocol::Tcp, "upstream", "127.0.0.2", "test", 0, 0, false});
        upstream->phase = lnk::NodeLink::Phase::Ready;
        test_access::LnkChannel_connections_(protocol->links()).emplace(1, upstream);
        lnk::FlowSpec spec{10, 7, RelayProtocol::Tcp};
        spec.previous = 1;
        spec.previous_peer = "upstream";
        protocol->prepare_flow(std::move(spec));
        protocol->commit_flow(10);
        flow = test_access::FlowProtocol_flows_(*protocol).at(10);
        check(upstream->credits.size() == 1 && credit() == test_flow::window_bytes,
              "commit did not immediately publish the initial window");
        check(deadline() == Clock::time_point::max(), "initial credit waited for merging");
        upstream->credits.clear();
        if (bind)
        {
            endpoint = protocol->bind_stream_flow(7, 10, true, io.get_executor());
            pump();
        }
    }
    Clock::time_point deadline() const
    {
        return test_access::FlowProtocol_credit_deadline_(*protocol);
    }
    std::uint64_t credit(std::uint64_t id = 10, bool reverse = false) const
    {
        return upstream->credits.at({id, reverse}).sequence;
    }
    void feed(std::size_t count, std::size_t size = chunk_size)
    {
        const auto pool = test_access::FlowProtocol_payload_pool_(*protocol);
        for (std::size_t index = 0; index < count; ++index)
        {
            test_access::FlowProtocol_stream_(*protocol).receive(flow,
                lnk::Frame{{LnkFrType::Data, false, static_cast<std::uint32_t>(size), 7, 10},
                           lnk::PooledBuffer(pool, size)});
        }
        pump();
    }
    std::optional<lnk::PooledBuffer> receive()
    {
        auto received = asio::co_spawn(io, endpoint->receive(), asio::use_future);
        pump();
        check(received.wait_for(0s) == std::future_status::ready, "buffered DATA was not delivered");
        return received.get();
    }
    void complete()
    {
        endpoint->complete_receive();
        pump();
    }
    void poll(Clock::time_point now)
    {
        test_access::FlowProtocol_poll(*protocol, now);
    }
    ~CreditFixture()
    {
        auto stopped = asio::co_spawn(io, protocol->stop(), asio::use_future);
        pump();
        stopped.get();
    }
};

void deadline_and_release()
{
    CreditFixture f;
    f.feed(test_flow::window_bytes / chunk_charge);
    const auto admitted = f.direction().buffered_bytes;
    auto data = f.receive();
    check(data && data->size() == chunk_size, "DATA payload changed");
    check(f.direction().buffered_bytes == admitted && f.direction().released_bytes == 0,
          "endpoint dequeue returned credit before downstream completion");
    data.reset();
    check(f.direction().buffered_bytes == admitted && f.upstream->credits.empty(),
          "returning a pool block returned credit without completion");
    const auto before = Clock::now();
    f.complete();
    const auto after = Clock::now();
    const auto deadline = f.deadline();
    check(deadline >= before + expected_batch_interval && deadline <= after + expected_batch_interval,
          "small release did not use the selected coalescing delay");
    check(f.direction().buffered_bytes == admitted - chunk_charge && f.direction().released_bytes == chunk_charge,
          "coalescing delayed local capacity release");
    check(f.direction().receive_limit == test_flow::window_bytes && f.upstream->credits.empty(),
          "small release published credit immediately");
    const auto remaining = f.direction().receive_limit - f.direction().received_bytes;
    check(remaining > 0 && remaining < chunk_charge, "fixture did not leave a nonzero unusable credit tail");
    f.poll(deadline - 1ns);
    check(f.upstream->credits.empty(), "credit published before its deadline");
    f.poll(deadline);
    check(f.credit() == test_flow::window_bytes + chunk_charge &&
          f.deadline() == Clock::time_point::max(),
          "deadline did not publish the cumulative released credit");
    // Leave the earlier CREDIT queued: a later update replaces it by key.
    for (int i = 0; i < 2; ++i)
    {
        auto next = f.receive();
        next.reset();
        f.complete();
    }
    f.poll(f.deadline());
    check(f.upstream->credits.size() == 1 && f.credit() == test_flow::window_bytes + 3 * chunk_charge,
          "pending CREDIT did not coalesce to the latest absolute limit");
    const auto published = f.credit();
    f.poll(deadline + 1ms);
    check(f.credit() == published, "already published credit was counted twice");
}

void threshold_publishes_before_batch()
{
    CreditFixture f;
    f.feed(threshold_frames);
    Clock::time_point first_deadline;
    for (std::size_t i = 1; i <= threshold_frames; ++i)
    {
        auto data = f.receive();
        data.reset();
        f.complete();
        if (i == 1) first_deadline = f.deadline();
        if (i < threshold_frames)
        {
            check(f.upstream->credits.empty(), "sub-threshold release published immediately");
            check(f.deadline() == first_deadline, "later release changed the shared batch deadline");
        }
    }
    check(f.credit() == test_flow::window_bytes + threshold_frames * chunk_charge,
          "W/4 release did not preempt the merge timer");
    // Another Flow may still need this node-wide batch. An empty batch clears it.
    check(f.deadline() == first_deadline, "immediate update changed the shared batch deadline");
    f.upstream->credits.clear();
    f.poll(first_deadline);
    check(f.deadline() == Clock::time_point::max() && f.upstream->credits.empty(),
          "threshold update was published again or left periodic idle scanning");
}

void flows_share_one_batch(bool close_first)
{
    CreditFixture f;
    f.feed(threshold_frames);
    auto first = f.receive();
    first.reset();
    f.complete();
    const auto deadline = f.deadline();

    // Opposite DATA direction on another endpoint, released after the batch starts.
    lnk::FlowSpec spec{11, 7, RelayProtocol::Tcp};
    spec.next = 1;
    spec.next_peer = "upstream";
    f.protocol->prepare_flow(std::move(spec));
    f.protocol->commit_flow(11);
    const auto other = test_access::FlowProtocol_flows_(*f.protocol).at(11);
    auto endpoint = f.protocol->bind_stream_flow(7, 11, false, f.io.get_executor());
    f.upstream->credits.clear();
    const auto pool = test_access::FlowProtocol_payload_pool_(*f.protocol);
    test_access::FlowProtocol_stream_(*f.protocol).receive(other,
        lnk::Frame{{LnkFrType::Data, true, chunk_size, 7, 11}, lnk::PooledBuffer(pool, chunk_size)});
    auto received = asio::co_spawn(f.io, endpoint->receive(), asio::use_future);
    f.pump();
    check(received.wait_for(0s) == std::future_status::ready, "second Flow did not receive DATA");
    auto payload = received.get();
    payload.reset();
    endpoint->complete_receive();
    f.pump();
    check(f.deadline() == deadline, "second Flow created its own credit deadline");
    check(f.upstream->credits.empty(), "second Flow published before the node batch");

    // An immediate refresh or close must preserve the other Flow's batch.
    if (close_first)
    {
        f.protocol->close_flow(10);
        f.pump();
    }
    else
    {
        for (std::size_t i = 1; i < threshold_frames; ++i)
        {
            auto data = f.receive();
            data.reset();
            f.complete();
        }
        check(f.credit() == test_flow::window_bytes + threshold_frames * chunk_charge,
              "immediate refresh did not publish the first Flow");
    }
    check(f.deadline() == deadline, "one Flow cancelled another Flow's batch");
    f.upstream->credits.clear();
    f.poll(deadline - 1ns);
    check(f.upstream->credits.empty(), "batch ran before its shared deadline");
    f.poll(deadline);
    check(f.upstream->credits.size() == 1 && f.credit(11, true) == test_flow::window_bytes + chunk_charge,
          "batch missed the later Flow or republished an already refreshed Flow");
    check(f.deadline() == Clock::time_point::max(), "completed batch kept a periodic timer alive");
    f.upstream->credits.clear();
    f.poll(deadline + 1ms);
    check(f.upstream->credits.empty(), "idle poll repeated credit or the initial window");
}

void full_data_window_retains_one_fin_slot()
{
    CreditFixture f(false); // All frames must fit before any endpoint can dequeue them.
    constexpr auto data_frames = test_flow::window_bytes / lnk::Frame::buffer_overhead;
    f.feed(data_frames, 0);
    for (int duplicate = 0; duplicate < 2; ++duplicate)
    {
        test_access::FlowProtocol_stream_(*f.protocol).receive(f.flow,
            lnk::Frame{{LnkFrType::Fin, false, 0, 7, 10}});
    }
    check(f.flow->phase == lnk::FlowState::Phase::Active &&
          f.direction().received_bytes == test_flow::window_bytes &&
          f.direction().buffered_bytes == test_flow::window_bytes + lnk::Frame::buffer_overhead,
          "full DATA window did not retain exactly one FIN position");
    f.endpoint = f.protocol->bind_stream_flow(7, 10, true, f.io.get_executor());
    f.pump();
    for (std::size_t index = 0; index < data_frames; ++index)
    {
        auto data = f.receive();
        check(data && data->size() == 0, "FIN overtook empty DATA");
        data.reset();
        f.complete();
        if (index == 0)
        {
            check(f.credit() == test_flow::window_bytes + lnk::Frame::buffer_overhead &&
                  f.deadline() == Clock::time_point::max(),
                  "exhausted window waited for a small-release deadline");
        }
    }
    check(!f.receive(), "FIN was lost behind a full DATA window");
    check(f.direction().buffered_bytes == 0 && f.direction().released_bytes == test_flow::window_bytes,
          "FIN created DATA credit or retained buffer charge");
}

void fin_and_close()
{
    {
        CreditFixture f;
        test_access::FlowProtocol_stream_(*f.protocol).receive(f.flow,
            lnk::Frame{{LnkFrType::Fin, false, 0, 7, 10}});
        f.pump();
        check(!f.receive(), "FIN did not become EOF");
        check(f.direction().buffered_bytes == 0 && f.direction().released_bytes == 0 &&
              f.deadline() == Clock::time_point::max() && f.upstream->credits.empty(),
              "FIN created DATA credit or a merge deadline");
    }
    {
        CreditFixture f;
        f.feed(1);
        auto data = f.receive();
        data.reset();
        f.complete();
        check(f.deadline() != Clock::time_point::max(), "close fixture did not arm credit");
        const auto released = f.direction().released_bytes;
        f.protocol->close_flow(10);
        f.pump();
        f.endpoint->complete_receive(); // Late downstream notification after close.
        f.pump();
        const auto deadline = f.deadline();
        f.poll(deadline);
        check(f.deadline() == Clock::time_point::max() &&
              f.upstream->credits.empty() && f.direction().released_bytes == released &&
              !test_access::FlowProtocol_flows_(*f.protocol).contains(10),
              "close or late completion published stale credit");
    }
}

void link_close()
{
    CreditFixture f(false);
    auto downstream = std::make_shared<lnk::NodeLink>(f.io.get_executor(),
        lnk::LinkSpec{2, 7, RelayProtocol::Tcp, "downstream", "127.0.0.3", "test", 0, 0, false});
    downstream->phase = lnk::NodeLink::Phase::Ready;
    test_access::LnkChannel_connections_(f.protocol->links()).emplace(2, downstream);
    lnk::FlowSpec transit{20, 7, RelayProtocol::Tcp};
    transit.previous = 1;
    transit.previous_peer = "upstream";
    transit.next = 2;
    transit.next_peer = "downstream";
    f.protocol->prepare_flow(std::move(transit));
    f.protocol->commit_flow(20);
    lnk::FlowSpec healthy{30, 7, RelayProtocol::Tcp};
    healthy.previous = 2;
    healthy.previous_peer = "downstream";
    f.protocol->prepare_flow(std::move(healthy));
    f.protocol->commit_flow(30);
    const auto &flows = test_access::FlowProtocol_flows_(*f.protocol);
    const auto flow = flows.at(20);
    const auto survivor = flows.at(30);
    auto pool = std::make_shared<std::pmr::synchronized_pool_resource>();
    const std::weak_ptr<std::pmr::synchronized_pool_resource> payloads = pool;
    // Hold both directions of a transit Flow on its two adjacent Links.
    f.upstream->writes.push({flow.get(), true},
        lnk::Frame{{LnkFrType::Data, true, 16, 7, 20}, PooledBuffer(pool, 16), flow});
    downstream->writes.push({flow.get(), false},
        lnk::Frame{{LnkFrType::Data, false, 16, 7, 20}, PooledBuffer(pool, 16), flow});
    downstream->writes.push({survivor.get(), true}, lnk::Frame{{LnkFrType::Fin, true, 0, 7, 30}, {}, survivor});
    f.upstream->controls.push_back(lnk::Frame{{LnkFrType::Ping, false, 0, 7}});
    pool.reset();
    check(!payloads.expired(), "close fixture did not retain queued payloads");

    f.protocol->links().close(1);
    check(f.upstream->phase == lnk::NodeLink::Phase::Closed &&
          f.upstream->writes.empty() && f.upstream->controls.empty() && f.upstream->credits.empty(),
          "closed Link retained queued traffic");
    check(payloads.expired(), "Link closure retained dependent Flow payloads on an adjacent Link");
    check(!flows.contains(10) && !flows.contains(20) && flows.contains(30),
          "Link closure removed the wrong Flows");
    check(downstream->phase == lnk::NodeLink::Phase::Ready && downstream->writes.size() == 1 &&
          downstream->writes.front().owner == survivor && downstream->credits.size() == 1 &&
          downstream->credits.contains({30, false}),
          "Link closure changed a healthy Flow's queued traffic or credit");
}

void run()
{
    deadline_and_release();
    threshold_publishes_before_batch();
    flows_share_one_batch(false);
    flows_share_one_batch(true);
    full_data_window_retains_one_fin_slot();
    fin_and_close();
    link_close();
}
} // namespace credit_tests

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
    lnk::Frame frame{header, PooledBuffer(std::make_shared<std::pmr::synchronized_pool_resource>(), header.body_length)};
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
        const auto count = static_cast<int>(window / used);
        for (int index = 0; index < count; ++index)
        {
            check(test_flow::try_send_data(channel, 1, false, BytesBuf(32768, static_cast<std::uint8_t>(index))),
                  "local queue rejected a peer-credit burst");
        }
        check(test_flow::try_send_data(channel, 2, false, {99}),
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
            check(test_flow::try_send_data(channel, 1, false, BytesBuf(32768, static_cast<std::uint8_t>(index))),
                  "local writer did not release capacity");
        }
        co_await credit(1, window); // Duplicate absolute limit cannot replenish the window.
        co_await credit(1, window - 1); // Old limit cannot replenish it either.
        co_await credit(2, window);
        auto small = co_await next_data();
        check(small.header.id == 2 && small.payload.size() == 1 && small.payload.data()[0] == 99,
              "credit-starved Flow blocked another Flow on shared Link");
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
        const auto excessive_charge = lnk::Frame::charge_for_payload(excessive.size());
        for (std::size_t index = 0; index <= window / excessive_charge; ++index)
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
        check(test_flow::try_send_data(channel, 2, false, {100}),
              "invalid credit closed shared Link or healthy Flow");
        check((co_await next_data()).header.id == 2, "healthy Flow could not progress after invalid credit");
        auto link = test_access::LnkChannel_connections_(channel->links()).at(1);
        // Stage complete credited bursts without yielding to the physical writer.
        // The peer checks observable DATA/FIN order, independently of queue storage.
        for (const std::uint64_t id : {10, 20, 30})
        {
            install(id);
            test_access::FlowProtocol_stream_(*channel).receive_credit(*link,
                LnkFrameHeader{LnkFrType::Credit, false, 0, 7, id, window});
            for (std::uint8_t index = 0; index < 3; ++index)
            {
                check(test_flow::try_send_data(channel, id, false, BytesBuf(index ? 4097 : 1, index)),
                      "credited burst was rejected");
            }
            check(test_flow::try_finish(channel, id, false), "credited FIN was rejected");
        }
        auto next_business = [&]() -> asio::awaitable<lnk::Frame> {
            for (;;)
            {
                auto frame = co_await read_frame(socket);
                if (frame.header.kind == LnkFrType::Data || frame.header.kind == LnkFrType::Fin)
                {
                    co_return frame;
                }
            }
        };
        for (std::uint8_t index = 0; index < 4; ++index)
        {
            for (const std::uint64_t id : {10, 20, 30})
            {
                const auto frame = co_await next_business();
                check(frame.header.id == id && frame.header.kind == (index == 3 ? LnkFrType::Fin : LnkFrType::Data),
                      "shared writer did not interleave Flows or preserve FIN order");
                check(index == 3 || (frame.payload.size() == (index ? 4097 : 1) && frame.payload.data()[0] == index),
                      "interleaving changed the payload");
            }
        }
        for (const std::uint64_t id : {40, 50})
        {
            install(id);
            test_access::FlowProtocol_stream_(*channel).receive_credit(*link,
                LnkFrameHeader{LnkFrType::Credit, false, 0, 7, id, window});
            check(test_flow::try_send_data(channel, id, false, {0x5a}), "close fixture could not enqueue");
        }
        channel->close_flow(40);
        check((co_await next_business()).header.id == 50, "closing a queued Flow removed or blocked a healthy Flow");
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
        for (std::uint64_t id : {10, 11, 12})
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
                check(co_await test_flow::send_data(nodes.front(), 7, 10, false, payload),
                      "three-hop backpressure closed sender");
            }
            co_await test_flow::finish(nodes.front(), 7, 10, false);
        };
        auto sent = asio::co_spawn(io, send(), asio::use_future);
        auto first = co_await test_flow::receive(nodes.back(), 7, 10, false);
        check(first && first->size() == 32768 && first->front() == 0, "first three-hop payload changed");
        co_await pause(120ms); // End socket would still be writing this delivered frame.
        check(sent.wait_for(0s) != std::future_status::ready, "three-hop sender ignored stalled consumer");
        for (const auto &node : nodes)
        {
            check(test_access::flow_buffered_bytes(*node) <= 4 * (window + lnk::Frame::buffer_overhead),
                  "three-hop queue grew beyond its windows");
        }
        check(co_await test_flow::send_data(nodes.front(), 7, 11, false, {99}),
              "healthy three-hop Flow could not send");
        auto small = co_await test_flow::receive(nodes.back(), 7, 11, false);
        check(small == BytesBuf{99}, "stalled Flow blocked healthy three-hop Flow");
        small = {};
        std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 11))->complete_receive();
        first = {};
        std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 10))->complete_receive();
        for (int index = 1; index < 64; ++index)
        {
            auto frame = co_await test_flow::receive(nodes.back(), 7, 10, false);
            check(frame == BytesBuf(32768, static_cast<std::uint8_t>(index)),
                  "resumed three-hop stream lost or reordered bytes");
            frame = {};
            std::get<std::shared_ptr<lnk::FlowStream>>(co_await test_flow::port(nodes.back(), 7, 10))->complete_receive();
        }
        auto fin = co_await test_flow::receive(nodes.back(), 7, 10, false);
        check(!fin, "three-hop FIN overtook or lost DATA");
        check(sent.wait_for(0s) == std::future_status::ready, "drained sender did not complete");
        sent.get();
        for (const auto &node : nodes)
        {
            check(test_access::flow_buffered_bytes(*node) == 0, "drained three-hop stream retained charge");
        }
        // Each direction exceeds the initial wire window, including empty DATA.
        constexpr std::array<std::size_t, 3> sizes{0, 1, 257};
        constexpr std::size_t frames = 1026;
        constexpr std::uint64_t wire_charge = (frames / 3) * (128 + 384 + 640);
        static_assert(wire_charge > test_flow::window_bytes);
        for (const bool reverse : {false, true})
        {
            const auto source = reverse ? nodes.back() : nodes.front();
            const auto sink = reverse ? nodes.front() : nodes.back();
            const auto sender = std::get<std::shared_ptr<lnk::FlowStream>>(
                co_await test_flow::port(source, 7, 12, reverse));
            const auto receiver = std::get<std::shared_ptr<lnk::FlowStream>>(
                co_await test_flow::port(sink, 7, 12, !reverse));
            auto send_chunks = [&]() -> asio::awaitable<void> {
                for (std::size_t index = 0; index < frames; ++index)
                {
                    const BytesBuf payload(sizes[index % sizes.size()], static_cast<std::uint8_t>(index));
                    auto data = sender->copy_payload(payload);
                    co_await sender->send(std::move(data));
                }
                co_await sender->finish_send();
            };
            auto sent_chunks = asio::co_spawn(io, send_chunks(), asio::use_future);
            for (std::size_t index = 0; index < frames; ++index)
            {
                auto data = co_await receiver->receive();
                check(data && std::ranges::equal(data->bytes(),
                          BytesBuf(sizes[index % sizes.size()], static_cast<std::uint8_t>(index))),
                      "three-hop stream lost, reordered or changed DATA");
                data.reset();
                receiver->complete_receive();
            }
            check(!(co_await receiver->receive()), "stream FIN overtook or lost DATA");
            check(sent_chunks.wait_for(0s) == std::future_status::ready, "stream sender did not drain");
            sent_chunks.get();
            for (const auto &node : nodes)
            {
                const auto flow = test_access::FlowProtocol_flows_(*node).at(12);
                const auto &direction = flow->stream->directions[std::size_t(reverse)];
                if (reverse ? flow->previous : flow->next)
                {
                    check(direction.sent_bytes == wire_charge, "sender charged incorrect DATA wire credit");
                }
                if (reverse ? flow->next : flow->previous)
                {
                    check(direction.received_bytes == wire_charge && direction.released_bytes == wire_charge,
                          "receiver returned different credit for DATA");
                }
                check(test_access::flow_buffered_bytes(*node) == 0, "stream retained local memory charge");
            }
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
        stream_admission_tests();
        flow_capacity_tests();
        flow_churn_tests();
        datagram_admission_tests();
        credit_tests::run();
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
