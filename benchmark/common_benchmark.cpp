#include "benchmark.h"
#include "async_event.h"
#include "lnk_channel.h"
#include "lru_cache.h"
#include "xfr_channel.h"
#include <asio/experimental/concurrent_channel.hpp>
#include <future>
#include <limits>
#include <thread>

namespace
{
using bench::require;
using namespace std::chrono_literals;

class CountingResource : public std::pmr::memory_resource
{
  public:
    std::size_t allocations = 0;
  private:
    void *do_allocate(std::size_t size, std::size_t alignment) override
    {
        ++allocations;
        return std::pmr::new_delete_resource()->allocate(size, alignment);
    }
    void do_deallocate(void *p, std::size_t size, std::size_t alignment) override
    {
        std::pmr::new_delete_resource()->deallocate(p, size, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override { return this == &other; }
};

void buffers(bench::Report &report)
{
    for (const std::size_t size : {1400, 4096, 32768, 65467})
    {
        CountingResource upstream;
        auto pool = std::make_shared<lnk::PayloadPool>(16 * 1024 * 1024, &upstream);
        // Prewarm 100 simultaneously live blocks, as in the expected Flow population.
        {
            std::vector<lnk::PooledBuffer> warm;
            for (int i = 0; i < 100; ++i)
                warm.emplace_back(pool, size);
        }
        const auto warmed = upstream.allocations;
        std::uint64_t checksum = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            lnk::PooledBuffer data(pool, size);
            data.bytes().front() = static_cast<std::uint8_t>(i);
            const auto address = data.data();
            lnk::PooledBuffer moved(std::move(data));
            require(moved.data() == address && !data.data(), "pool move copied storage");
            checksum += moved.bytes().front();
        }
        report.add("pool/reuse/" + std::to_string(size), report.iterations, 0, timer, checksum,
                   {{"block_bytes", size}, {"warmup_allocations", warmed}, {"steady_allocations", upstream.allocations - warmed}});
        require(upstream.allocations == warmed, "warmed pool kept allocating");

        const BytesBuf source(size, 0x5a);
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            lnk::PooledBuffer data(pool, source);
            checksum += data.bytes().front() + data.bytes().back();
        }
        report.add("pool/copy/" + std::to_string(size), report.iterations, report.iterations * size, timer, checksum,
                   {{"steady_allocations", upstream.allocations - warmed}});
        require(checksum == report.iterations * 180 && upstream.allocations == warmed, "pooled copy changed data or allocated");
    }
}

void codecs(bench::Report &report)
{
    std::uint64_t checksum = 0;
    bench::Timer timer;
    for (std::size_t i = 1; i <= report.iterations; ++i)
    {
        const LnkFrameHeader frame{LnkFrType::Data, bool(i % 2), 32768, 1, i};
        const auto parsed = LnkFrameHeader::decode(frame.encode());
        require(parsed.id == i && parsed.body_length == 32768, "link frame roundtrip failed");
        checksum += parsed.id;
    }
    report.add("codec/link_header", report.iterations, 0, timer, checksum, {{"wire_bytes", LnkFrameHeader::length}});
    checksum = 0;
    timer = {};
    for (std::size_t i = 1; i <= report.iterations; ++i)
    {
        const auto decoded = DatagramHeader::decode(DatagramHeader::encode(i));
        require(decoded && *decoded == i, "datagram header roundtrip failed");
        checksum += *decoded;
    }
    report.add("codec/datagram_header", report.iterations, 0, timer, checksum);

    for (const std::size_t body_size : {256, 128 * 1024})
    {
        const CtrlMessage message(CtrlCommand::TopologySnapshot, njson{{"value", std::string(body_size, 'x')}});
        const auto count = std::max<std::size_t>(1, report.iterations / (body_size > 1024 ? 1000 : 100));
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < count; ++i)
        {
            const auto wire = WireMessage::pack(message);
            MessageReceiver receiver;
            std::optional<CtrlMessage> received;
            std::size_t offset = 0;
            while (offset < wire.size())
            {
                const auto header = std::span(wire).subspan(offset, WireMessage::header_length);
                const auto length = WireMessage::decode_length(std::span<const std::uint8_t, 4>(header));
                offset += WireMessage::header_length;
                received = receiver.receive(std::span(wire).subspan(offset, length));
                offset += length;
            }
            require(received && received->command == message.command && received->params == message.params,
                    "control message reassembly failed");
            checksum += wire.size();
        }
        report.add("codec/control_roundtrip/" + std::to_string(body_size), count, count * body_size, timer, checksum,
                   {{"fragmented", body_size > WireMessage::max_payload_length}});
    }
}

void caches(bench::Report &report)
{
    for (const std::size_t capacity : {100, 10000})
    {
        LRUCache<std::uint64_t, std::uint64_t> cache(capacity, 1h);
        const auto now = bench::Clock::now();
        for (std::size_t i = 0; i < capacity; ++i)
            cache.put(i, i + 1, now);
        std::uint64_t checksum = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            const auto value = cache.get(i % capacity, now);
            require(value && *value == i % capacity + 1, "cache lost warmed entry");
            checksum += *value;
        }
        report.add("cache/hit/" + std::to_string(capacity), report.iterations, 0, timer, checksum);
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            cache.put(capacity + i, i + 1, now);
            const auto value = cache.get(capacity + i, now);
            require(value && *value == i + 1, "cache churn lost entry");
            checksum += *value;
        }
        report.add("cache/eviction/" + std::to_string(capacity), report.iterations, 0, timer, checksum);
        require(!cache.get(0, now), "LRU did not evict oldest entry");
    }
}

void counters(bench::Report &report)
{
    for (const unsigned workers : {1, 4})
    {
        TrafficCounter counter;
        bench::Timer timer;
        const auto work = [&] {
            for (std::size_t i = 0; i < report.iterations; ++i)
                counter.add(32768);
        };
        if (workers == 1)
            work();
        else
        {
            std::vector<std::thread> threads;
            for (unsigned i = 0; i < workers; ++i)
                threads.emplace_back(work);
            for (auto &thread : threads)
                thread.join();
        }
        report.add("counter/add/" + std::to_string(workers), report.iterations * workers, 0, timer, counter.total(),
                   {{"workers", workers}, {"includes_thread_start", workers > 1}});
        const auto expected = report.iterations * workers * 32768ULL;
        require(counter.total() == expected && counter.take_interval() == expected && counter.take_interval() == 0,
                "counter lost bytes");
    }
}

asio::awaitable<void> limited(bench::Report &report)
{
    TokenBucket unlimited;
    bench::Timer timer;
    for (std::size_t i = 0; i < report.iterations; ++i)
        co_await unlimited.consume(32768);
    report.add("limiter/consume_unlimited", report.iterations, 0, timer, report.iterations);
    for (const std::uint64_t rate : {12500000, 125000000})
    {
        TokenBucket limiter(rate, 32768);
        co_await limiter.consume(32768); // Remove the initial burst before timing.
        const auto count = std::min<std::size_t>(128, report.iterations);
        timer = {};
        for (std::size_t i = 0; i < count; ++i)
            co_await limiter.consume(32768);
        report.add("limiter/consume/" + std::to_string(rate * 8 / 1000000), count, count * 32768, timer, count,
                   {{"target_mbps", rate * 8 / 1000000}, {"expected_seconds", count * 32768.0 / rate}});
    }
}

void limiters(bench::Report &report)
{
    for (const bool admit : {false, true})
    {
        TokenBucket limiter(1, admit ? report.iterations * 1400ULL : 0);
        limiter.reserve(0, bench::Clock::now());
        std::uint64_t admitted = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < report.iterations; ++i)
            admitted += limiter.try_consume(1400);
        report.add(admit ? "limiter/try_admit" : "limiter/try_drop", report.iterations, 0, timer, admitted);
        require(admitted == (admit ? report.iterations : 0), "limiter admission wrong");
    }
    TokenBucket debt(125000000, 0);
    const auto now = bench::Clock::now();
    auto deadline = now;
    bench::Timer timer;
    for (std::size_t i = 0; i < report.iterations; ++i)
        deadline = debt.reserve(1400, now);
    const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now).count();
    report.add("limiter/reserve_debt", report.iterations, 0, timer, nanoseconds);
    require(nanoseconds >= static_cast<std::int64_t>(report.iterations * 11200ULL), "limiter lost debt");
    asio::io_context io(1);
    auto result = asio::co_spawn(io, limited(report), asio::cancel_after(30s, asio::use_future));
    io.run();
    result.get();
}

void channels(bench::Report &report)
{
    asio::io_context io(1);
    asio::experimental::concurrent_channel<void(asio::error_code, lnk::PooledBuffer)> queue(io, 64);
    CountingResource upstream;
    auto pool = std::make_shared<lnk::PayloadPool>(8 * 1024 * 1024, &upstream);
    {
        std::vector<lnk::PooledBuffer> warm;
        for (int i = 0; i < 66; ++i)
            warm.emplace_back(pool, 32768);
    }
    const auto warmed = upstream.allocations;
    auto producer = [&]() -> asio::awaitable<void> {
        for (std::uint64_t i = 1; i <= report.iterations; ++i)
        {
            lnk::PooledBuffer data(pool, 32768);
            std::memcpy(data.data(), &i, sizeof(i));
            co_await queue.async_send(asio::error_code{}, std::move(data), asio::use_awaitable);
        }
    };
    std::uint64_t checksum = 0;
    auto consumer = [&]() -> asio::awaitable<void> {
        for (std::uint64_t i = 1; i <= report.iterations; ++i)
        {
            auto data = co_await queue.async_receive(asio::use_awaitable);
            std::uint64_t sequence = 0;
            std::memcpy(&sequence, data.data(), sizeof(sequence));
            require(sequence == i, "channel reordered pooled payloads");
            checksum += sequence;
        }
    };
    bench::Timer timer;
    auto result = asio::co_spawn(io, await_transfers(producer(), consumer()), asio::cancel_after(30s, asio::use_future));
    io.run();
    result.get();
    report.add("channel/pooled_handoff", report.iterations, 0, timer, checksum,
               {{"capacity", 64}, {"block_bytes", 32768}, {"steady_payload_allocations", upstream.allocations - warmed}});
    require(upstream.allocations == warmed, "channel handoff allocated extra payload storage");
}

void broadcasts(bench::Report &report)
{
    asio::io_context io(1);
    for (const std::size_t fanout : {1, 100})
    {
        const auto rounds = std::max<std::size_t>(1, report.iterations / 1000);
        std::uint64_t notified = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < rounds; ++i)
        {
            AsyncEvent event(io.get_executor());
            std::vector<std::future<bool>> waiters;
            io.restart();
            for (std::size_t j = 0; j < fanout; ++j)
                waiters.push_back(asio::co_spawn(io, event.wait(), asio::use_future));
            io.poll();
            event.notify_all();
            io.run();
            for (auto &waiter : waiters)
            {
                require(waiter.get(), "event failed to notify registered waiter");
                ++notified;
            }
        }
        report.add("event/broadcast/" + std::to_string(fanout), rounds * fanout, 0, timer, notified,
                   {{"includes_registration", true}, {"fanout", fanout}});
    }
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 100000);
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            buffers(report);
            codecs(report);
            caches(report);
            counters(report);
            limiters(report);
            channels(report);
            broadcasts(report);
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
