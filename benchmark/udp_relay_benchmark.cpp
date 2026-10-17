#include "relay_workload.h"
#include <asio/experimental/parallel_group.hpp>
#include <array>
#include <cstring>
#include <vector>

namespace
{
using namespace relay_bench;

struct Sockets
{
    asio::io_context io{1};
    const udp::endpoint loopback{asio::ip::address_v4::loopback(), 0};
    udp::socket local{io, loopback}, transfer{io, loopback};
    udp::socket local_peer{io, loopback}, transfer_peer{io, loopback};
    const DatagramHeader::Buffer header = DatagramHeader::encode(1);

    Sockets()
    {
        local.connect(local_peer.local_endpoint());
        local_peer.connect(local.local_endpoint());
        transfer.connect(transfer_peer.local_endpoint());
        transfer_peer.connect(transfer.local_endpoint());
    }

    void run(asio::awaitable<void> traffic)
    {
        auto task = [&]() -> asio::awaitable<void> {
            const auto [order, relay_error, traffic_error] = co_await asio::experimental::make_parallel_group(
                asio::co_spawn(io, relay_udp_connected(local, transfer, header), asio::deferred),
                asio::co_spawn(io, std::move(traffic), asio::deferred)
            ).async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
            if (traffic_error) std::rethrow_exception(traffic_error);
            if (order[0] == 0)
            {
                if (relay_error) std::rethrow_exception(relay_error);
                throw std::runtime_error("UDP relay exited before traffic completed");
            }
        };
        relay_bench::run(io, task());
    }
};

BytesBuf packet(std::size_t size, unsigned direction, std::uint64_t sequence)
{
    BytesBuf result(size, static_cast<std::uint8_t>(0x5a + direction));
    std::memcpy(result.data(), &sequence, sizeof(sequence));
    return result;
}

void roundtrip(bench::Report &report)
{
    Sockets sockets;
    auto traffic = [&]() -> asio::awaitable<void> {
        BytesBuf wire(1400 + sockets.header.size()), returned(1400);
        std::vector<double> latency;
        latency.reserve(report.iterations);
        bench::Timer timer;
        for (std::uint64_t sequence = 0; sequence < report.iterations; ++sequence)
        {
            const auto data = packet(1400, 0, sequence);
            const auto began = bench::Clock::now();
            co_await sockets.local_peer.async_send(asio::buffer(data), asio::use_awaitable);
            const auto size = co_await sockets.transfer_peer.async_receive(asio::buffer(wire), asio::use_awaitable);
            require(size == wire.size() && std::equal(sockets.header.begin(), sockets.header.end(), wire.begin()) &&
                    std::equal(data.begin(), data.end(), wire.begin() + sockets.header.size()), "UDP forward packet changed");
            co_await sockets.transfer_peer.async_send(asio::buffer(wire), asio::use_awaitable);
            const auto received = co_await sockets.local_peer.async_receive(asio::buffer(returned), asio::use_awaitable);
            require(received == data.size() && returned == data, "UDP reverse packet changed");
            latency.push_back(std::chrono::duration<double, std::micro>(bench::Clock::now() - began).count());
        }
        timer.stop();
        std::sort(latency.begin(), latency.end());
        auto percentile = [&](std::size_t percent) { return latency[(latency.size() * percent + 99) / 100 - 1]; };
        report.add("relay/udp/roundtrip", report.iterations, report.iterations * 1400 * 2, timer, report.iterations,
                   {{"packet_bytes", 1400}, {"in_flight", 1}, {"rtt_p50_us", percentile(50)},
                    {"rtt_p95_us", percentile(95)}, {"rtt_p99_us", percentile(99)}});
    };
    sockets.run(traffic());
}

void saturation(bench::Report &report)
{
    Sockets sockets;
    struct Direction
    {
        std::vector<bool> seen;
        std::uint64_t received = 0, reordered = 0, highest = 0;
    };
    std::array<Direction, 2> directions;
    for (auto &direction : directions) direction.seen.resize(report.iterations);
    bool draining = false;
    bench::Timer timer;
    double injection_seconds = 0;
    auto send = [&](udp::socket &socket, unsigned direction) -> asio::awaitable<void> {
        auto data = packet(1400, direction, 0);
        BytesBuf wire;
        if (direction) wire.assign(sockets.header.begin(), sockets.header.end());
        wire.insert(wire.end(), data.begin(), data.end());
        for (std::uint64_t sequence = 0; sequence < report.iterations; ++sequence)
        {
            std::memcpy(wire.data() + (direction ? sockets.header.size() : 0), &sequence, sizeof(sequence));
            require(co_await socket.async_send(asio::buffer(wire), asio::use_awaitable) == wire.size(),
                    "truncated UDP injection");
        }
    };
    auto receive = [&](udp::socket &socket, unsigned direction) -> asio::awaitable<void> {
        BytesBuf wire(1400 + sockets.header.size() + 1);
        auto &stats = directions[direction];
        const auto expected = packet(1400, direction, 0);
        for (;;)
        {
            const auto [error, size] = co_await socket.async_receive(asio::buffer(wire), asio::as_tuple(asio::use_awaitable));
            if (error == asio::error::operation_aborted && draining) co_return;
            if (error) throw asio::system_error(error);
            const auto prefix = direction ? 0 : sockets.header.size();
            require(size == 1400 + prefix, "UDP saturation payload size changed");
            require(direction || std::equal(sockets.header.begin(), sockets.header.end(), wire.begin()), "UDP header changed");
            std::uint64_t sequence;
            std::memcpy(&sequence, wire.data() + prefix, sizeof(sequence));
            require(sequence < report.iterations && !stats.seen[sequence], "UDP invalid or duplicate sequence");
            require(std::equal(expected.begin() + 8, expected.end(), wire.begin() + prefix + 8), "UDP saturation payload changed");
            stats.seen[sequence] = true;
            if (stats.received && sequence < stats.highest) ++stats.reordered;
            stats.highest = std::max(stats.highest, sequence);
            ++stats.received;
        }
    };
    auto inject_and_drain = [&]() -> asio::awaitable<void> {
        co_await (send(sockets.local_peer, 0) && send(sockets.transfer_peer, 1));
        injection_seconds = std::chrono::duration<double>(bench::Clock::now() - timer.wall).count();
        asio::steady_timer drain(sockets.io, 200ms);
        co_await drain.async_wait(asio::use_awaitable);
        draining = true;
        sockets.local_peer.cancel();
        sockets.transfer_peer.cancel();
    };
    auto traffic = [&]() -> asio::awaitable<void> {
        timer = bench::Timer{};
        co_await (inject_and_drain() && receive(sockets.transfer_peer, 0) && receive(sockets.local_peer, 1));
        timer.stop();
        const auto received = directions[0].received + directions[1].received;
        require(directions[0].received > 0 && directions[1].received > 0, "UDP relay made no progress");
        const auto offered = report.iterations * 2;
        report.add("relay/udp/saturation", received, received * 1400, timer, received,
                   {{"packet_bytes", 1400}, {"directions", 2}, {"offered_packets", offered},
                    {"received_packets", received}, {"lost_packets", offered - received},
                    {"loss_pct", (offered - received) * 100.0 / offered},
                    {"received_by_direction", {directions[0].received, directions[1].received}},
                    {"reordered_packets", directions[0].reordered + directions[1].reordered},
                    {"injection_seconds", injection_seconds}, {"offered_pps", offered / injection_seconds},
                    {"drain_ms", 200}, {"measurement_includes_drain", true}});
    };
    sockets.run(traffic());
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 65536);
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            roundtrip(report);
            saturation(report);
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
