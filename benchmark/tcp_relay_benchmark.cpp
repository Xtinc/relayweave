#include "relay_workload.h"

namespace
{
using namespace relay_bench;

void direct(bench::Report &report, std::size_t size)
{
    asio::io_context io(1);
    tcp::socket left(io), right(io);
    connect(left, right);
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer timer;
        co_await (business(left, report.iterations, size, 0) && business(right, report.iterations, size, 1));
        report.add("direct/tcp/" + std::to_string(size), report.iterations * 2, report.iterations * size * 2,
                   timer, report.iterations * size * 2,
                   {{"chunk_bytes", size}, {"directions", 2}, {"includes_setup", false}});
    };
    run(io, task());
}

void tcp_relay(bench::Report &report, std::size_t size)
{
    asio::io_context io(1);
    tcp::socket left_client(io), left_server(io), right_client(io), right_server(io);
    connect(left_client, left_server);
    connect(right_client, right_server);
    TrafficCounter outgoing, incoming;
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer timer;
        co_await (relay_tcp(left_server, right_server, nullptr, nullptr, &outgoing, &incoming) &&
                  business(left_client, report.iterations, size, 0) && business(right_client, report.iterations, size, 1));
        timer.stop();
        require(outgoing.total() == report.iterations * size && incoming.total() == report.iterations * size,
                "TCP relay counters lost payload bytes");
        report.add("relay/tcp/" + std::to_string(size), report.iterations * 2, report.iterations * size * 2,
                   timer, outgoing.total() + incoming.total(),
                   {{"chunk_bytes", size}, {"directions", 2}, {"includes_setup", false}});
    };
    run(io, task());
}

} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 4096);
        for (unsigned i = 0; i < report.repeats; ++i)
            for (const auto size : {1400u, 32768u})
            {
                direct(report, size);
                tcp_relay(report, size);
            }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
