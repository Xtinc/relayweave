#include "flow_workload.h"

// Unrestricted multi-hop Flow throughput; vary nodes/flows/chunk size to measure relay cost.
int main(int argc, char **argv)
{
    try
    {
        flow_bench::Options defaults;
        auto options = flow_bench::options(argc, argv, defaults, false);
        asio::io_context io(1);
        flow_bench::Benchmark benchmark(io, std::move(options));
        auto result = asio::co_spawn(io, benchmark.run(), asio::use_future);
        io.run();
        result.get();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
