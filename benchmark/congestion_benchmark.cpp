#include "flow_workload.h"

// Credit backpressure under bandwidth steps, recovery and an optional stalled consumer.
int main(int argc, char **argv)
{
    try
    {
        flow_bench::Options defaults;
        defaults.rates = {1000, 100, 500, 1000};
        auto options = flow_bench::options(argc, argv, defaults, true);
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
