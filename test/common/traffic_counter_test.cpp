#include "xfr_channel.h"

#include <array>
#include <atomic>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>

namespace
{
void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

void sampling_and_saturation()
{
    TrafficCounter traffic;
    require(traffic.total() == 0 && traffic.take_interval() == 0, "New counter was not empty");
    traffic.add(6);
    traffic.add(0);
    traffic.add(4);
    require(traffic.total() == 10, "Total did not accumulate transferred bytes");
    require(traffic.take_interval() == 10 && traffic.take_interval() == 0,
            "Sampling did not reset just the interval");
    require(traffic.total() == 10, "Sampling changed lifetime traffic");

    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    traffic.add(static_cast<std::size_t>(maximum));
    traffic.add(1);
    require(traffic.total() == maximum && traffic.take_interval() == maximum,
            "Traffic totals or intervals wrapped around on overflow");
    traffic.add(3);
    require(traffic.total() == maximum && traffic.take_interval() == 3,
            "Saturated lifetime total prevented new interval sampling");
}

void concurrent_add_and_sample()
{
    TrafficCounter traffic;
    std::atomic<unsigned> completed{};
    std::array<std::jthread, 4> writers;
    constexpr unsigned iterations = 10000;
    constexpr std::size_t bytes = 7;
    for (auto &writer : writers)
    {
        writer = std::jthread([&] {
            for (unsigned i = 0; i < iterations; ++i)
                traffic.add(bytes);
            completed.fetch_add(1);
        });
    }
    std::uint64_t sampled = 0;
    while (completed.load() != writers.size())
    {
        sampled += traffic.take_interval();
        std::this_thread::yield();
    }
    for (auto &writer : writers)
        writer.join();
    sampled += traffic.take_interval();
    constexpr auto expected = writers.size() * iterations * bytes;
    require(traffic.total() == expected && sampled == expected,
            "Concurrent accounting or sampling lost/doubled transferred bytes");
    require(traffic.take_interval() == 0, "Sampling retained bytes from earlier intervals");
}
} // namespace

int main()
{
    try
    {
        sampling_and_saturation();
        concurrent_add_and_sample();
        std::cout << "[PASS] TrafficCounter sampling, saturation and concurrent writers\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
