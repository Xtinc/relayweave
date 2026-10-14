#pragma once

#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace bench
{
using Clock = std::chrono::steady_clock;
using Json = nlohmann::json;

inline void require(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

struct Timer
{
    Clock::time_point wall = Clock::now();
    std::clock_t cpu = std::clock();
};

// Shared output only; workloads and their setup remain in each component benchmark.
class Report
{
  public:
    std::size_t iterations;
    unsigned repeats = 3;

    Report(int argc, char **argv, std::size_t default_iterations) : iterations(default_iterations)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string_view option(argv[i]);
            if (option == "--help")
            {
                std::cout << BENCHMARK_NAME << " [--iterations N] [--repeats N] [--output FILE.json]\n"
                          << "Default iterations: " << iterations << ", repeats: " << repeats << '\n';
                std::exit(0);
            }
            require(i + 1 < argc, "option requires a value");
            const std::string value(argv[++i]);
            if (option == "--output")
                output_ = value;
            else
            {
                std::size_t consumed = 0;
                const auto number = std::stoull(value, &consumed);
                require(consumed == value.size() && number > 0 && number <= 1000000, "invalid iteration/repeat count");
                if (option == "--iterations")
                    iterations = number;
                else if (option == "--repeats")
                {
                    require(number <= 20, "repeats must be in [1, 20]");
                    repeats = static_cast<unsigned>(number);
                }
                else
                    throw std::invalid_argument("unknown option: " + std::string(option));
            }
        }
    }

    void add(std::string name, std::uint64_t operations, std::uint64_t payload_bytes, const Timer &start,
             std::uint64_t checksum, Json details = Json::object())
    {
        const auto seconds = std::chrono::duration<double>(Clock::now() - start.wall).count();
        const auto cpu_seconds = static_cast<double>(std::clock() - start.cpu) / CLOCKS_PER_SEC;
        samples_.push_back({{"case", std::move(name)}, {"operations", operations}, {"payload_bytes", payload_bytes},
                            {"seconds", seconds}, {"cpu_seconds", cpu_seconds},
                            {"ns_per_operation", seconds * 1e9 / operations}, {"operations_per_second", operations / seconds},
                            {"payload_mbps", payload_bytes * 8.0 / seconds / 1e6},
                            {"checksum", checksum}, {"details", std::move(details)}});
    }

    void save() const
    {
        if (!output_.parent_path().empty())
            std::filesystem::create_directories(output_.parent_path());
        std::ofstream file(output_);
        require(bool(file), "cannot open benchmark output");
        file << Json{{"schema", 1}, {"target", BENCHMARK_NAME}, {"build", BENCHMARK_BUILD},
                     {"compiler", BENCHMARK_COMPILER}, {"iterations", iterations}, {"repeats", repeats},
                     {"passed", true}, {"samples", samples_}}.dump(2) << '\n';
        file.close();
        require(bool(file), "cannot save benchmark output");
        std::cout << "[PASS] " << BENCHMARK_NAME << ": " << samples_.size() << " samples -> " << output_ << '\n';
    }

  private:
    std::filesystem::path output_ = std::filesystem::path(BENCHMARK_RESULTS_DIR) / (std::string(BENCHMARK_NAME) + ".json");
    Json samples_ = Json::array();
};
} // namespace bench
