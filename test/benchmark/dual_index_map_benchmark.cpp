#include "dualindex_map.h"
#include "std_dual_index_map.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>

namespace
{
using Clock = std::chrono::steady_clock;
constexpr std::uint64_t entry_count = 200000;

template <class Map> std::pair<std::chrono::milliseconds, std::uint64_t> run_workload()
{
    const auto start = Clock::now();
    Map values;
    std::uint64_t checksum = 0;

    for (std::uint64_t key = 0; key < entry_count; ++key)
        values.insert(key, key + 1, key * 3);

    for (int pass = 0; pass < 4; ++pass)
    {
        for (std::uint64_t key = 0; key < entry_count; ++key)
        {
            if (const auto value = values.find_primary(key))
                checksum += *value;
        }
    }

    for (std::uint64_t key = 0; key < entry_count; key += 2)
        values.set_secondary(key, entry_count + key + 1);

    for (std::uint64_t key = 1; key < entry_count; key += 3)
        values.erase_primary(key);

    checksum += values.size();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
    return {elapsed, checksum};
}
} // namespace

int main()
{
    using Reference = StdDualIndexMap<std::uint64_t, std::uint64_t, std::uint64_t, SecondaryKeyMode::Unique>;
    using Candidate = DualIndexMap<std::uint64_t, std::uint64_t, std::uint64_t, SecondaryKeyMode::Unique>;

    const auto [reference_time, reference_checksum] = run_workload<Reference>();
    const auto [candidate_time, candidate_checksum] = run_workload<Candidate>();
    if (reference_checksum != candidate_checksum)
    {
        std::cerr << "Workload checksum mismatch\n";
        return 1;
    }

    std::cout << "entries=" << entry_count << '\n'
              << "std containers: " << reference_time.count() << " ms\n"
              << "intrusive hash: " << candidate_time.count() << " ms\n";
    if (candidate_time.count())
    {
        std::cout << std::fixed << std::setprecision(2)
                  << "speed ratio: " << static_cast<double>(reference_time.count()) / candidate_time.count() << "x\n";
    }
    return 0;
}
