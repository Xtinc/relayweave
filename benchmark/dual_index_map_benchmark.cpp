#include "dualindex_map.h"
#include "std_dual_index_map.h"
#include "benchmark.h"

#include <cstdint>
#include <iostream>

namespace
{
template <class Map> std::uint64_t run_workload(bench::Report &report, std::string_view implementation)
{
    const auto entry_count = report.iterations;
    Map values;
    std::uint64_t checksum = 0;
    const std::string prefix = "index/" + std::string(implementation) + "/";
    bench::Timer timer;

    for (std::uint64_t key = 0; key < entry_count; ++key)
        values.insert(key, key + 1, key * 3);
    report.add(prefix + "insert", entry_count, 0, timer, values.size());

    timer = {};
    for (int pass = 0; pass < 4; ++pass)
    {
        for (std::uint64_t key = 0; key < entry_count; ++key)
        {
            const auto value = values.find_primary(key);
            bench::require(value && *value == key * 3, "index lookup failed");
            checksum += *value;
        }
    }
    report.add(prefix + "lookup", 4 * entry_count, 0, timer, checksum);

    timer = {};
    for (std::uint64_t key = 0; key < entry_count; key += 2)
        values.set_secondary(key, entry_count + key + 1);
    report.add(prefix + "secondary_update", (entry_count + 1) / 2, 0, timer, values.size());

    timer = {};
    for (std::uint64_t key = 1; key < entry_count; key += 3)
        values.erase_primary(key);
    report.add(prefix + "erase", (entry_count + 1) / 3, 0, timer, values.size());

    checksum += values.size();
    bench::require(values.size() == entry_count - (entry_count + 1) / 3, "index erase count wrong");
    return checksum;
}
} // namespace

int main(int argc, char **argv)
{
    using Reference = StdDualIndexMap<std::uint64_t, std::uint64_t, std::uint64_t, SecondaryKeyMode::Unique>;
    using Candidate = DualIndexMap<std::uint64_t, std::uint64_t, std::uint64_t, SecondaryKeyMode::Unique>;

    try
    {
        bench::Report report(argc, argv, 200000);
        bench::require(report.iterations >= 2, "index benchmark needs at least two entries");
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            const auto reference_checksum = run_workload<Reference>(report, "std");
            const auto candidate_checksum = run_workload<Candidate>(report, "intrusive");
            bench::require(reference_checksum == candidate_checksum, "index workload checksum mismatch");
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
