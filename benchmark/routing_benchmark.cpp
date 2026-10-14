#include "benchmark.h"
#include "agent_routing.h"
#include "link_quality.h"
#include "route_graph.h"
#include "routing_fixture.h"
#include "topology.h"
#include <cmath>

namespace
{
using bench::require;
using namespace std::chrono_literals;

std::vector<RouteGraph::Link> links(std::size_t nodes, std::size_t degree)
{
    std::vector<RouteGraph::Link> result;
    for (std::size_t from = 0; from < nodes; ++from)
        for (std::size_t offset = 1; offset <= degree; ++offset)
        {
            const auto stride = std::size_t{1} << (offset - 1);
            if (stride < nodes)
                result.push_back({std::to_string(from), std::to_string((from + stride) % nodes), double(offset)});
        }
    return result;
}

void graphs(bench::Report &report)
{
    for (const auto nodes : {32U, 128U, 512U, 1024U})
    {
        const auto edges = links(nodes, 8);
        const std::vector<RouteGraph::Entry> entries{{"0", 0.0}, {std::to_string(nodes / 2), 1.0}};
        std::uint64_t checksum = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            const RouteGraph graph(edges);
            checksum += edges.size();
        }
        report.add("route/build/" + std::to_string(nodes), report.iterations, 0, timer, checksum,
                   {{"nodes", nodes}, {"edges", edges.size()}, {"includes_destruction", true}});
        const RouteGraph graph(edges);
        for (const std::size_t layers : {4, 8})
        {
            const auto expected = graph.shortest_paths(entries, layers);
            require(expected.at("0").cost == 0.0 && expected.at("1").cost == 3.0, "route cost wrong");
            checksum = 0;
            timer = {};
            for (std::size_t i = 0; i < report.iterations; ++i)
            {
                const auto paths = graph.shortest_paths(entries, layers);
                require(paths.size() == expected.size() && paths.at("1").nodes == expected.at("1").nodes,
                        "route search changed deterministic result");
                checksum += paths.size();
            }
            report.add("route/search/" + std::to_string(nodes) + "/" + std::to_string(layers), report.iterations, 0,
                       timer, checksum, {{"nodes", nodes}, {"edges", edges.size()}, {"entries", 2},
                                        {"max_nodes", layers}, {"reachable", expected.size()}});
        }
    }
}

void quality(bench::Report &report)
{
    constexpr std::size_t link_count = 100;
    std::array<LinkQuality, link_count> metrics;
    const auto now = bench::Clock::now();
    const auto count = report.iterations * 1000;
    std::uint64_t accepted = 0;
    bench::Timer timer;
    for (std::size_t i = 0; i < count; ++i)
        accepted += metrics[i % link_count].record((i / link_count) % 20 != 0, 20.0 + i % 40, now + std::chrono::milliseconds(i));
    report.add("quality/record", count, 0, timer, accepted, {{"links", link_count}, {"loss_fraction", 0.05}});
    require(accepted == count, "quality rejected ordered samples");
    std::uint64_t completed = 0;
    timer = {};
    for (std::size_t i = 0; i < count; ++i)
    {
        const auto assessment = metrics[i % link_count].assess(now + std::chrono::milliseconds(count));
        require(std::isfinite(assessment.quality.confidence), "quality assessment is nonfinite");
        completed += assessment.total_completed;
    }
    report.add("quality/assess", count, 0, timer, completed, {{"links", link_count}});
}

void snapshots(bench::Report &report)
{
    asio::io_context io(1);
    for (const auto nodes : {32U, 128U, 512U, 1024U})
    {
        njson snapshot{{"request_id", 1U}, {"epoch", 1U}, {"snapshot_version", 1U}, {"created_age_ms", 0U},
                       {"nodes", njson::array()}, {"links", njson::array()}};
        for (unsigned i = 0; i < nodes; ++i)
            snapshot["nodes"].push_back({{"node_id", std::to_string(i)}, {"address", "192.0.2.1"},
                                         {"report_age_ms", 0U}, {"available", 1.0},
                                         {"control_queue_delay_us", 0U}, {"transfer_tcp_queue_delay_us", 0U},
                                         {"transfer_udp_queue_delay_us", 0U}});
        for (const auto &edge : links(nodes, 8))
            snapshot["links"].push_back({{"source", edge.from}, {"destination", edge.to}, {"age_ms", 0U},
                                         {"rtt_ms", edge.cost}, {"jitter_ms", 0.0}, {"loss_rate", 0.0},
                                         {"transmitted", 100U}, {"received", 100U}, {"completed", 100U},
                                         {"quality", quality_json({edge.cost, 1.0, true})}});
        test_routing::Fixture routing(io, true, 8);
        const auto now = bench::Clock::now();
        bench::Timer timer;
        std::uint64_t checksum = 0;
        for (std::size_t i = 1; i <= report.iterations; ++i)
        {
            snapshot["request_id"] = i;
            snapshot["snapshot_version"] = i;
            routing.begin_request(i, now);
            require(routing.accept_snapshot(snapshot, now), "agent rejected benchmark topology");
            checksum += routing.epoch();
        }
        report.add("agent/snapshot_accept/" + std::to_string(nodes), report.iterations, 0, timer, checksum,
                   {{"nodes", nodes}, {"edges", snapshot["links"].size()}});
        timer = {};
        for (std::size_t i = 0; i < report.iterations; ++i)
            Topology::validate_snapshot(snapshot, "benchmark");
        report.add("topology/snapshot_validate/" + std::to_string(nodes), report.iterations, 0, timer, report.iterations);
        const std::vector<RouteGraph::Entry> entries{{"0", 1.0}, {std::to_string(nodes / 2), 2.0}};
        const auto expected = test_routing::candidates(routing, now, entries);
        require(!expected.empty(), "agent candidate setup yielded no route");
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            const auto paths = routing.candidate_paths(now);
            require(paths.size() == expected.size(), "agent candidate count changed");
            checksum += paths.size();
        }
        report.add("agent/candidates/" + std::to_string(nodes), report.iterations, 0, timer, checksum,
                   {{"ingresses", entries.size()}, {"max_nodes", 8}, {"reachable", expected.size()}});
        routing.stop();
    }
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 10);
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            graphs(report);
            quality(report);
            snapshots(report);
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
