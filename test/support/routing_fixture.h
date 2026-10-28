#ifndef RELAYWEAVE_TEST_ROUTING_FIXTURE_H
#define RELAYWEAVE_TEST_ROUTING_FIXTURE_H

#include "agent_routing.h"
#include "member_access.h"

TEST_MEMBER(AgentRouting, probes_)
TEST_MEMBER(AgentRouting, candidate_paths)
TEST_MEMBER(ProbeSet, resolved_)
TEST_MEMBER(ProbeSet, icmp_)
TEST_MEMBER(ICMP, sessions_)

namespace test_routing
{
struct Fixture : AgentRouting
{
    Fixture(asio::io_context &io, std::size_t max_nodes)
        : AgentRouting(io, max_nodes), io(io) {}
    asio::io_context &io;
};

// Populate existing measurement state without opening raw sockets or changing
// the production route calculator's inputs. All candidates use measured_entries().
inline std::vector<RouteGraph::Path> candidates(Fixture &routing, const std::string &destination,
                                                AgentRouting::Clock::time_point now,
                                                std::span<const RouteGraph::Entry> entries)
{
    auto &probes = test_access::AgentRouting_probes_(routing);
    auto &icmp = test_access::ProbeSet_icmp_(probes);
    auto &resolved = test_access::ProbeSet_resolved_(probes);
    auto &io = routing.io;
    if (!icmp)
    {
        icmp.emplace(io, std::chrono::seconds(1), LinkQuality::scoring_timeout);
    }
    auto &sessions = test_access::ICMP_sessions_(*icmp);
    sessions.clear();
    resolved.clear();
    const auto sampled = LinkQuality::Clock::now();
    std::uint32_t address = 0x7f000001;
    for (const auto &entry : entries)
    {
        const asio::ip::address_v4 destination(address++);
        resolved[destination] = {entry.node};
        auto &session = sessions.try_emplace(destination, io).first->second;
        auto &quality = session.state.quality;
        quality.record(true, entry.access_cost, sampled - LinkQuality::half_life);
        for (int sample = 0; sample < 32; ++sample)
        {
            quality.record(true, entry.access_cost, sampled);
        }
    }
    return test_access::AgentRouting_candidate_paths(routing, destination, now);
}
} // namespace test_routing
#endif
