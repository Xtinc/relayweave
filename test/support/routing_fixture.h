#ifndef RELAYWEAVE_TEST_ROUTING_FIXTURE_H
#define RELAYWEAVE_TEST_ROUTING_FIXTURE_H

#include "agent_routing.h"
#include "member_access.h"

TEST_MEMBER(AgentRouting, probes_)
TEST_MEMBER(ProbeSet, resolved_)
TEST_MEMBER(ProbeSet, icmp_)
TEST_MEMBER(ICMP, sessions_)
TEST_MEMBER(LinkQuality, ema_)
TEST_MEMBER(LinkQuality, first_sample_)
TEST_MEMBER(LinkQuality, last_sample_)
TEST_MEMBER(LinkQuality, last_success_)

namespace test_routing
{
struct Fixture : AgentRouting
{
    Fixture(asio::io_context &io, bool discover_entries, std::size_t max_nodes)
        : AgentRouting(io, discover_entries, max_nodes), io(io) {}
    asio::io_context &io;
};

// Populate existing measurement state without opening raw sockets or changing
// the production route calculator's inputs. All candidates use measured_entries().
inline AgentRouting::CandidatePaths candidates(Fixture &routing, AgentRouting::Clock::time_point now,
                                               std::span<const RouteGraph::Entry> entries)
{
    auto &probes = test_access::AgentRouting_probes_(routing);
    auto &icmp = test_access::ProbeSet_icmp_(probes);
    auto &resolved = test_access::ProbeSet_resolved_(probes);
    auto &io = routing.io;
    if (!icmp) icmp.emplace(io, std::chrono::seconds(1), std::chrono::milliseconds(800));
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
        test_access::LinkQuality_first_sample_(quality) = sampled - std::chrono::hours(1);
        test_access::LinkQuality_last_sample_(quality) = sampled;
        test_access::LinkQuality_last_success_(quality) = sampled;
        for (auto &ema : test_access::LinkQuality_ema_(quality))
        {
            ema.completed_weight = ema.received_weight = 10000.0;
            ema.rtt_sum = 10000.0 * entry.access_cost;
            ema.rtt_square_sum = 10000.0 * entry.access_cost * entry.access_cost;
        }
    }
    return routing.candidate_paths(now);
}
} // namespace test_routing
#endif
