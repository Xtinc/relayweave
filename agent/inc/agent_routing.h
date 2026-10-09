#ifndef RELAYWEAVE_AGENT_ROUTING_H
#define RELAYWEAVE_AGENT_ROUTING_H

#include "probe_set.h"
#include "route_graph.h"
#include <chrono>
#include <nlohmann/json.hpp>

class AgentRouting
{
  public:
    using Clock = std::chrono::steady_clock;
    using CandidatePaths = std::map<std::string, std::vector<RouteGraph::Path>>;

    AgentRouting(asio::io_context &io, bool discover_entries, std::size_t max_nodes);
    // Returns true only when the required targets changed.
    bool set_required_targets(ProbeSet::Targets targets);
    void begin_request(std::uint64_t request_id, Clock::time_point now);
    bool request_pending(Clock::time_point now) const noexcept;
    bool accept_snapshot(const nlohmann::json &snapshot, Clock::time_point now);
    void invalidate_snapshot();
    std::uint64_t epoch() const noexcept
    {
        return epoch_;
    }
    void stop();
    asio::awaitable<bool> refresh_probes();
    // Must be awaited on the Agent control io_context, like refresh_probes().
    asio::awaitable<void> close();
    // One best path per usable ingress, ordered by total cost and node sequence.
    // The first candidate for each destination is its best path.
    // Defaults to measured ingress entries; explicit entries allow calculation without raw sockets.
    CandidatePaths candidate_paths(Clock::time_point now,
                                   std::optional<std::span<const RouteGraph::Entry>> entries = std::nullopt) const;

  private:
    static constexpr auto REQUEST_TIMEOUT = std::chrono::seconds(10);
    static constexpr auto SNAPSHOT_EXPIRY = std::chrono::seconds(15);
    static constexpr std::size_t MAX_PATH_NODES = 8;
    static constexpr std::size_t MAX_SNAPSHOT_ENTRIES = 65536;
    static constexpr std::size_t MAX_SNAPSHOT_NODES = 1024;

    struct Pending
    {
        std::uint64_t request_id = 0;
        Clock::time_point started;
    };

    struct Link
    {
        std::string from;
        std::string to;
        LinkQuality::Summary quality;
        std::chrono::milliseconds age;
    };

    RouteGraph graph(Clock::time_point now) const;
    std::vector<RouteGraph::Entry> measured_entries() const;
    void update_targets();

    bool discover_entries_;
    std::size_t max_nodes_;
    ProbeSet probes_;
    ProbeSet::Targets required_targets_;
    ProbeSet::Targets nodes_;
    std::map<std::string, std::chrono::milliseconds> report_ages_;
    std::vector<Link> links_;
    Pending pending_;
    Clock::time_point snapshot_at_;
    std::uint64_t epoch_ = 0;
    std::uint64_t version_ = 0;
};

#endif
