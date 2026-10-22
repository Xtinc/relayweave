#ifndef RELAYWEAVE_AGENT_ROUTING_H
#define RELAYWEAVE_AGENT_ROUTING_H

#include "probe_set.h"
#include "agent_common.h"
#include "lru_cache.h"
#include "route_graph.h"
#include <chrono>
#include <nlohmann/json.hpp>

class AgentRouting
{
  public:
    using Clock = std::chrono::steady_clock;

    /**
     * @brief Own topology, probe targets, and the destination cache on control_io.
     * @param io Single-threaded control execution domain, valid through close().
     * @param max_nodes Maximum real Nodes per path, in [1, 8].
     */
    AgentRouting(asio::io_context &io, std::size_t max_nodes);
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
    /** @brief Cancel probe refresh on control_io; close() clears cached routes and drains probes. */
    void stop();
    asio::awaitable<bool> refresh_probes();
    // Must be awaited on the Agent control io_context, like refresh_probes().
    asio::awaitable<void> close();
    /**
     * @brief Reuse the destination cache or calculate and log candidate paths on control_io.
     * @param service Service name and protocol included in diagnostic logs.
     * @param destination Destination Node identity used as the shared cache key.
     * @return Best Node sequence, or empty when no measured route is available.
     */
    std::vector<std::string> select_path(const ServiceKey &service, const std::string &destination);

  private:
    static constexpr auto REQUEST_TIMEOUT = std::chrono::seconds(10);
    static constexpr auto SNAPSHOT_EXPIRY = std::chrono::seconds(15);
    static constexpr std::size_t MAX_PATH_NODES = 8;
    static constexpr std::size_t MAX_SNAPSHOT_ENTRIES = 65536;
    static constexpr std::size_t MAX_SNAPSHOT_NODES = 1024;

    static constexpr std::size_t MAX_LOGGED_CANDIDATES = 3;
    static constexpr std::size_t PATH_CACHE_CAPACITY = 16;
    static constexpr auto PATH_CACHE_TTL = std::chrono::seconds(15);

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

    /** @brief Compute one path per measured ingress to this destination, ordered by cost and Nodes. */
    std::vector<RouteGraph::Path> candidate_paths(const std::string &destination, Clock::time_point now) const;
    RouteGraph graph(Clock::time_point now) const;
    double bandwidth_penalty(const std::string &node, Clock::time_point now) const;
    std::vector<RouteGraph::Entry> measured_entries() const;
    void update_targets();

    // Destination cache has its own TTL; routine snapshot/probe updates retain valid entries.
    LRUCache<std::string, std::vector<std::string>> path_cache_{PATH_CACHE_CAPACITY, PATH_CACHE_TTL};
    std::size_t max_nodes_;
    ProbeSet probes_;
    ProbeSet::Targets required_targets_;
    ProbeSet::Targets nodes_;
    std::map<std::string, std::chrono::milliseconds> report_ages_;
    std::map<std::string, double> available_;
    std::vector<Link> links_;
    Pending pending_;
    Clock::time_point snapshot_at_;
    std::uint64_t epoch_ = 0;
    std::uint64_t version_ = 0;
};

#endif
