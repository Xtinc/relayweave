#include "agent_routing.h"
#include "link_quality.h"
#include "message.h"
#include <algorithm>
#include <cmath>
#include <set>

static std::chrono::milliseconds age(std::uint64_t value)
{
    return std::chrono::milliseconds(std::min(value, std::uint64_t(std::chrono::milliseconds::max().count())));
}

AgentRouting::AgentRouting(asio::io_context &io, bool discover_entries, std::size_t max_nodes)
    : discover_entries_(discover_entries), max_nodes_(max_nodes), probes_(io)
{
    if (max_nodes == 0 || max_nodes > MAX_PATH_NODES)
    {
        throw std::invalid_argument("routing.max_nodes must be between 1 and 16");
    }
}

bool AgentRouting::set_required_targets(ProbeSet::Targets targets)
{
    if (targets == required_targets_)
    {
        return false;
    }
    required_targets_ = std::move(targets);
    update_targets();
    return true;
}

void AgentRouting::update_targets()
{
    auto targets = discover_entries_ ? nodes_ : ProbeSet::Targets{};
    for (const auto &[id, host] : required_targets_)
    {
        targets.insert_or_assign(id, host);
    }
    probes_.set_targets(std::move(targets));
}

void AgentRouting::begin_request(std::uint64_t request_id, Clock::time_point now)
{
    pending_ = Pending{};
    pending_.request_id = request_id;
    pending_.started = now;
}

bool AgentRouting::request_pending(Clock::time_point now) const noexcept
{
    return pending_.request_id != 0 && now >= pending_.started && now - pending_.started < REQUEST_TIMEOUT;
}

bool AgentRouting::accept_snapshot(const nlohmann::json &snapshot, Clock::time_point now)
{
    // Validate identity before the exception boundary: unrelated data must not
    // terminate the active request, including a malformed request_id.
    const auto request = config::require_unsigned(snapshot, "request_id", true);
    if (request != pending_.request_id || !request_pending(now))
    {
        return false;
    }
    try
    {
        using namespace config;
        const auto epoch = require_unsigned(snapshot, "epoch", true);
        const auto version = require_unsigned(snapshot, "snapshot_version", true);
        const auto created_age = age(require_unsigned(snapshot, "created_age_ms"));
        if (created_age >= SNAPSHOT_EXPIRY || !snapshot.at("nodes").is_array() || !snapshot.at("links").is_array())
        {
            throw std::invalid_argument("Invalid topology snapshot age or arrays");
        }

        if (created_age + std::chrono::duration_cast<std::chrono::milliseconds>(now - pending_.started) >=
            SNAPSHOT_EXPIRY)
        {
            return false;
        }

        if (epoch == epoch_ && version < version_)
        {
            return false;
        }

        if (snapshot.at("nodes").size() + snapshot.at("links").size() > MAX_SNAPSHOT_ENTRIES)
        {
            throw std::invalid_argument("Topology exceeds Agent entry limit");
        }

        std::map<std::string, std::string> nodes;
        std::vector<Link> links;
        std::map<std::string, std::chrono::milliseconds> report_ages;
        std::set<std::pair<std::string, std::string>> edges;
        for (const auto &node : snapshot.at("nodes"))
        {
            const auto id = required_string(node, "node_id", "topology.snapshot");
            const auto host = required_string(node, "address", "topology.snapshot");
            if (!nodes.emplace(id, host).second || nodes.size() > MAX_SNAPSHOT_NODES)
            {
                throw std::invalid_argument("Topology contains duplicate or too many nodes");
            }
            report_ages[id] = node.at("report_age_ms").is_null() ? std::chrono::milliseconds::max()
                                                                 : age(require_unsigned(node, "report_age_ms"));
        }

        for (const auto &entry : snapshot.at("links"))
        {
            const auto source = required_string(entry, "source", "topology.snapshot");
            const auto target = required_string(entry, "destination", "topology.snapshot");
            const auto quality = parse_quality(entry.at("quality"));
            if (source == target || !nodes.contains(source) || !nodes.contains(target) ||
                !edges.emplace(source, target).second)
            {
                throw std::invalid_argument("Topology contains an invalid directed link");
            }
            const auto link_age = entry.at("age_ms").is_null() ? std::chrono::milliseconds::max()
                                                               : age(require_unsigned(entry, "age_ms"));
            links.push_back({source, target, quality, link_age});
        }

        nodes_ = std::move(nodes);
        report_ages_ = std::move(report_ages);
        update_targets();
        links_ = std::move(links);
        snapshot_at_ = pending_.started - created_age;
        epoch_ = epoch;
        version_ = version;
        pending_ = Pending{};
        return true;
    }
    catch (...)
    {
        pending_ = Pending{}; // A malformed snapshot terminates the whole request.
        throw;
    }
}

void AgentRouting::invalidate_snapshot()
{
    nodes_.clear();
    report_ages_.clear();
    links_.clear();
    pending_ = Pending{};
    epoch_ = version_ = 0;
    update_targets();
}

asio::awaitable<bool> AgentRouting::refresh_probes()
{
    co_return co_await probes_.refresh();
}

void AgentRouting::stop()
{
    invalidate_snapshot();
    probes_.stop();
}

asio::awaitable<void> AgentRouting::close()
{
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    stop();
    co_await probes_.close();
}

RouteGraph AgentRouting::graph(Clock::time_point now) const
{
    if (links_.empty() || now < snapshot_at_)
    {
        return RouteGraph({});
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - snapshot_at_);
    if (elapsed >= SNAPSHOT_EXPIRY)
    {
        return RouteGraph({});
    }

    std::vector<RouteGraph::Link> links;
    for (const auto &link : links_)
    {
        const auto report = report_ages_.find(link.from);
        if (report == report_ages_.end() || report->second >= SNAPSHOT_EXPIRY - elapsed)
        {
            continue;
        }

        const auto current_age = link.age + std::min(elapsed, std::chrono::milliseconds::max() - link.age);
        if (const auto cost = routing_cost(link.quality, current_age))
        {
            links.push_back({link.from, link.to, *cost});
        }
    }
    return RouteGraph(links);
}

std::vector<RouteGraph::Entry> AgentRouting::measured_entries() const
{
    std::vector<RouteGraph::Entry> entries;
    for (const auto &metric : probes_.metrics())
    {
        const auto age =
            metric.assessment.last_success_age == ICMP::Duration::max()
                ? std::chrono::milliseconds::max()
                : std::chrono::duration_cast<std::chrono::milliseconds>(metric.assessment.last_success_age);
        if (const auto cost = routing_cost(metric.assessment.quality, age); cost && !metric.node_id.empty())
        {
            entries.push_back({metric.node_id, *cost});
        }
    }
    return entries;
}

AgentRouting::CandidatePaths AgentRouting::candidate_paths(
    Clock::time_point now, std::optional<std::span<const RouteGraph::Entry>> entries) const
{
    std::vector<RouteGraph::Entry> measured;
    if (!entries)
    {
        measured = measured_entries();
        entries = measured;
    }

    if (entries->empty())
    {
        return {};
    }

    // Keep the lowest access cost for each ingress, validating before merging duplicates.
    std::map<std::string, double> ingress_costs;
    for (const auto &entry : *entries)
    {
        if (entry.node.empty() || !std::isfinite(entry.access_cost) || entry.access_cost < 0.0)
        {
            throw std::invalid_argument("Route entries require a node and finite nonnegative cost");
        }
        auto &cost = ingress_costs.try_emplace(entry.node, entry.access_cost).first->second;
        cost = std::min(cost, entry.access_cost);
    }

    const auto routes = graph(now);
    CandidatePaths result;
    for (const auto &[node, cost] : ingress_costs)
    {
        const RouteGraph::Entry entry{node, cost};
        for (auto &[destination, path] : routes.shortest_paths(std::span(&entry, 1), max_nodes_))
        {
            result[destination].push_back(std::move(path));
        }
    }
    for (auto &[_, paths] : result)
    {
        std::sort(paths.begin(), paths.end(), [](const auto &left, const auto &right) {
            return left.cost != right.cost ? left.cost < right.cost : left.nodes < right.nodes;
        });
    }
    return result;
}
