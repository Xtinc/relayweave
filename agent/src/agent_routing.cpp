#include "agent_routing.h"
#include "link_quality.h"
#include "message.h"
#include <algorithm>
#include <cmath>
#include <sstream>
#include <tuple>

static std::chrono::milliseconds age(std::uint64_t value)
{
    return std::chrono::milliseconds(std::min(value, std::uint64_t(std::chrono::milliseconds::max().count())));
}

AgentRouting::AgentRouting(asio::io_context &io, std::size_t max_nodes) : max_nodes_(max_nodes), probes_(io)
{
    if (max_nodes == 0 || max_nodes > MAX_PATH_NODES)
    {
        throw std::invalid_argument("routing.max_nodes must be between 1 and 8");
    }
}

bool AgentRouting::set_required_targets(ProbeSet::Targets targets)
{
    if (targets == required_targets_)
    {
        return false;
    }
    path_cache_.clear();
    required_targets_ = std::move(targets);
    update_targets();
    return true;
}

void AgentRouting::update_targets()
{
    auto targets = required_targets_;
    for (const auto &[id, node] : snapshot_.nodes)
    {
        targets.try_emplace(id, node.address);
    }
    probes_.set_targets(std::move(targets));
}

void AgentRouting::begin_request(std::uint64_t request_id, Clock::time_point now)
{
    pending_ = Pending{request_id, now};
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
        Snapshot next;
        next.epoch = require_unsigned(snapshot, "epoch", true);
        next.version = require_unsigned(snapshot, "snapshot_version", true);
        const auto created_age = age(require_unsigned(snapshot, "created_age_ms"));
        const auto &nodes = snapshot.at("nodes");
        const auto &links = snapshot.at("links");
        if (created_age >= SNAPSHOT_EXPIRY || !nodes.is_array() || !links.is_array())
        {
            throw std::invalid_argument("Invalid topology snapshot age or arrays");
        }

        if (created_age + std::chrono::duration_cast<std::chrono::milliseconds>(now - pending_.started) >=
            SNAPSHOT_EXPIRY)
        {
            return false;
        }

        if (next.epoch == snapshot_.epoch && next.version < snapshot_.version)
        {
            return false;
        }

        if (nodes.size() + links.size() > MAX_SNAPSHOT_ENTRIES || nodes.size() > MAX_SNAPSHOT_NODES)
        {
            throw std::invalid_argument("Topology exceeds Agent entry limit");
        }

        next.created = pending_.started - created_age;
        for (const auto &node : nodes)
        {
            auto id = required_string(node, "node_id", "topology.snapshot");
            Node parsed{required_string(node, "address", "topology.snapshot"),
                        node.at("report_age_ms").is_null() ? std::chrono::milliseconds::max()
                                                           : age(require_unsigned(node, "report_age_ms")),
                        std::nullopt};
            const auto value = node.find("available");
            if (value != node.end() && !value->is_null())
            {
                if (!value->is_number() || !std::isfinite(value->get<double>()) || value->get<double>() < 0.0 ||
                    value->get<double>() > 1.0)
                {
                    throw std::invalid_argument("Node available must be a finite number between 0 and 1");
                }
                parsed.available = value->get<double>();
            }
            if (!next.nodes.emplace(std::move(id), std::move(parsed)).second)
            {
                throw std::invalid_argument("Topology contains duplicate nodes");
            }
        }

        next.links.reserve(links.size());
        for (const auto &entry : links)
        {
            auto source = required_string(entry, "source", "topology.snapshot");
            auto target = required_string(entry, "destination", "topology.snapshot");
            const auto quality = parse_quality(entry.at("quality"));
            const auto link_age = entry.at("age_ms").is_null() ? std::chrono::milliseconds::max()
                                                               : age(require_unsigned(entry, "age_ms"));
            if (source == target || !next.nodes.contains(source) || !next.nodes.contains(target))
            {
                throw std::invalid_argument("Topology contains an invalid directed link");
            }
            next.links.push_back({std::move(source), std::move(target), quality, link_age});
        }
        std::sort(next.links.begin(), next.links.end(), [](const Link &left, const Link &right) {
            return std::tie(left.from, left.to) < std::tie(right.from, right.to);
        });
        if (std::adjacent_find(next.links.begin(), next.links.end(), [](const Link &left, const Link &right) {
                return left.from == right.from && left.to == right.to;
            }) != next.links.end())
        {
            throw std::invalid_argument("Topology contains duplicate directed links");
        }

        if (snapshot_.epoch != next.epoch)
        {
            path_cache_.clear();
        }
        snapshot_ = std::move(next);
        update_targets();
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
    path_cache_.clear();
    snapshot_ = Snapshot{};
    pending_ = Pending{};
    update_targets();
}

asio::awaitable<bool> AgentRouting::refresh_probes()
{
    return probes_.refresh();
}

void AgentRouting::stop()
{
    probes_.stop();
}

asio::awaitable<void> AgentRouting::close()
{
    return probes_.close();
}

RouteGraph AgentRouting::graph(Clock::time_point now) const
{
    if (snapshot_.links.empty() || now < snapshot_.created)
    {
        return RouteGraph({});
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - snapshot_.created);
    if (elapsed >= SNAPSHOT_EXPIRY)
    {
        return RouteGraph({});
    }

    std::vector<RouteGraph::Link> links;
    for (const auto &link : snapshot_.links)
    {
        if (snapshot_.nodes.at(link.from).report_age >= SNAPSHOT_EXPIRY - elapsed)
        {
            continue;
        }

        const auto current_age = link.age + std::min(elapsed, std::chrono::milliseconds::max() - link.age);
        if (const auto cost = routing_cost(link.quality, current_age))
        {
            links.push_back({link.from, link.to, *cost + bandwidth_penalty(link.to, now)});
        }
    }
    return RouteGraph(links);
}

double AgentRouting::bandwidth_penalty(const std::string &node, Clock::time_point now) const
{
    const auto found = snapshot_.nodes.find(node);
    if (found == snapshot_.nodes.end() || !found->second.available || now < snapshot_.created)
    {
        return 0.0;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - snapshot_.created);
    if (elapsed >= SNAPSHOT_EXPIRY || found->second.report_age >= SNAPSHOT_EXPIRY - elapsed)
    {
        return 0.0;
    }
    // Charge the destination's headroom; cap saturation at a finite penalty.
    return 10.0 * (1.0 / std::max(*found->second.available, 0.05) - 1.0);
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

std::vector<RouteGraph::Path> AgentRouting::candidate_paths(const std::string &destination, Clock::time_point now) const
{
    auto entries = measured_entries();
    if (entries.empty())
    {
        return {};
    }

    const auto routes = graph(now);
    std::vector<RouteGraph::Path> result;
    for (auto &entry : entries)
    {
        entry.access_cost += bandwidth_penalty(entry.node, now);
        auto paths = routes.shortest_paths(std::span(&entry, 1), max_nodes_);
        if (const auto path = paths.find(destination); path != paths.end())
        {
            result.push_back(std::move(path->second));
        }
    }
    std::sort(result.begin(), result.end(), [](const auto &left, const auto &right) {
        return left.cost != right.cost ? left.cost < right.cost : left.nodes < right.nodes;
    });
    return result;
}

std::vector<std::string> AgentRouting::select_path(const ServiceKey &service, const std::string &destination)
{
    const auto now = AgentRouting::Clock::now();
    if (const auto cached = path_cache_.get(destination, now))
    {
        return *cached;
    }

    auto candidates = candidate_paths(destination, now);
    // Keep one complete log record per path so journald preserves level and service context.
    if (proxy_is_debug_enabled())
    {
        const auto count = candidates.size();
        PROXY_DEBUG_PRINT("Routes service=%s/%s -> %s candidates=%zu max_nodes=%zu%s", service.service.c_str(),
                          relay_protocol_name(service.protocol).data(), destination.c_str(), count, max_nodes_,
                          count == 0 ? " cost=unavailable" : "");
        for (std::size_t index = 0; index < std::min(count, MAX_LOGGED_CANDIDATES); ++index)
        {
            const auto &candidate = candidates[index];
            std::ostringstream path;
            path << "agent";
            for (const auto &node : candidate.nodes)
            {
                path << " -> " << node;
            }
            PROXY_DEBUG_PRINT("Route #%zu cost=%.3f service=%s/%s: %s%s", index + 1, candidate.cost,
                              service.service.c_str(), relay_protocol_name(service.protocol).data(), path.str().c_str(),
                              index == 0 ? " *" : "");
        }
    }

    auto best = candidates.empty() ? std::vector<std::string>{} : std::move(candidates.front().nodes);
    path_cache_.put(destination, best);
    return best;
}
