#include "route_graph.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace
{
bool better(const RouteGraph::Path &candidate, const RouteGraph::Path &current) noexcept
{
    if (candidate.cost != current.cost)
    {
        return candidate.cost < current.cost;
    }
    return candidate.nodes < current.nodes;
}
} // namespace

RouteGraph::RouteGraph(std::span<const Link> links)
{
    for (const auto &link : links)
    {
        if (link.from.empty() || link.to.empty() || link.from == link.to ||
            !std::isfinite(link.cost) || link.cost < 0.0)
        {
            continue;
        }
        adjacency_[link.from].push_back({link.to, link.cost});
        adjacency_.try_emplace(link.to);
    }

    for (auto &entry : adjacency_)
    {
        auto &edges = entry.second;
        std::sort(edges.begin(), edges.end(), [](const Arc &left, const Arc &right) {
            if (left.to != right.to)
            {
                return left.to < right.to;
            }
            return left.cost < right.cost;
        });
        edges.erase(std::unique(edges.begin(), edges.end(), [](const Arc &left, const Arc &right) {
                        return left.to == right.to;
                    }),
                    edges.end());
    }
}

std::optional<RouteGraph::Path> RouteGraph::shortest_path(std::string_view source, std::string_view destination,
                                                         std::size_t max_nodes) const
{
    if (source.empty() || destination.empty())
    {
        throw std::invalid_argument("Route endpoints must not be empty");
    }
    if (max_nodes == 0)
    {
        throw std::invalid_argument("Route node limit must be positive");
    }

    const std::vector<Entry> entries{{std::string(source), 0.0}};
    const auto paths = search(entries, max_nodes, false);
    const auto found = paths.find(std::string(destination));
    return found == paths.end() ? std::nullopt : std::optional(found->second);
}

std::map<std::string, RouteGraph::Path> RouteGraph::shortest_paths(
    std::span<const Entry> entries, std::size_t max_nodes) const
{
    return search(entries, max_nodes, true);
}

std::map<std::string, RouteGraph::Path> RouteGraph::search(
    std::span<const Entry> entries, std::size_t max_nodes, bool charge_first_node) const
{
    if (max_nodes == 0)
    {
        throw std::invalid_argument("Route node limit must be positive");
    }
    std::map<std::string, Path> current;
    for (const auto &entry : entries)
    {
        if (entry.node.empty() || !std::isfinite(entry.access_cost) || entry.access_cost < 0.0)
        {
            throw std::invalid_argument("Route entries require a node and finite nonnegative cost");
        }
        Path path{{entry.node}, entry.access_cost};
        const auto previous = current.find(entry.node);
        if (previous == current.end() || better(path, previous->second))
        {
            current.insert_or_assign(entry.node, std::move(path));
        }
    }
    // Entries may be isolated and absent from the graph, but are still valid direct paths.
    const auto node_limit = std::min(max_nodes, adjacency_.size() + current.size());
    std::map<std::string, Path> results;
    for (std::size_t node_count = 1; node_count <= node_limit && !current.empty(); ++node_count)
    {
        std::map<std::string, Path> next;
        for (const auto &[node, path] : current)
        {
            const auto best = results.find(node);
            if (best == results.end() || better(path, best->second))
            {
                results.insert_or_assign(node, path);
            }
            if (node_count == node_limit)
            {
                continue;
            }
            const auto outgoing = adjacency_.find(node);
            if (outgoing == adjacency_.end())
            {
                continue;
            }
            for (const auto &edge : outgoing->second)
            {
                Path candidate{path.nodes, path.cost + edge.cost};
                if (charge_first_node || node_count > 1)
                {
                    candidate.cost += relay_penalty;
                }
                if (!std::isfinite(candidate.cost))
                {
                    continue;
                }
                candidate.nodes.push_back(edge.to);
                const auto previous = next.find(edge.to);
                if (previous == next.end() || better(candidate, previous->second))
                {
                    next.insert_or_assign(edge.to, std::move(candidate));
                }
            }
        }
        current = std::move(next);
    }
    return results;
}
