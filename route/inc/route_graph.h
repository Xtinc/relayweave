#ifndef RELAYWEAVE_ROUTE_GRAPH_H
#define RELAYWEAVE_ROUTE_GRAPH_H

#include <cstddef>
#include <map>
#include <span>
#include <string>
#include <vector>

class RouteGraph
{
  public:
    struct Link
    {
        std::string from;
        std::string to;
        double cost = 0.0;
    };

    struct Entry
    {
        std::string node;
        double access_cost = 0.0;
    };

    struct Path
    {
        std::vector<std::string> nodes;
        double cost = 0.0;
    };

    explicit RouteGraph(std::span<const Link> links);

    // Starts at real ingress Nodes. Every expansion charges the departing Node as a relay.
    [[nodiscard]] std::map<std::string, Path> shortest_paths(
        std::span<const Entry> entries, std::size_t max_nodes) const;

  private:
    struct Arc
    {
        std::string to;
        double cost = 0.0;
    };

    static constexpr double relay_penalty = 2.0;

    std::map<std::string, std::vector<Arc>> adjacency_;
};

#endif // RELAYWEAVE_ROUTE_GRAPH_H
