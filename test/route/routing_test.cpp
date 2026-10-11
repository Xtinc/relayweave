#include "route_graph.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace
{
constexpr std::size_t default_test_node_limit = 4;

struct Position
{
    double x = 0.0;
    double y = 0.0;
};

struct GraphCase
{
    std::string id;
    std::string title;
    std::string explanation;
    std::vector<RouteGraph::Link> links;
    std::map<std::string, Position> positions;
    std::string source;
    std::string destination;
    std::size_t max_nodes = default_test_node_limit;
    std::vector<std::string> expected_nodes;
    double expected_cost = 0.0;
    RouteGraph::Path actual;
};

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

void require_near(double actual, double expected, const std::string &message)
{
    if (std::abs(actual - expected) > 1.0e-9)
    {
        throw std::runtime_error(message);
    }
}

RouteGraph::Link link(std::string from, std::string to, double weight)
{
    return {std::move(from), std::move(to), weight};
}

std::vector<GraphCase> graph_cases()
{
    return {
        {
            "weighted-choice",
            "加权路径竞争",
            "直连权重 30；A→B→D 与 A→F→D 的总代价都为 14，确定性规则选择字典序更小的 B。",
            {
                link("A", "D", 30.0),
                link("A", "B", 5.0),
                link("B", "D", 5.0),
                link("A", "C", 7.0),
                link("C", "D", 4.0),
                link("A", "E", 3.0),
                link("E", "D", 9.0),
                link("A", "F", 8.0),
                link("F", "D", 2.0),
                link("A", "G", 2.0),
                link("G", "D", 20.0),
                link("A", "H", 6.0),
                link("H", "D", 8.0),
            },
            {
                {"A", {65, 210}},
                {"B", {365, 45}},
                {"C", {365, 110}},
                {"E", {365, 175}},
                {"F", {365, 245}},
                {"G", {365, 310}},
                {"H", {365, 375}},
                {"D", {700, 210}},
            },
            "A",
            "D",
            4,
            {"A", "B", "D"},
            14.0,
        },
        {
            "layered-state",
            "分层状态不能合并",
            "A→B→C→X 到达 X 时已经用完四层；算法仍保留 (X, 2) 状态并找到 A→X→D。",
            {
                link("A", "B", 0.1),
                link("B", "C", 0.1),
                link("C", "X", 0.1),
                link("A", "E", 0.2),
                link("E", "C", 0.2),
                link("A", "X", 5.0),
                link("X", "D", 1.0),
                link("A", "Y", 4.0),
                link("Y", "D", 4.0),
                link("A", "Z", 6.0),
                link("Z", "D", 0.5),
            },
            {
                {"A", {65, 210}},
                {"B", {210, 45}},
                {"C", {365, 105}},
                {"E", {210, 170}},
                {"X", {505, 105}},
                {"Y", {505, 230}},
                {"Z", {505, 350}},
                {"D", {700, 210}},
            },
            "A",
            "D",
            4,
            {"A", "X", "D"},
            10.0,
        },
        {
            "node-budget",
            "最大层数约束",
            "A→B→C→E→D 的基础边权之和只有 2，但需要五个节点；其余中继候选更贵，因此四层限制下选择 A→D。",
            {
                link("A", "B", 0.5),
                link("B", "C", 0.5),
                link("C", "E", 0.5),
                link("E", "D", 0.5),
                link("A", "D", 10.0),
                link("A", "F", 3.0),
                link("F", "D", 9.0),
                link("A", "G", 6.0),
                link("G", "D", 5.0),
                link("A", "H", 2.0),
                link("H", "D", 20.0),
            },
            {
                {"A", {65, 210}},
                {"B", {205, 45}},
                {"C", {350, 45}},
                {"E", {505, 45}},
                {"F", {365, 180}},
                {"G", {365, 245}},
                {"H", {365, 310}},
                {"D", {700, 210}},
            },
            "A",
            "D",
            4,
            {"A", "D"},
            12.0,
        },
    };
}

std::optional<RouteGraph::Path> path_to(const RouteGraph &graph, std::string_view source,
                                       std::string_view destination, std::size_t max_nodes)
{
    const RouteGraph::Entry entry{std::string(source), 0.0};
    const auto paths = graph.shortest_paths(std::span(&entry, 1), max_nodes);
    const auto found = paths.find(std::string(destination));
    return found == paths.end() ? std::nullopt : std::optional(found->second);
}

void run_case(GraphCase &test)
{
    const RouteGraph graph(test.links);
    const auto result = path_to(graph, test.source, test.destination, test.max_nodes);
    require(result.has_value(), test.id + ": no route found");
    require(result->nodes == test.expected_nodes, test.id + ": wrong path");
    require_near(result->cost, test.expected_cost, test.id + ": wrong cost");
    test.actual = *result;
}

void test_directed_edges()
{
    const std::vector links{
        link("A", "B", 1.0),
        link("B", "A", 20.0),
    };
    const RouteGraph graph(links);
    const auto forward = path_to(graph, "A", "B", 2);
    const auto reverse = path_to(graph, "B", "A", 2);
    require(forward && reverse, "directed edges were not both reachable");
    require_near(forward->cost, 3.0, "A to B used the reverse edge weight");
    require_near(reverse->cost, 22.0, "B to A used the forward edge weight");
}

void test_invalid_links_are_filtered()
{
    const std::vector<RouteGraph::Link> links{
        link("A", "B", -1.0), link("A", "C", std::numeric_limits<double>::quiet_NaN()),
        link("A", "D", std::numeric_limits<double>::infinity()), link("A", "A", 0.0), link("", "E", 1.0)};
    const RouteGraph graph(links);
    for (const auto &target : {"B", "C", "D", "E"})
    {
        require(!path_to(graph, "A", target, 2), "invalid edge was accepted");
    }
}

void test_multiple_entries()
{
    const std::vector links{link("A", "D", 5.0), link("A", "D", 8.0), link("B", "D", 5.0), link("D", "E", 1.0)};
    const RouteGraph graph(links);
    const std::vector<RouteGraph::Entry> entries{{"A", 3.0}, {"B", 3.0}, {"D", 50.0}, {"isolated", 7.0}};
    const auto paths = graph.shortest_paths(entries, 3);
    require(paths.at("D").nodes == std::vector<std::string>({"A", "D"}), "entry tie was not deterministic");
    require_near(paths.at("D").cost, 10.0, "access or ingress relay penalty was wrong");
    require_near(paths.at("E").cost, 13.0, "batch destination relay penalty was wrong");
    require(paths.at("isolated").nodes == std::vector<std::string>({"isolated"}), "isolated direct entry disappeared");
    const auto limited = graph.shortest_paths(entries, 1);
    require_near(limited.at("D").cost, 50.0, "real-node budget did not limit expansions");
    require(!limited.contains("E"), "one-node budget invented a multi-hop path");
    const RouteGraph empty(std::span<const RouteGraph::Link>{});
    require_near(empty.shortest_paths(entries, 1).at("isolated").cost, 7.0, "direct access needs topology");
}

void test_caller_defined_node_limit()
{
    const std::vector links{
        link("A", "B", 1.0),
        link("B", "C", 1.0),
        link("C", "D", 1.0),
        link("D", "E", 1.0),
    };
    const RouteGraph graph(links);
    require(!path_to(graph, "A", "E", 4), "four-node query exceeded its limit");
    const auto result = path_to(graph, "A", "E", 5);
    require(result.has_value(), "five-node query did not reach its destination");
    require(result->nodes == std::vector<std::string>({"A", "B", "C", "D", "E"}),
            "five-node query returned the wrong path");
    require_near(result->cost, 12.0, "five-node query returned the wrong cost");
}

bool better(const RouteGraph::Path &candidate, const RouteGraph::Path &current)
{
    if (candidate.cost != current.cost)
    {
        return candidate.cost < current.cost;
    }
    return candidate.nodes < current.nodes;
}

void enumerate_paths(const std::vector<RouteGraph::Link> &links, std::string_view destination,
                     std::size_t max_nodes, RouteGraph::Path path,
                     std::optional<RouteGraph::Path> &best)
{
    if (path.nodes.back() == destination)
    {
        if (!best || better(path, *best))
        {
            best = std::move(path);
        }
        return;
    }
    if (path.nodes.size() == max_nodes)
    {
        return;
    }

    for (const auto &edge : links)
    {
        if (edge.from != path.nodes.back() ||
            std::find(path.nodes.begin(), path.nodes.end(), edge.to) != path.nodes.end())
        {
            continue;
        }
        auto next = path;
        next.cost += edge.cost;
        next.cost += 2.0;
        next.nodes.push_back(edge.to);
        enumerate_paths(links, destination, max_nodes, std::move(next), best);
    }
}

void test_against_exhaustive_search()
{
    const std::vector<std::string> nodes{"A", "B", "C", "D", "E", "F"};
    std::mt19937 random(0x524f5554U);
    for (std::size_t case_index = 0; case_index != 200; ++case_index)
    {
        std::vector<RouteGraph::Link> links;
        for (const auto &from : nodes)
        {
            for (const auto &to : nodes)
            {
                if (from != to && random() % 4 == 0)
                {
                    links.push_back(link(from, to, 1.0 + static_cast<double>(random() % 9)));
                }
            }
        }

        const auto source_index = random() % nodes.size();
        auto destination_index = random() % nodes.size();
        if (destination_index == source_index)
        {
            destination_index = (destination_index + 1) % nodes.size();
        }
        const auto max_nodes = 1 + random() % default_test_node_limit;
        std::optional<RouteGraph::Path> expected;
        enumerate_paths(links, nodes[destination_index], max_nodes,
                        RouteGraph::Path{{nodes[source_index]}, 0.0}, expected);

        const RouteGraph graph(links);
        const auto actual = path_to(graph, nodes[source_index], nodes[destination_index], max_nodes);
        const auto context = "exhaustive case " + std::to_string(case_index);
        require(actual.has_value() == expected.has_value(), context + ": reachability differs");
        if (actual)
        {
            require(actual->nodes == expected->nodes, context + ": path differs");
            require_near(actual->cost, expected->cost, context + ": cost differs");
        }
        const std::vector<RouteGraph::Entry> entries{{nodes[0], double(random() % 8)},
                                                     {nodes[1], double(random() % 8)},
                                                     {"isolated", double(random() % 8)}};
        const auto all_paths = graph.shortest_paths(entries, max_nodes);
        for (const auto &destination : nodes)
        {
            std::optional<RouteGraph::Path> batch_expected;
            for (const auto &entry : entries)
            {
                enumerate_paths(links, destination, max_nodes, {{entry.node}, entry.access_cost}, batch_expected);
            }
            const auto found = all_paths.find(destination);
            require((found != all_paths.end()) == batch_expected.has_value(), context + ": batch reachability differs");
            if (batch_expected)
            {
                require(found->second.nodes == batch_expected->nodes, context + ": batch path differs");
                require_near(found->second.cost, batch_expected->cost, context + ": batch cost differs");
            }
        }

    }
}

bool selected_edge(const RouteGraph::Path &path, const RouteGraph::Link &link)
{
    for (std::size_t index = 1; index < path.nodes.size(); ++index)
    {
        if (path.nodes[index - 1] == link.from && path.nodes[index] == link.to)
        {
            return true;
        }
    }
    return false;
}

std::string number(double value)
{
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(value < 1.0 ? 1 : 0) << value;
    return stream.str();
}

std::string path_text(const RouteGraph::Path &path)
{
    std::ostringstream stream;
    for (std::size_t index = 0; index < path.nodes.size(); ++index)
    {
        if (index != 0)
        {
            stream << " → ";
        }
        stream << path.nodes[index];
    }
    return stream.str();
}

void render_case(std::ostream &output, const GraphCase &test)
{
    output << "<article><h2>" << test.title << "</h2><p>" << test.explanation << "</p>"
           << "<div class=\"result\">最短路径：<strong>" << path_text(test.actual)
           << "</strong><span>总代价 " << number(test.actual.cost) << "</span><span>最多 " << test.max_nodes
           << " 个节点</span></div>"
           << "<svg viewBox=\"0 0 760 420\" role=\"img\" aria-label=\"" << test.title << "\">"
           << "<defs><marker id=\"arrow-" << test.id
           << "\" markerWidth=\"8\" markerHeight=\"8\" refX=\"7\" refY=\"4\" orient=\"auto\">"
           << "<path class=\"arrow-normal\" d=\"M0,0 L8,4 L0,8 Z\"/></marker>"
           << "<marker id=\"arrow-selected-" << test.id
           << "\" markerWidth=\"8\" markerHeight=\"8\" refX=\"7\" refY=\"4\" orient=\"auto\">"
           << "<path class=\"arrow-selected\" d=\"M0,0 L8,4 L0,8 Z\"/></marker></defs>";

    for (const auto &edge : test.links)
    {
        const auto from = test.positions.at(edge.from);
        const auto to = test.positions.at(edge.to);
        const auto chosen = selected_edge(test.actual, edge);
        const auto delta_x = to.x - from.x;
        const auto delta_y = to.y - from.y;
        const auto length = std::hypot(delta_x, delta_y);
        const auto start_x = from.x + delta_x * 28.0 / length;
        const auto start_y = from.y + delta_y * 28.0 / length;
        const auto end_x = to.x - delta_x * 32.0 / length;
        const auto end_y = to.y - delta_y * 32.0 / length;
        const auto label_x = (start_x + end_x) / 2.0;
        const auto label_y = (start_y + end_y) / 2.0 - 8.0;
        output << "<g class=\"edge" << (chosen ? " selected" : "") << "\">"
               << "<line x1=\"" << start_x << "\" y1=\"" << start_y << "\" x2=\"" << end_x
               << "\" y2=\"" << end_y << "\" marker-end=\"url(#arrow-"
               << (chosen ? "selected-" : "") << test.id << ")\"/>"
               << "<text x=\"" << label_x << "\" y=\"" << label_y << "\">w=" << number(edge.cost)
               << "</text></g>";
    }

    for (const auto &[name, position] : test.positions)
    {
        const auto chosen = std::find(test.actual.nodes.begin(), test.actual.nodes.end(), name) != test.actual.nodes.end();
        output << "<g class=\"node" << (chosen ? " selected" : "") << "\"><circle cx=\"" << position.x
               << "\" cy=\"" << position.y << "\" r=\"27\"/><text x=\"" << position.x << "\" y=\""
               << position.y + 5 << "\">" << name << "</text></g>";
    }
    output << "</svg></article>";
}

void write_report(const std::filesystem::path &path, const std::vector<GraphCase> &cases)
{
    std::ofstream output(path);
    if (!output)
    {
        throw std::runtime_error("cannot create routing visualization: " + path.string());
    }
    output << R"HTML(<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>分层带权有向图测试</title><style>
:root{color-scheme:light dark;--bg:#f6f7f9;--panel:#fff;--text:#17202a;--muted:#5d6975;--line:#7a8794;--path:#0b8f55;--path-soft:#dcf5e8;--border:#dce1e7}
@media(prefers-color-scheme:dark){:root{--bg:#11151a;--panel:#1a2027;--text:#edf2f7;--muted:#aeb8c2;--line:#84909c;--path:#4fd19a;--path-soft:#193d30;--border:#333d47}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,sans-serif}main{max-width:980px;margin:auto;padding:28px 20px 48px}h1{margin:0 0 6px;font-size:25px}header p,article>p{color:var(--muted);margin:0}article{margin-top:22px;padding:20px;background:var(--panel);border:1px solid var(--border);border-radius:12px}h2{font-size:18px;margin:0 0 4px}.result{display:flex;gap:20px;flex-wrap:wrap;margin:14px 0 2px}.result span{color:var(--muted)}svg{display:block;width:100%;height:auto}.arrow-normal{fill:var(--line)}.arrow-selected{fill:var(--path)}.edge line{stroke:var(--line);stroke-width:2}.edge text{fill:var(--muted);font-size:13px;text-anchor:middle}.edge.selected line{stroke:var(--path);stroke-width:5}.edge.selected text{fill:var(--path);font-weight:600}.node circle{fill:var(--panel);stroke:var(--line);stroke-width:2}.node text{fill:var(--text);font-size:15px;font-weight:600;text-anchor:middle}.node.selected circle{fill:var(--path-soft);stroke:var(--path);stroke-width:4}@media(max-width:600px){main{padding:18px 10px}article{padding:12px}.result{gap:8px 14px}}
</style></head><body><main><header><h1>分层带权有向图测试</h1><p>绿色粗线为 RouteGraph 实际返回的最短路径；边标签是基础权重，中继节点额外计入固定惩罚 2。</p></header>)HTML";
    for (const auto &test : cases)
    {
        render_case(output, test);
    }
    output << "</main></body></html>\n";
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        auto cases = graph_cases();
        for (auto &test : cases)
        {
            run_case(test);
        }
        test_directed_edges();
        test_invalid_links_are_filtered();
        test_multiple_entries();
        test_caller_defined_node_limit();
        test_against_exhaustive_search();
        const std::filesystem::path report = argc > 1 ? argv[1] : "routing_test_report.html";
        write_report(report, cases);
        std::cout << "[PASS] routing tests\n[INFO] visualization: " << report << '\n';
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
