#include "relay_agent.h"
#include <fstream>
#include "link_quality.h"

#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace
{
using Json = nlohmann::json;
auto at(int seconds)
{
    return AgentRouting::Clock::time_point(std::chrono::seconds(seconds));
}
void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}
Json edge(std::string from, std::string to, double cost, unsigned age = 0)
{
    LinkQuality::Summary quality;
    quality.cost = cost;
    quality.confidence = 1.0;
    quality.usable = true;
    return {{"source", std::move(from)}, {"destination", std::move(to)}, {"age_ms", age},
            {"quality", quality_json(quality)}};
}
Json snapshot(unsigned request, Json nodes, Json links, unsigned version = 1)
{
    return {{"request_id", request}, {"epoch", 11u}, {"snapshot_version", version}, {"created_age_ms", 100u},
            {"nodes", nodes}, {"links", links}};
}
Json node(std::string id, std::string address)
{
    return {{"node_id", std::move(id)}, {"address", std::move(address)}, {"report_age_ms", 0u}};
}
std::optional<RouteGraph::Path> calculate_path(const AgentRouting &routing, std::string_view destination,
                                     std::span<const RouteGraph::Entry> access, AgentRouting::Clock::time_point now)
{
    const auto paths = routing.candidate_paths(now, access);
    const auto found = paths.find(std::string(destination));
    return found == paths.end() ? std::nullopt : std::optional(found->second.front());
}

template <typename Fn> void rejects(Fn function, const char *reason)
{
    bool rejected = false;
    try
    {
        function();
    }
    catch (const std::exception &)
    {
        rejected = true;
    }
    require(rejected, reason);
}
}

int main()
{
    try
    {
        asio::io_context io(1);
        require(AgentConfig{}.routing_max_nodes == 4, "Default real Node budget changed");
        const auto data = std::filesystem::path(__FILE__).parent_path() / "data";
        const auto temporary = std::filesystem::temp_directory_path() /
            ("relayweave-routing-config-" + std::to_string(AgentRouting::Clock::now().time_since_epoch().count()) + ".json");
        ScopeGuard cleanup([temporary] { std::error_code ignored; std::filesystem::remove(temporary, ignored); });
        Json settings{{"server", {{"host", "127.0.0.1"}, {"port", 18443}}},
            {"certificate", {{"ca_file", (data / "tls_channel_test_ca.pem").string()},
                             {"certificate_chain", (data / "tls_channel_test_client.pem").string()},
                             {"private_key", (data / "tls_channel_test_client.key").string()}}}};
        const auto write_settings = [&] { std::ofstream file(temporary); file << settings; };
        write_settings();
        require(load_agent_config(temporary).routing_max_nodes == 4, "Omitted routing did not default to four nodes");
        for (unsigned maximum : {1u, 4u, 8u})
        {
            settings["routing"] = {{"max_nodes", maximum}};
            write_settings();
            require(load_agent_config(temporary).routing_max_nodes == maximum, "Explicit Node budget lost");
        }
        for (unsigned maximum : {0u, 9u, 16u})
        {
            settings["routing"] = {{"max_nodes", maximum}};
            write_settings();
            rejects([&] { load_agent_config(temporary); }, "Out of range Node budget accepted by parser");
        }

        rejects([&] { AgentRouting invalid(io, false, 9); }, "Nine-node Agent budget accepted");
        rejects([&] { AgentRouting invalid(io, false, 0); }, "Zero-node Agent budget accepted");
        AgentRouting eight(io, false, 8);
        Json chain_nodes = Json::array();
        Json chain_links = Json::array();
        for (unsigned index = 0; index < 8; ++index)
        {
            chain_nodes.push_back(node(std::to_string(index), "192.0.2." + std::to_string(index + 1)));
            if (index != 0)
            {
                chain_links.push_back(edge(std::to_string(index - 1), std::to_string(index), 1.0));
            }
        }
        eight.begin_request(1, at(0));
        eight.accept_snapshot(snapshot(1, chain_nodes, chain_links), at(0));
        const std::vector<RouteGraph::Entry> ingress{{"0", 1.0}};
        const auto whole = calculate_path(eight, "7", ingress, at(0));
        require(whole && whole->nodes.size() == 8 && whole->nodes.front() == "0" && whole->nodes.back() == "7",
                "Agent or endpoints incorrectly counted against eight-node budget");
        AgentRouting four(io, false, 4);
        four.begin_request(1, at(0));
        four.accept_snapshot(snapshot(1, chain_nodes, chain_links), at(0));
        require(calculate_path(four, "3", ingress, at(0)).has_value() && !calculate_path(four, "4", ingress, at(0)),
                "Configured four-node limit omitted endpoints");
        AgentRouting routing(io, true, 4);
        const auto first = snapshot(1, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                                    Json::array({edge("A", "D", 5.0)}));
        routing.begin_request(1, at(0));
        require(routing.accept_snapshot(first, at(0)), "Complete snapshot was not applied");
        require(!routing.accept_snapshot(first, at(0)), "Duplicate snapshot was applied");
        const std::vector<RouteGraph::Entry> access{{"A", 3.0}, {"D", 50.0}};
        auto path = calculate_path(routing, "D", access, at(0));
        require(path && path->nodes == std::vector<std::string>({"A", "D"}), "Best entry was not selected");
        require(std::abs(path->cost - 10.0) < 1e-9, "Access edge/hop penalty was omitted");
        require(routing.candidate_paths(at(0)).empty(), "Unmeasured ingress invented candidate routes");
        require(routing.candidate_paths(at(0), std::span<const RouteGraph::Entry>{}).empty(),
                "Explicit empty ingress invented candidate routes");
        const auto candidates = routing.candidate_paths(at(0), access);
        const auto &to_d = candidates.at("D");
        require(to_d.size() == 2 && to_d[0].cost < to_d[1].cost &&
                    to_d[1].nodes == std::vector<std::string>({"D"}) && to_d[1].cost == 50.0,
                "Per-ingress candidates lost an alternative or are not ordered by cost");
        const std::vector<RouteGraph::Entry> repeated{{"A", 50.0}, {"D", 10.0}, {"A", 3.0}};
        const auto tied = routing.candidate_paths(at(0), repeated).at("D");
        require(tied.size() == 2 && tied[0].nodes == path->nodes && tied[1].nodes.front() == "D" &&
                    tied[0].cost == tied[1].cost,
                "Repeated ingress or equal-cost candidates are not deterministic");
        rejects([&] { routing.candidate_paths(at(0), std::vector<RouteGraph::Entry>{{"A", -1.0}}); },
                "Invalid diagnostic entry was accepted");
        rejects([&] {
            routing.candidate_paths(at(0), std::vector<RouteGraph::Entry>{
                {"A", 3.0}, {"A", std::numeric_limits<double>::quiet_NaN()}});
        }, "Duplicate ingress masked an invalid access cost");
        const std::vector<RouteGraph::Entry> only_a{access.front()};
        require(!calculate_path(routing, "D", only_a, at(15)), "Expired snapshot remained routable");
        require(!calculate_path(routing, "A", std::span(access).subspan(1), at(0)), "Reverse edge was invented");
        const auto direct = calculate_path(routing, "D", access, at(15));
        require(direct && direct->nodes == std::vector<std::string>({"D"}), "Local direct path needs stale topology");
        const auto expired_candidates = routing.candidate_paths(at(15), access);
        require(expired_candidates.at("D").size() == 1 && expired_candidates.at("D")[0].cost == 50.0,
                "Diagnostic candidates reused stale remote edges");

        routing.begin_request(2, at(1));
        const auto invalid_nodes = snapshot(2, Json::array({node("A", "192.0.2.1"), node("A", "192.0.2.3")}),
                                            Json::array());
        rejects([&] { routing.accept_snapshot(invalid_nodes, at(1)); }, "Duplicate node was accepted");
        require(calculate_path(routing, "D", only_a, at(1)).has_value(), "Invalid response erased last good snapshot");
        require(!routing.accept_snapshot(snapshot(2, Json::array(), Json::array()), at(11)), "Timed-out response applied");
        routing.begin_request(3, at(1));
        rejects([&] { routing.accept_snapshot(snapshot(3, Json::array({node("A", "192.0.2.1")}),
                                                     Json::array({edge("A", "unknown", 1.0)})), at(1)); },
                "Unknown endpoint was accepted");
        routing.begin_request(4, at(1));
        auto invalid = snapshot(4, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                             Json::array({edge("A", "D", 1.0)}));
        invalid["links"][0]["quality"]["confidence"] = 2.0;
        rejects([&] { routing.accept_snapshot(invalid, at(1)); }, "Invalid quality was accepted");

        AgentRouting one_hop(io, true, 1);
        one_hop.begin_request(1, at(0));
        one_hop.accept_snapshot(first, at(0));
        require(!calculate_path(one_hop, "D", only_a, at(0)), "Virtual Agent consumed or bypassed Node budget");
        require(!one_hop.candidate_paths(at(0), only_a).contains("D"),
                "Diagnostic candidates bypassed the real Node budget");
        AgentRouting producer(io, false, 4);
        require(producer.set_required_targets({{"entry", "127.0.0.1"}}), "New required targets were not detected");
        require(!producer.set_required_targets({{"entry", "127.0.0.1"}}), "Identical targets appeared changed");
        require(producer.set_required_targets({{"entry", "127.0.0.2"}}), "Changed target host was not detected");
        require(producer.set_required_targets({}), "Removed targets were not detected");
        require(!producer.set_required_targets({}), "Repeated empty targets appeared changed");
        producer.begin_request(1, at(0));
        producer.accept_snapshot(first, at(0));
        require(!calculate_path(producer, "@agent", {}, at(0)), "Virtual source invented a zero-hop service route");

        routing.begin_request(5, at(0));
        routing.accept_snapshot(snapshot(5, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                                 Json::array({edge("A", "D", 1.0, 44900)}), 2), at(0));
        require(!calculate_path(routing, "D", only_a, at(0)), "Snapshot creation age was not added to edge age");
        // A fresh snapshot must not extend the life of an old source report.
        routing.begin_request(6, at(0));
        auto aged_report = snapshot(6, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                                Json::array({edge("A", "D", 5.0)}), 3);
        aged_report["created_age_ms"] = 0u;
        aged_report["nodes"][0]["report_age_ms"] = 14000u;
        routing.accept_snapshot(aged_report, at(0));
        require(calculate_path(routing, "D", only_a, at(0)).has_value(), "Fresh source report was rejected");
        require(!calculate_path(routing, "D", only_a, at(1)), "Source report remained usable at exactly 15 seconds");
        require(!calculate_path(routing, "D", only_a, at(2)), "16-second source report was masked by a fresh snapshot");
        const auto aged_candidates = routing.candidate_paths(at(2), access).at("D");
        require(aged_candidates.size() == 1 && aged_candidates[0].nodes == std::vector<std::string>({"D"}),
                "Candidate logging accepted a 16-second source report");
        require(calculate_path(routing, "D", access, at(2))->nodes == std::vector<std::string>({"D"}),
                "Stale remote report disabled valid direct access");

        routing.begin_request(7, at(0));
        auto fresh = snapshot(7, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                          Json::array({edge("A", "D", 5.0)}), 4);
        fresh["created_age_ms"] = 0u;
        routing.accept_snapshot(fresh, at(0));
        require(!calculate_path(routing, "D", only_a, at(-1)), "Snapshot was used before its creation time");
        require(!calculate_path(routing, "D", only_a, at(15)), "Snapshot/report expiry boundary was ignored");

        routing.begin_request(8, at(0));
        auto bad = snapshot(8, Json::array({node("A", "192.0.2.1"), node("A", "192.0.2.3")}), Json::array(), 5);
        rejects([&] { routing.accept_snapshot(bad, at(0)); }, "Malformed request did not terminate");
        bad["nodes"][1]["node_id"] = "D";
        require(!routing.accept_snapshot(bad, at(0)), "A terminated request resumed");

        // Quality changes must change recommendations without changing service ownership.
        routing.begin_request(9, at(0));
        auto competition = snapshot(9,
            Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
            Json::array({edge("A", "D", 5.0)}), 6);
        competition["created_age_ms"] = 0u;
        routing.accept_snapshot(competition, at(0));
        require(calculate_path(routing, "D", access, at(0))->nodes == std::vector<std::string>({"A", "D"}),
                "Good remote quality did not win over direct access");
        routing.begin_request(10, at(0));
        competition["request_id"] = 10u;
        competition["snapshot_version"] = 7u;
        competition["links"][0]["quality"]["cost"] = 60.0;
        routing.accept_snapshot(competition, at(0));
        require(calculate_path(routing, "D", access, at(0))->nodes == std::vector<std::string>({"D"}),
                "Degraded quality did not change the recommendation");

        routing.invalidate_snapshot();
        require(!calculate_path(routing, "D", only_a, at(0)), "Disconnected topology remained routable");

        routing.begin_request(1, at(0));
        rejects([&] { routing.accept_snapshot(Json{{"request_id", "invalid"}}, at(0)); },
                "Malformed request identity was accepted");
        require(routing.request_pending(at(0)), "Unrelated malformed response aborted the active request");
        require(routing.accept_snapshot(first, at(0)), "Active request did not survive an unrelated malformed response");

        // Limiting diagnostic logs must not truncate the routing result.
        AgentRouting many_ingresses(io, false, 4);
        many_ingresses.begin_request(1, at(0));
        many_ingresses.accept_snapshot(snapshot(1,
            Json::array({node("A", "127.0.0.1"), node("B", "127.0.0.2"), node("C", "127.0.0.3"),
                         node("E", "127.0.0.4"), node("D", "127.0.0.5")}),
            Json::array({edge("A", "D", 5.0), edge("B", "D", 5.0), edge("C", "D", 5.0), edge("E", "D", 5.0)})),
            at(0));
        const std::vector<RouteGraph::Entry> all_ingresses{{"A", 1.0}, {"B", 2.0}, {"C", 3.0}, {"E", 4.0}, {"D", 50.0}};
        const auto all_candidates = many_ingresses.candidate_paths(at(0), all_ingresses).at("D");
        require(all_candidates.size() == 5 && all_candidates.front().nodes == std::vector<std::string>({"A", "D"}) &&
                    all_candidates.front().cost == 8.0 && all_candidates.back().cost == 50.0,
                "Candidate routing dropped alternatives or failed to select the lowest cost path");
        std::cout << "[PASS] Agent complete snapshots and service paths\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
