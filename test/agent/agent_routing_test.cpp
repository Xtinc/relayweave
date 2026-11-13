#include "node_address_fixture.h"
#include "test_data.h"
#include "relay_agent.h"
#include "routing_fixture.h"
#include <fstream>
#include "link_quality.h"

#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>

TEST_MEMBER(ProbeSet, targets_)
TEST_MEMBER(AgentRouting, path_available)

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
    return {{"node_id", std::move(id)}, {"network_ports", test_node_address(std::move(address)).to_params().at("network_ports")}, {"report_age_ms", 0u}};
}
std::optional<RouteGraph::Path> calculate_path(test_routing::Fixture &routing, std::string_view destination,
                                     std::span<const RouteGraph::Entry> access, AgentRouting::Clock::time_point now)
{
    const auto paths = test_routing::candidates(routing, std::string(destination), now, access);
    return paths.empty() ? std::nullopt : std::optional(paths.front());
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

void test_network_selection(asio::io_context &io)
{
    test_routing::Fixture routing(io, 4, "vpc-a");
    const auto now = AgentRouting::Clock::now();
    auto a = node("A", "192.0.2.1");
    a.update(test_node_address({{"default", "192.0.2.1"}, {"vpc-a", "192.168.1.1"}}).to_params());
    auto b = node("B", "192.0.2.2");
    b.update(test_node_address({{"default", "192.0.2.2"}, {"vpc-a", "192.168.1.2"}}).to_params());
    auto topology = snapshot(1, Json::array({a, b}), Json::array({edge("A", "B", 2.0)}));
    topology["created_age_ms"] = 0u;
    routing.set_required_targets({{"A", "192.0.2.99"}});
    routing.begin_request(1, now);
    require(routing.accept_snapshot(topology, now), "Labeled snapshot rejected");
    const auto &targets = test_access::ProbeSet_targets_(test_access::AgentRouting_probes_(routing));
    require(targets.at("A") == "192.168.1.1" && targets.at("B") == "192.168.1.2",
            "Stale discovery or primary host overrode selected topology addresses");
    const std::vector<RouteGraph::Entry> initial{{"A", 1.0}};
    test_routing::candidates(routing, "B", now, initial);
    const ServiceKey service{"network-test", RelayProtocol::Tcp};
    require(routing.select_path(service, "B") == std::vector<std::string>({"A", "B"}), "Initial path not cached");
    topology["request_id"] = 2u;
    topology["snapshot_version"] = 2u;
    topology["nodes"][1]["network_ports"][1]["address"] = "192.168.1.3";
    routing.begin_request(2, now);
    require(routing.accept_snapshot(topology, now) && targets.at("B") == "192.168.1.3",
            "Selected address update did not replace the probe target");
    const std::vector<RouteGraph::Entry> changed{{"A", 1.0}, {"B", 0.1}};
    test_routing::candidates(routing, "B", now, changed);
    require(routing.select_path(service, "B") == std::vector<std::string>({"B"}),
            "Selected address change retained the old cached path");
    topology["request_id"] = 3u;
    topology["snapshot_version"] = 3u;
    topology["nodes"][1]["network_ports"][1]["label"] = "other";
    routing.begin_request(3, now);
    require(routing.accept_snapshot(topology, now) && targets.at("B") == "192.0.2.2",
            "Missing Agent label did not fall back to default");
}

void test_bandwidth(asio::io_context &io)
{
    test_routing::Fixture routing(io, 4);
    auto topology = snapshot(1,
        Json::array({node("A", "192.0.2.1"), node("B", "192.0.2.2"),
                     node("C", "192.0.2.3"), node("D", "192.0.2.4")}),
        Json::array({edge("A", "B", 1.0), edge("B", "D", 1.0),
                     edge("A", "C", 5.0), edge("C", "D", 5.0)}));
    topology["created_age_ms"] = 0u;
    for (auto &item : topology["nodes"]) item["available"] = 1.0;
    unsigned version = 0;
    const auto apply = [&] {
        topology["request_id"] = ++version;
        topology["snapshot_version"] = version;
        routing.begin_request(version, at(0));
        require(routing.accept_snapshot(topology, at(0)), "Bandwidth snapshot rejected");
    };
    const std::vector<RouteGraph::Entry> access{{"A", 3.0}, {"D", 50.0}};
    apply();
    require(calculate_path(routing, "D", access, at(0))->cost == 9.0, "Idle bandwidth changed route cost");
    topology["nodes"][1]["available"] = 0.5;
    apply();
    require(calculate_path(routing, "D", access, at(0))->nodes == std::vector<std::string>({"A", "C", "D"}),
            "Destination bandwidth did not affect search within the same ingress");
    topology["nodes"][0]["available"] = 0.1;
    apply();
    require(calculate_path(routing, "D", access, at(0))->nodes == std::vector<std::string>({"D"}),
            "Congested ingress did not lose to direct access");

    topology["links"] = Json::array({edge("A", "B", 1.0), edge("B", "D", 1.0)});
    topology["nodes"][0]["available"] = 0.5;
    topology["nodes"][1]["available"] = 0.25;
    topology["nodes"][3]["available"] = 0.2;
    apply();
    const auto paths = test_routing::candidates(routing, "D", at(0), access);
    require(paths.size() == 2 && std::abs(paths[0].cost - 89.0) < 1e-9 && std::abs(paths[1].cost - 90.0) < 1e-9,
            "Ingress, intermediate or endpoint bandwidth was omitted or counted twice");

    const auto direct_cost = [&](int seconds = 0) {
        return calculate_path(routing, "D", std::span(access).subspan(1), at(seconds))->cost;
    };
    for (const auto &[available, penalty] : std::vector<std::pair<double, double>>{
             {1.0, 0.0}, {0.5, 10.0}, {0.2, 40.0}, {0.05, 190.0}, {0.0, 190.0}})
    {
        topology["nodes"][3]["available"] = available;
        apply();
        require(std::abs(direct_cost() - 50.0 - penalty) < 1e-9, "Bandwidth formula or saturation cap incorrect");
    }
    topology["nodes"][3]["report_age_ms"] = 14000u;
    apply();
    require(direct_cost() == 240.0 && direct_cost(1) == 50.0 && direct_cost(15) == 50.0,
            "Expired report or snapshot still charged bandwidth");
    topology["nodes"][3]["report_age_ms"] = nullptr;
    apply();
    require(direct_cost() == 50.0, "Unknown report age charged bandwidth");
    topology["nodes"][3]["report_age_ms"] = 0u;
    topology["nodes"][3]["available"] = nullptr;
    apply();
    require(direct_cost() == 50.0, "Unknown availability charged bandwidth");
    topology["nodes"][3]["available"] = 0.5;
    apply();
    for (const auto &value : std::vector<Json>{-0.1, 1.1, "0.5", true, std::numeric_limits<double>::infinity()})
    {
        topology["nodes"][3]["available"] = value;
        topology["request_id"] = ++version;
        routing.begin_request(version, at(0));
        rejects([&] { routing.accept_snapshot(topology, at(0)); }, "Invalid availability accepted");
        require(direct_cost() == 60.0, "Invalid reply replaced last valid availability");
    }
    routing.invalidate_snapshot();
    require(direct_cost() == 50.0, "Disconnected snapshot retained bandwidth penalty");
}

void test_configured_paths(asio::io_context &io)
{
    test_routing::Fixture routing(io, 4);
    const auto now = AgentRouting::Clock::now();
    auto topology = snapshot(1, Json::array({node("A", "192.0.2.1"), node("X", "192.0.2.2"),
                                           node("Y", "192.0.2.3"), node("D", "192.0.2.4")}),
                             Json::array({edge("A", "D", 1.0), edge("A", "X", 5.0), edge("X", "D", 5.0),
                                          edge("A", "Y", 6.0), edge("Y", "D", 6.0)}));
    topology["created_age_ms"] = 0u;
    unsigned sequence = 0;
    const auto update = [&] {
        topology["request_id"] = ++sequence;
        topology["snapshot_version"] = sequence;
        routing.begin_request(sequence, AgentRouting::Clock::now());
        require(routing.accept_snapshot(topology, AgentRouting::Clock::now()), "Configured-route snapshot rejected");
    };
    update();
    const std::vector<RouteGraph::Entry> entries{{"A", 1.0}, {"D", 50.0}};
    test_routing::candidates(routing, "D", AgentRouting::Clock::now(), entries);
    const ServiceKey service{"shared", RelayProtocol::Tcp};
    const std::vector<std::string> automatic{"A", "D"};
    const std::vector<std::string> via_x{"A", "X", "D"};
    const std::vector<std::string> via_y{"A", "Y", "D"};
    require(routing.select_path(service, "D") == automatic, "Automatic baseline route changed");
    require(routing.select_path(service, "D", via_x) == via_x, "Cached cheaper route overrode configured path");
    require(routing.select_path(service, "D", via_y) == via_y, "Listeners to the same service shared manual paths");
    require(routing.select_path(service, "D", {"D"}) == std::vector<std::string>({"D"}),
            "Single-Node configured path was not preferred");
    require(routing.select_path(service, "D") == automatic, "Configured path polluted the automatic cache");
    for (const auto &invalid : std::vector<std::vector<std::string>>{
             {"A", "missing", "D"}, {"A", "X"}, {"X", "A", "D"},
             {"A", "X", "Y", "D"}, {"A", "X", "Y", "missing", "D"}})
    {
        require(routing.select_path(service, "D", invalid) == automatic,
                "Unavailable or invalid configured path did not fall back automatically");
    }
    require(!test_access::AgentRouting_path_available(routing, via_x, "D", now + std::chrono::seconds(16)),
            "Expired topology accepted a configured path");
    topology["nodes"][1]["report_age_ms"] = 15000u;
    update();
    require(routing.select_path(service, "D", via_x) == automatic, "Stale Node report accepted configured path");
    topology["nodes"][1]["report_age_ms"] = 0u;
    topology["links"][1]["quality"]["usable"] = false;
    update();
    require(routing.select_path(service, "D", via_x) == automatic, "Unusable link accepted configured path");
    topology["links"][1]["quality"]["usable"] = true;
    topology["links"][1]["age_ms"] = 45000u;
    update();
    require(routing.select_path(service, "D", via_x) == automatic, "Expired link accepted configured path");
    topology["links"][1]["age_ms"] = 0u;
    update();
    require(routing.select_path(service, "D", via_x) == via_x, "Recovered configured path remained behind cache");
    require(routing.select_path(service, "Y", via_x) == std::vector<std::string>({"A", "Y"}),
            "Service relocation did not reject the old configured destination");

    test_routing::Fixture limited(io, 2);
    limited.begin_request(sequence, AgentRouting::Clock::now());
    require(limited.accept_snapshot(topology, AgentRouting::Clock::now()), "Limited-route snapshot rejected");
    test_routing::candidates(limited, "D", AgentRouting::Clock::now(), entries);
    require(limited.select_path(service, "D", via_x) == automatic, "Configured path bypassed max_nodes");

    test_routing::candidates(routing, "D", AgentRouting::Clock::now(), std::span(&entries[1], 1));
    require(routing.select_path(service, "D", via_x) == automatic, "Unmeasured ingress accepted configured path");
    test_routing::candidates(routing, "D", AgentRouting::Clock::now(), entries);
    topology["nodes"].erase(1);
    topology["links"].erase(topology["links"].begin() + 1, topology["links"].begin() + 3);
    update();
    require(routing.select_path(service, "D", via_x) == automatic, "Offline Node did not trigger automatic fallback");

    test_routing::Fixture empty(io, 4);
    require(empty.select_path(service, "D", via_x).empty(), "No-topology fallback changed direct behavior");
}
}

int main()
{
    try
    {
        asio::io_context io(1);
        test_bandwidth(io);
        test_network_selection(io);
        test_configured_paths(io);
        require(AgentConfig{}.routing_max_nodes == 4, "Default real Node budget changed");
        const auto data = test_data::directory();
        const auto temporary = std::filesystem::temp_directory_path() /
            ("relayweave-routing-config-" + std::to_string(AgentRouting::Clock::now().time_since_epoch().count()) + ".json");
        ScopeGuard cleanup([temporary] { std::error_code ignored; std::filesystem::remove(temporary, ignored); });
        Json settings{{"server", {{"host", "127.0.0.1"}, {"port", 18443}}},
            {"certificate", {{"ca_file", (data / "tls_channel_test_ca.pem").string()},
                             {"certificate_chain", (data / "tls_channel_test_client.pem").string()},
                             {"private_key", (data / "tls_channel_test_client.key").string()}}}};
        const auto write_settings = [&] { std::ofstream file(temporary); file << settings; };
        write_settings();
        require(load_agent_config(temporary).network_label == "default", "Missing Agent network label did not default");
        settings["network_label"] = "lo:host-a";
        write_settings();
        require(load_agent_config(temporary).network_label == "lo:host-a", "Agent network label was not parsed");
        for (const Json invalid : {Json(""), Json(1), Json(nullptr)})
        {
            settings["network_label"] = invalid;
            write_settings();
            rejects([&] { load_agent_config(temporary); }, "Invalid Agent network label accepted");
        }
        settings.erase("network_label");
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

        settings.erase("routing");
        settings["forwards"] = Json::array({Json{{"service", "shared"}, {"protocol", "tcp"},
                                                  {"listen_address", "127.0.0.1"}, {"listen_port", 10022}},
                                             Json{{"service", "shared"}, {"protocol", "tcp"},
                                                  {"listen_address", "127.0.0.1"}, {"listen_port", 10023},
                                                  {"path", {"A", "X", "D"}}}});
        write_settings();
        const auto forwards = load_agent_config(temporary).forwards;
        require(forwards[0].path.empty() && forwards[1].path == std::vector<std::string>({"A", "X", "D"}),
                "Per-forward path was not parsed independently");
        settings["forwards"][1]["path"] = Json::array();
        write_settings();
        require(load_agent_config(temporary).forwards[1].path.empty(), "Empty path did not select automatically");
        settings["forwards"][1]["path"] = {"A", "B", "C", "D", "E", "F", "G", "H"};
        write_settings();
        require(load_agent_config(temporary).forwards[1].path.size() == 8, "Eight-Node configured path rejected");
        for (const Json invalid : {Json(nullptr), Json(1), Json("A"), Json::array({1}), Json::array({""}),
                                   Json::array({"all"}), Json::array({std::string(257, 'x')}),
                                   Json::array({"A", "B", "A"}),
                                   Json::array({"A", "B", "C", "D", "E", "F", "G", "H", "I"})})
        {
            settings["forwards"][1]["path"] = invalid;
            write_settings();
            rejects([&] { load_agent_config(temporary); }, "Malformed configured path was accepted");
        }
        settings.erase("forwards");
        settings["relay"] = {{"udp_idle_timeout_ms", 250}};
        write_settings();
        const auto udp_settings = load_agent_config(temporary);
        require(udp_settings.udp_idle_timeout == std::chrono::milliseconds(250),
                "UDP idle configuration was not loaded");
        for (const Json invalid : {Json(0), Json(-1), Json(1.5), Json("invalid")})
        {
            settings["relay"] = {{"udp_idle_timeout_ms", invalid}};
            write_settings();
            rejects([&] { load_agent_config(temporary); }, "Invalid UDP idle timeout was accepted");
        }
        settings.erase("relay");
        write_settings();
        const auto defaults = load_agent_config(temporary);
        require(defaults.udp_idle_timeout == std::chrono::seconds(60),
                "UDP lifecycle defaults changed");

        rejects([&] { AgentRouting invalid(io, 9); }, "Nine-node Agent budget accepted");
        rejects([&] { AgentRouting invalid(io, 0); }, "Zero-node Agent budget accepted");
        test_routing::Fixture eight(io, 8);
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
        test_routing::Fixture four(io, 4);
        four.begin_request(1, at(0));
        four.accept_snapshot(snapshot(1, chain_nodes, chain_links), at(0));
        require(calculate_path(four, "3", ingress, at(0)).has_value() && !calculate_path(four, "4", ingress, at(0)),
                "Configured four-node limit omitted endpoints");
        test_routing::Fixture routing(io, 4);
        const auto first = snapshot(1, Json::array({node("A", "192.0.2.1"), node("D", "192.0.2.2")}),
                                    Json::array({edge("A", "D", 5.0)}));
        routing.begin_request(1, at(0));
        require(routing.accept_snapshot(first, at(0)), "Complete snapshot was not applied");
        require(!routing.accept_snapshot(first, at(0)), "Duplicate snapshot was applied");
        const std::vector<RouteGraph::Entry> access{{"A", 3.0}, {"D", 50.0}};
        require(test_access::AgentRouting_candidate_paths(routing, "D", at(0)).empty(), "Unmeasured ingress invented candidate routes");
        auto path = calculate_path(routing, "D", access, at(0));
        require(path && path->nodes == std::vector<std::string>({"A", "D"}), "Best entry was not selected");
        require(std::abs(path->cost - 10.0) < 1e-9, "Access edge/hop penalty was omitted");
        require(test_routing::candidates(routing, "D", at(0), std::span<const RouteGraph::Entry>{}).empty(),
                "Empty measurement set invented candidate routes");
        const auto to_d = test_routing::candidates(routing, "D", at(0), access);
        require(to_d.size() == 2 && to_d[0].cost < to_d[1].cost &&
                    to_d[1].nodes == std::vector<std::string>({"D"}) && to_d[1].cost == 50.0,
                "Per-ingress candidates lost an alternative or are not ordered by cost");
        const std::vector<RouteGraph::Entry> tied_access{{"D", 10.0}, {"A", 3.0}};
        const auto tied = test_routing::candidates(routing, "D", at(0), tied_access);
        require(tied.size() == 2 && tied[0].nodes == path->nodes && tied[1].nodes.front() == "D" &&
                    tied[0].cost == tied[1].cost,
                "Equal-cost candidates are not ordered deterministically");
        const std::vector<RouteGraph::Entry> only_a{access.front()};
        require(!calculate_path(routing, "D", only_a, at(15)), "Expired snapshot remained routable");
        require(!calculate_path(routing, "A", std::span(access).subspan(1), at(0)), "Reverse edge was invented");
        const auto direct = calculate_path(routing, "D", access, at(15));
        require(direct && direct->nodes == std::vector<std::string>({"D"}), "Local direct path needs stale topology");
        const auto expired_candidates = test_routing::candidates(routing, "D", at(15), access);
        require(expired_candidates.size() == 1 && expired_candidates[0].cost == 50.0,
                "Measured candidates reused stale remote edges");

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

        routing.begin_request(5, at(1));
        auto duplicate_links = snapshot(5, first.at("nodes"),
                                        Json::array({edge("A", "D", 1.0), edge("D", "A", 3.0),
                                                     edge("A", "D", 2.0)}), 2);
        duplicate_links["epoch"] = 12u;
        duplicate_links["nodes"][1]["available"] = 0.1;
        rejects([&] { routing.accept_snapshot(duplicate_links, at(1)); }, "Duplicate directed link was accepted");
        const auto retained = calculate_path(routing, "D", only_a, at(1));
        require(retained && retained->cost == path->cost && retained->nodes == path->nodes && routing.epoch() == 11u,
                "Late snapshot failure replaced the previous topology, availability or epoch");
        require(!routing.request_pending(at(1)), "Invalid links did not terminate the snapshot request");

        test_routing::Fixture one_hop(io, 1);
        one_hop.begin_request(1, at(0));
        one_hop.accept_snapshot(first, at(0));
        require(!calculate_path(one_hop, "D", only_a, at(0)), "Virtual Agent consumed or bypassed Node budget");
        require(test_routing::candidates(one_hop, "D", at(0), only_a).empty(),
                "Measured candidates bypassed the real Node budget");
        test_routing::Fixture producer(io, 4);
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
        const auto aged_candidates = test_routing::candidates(routing, "D", at(2), access);
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

        test_routing::Fixture many_ingresses(io, 4);
        many_ingresses.begin_request(1, at(0));
        many_ingresses.accept_snapshot(snapshot(1,
            Json::array({node("A", "127.0.0.1"), node("B", "127.0.0.2"), node("C", "127.0.0.3"),
                         node("E", "127.0.0.4"), node("D", "127.0.0.5")}),
            Json::array({edge("A", "D", 5.0), edge("B", "D", 5.0), edge("C", "D", 5.0), edge("E", "D", 5.0)})),
            at(0));
        const std::vector<RouteGraph::Entry> all_ingresses{{"A", 1.0}, {"B", 2.0}, {"C", 3.0}, {"E", 4.0}, {"D", 50.0}};
        const auto all_candidates = test_routing::candidates(many_ingresses, "D", at(0), all_ingresses);
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
