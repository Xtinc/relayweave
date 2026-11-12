#include "node_address_fixture.h"
#include "node_test_config.h"
#include "topology.h"
#include "node_control.h"
#include "member_access.h"

#include <iostream>

TEST_MEMBER(Topology, probes_)
TEST_MEMBER(ProbeSet, targets_)

namespace
{
void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

njson link(std::string destination, double rtt, double jitter, double loss)
{
    return njson{{"destination", std::move(destination)},
                 {"rtt_ms", rtt},
                 {"jitter_ms", jitter},
                 {"loss_rate", loss},
                 {"transmitted", std::uint64_t{20}},
                 {"received", std::uint64_t{19}},
                 {"completed", std::uint64_t{20}},
                 {"age_ms", std::uint64_t{100}},
                 {"quality", quality_json(LinkQuality::Summary{rtt, 0.8, true})}};
}

njson report(std::string source, std::uint64_t sequence, njson links, std::uint32_t delay)
{
    return njson{{"source", std::move(source)},
                 {"epoch", std::uint64_t{101}},
                 {"members_version", std::uint64_t{7}},
                 {"sequence", sequence},
                 {"control_queue_delay_us", delay},
                 {"transfer_tcp_queue_delay_us", delay + 1},
                 {"transfer_udp_queue_delay_us", delay + 2},
                 {"available", 1.0},
                 {"links", std::move(links)}};
}
} // namespace

int main()
{
    const char *stage = "initialise";
    try
    {
        asio::io_context control_io(1);
        asio::io_context tcp_io(1);
        asio::io_context udp_io(1);
        TestClusterDataIO cluster_data;
        auto settings = make_test_node_config();
        settings.control.address = settings.tcp.address = settings.datagram.address =
            "127.0.0.1";
        asio::ssl::context context(asio::ssl::context::tls);
        context.use_certificate_chain_file(settings.certificate_chain.string());
        context.use_private_key_file(settings.private_key.string(), asio::ssl::context::pem);
        context.load_verify_file(settings.server_ca_file.string());

        stage = "construct RelayNode";
        auto owner = std::make_shared<RelayNode>(control_io, tcp_io, udp_io, cluster_data.io, context, settings);
        stage = "construct topology";
        auto cluster = std::make_shared<ClusterMgr>(*owner, control_io.get_executor(), context, settings.cluster,
                                                    settings.channel, 16);
        RegistryMgr registry(16, 16, 16);
        NodeMetrics metrics(control_io.get_executor(), tcp_io.get_executor(), udp_io.get_executor(), registry, 100);
        Topology topology(control_io, "master", true, *cluster, metrics);

        stage = "update members";
        topology.update_members(njson{{"source", "master"},
                                      {"master", "master"},
                                      {"epoch", std::uint64_t{101}},
                                      {"version", std::uint64_t{7}},
                                      {"members", njson::array({test_node_member("master", "192.0.2.1"),
                                                                 test_node_member("slave", "192.0.2.2")})}});
        bool rejected_master_change = false;
        try
        {
            topology.update_members(njson{{"source", "slave"},
                                          {"master", "slave"},
                                          {"epoch", std::uint64_t{102}},
                                          {"version", std::uint64_t{1}},
                                          {"members", njson::array({test_node_member("master", "192.0.2.1"),
                                                                     test_node_member("slave", "192.0.2.2")})}});
        }
        catch (const std::invalid_argument &)
        {
            rejected_master_change = true;
        }
        require(rejected_master_change, "Topology accepted a membership list from a non-master Node");
        stage = "update reports";
        auto master_report = report("master", 1, njson::array({link("slave", 12.0, 1.0, 0.05)}), 11);
        master_report["available"] = 0.25;
        topology.update_report(master_report);
        topology.update_report(report("slave", 1, njson::array({link("master", 15.0, 2.0, 0.10)}), 21));

        stage = "snapshot";
        auto snapshot = topology.snapshot_message(55);
        require(snapshot.has_value(), "Topology snapshot was missing");
        const auto &params = config::message_params(*snapshot);
        Topology::validate_snapshot(params, "topology.snapshot");
        require(params.at("request_id") == 55 && params.at("epoch") == 101,
                "Topology snapshot identity is incorrect");
        require(params.at("nodes").size() == 2 && params.at("links").size() == 2,
                "Topology snapshot did not contain all nodes and directed links");
        require(params.at("nodes").at(0).at("control_queue_delay_us") == 11 &&
                    params.at("nodes").at(1).at("control_queue_delay_us") == 21,
                "Topology snapshot did not preserve Node queue delay");
        require(params.at("nodes").at(0).at("available") == 0.25 &&
                    params.at("nodes").at(1).at("available") == 1.0, "Topology lost bandwidth availability");

        const auto version = params.at("snapshot_version").get<std::uint64_t>();
        topology.update_report(report("slave", 1, njson::array({link("master", 99.0, 9.0, 0.4)}), 99));
        snapshot = topology.snapshot_message(56);
        require(config::message_params(*snapshot).at("snapshot_version") == version,
                "Duplicate report sequence changed the immutable snapshot");

        stage = "label addresses and probes";
        const auto a = test_node_address({{"default", "192.0.2.1"}, {"vpc-a", "10.20.0.1"}});
        const auto b = test_node_address({{"default", "192.0.2.2"}, {"vpc-a", "10.20.0.2"}});
        const auto c = test_node_address({{"default", "192.0.2.3"}, {"vpc-b", "10.30.0.3"}});
        const auto d = test_node_address("192.0.2.4");
        require(a.address_for(b) == "10.20.0.1" && b.address_for(a) == "10.20.0.2",
                "Same-label members did not select their own label IP");
        require(a.address_for(c) == a.address_for("default") && c.address_for(a) == c.address_for("default") &&
                    a.address_for(d) == a.address_for("default") && d.address_for(a) == d.address_for("default") &&
                    d.address_for(test_node_address("192.0.2.5")) == d.address_for("default"),
                "Cross-label or default members did not select public addresses");
        require(NodeAddress::from_params(a.to_params(), "test") == a, "Member metadata did not round-trip");
        Topology labeled(control_io, "master", true, *cluster, metrics);
        njson labeled_members = njson::array();
        for (const auto &[id, member] : std::vector<std::pair<std::string, NodeAddress>>{
                 {"master", a}, {"slave", b}, {"other", c}, {"public", d}})
        {
            auto params = member.to_params();
            params["node_id"] = id;
            labeled_members.push_back(std::move(params));
        }
        njson membership{{"source", "master"}, {"master", "master"}, {"epoch", 201U},
                         {"version", 1U}, {"members", labeled_members}};
        labeled.update_members(membership);
        const auto &targets = test_access::ProbeSet_targets_(test_access::Topology_probes_(labeled));
        require(targets.at("slave") == "10.20.0.2" && targets.at("other") == "192.0.2.3" &&
                    targets.at("public") == "192.0.2.4" && !targets.contains("master"),
                "Probes did not follow per-peer address selection");
        const auto public_snapshot = labeled.snapshot_message(57);
        for (const auto &node : public_snapshot->params->at("nodes"))
        {
            const auto &member = labeled.members().at(node.at("node_id").get<std::string>());
            require(NodeAddress::from_params(node, "snapshot") == member,
                    "Agent snapshot lost labeled addresses");
        }
        membership["version"] = 2U;
        membership["members"][1]["network_ports"][1]["label"] = "vpc-b";
        labeled.update_members(membership);
        require(targets.at("slave") == "192.0.2.2", "Label changes did not replace probe targets");
        membership["version"] = 3U;
        membership["members"][1].erase("network_ports");
        bool rejected_legacy = false;
        try { labeled.update_members(membership); }
        catch (const std::exception &) { rejected_legacy = true; }
        require(rejected_legacy && labeled.members().at("slave").address_for("vpc-b") == "10.20.0.2",
                "Legacy membership was accepted or partially applied");

        std::cout << "[PASS] topology aggregation, label selection, probes and public snapshots\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << stage << ": " << exception.what() << '\n';
        return 1;
    }
}
