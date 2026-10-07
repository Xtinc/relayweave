#include "node_test_config.h"
#include "topology.h"

#include <iostream>

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
                 {"links", std::move(links)}};
}
} // namespace

int main()
{
    const char *stage = "initialise";
    try
    {
        asio::io_context control_io(1), tcp_io(1), udp_io(1);
        auto settings = make_test_node_config();
        settings.control.address = settings.tcp.address = settings.tls.address = settings.datagram.address =
            "127.0.0.1";
        asio::ssl::context context(asio::ssl::context::tls);
        context.use_certificate_chain_file(settings.certificate_chain.string());
        context.use_private_key_file(settings.private_key.string(), asio::ssl::context::pem);
        context.load_verify_file(settings.server_ca_file.string());

        stage = "construct RelayNode";
        auto owner = std::make_shared<RelayNode>(control_io, tcp_io, udp_io, context, settings);
        stage = "construct topology";
        auto cluster = std::make_shared<ClusterMgr>(*owner, control_io.get_executor(), context, settings.cluster,
                                                    settings.channel, 16);
        std::atomic<std::uint32_t> control_delay{1}, tcp_delay{2}, udp_delay{3};
        Topology topology(control_io, "master", true, *cluster, control_delay, tcp_delay, udp_delay);

        stage = "update members";
        topology.update_members(njson{{"source", "master"},
                                      {"master", "master"},
                                      {"epoch", std::uint64_t{101}},
                                      {"version", std::uint64_t{7}},
                                      {"members", njson::array({njson{{"node_id", "master"},
                                                                        {"address", "192.0.2.1"}},
                                                                 njson{{"node_id", "slave"},
                                                                       {"address", "192.0.2.2"}}})}});
        bool rejected_master_change = false;
        try
        {
            topology.update_members(njson{{"source", "slave"},
                                          {"master", "slave"},
                                          {"epoch", std::uint64_t{102}},
                                          {"version", std::uint64_t{1}},
                                          {"members", njson::array({njson{{"node_id", "master"},
                                                                            {"address", "192.0.2.1"}},
                                                                     njson{{"node_id", "slave"},
                                                                           {"address", "192.0.2.2"}}})}});
        }
        catch (const std::invalid_argument &)
        {
            rejected_master_change = true;
        }
        require(rejected_master_change, "Topology accepted a membership list from a non-master Node");
        stage = "update reports";
        topology.update_report(report("master", 1, njson::array({link("slave", 12.0, 1.0, 0.05)}), 11));
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

        const auto version = params.at("snapshot_version").get<std::uint64_t>();
        topology.update_report(report("slave", 1, njson::array({link("master", 99.0, 9.0, 0.4)}), 99));
        snapshot = topology.snapshot_message(56);
        require(config::message_params(*snapshot).at("snapshot_version") == version,
                "Duplicate report sequence changed the immutable snapshot");

        std::cout << "[PASS] topology aggregation and immutable snapshot\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << stage << ": " << exception.what() << '\n';
        return 1;
    }
}
