#include "test_data.h"
#include "relay_node.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace
{
using namespace std::chrono_literals;
void require(bool condition, const std::string &message)
{
    if (!condition)
        throw std::runtime_error(message);
}

void verify_server_config()
{
    const auto modern_file = test_data::directory() / "node_config_modern.json";
    const auto legacy_file = test_data::directory() / "node_config_legacy.json";
    require(std::filesystem::is_regular_file(modern_file) && std::filesystem::is_regular_file(legacy_file),
            "Missing Node configuration fixtures");
    const auto modern = load_node_config(modern_file);
    require(modern.cluster.role == ClusterConfig::Role::Master && modern.cluster.node_id == "master-1" &&
                modern.cluster.control_port == 18447, "Cluster configuration was not parsed");
    require(modern.control.advertise_address == "198.51.100.10",
            "Modern server advertise_address was not parsed");
    require(modern.control.max_connections == 12, "Modern server max_connections was not parsed");
    require(modern.control.max_services == 20 && modern.control.max_services_per_session == 5,
            "Modern server service limits were not parsed");
    require(modern.tcp.max_setup_connections == 7 && modern.tcp.max_relays == 9,
            "Modern server TCP limits were not parsed");
    require(modern.tcp.setup_timeout == 1500ms, "Modern server TCP setup timeout was not parsed");
    require(modern.tcp.traffic.rx_bytes_per_second == 1001 && modern.tcp.traffic.rx_burst_bytes == 1002 &&
                modern.tcp.traffic.tx_bytes_per_second == 1003 && modern.tcp.traffic.tx_burst_bytes == 1004,
            "Modern server TCP traffic limits were not parsed");
    require(modern.tls.address == "127.0.0.1" && modern.tls.port == 18446,
            "TLS data listener was not parsed");
    require(modern.tls.max_setup_connections == 8 && modern.tls.max_relays == 10 &&
                modern.tls.setup_timeout == 1800ms,
            "TLS data limits were not parsed");
    require(modern.tls.traffic.rx_bytes_per_second == 3001 && modern.tls.traffic.rx_burst_bytes == 3002 &&
                modern.tls.traffic.tx_bytes_per_second == 3003 && modern.tls.traffic.tx_burst_bytes == 3004,
            "TLS data traffic limits were not parsed independently");
    require(modern.datagram.address == "127.0.0.1" && modern.datagram.port == 18445,
            "UDP listener was not parsed");
    require(modern.datagram.max_relays == 11 && modern.datagram.setup_timeout == 2500ms,
            "Modern server UDP limits were not parsed");
    require(modern.datagram.traffic.rx_bytes_per_second == 2001 &&
                modern.datagram.traffic.rx_burst_bytes == 2002 &&
                modern.datagram.traffic.tx_bytes_per_second == 2003 &&
                modern.datagram.traffic.tx_burst_bytes == 2004,
            "Modern server UDP traffic limits were not parsed independently");

    bool legacy_rejected = false;
    try
    {
        static_cast<void>(load_node_config(legacy_file));
    }
    catch (const std::exception &)
    {
        legacy_rejected = true;
    }
    require(legacy_rejected, "Legacy server configuration was not rejected");

    auto baseline = config::load_json(modern_file);
    // Keep relative certificate fixtures valid when generated configurations are
    // written outside the read-only source data directory.
    for (auto &[_, value] : baseline["certificate"].items())
        value = (test_data::directory() / value.get<std::string>()).string();
    const auto temporary = std::filesystem::temp_directory_path() /
        ("relayweave-node-config-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".json");
    ScopeGuard cleanup([temporary]() noexcept {
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
    });
    const auto write_candidate = [&](const njson &candidate) {
        std::ofstream stream(temporary);
        require(static_cast<bool>(stream), "Could not create server configuration fixture");
        stream << candidate.dump(2);
    };
    // Positive control: invalid-field tests must not pass merely because a
    // relocated fixture cannot find its certificates.
    write_candidate(baseline);
    static_cast<void>(load_node_config(temporary));
    const auto require_rejected = [&](njson candidate, std::string_view description) {
        write_candidate(candidate);
        bool rejected = false;
        try
        {
            static_cast<void>(load_node_config(temporary));
        }
        catch (const std::exception &)
        {
            rejected = true;
        }
        require(rejected, "Invalid server configuration was accepted: " + std::string(description));
    };

    for (const auto *object : {"control", "cluster", "tcp", "tls", "udp", "certificate"})
    {
        auto candidate = baseline;
        candidate.erase(object);
        require_rejected(std::move(candidate), std::string("missing ") + object);
    }
    for (const auto *object : {"control", "tcp", "tls", "udp"})
    {
        for (const auto *field : {"address", "port"})
        {
            auto candidate = baseline;
            candidate[object].erase(field);
            require_rejected(std::move(candidate), std::string("missing ") + object + "." + field);
        }
    }
    for (const auto *field : {"address", "control_port", "tcp_port", "udp_port"})
    {
        auto candidate = baseline;
        candidate["cluster"].erase(field);
        require_rejected(candidate, std::string("missing cluster.") + field);
        if (std::string_view(field) == "address")
        {
            continue;
        }
        for (auto value : {-1, 0, 65536})
        {
            candidate = baseline;
            candidate["cluster"][field] = value;
            require_rejected(candidate, std::string("invalid cluster.") + field);
        }
    }
    {
        auto candidate = baseline;
        candidate["cluster"]["port"] = 18447;
        require_rejected(candidate, "legacy cluster.port");
    }
    for (const auto *field : {"control_port", "tcp_port"})
    {
        auto candidate = baseline;
        candidate["cluster"][field] = candidate["tcp"]["port"];
        require_rejected(candidate, "conflicting TCP listeners");
    }
    {
        auto candidate = baseline;
        candidate["cluster"]["udp_port"] = candidate["udp"]["port"];
        require_rejected(candidate, "conflicting UDP listeners");
    }
    {
        auto candidate = baseline;
        candidate["control"].erase("advertise_address");
        require_rejected(std::move(candidate), "missing control.advertise_address");
    }
    for (const auto *address : {"0.0.0.0", "::"})
    {
        auto candidate = baseline;
        candidate["control"]["advertise_address"] = address;
        require_rejected(std::move(candidate), "unspecified control.advertise_address");
    }
    for (const auto *role : {"hub", "member", "disabled", ""})
    {
        auto candidate = baseline;
        candidate["cluster"]["role"] = role;
        require_rejected(std::move(candidate), "invalid cluster role");
    }
    for (const auto *field : {"role", "node_id"})
    {
        auto candidate = baseline;
        candidate["cluster"].erase(field);
        require_rejected(std::move(candidate), "missing cluster field");
    }
    {
        auto candidate = baseline;
        candidate["cluster"]["node_id"] = "all";
        require_rejected(std::move(candidate), "reserved cluster node_id");
    }
    {
        auto candidate = baseline;
        candidate["certificate"].erase("server_ca_file");
        require_rejected(std::move(candidate), "missing cluster trust CA");
    }
    {
        auto candidate = baseline;
        candidate["log"]["debug_enable"] = "yes";
        require_rejected(std::move(candidate), "non-boolean log.debug_enable");
    }
    for (const auto *object : {"tcp", "tls", "udp"})
    {
        auto candidate = baseline;
        candidate[object].erase("max_relays");
        require_rejected(std::move(candidate), std::string("missing ") + object + ".max_relays");
    }
    for (const auto *field : {"listen", "data_listen", "tls_data", "udp_listen", "pipe"})
    {
        auto candidate = baseline;
        candidate[field] = njson::object();
        require_rejected(std::move(candidate), std::string("legacy root field ") + field);
    }
    auto legacy_udp_wait = baseline;
    legacy_udp_wait["udp"]["service_wait_timeout_ms"] = 1000;
    require_rejected(std::move(legacy_udp_wait), "removed udp.service_wait_timeout_ms");

    auto legacy_capacity = baseline;
    legacy_capacity["tcp"]["max_pending_relays"] = 1;
    require_rejected(std::move(legacy_capacity), "legacy tcp.max_pending_relays");
}

} // namespace

int main()
{
    try
    {
        verify_server_config();
        std::cout << "[PASS] Node configuration fields and invalid/legacy rejection\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
