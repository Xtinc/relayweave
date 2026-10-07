#pragma once
#include "relay_node.h"

inline NodeConfig make_test_node_config()
{
    NodeConfig config;
    const auto data = std::filesystem::path(__FILE__).parent_path() / "data";
    config.certificate_chain = data / "tls_channel_test_server.pem";
    config.private_key = data / "tls_channel_test_server.key";
    config.server_ca_file = data / "tls_channel_test_ca.pem";
    config.cluster = {ClusterConfig::Role::Master, "test-master", "127.0.0.1", 0};
    config.control.advertise_address = "127.0.0.1";
    return config;
}
