#include "test_data.h"
#ifndef RELAYWEAVE_NODE_TEST_CONFIG_H
#define RELAYWEAVE_NODE_TEST_CONFIG_H
#include "relay_node.h"
#include <thread>

// Explicit fourth execution domain in fixtures, including fixtures that only use controls.
struct TestClusterDataIO
{
    asio::io_context io{1};
    asio::executor_work_guard<asio::io_context::executor_type> work{io.get_executor()};
    std::thread thread{[this] { io.run(); }};
    ~TestClusterDataIO()
    {
        work.reset();
        io.stop();
        thread.join();
    }
};

inline void configure_test_cluster_data(ClusterConfig &config)
{
    asio::io_context io;
    asio::ip::tcp::acceptor tcp(io, {asio::ip::address_v4::loopback(), 0});
    asio::ip::udp::socket udp(io, {asio::ip::address_v4::loopback(), 0});
    config.tcp_port = tcp.local_endpoint().port();
    config.udp_port = udp.local_endpoint().port();
}

inline NodeConfig make_test_node_config()
{
    NodeConfig config;
    const auto data = test_data::directory();
    config.certificate_chain = data / "tls_channel_test_server.pem";
    config.private_key = data / "tls_channel_test_server.key";
    config.server_ca_file = data / "tls_channel_test_ca.pem";
    config.cluster = {ClusterConfig::Role::Master, "test-master", "127.0.0.1", 0};
    configure_test_cluster_data(config.cluster);
    config.control.advertise_address = "127.0.0.1";
    return config;
}

#endif
