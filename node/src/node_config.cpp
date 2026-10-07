#include "relay_node.h"
#include <limits>

namespace
{
TrafficLimitConfig parse_traffic_limit_config(const njson &parent, std::string_view location)
{
    using namespace config;
    TrafficLimitConfig result;
    result.rx_bytes_per_second =
        static_cast<std::size_t>(optional_unsigned(parent, "rx_bytes_per_second", result.rx_bytes_per_second, 0,
                                                   std::numeric_limits<std::uint64_t>::max(), location));
    result.rx_burst_bytes = static_cast<std::size_t>(optional_unsigned(
        parent, "rx_max_burst_bytes", result.rx_burst_bytes, 0, std::numeric_limits<std::uint64_t>::max(), location));
    result.tx_bytes_per_second =
        static_cast<std::size_t>(optional_unsigned(parent, "tx_bytes_per_second", result.tx_bytes_per_second, 0,
                                                   std::numeric_limits<std::uint64_t>::max(), location));
    result.tx_burst_bytes = static_cast<std::size_t>(optional_unsigned(
        parent, "tx_max_burst_bytes", result.tx_burst_bytes, 0, std::numeric_limits<std::uint64_t>::max(), location));
    return result;
}

std::size_t required_capacity(const njson &parent, std::string_view name, std::string_view location)
{
    if (!parent.contains(name))
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " is required");
    return static_cast<std::size_t>(config::optional_unsigned(parent, name, 0, 1, config::max_queue_size, location));
}

StreamNodeConfig parse_stream_node_config(const njson &root, std::string_view name)
{
    using namespace config;
    const auto &stream = required_object(root, name, "root");
    reject_unknown_fields(stream,
                          {"address", "port", "max_setup_connections", "max_relays", "setup_timeout_ms",
                           "rx_bytes_per_second", "rx_max_burst_bytes", "tx_bytes_per_second", "tx_max_burst_bytes"},
                          name);

    StreamNodeConfig result;
    result.address = required_string(stream, "address", name);
    result.port = required_port(stream, "port", name);
    result.max_relays = required_capacity(stream, "max_relays", name);
    result.max_setup_connections = static_cast<std::size_t>(optional_unsigned(
        stream, "max_setup_connections", result.max_setup_connections, 1, max_connection_count, name));
    result.setup_timeout = optional_duration(stream, "setup_timeout_ms", result.setup_timeout, name);
    result.traffic = parse_traffic_limit_config(stream, name);
    return result;
}
} // namespace

NodeConfig load_node_config(const std::filesystem::path &path)
{
    using namespace config;
    const auto root = load_json(path);

    reject_unknown_fields(root, {"log", "control", "cluster", "tcp", "tls", "udp", "certificate", "channel"},
                          "root");
    const auto &control = required_object(root, "control", "root");
    reject_unknown_fields(
        control,
        {"address", "advertise_address", "port", "max_connections", "max_services", "max_services_per_session"},
        "control");
    const auto &udp = required_object(root, "udp", "root");
    reject_unknown_fields(udp,
                          {"address", "port", "max_relays", "service_wait_timeout_ms", "rx_bytes_per_second",
                           "rx_max_burst_bytes", "tx_bytes_per_second", "tx_max_burst_bytes"},
                          "udp");
    const auto &certificate = required_object(root, "certificate", "root");
    reject_unknown_fields(certificate, {"ca_file", "server_ca_file", "certificate_chain", "private_key"},
                          "certificate");

    NodeConfig result;
    const auto &cluster = required_object(root, "cluster", "root");
    reject_unknown_fields(cluster, {"role", "node_id", "address", "port"}, "cluster");
    const auto role = required_string(cluster, "role", "cluster");
    if (role != "master" && role != "slave")
        throw std::invalid_argument("cluster.role must be master or slave");
    result.cluster.role = role == "master" ? ClusterConfig::Role::Master : ClusterConfig::Role::Slave;
    result.cluster.node_id = required_string(cluster, "node_id", "cluster");
    if (result.cluster.node_id == cluster_broadcast_target)
        throw std::invalid_argument("cluster.node_id must not be all");
    result.cluster.address = required_string(cluster, "address", "cluster");
    result.cluster.port = required_port(cluster, "port", "cluster");

    result.control.address = required_string(control, "address", "control");
    result.control.advertise_address = required_string(control, "advertise_address", "control");
    asio::error_code advertise_error;
    const auto advertise_ip = asio::ip::make_address(result.control.advertise_address, advertise_error);
    if (!advertise_error && advertise_ip.is_unspecified())
        throw std::invalid_argument("control.advertise_address must identify a reachable server address");
    result.control.port = required_port(control, "port", "control");
    result.control.max_connections = static_cast<std::size_t>(optional_unsigned(
        control, "max_connections", result.control.max_connections, 1, max_connection_count, "control"));
    result.control.max_services = static_cast<std::size_t>(optional_unsigned(
        control, "max_services", result.control.max_services, 1, NodeConfig::maximum_services, "control"));
    result.control.max_services_per_session = static_cast<std::size_t>(
        optional_unsigned(control, "max_services_per_session", result.control.max_services_per_session, 1,
                          NodeConfig::maximum_services, "control"));
    if (result.control.max_services_per_session > result.control.max_services)
        throw std::invalid_argument("control.max_services_per_session must not exceed control.max_services");

    result.tcp = parse_stream_node_config(root, "tcp");
    result.tls = parse_stream_node_config(root, "tls");
    result.datagram.address = required_string(udp, "address", "udp");
    result.datagram.port = required_port(udp, "port", "udp");
    result.datagram.max_relays = required_capacity(udp, "max_relays", "udp");
    result.datagram.service_wait_timeout =
        optional_duration(udp, "service_wait_timeout_ms", result.datagram.service_wait_timeout, "udp");
    result.datagram.traffic = parse_traffic_limit_config(udp, "udp");

    const auto base = configuration_directory(path);
    result.ca_file = required_file(certificate, "ca_file", base, "certificate");
    result.server_ca_file = required_file(certificate, "server_ca_file", base, "certificate");
    result.certificate_chain = required_file(certificate, "certificate_chain", base, "certificate");
    result.private_key = required_file(certificate, "private_key", base, "certificate");
    result.channel = parse_channel_config(root);
    initialize_logger_config(root);
    return result;
}
