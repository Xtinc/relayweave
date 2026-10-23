#include "relay_agent.h"
#include "forwarder.h"
#include <future>
#include <unordered_set>
#include <utility>

AgentConfig load_agent_config(const std::filesystem::path &path)
{
    using namespace config;
    const auto root = load_json(path);
    reject_unknown_fields(
        root, {"log", "server", "certificate", "services", "forwards", "relay", "reconnect", "channel", "routing"},
        "root");

    const auto &server = required_object(root, "server", "root");
    reject_unknown_fields(server, {"host", "port", "connect_timeout_ms"}, "server");
    const auto &certificate = required_object(root, "certificate", "root");
    reject_unknown_fields(certificate, {"ca_file", "certificate_chain", "private_key", "server_name"}, "certificate");

    AgentConfig result;
    result.host = required_string(server, "host", "server");
    result.port = required_port(server, "port", "server");
    result.connect_timeout = optional_duration(server, "connect_timeout_ms", result.connect_timeout, "server");
    const auto base = configuration_directory(path);
    result.ca_file = required_file(certificate, "ca_file", base, "certificate");
    result.certificate_chain = required_file(certificate, "certificate_chain", base, "certificate");
    result.private_key = required_file(certificate, "private_key", base, "certificate");
    const auto server_name_iter = certificate.find("server_name");
    if (server_name_iter != certificate.end())
    {
        if (!server_name_iter->is_string())
        {
            throw std::runtime_error("certificate.server_name must be a string");
        }
        result.server_name = server_name_iter->get<std::string>();
    }
    result.channel = parse_channel_config(root);
    if (const auto routing = root.find("routing"); routing != root.end())
    {
        reject_unknown_fields(*routing, {"max_nodes"}, "routing");
        if (routing->contains("max_nodes"))
        {
            const auto maximum = require_unsigned(*routing, "max_nodes", true);
            if (maximum > 8)
            {
                throw std::runtime_error("routing.max_nodes must be between 1 and 8");
            }
            result.routing_max_nodes = static_cast<std::size_t>(maximum);
        }
    }

    const auto services = root.find("services");
    if (services != root.end())
    {
        if (!services->is_array())
        {
            throw std::runtime_error("root.services must be an array");
        }
        std::unordered_set<std::string> names;
        for (std::size_t index = 0; index < services->size(); ++index)
        {
            const auto &service = services->at(index);
            const auto location = "services[" + std::to_string(index) + "]";
            reject_unknown_fields(service, {"name", "target_host", "target_port", "protocol"}, location);
            AgentServiceConfig parsed;
            parsed.name = required_string(service, "name", location);
            if (check_illegal_service_name(parsed.name))
            {
                throw std::runtime_error(location + ".name contains invalid characters or has an invalid length");
            }
            parsed.target_host = required_string(service, "target_host", location);
            parsed.target_port = required_port(service, "target_port", location);
            parsed.protocol = required_protocol(service, location);
            if (!names.insert(parsed.name).second)
            {
                throw std::runtime_error("Duplicate service name: " + parsed.name);
            }
            result.services.push_back(std::move(parsed));
        }
    }

    const auto forwards = root.find("forwards");
    if (forwards != root.end())
    {
        if (!forwards->is_array())
        {
            throw std::runtime_error("root.forwards must be an array");
        }
        std::unordered_set<std::string> listen_endpoints;
        for (std::size_t index = 0; index < forwards->size(); ++index)
        {
            const auto &forward = forwards->at(index);
            const auto location = "forwards[" + std::to_string(index) + "]";
            reject_unknown_fields(forward, {"service", "listen_address", "listen_port", "protocol"}, location);
            AgentForwardConfig parsed;
            parsed.service = required_string(forward, "service", location);
            if (check_illegal_service_name(parsed.service))
            {
                throw std::runtime_error(location + ".service contains invalid characters or has an invalid length");
            }
            parsed.listen_address = required_string(forward, "listen_address", location);
            parsed.listen_port = required_port(forward, "listen_port", location);
            parsed.protocol = required_protocol(forward, location);
            const auto endpoint_key = std::string(parsed.protocol == RelayProtocol::Udp ? "udp" : "stream") + ":" +
                                      parsed.listen_address + ":" + std::to_string(parsed.listen_port);
            if (!listen_endpoints.insert(endpoint_key).second)
            {
                throw std::runtime_error("Duplicate forward listen endpoint: " + endpoint_key);
            }
            result.forwards.push_back(std::move(parsed));
        }
    }

    const auto relay = root.find("relay");
    if (relay != root.end())
    {
        reject_unknown_fields(*relay, {"open_timeout_ms"}, "relay");
        result.relay_open_timeout = optional_duration(*relay, "open_timeout_ms", result.relay_open_timeout, "relay");
    }

    const auto reconnect = root.find("reconnect");
    if (reconnect != root.end())
    {
        reject_unknown_fields(*reconnect, {"initial_delay_ms", "max_delay_ms"}, "reconnect");
        result.reconnect_initial_delay =
            optional_duration(*reconnect, "initial_delay_ms", result.reconnect_initial_delay, "reconnect");
        result.reconnect_max_delay =
            optional_duration(*reconnect, "max_delay_ms", result.reconnect_max_delay, "reconnect");
    }
    if (result.reconnect_initial_delay > result.reconnect_max_delay)
    {
        throw std::runtime_error("reconnect.initial_delay_ms must not exceed reconnect.max_delay_ms");
    }
    initialize_logger_config(root);
    return result;
}

RelayAgent::RelayAgent(asio::io_context &control_io, asio::io_context &transfer_io, asio::ssl::context &ssl_context,
                       AgentConfig config)
    : control_executor_(control_io.get_executor()), transfer_executor_(transfer_io.get_executor()),
      config_(std::move(config)), routing_(control_io, config_.routing_max_nodes),
      connections_(*this, control_executor_, ssl_context, config_),
      forwarder_(std::make_unique<Forwarder>(*this, transfer_executor_, ssl_context, config_.server_name,
                                             config_.channel.handshake_timeout, config_.connect_timeout,
                                             config_.relay_open_timeout, config_.services, config_.forwards)),
      control_(control_executor_, transfer_executor_, config_, connections_, routing_, *forwarder_)
{
    if (&control_io == &transfer_io)
    {
        throw std::invalid_argument("Control and transfer channels require distinct io_context instances");
    }
}

RelayAgent::~RelayAgent() = default;

void RelayAgent::start()
{
    if (state_ != State::Created)
    {
        PROXY_ERROR_PRINT("RelayWeave Agent can only be started once.");
        return;
    }
    forwarder_->start();
    state_ = State::Running;
    connections_.start();
    control_.start();
}

void RelayAgent::stop()
{
    if (state_ == State::Stopped)
    {
        return;
    }
    asio::co_spawn(control_executor_, stop_on_control_executor(), asio::use_future).get();
}

asio::awaitable<RelaySelection> RelayAgent::select_relay(ServiceKey service, std::string destination,
                                                         std::chrono::steady_clock::time_point deadline)
{
    return asio::co_spawn(control_executor_,
                          control_.select_relay(std::move(service), std::move(destination), deadline),
                          asio::use_awaitable);
}

asio::awaitable<void> RelayAgent::stop_on_control_executor()
{
    state_ = State::Stopping;
    PROXY_INFO_PRINT("Agent stopping");
    control_.stop();
    routing_.stop();
    connections_.stop();
    asio::post(transfer_executor_, [this] { forwarder_->invalidate_relays("agent stopping"); });

    co_await control_.close();
    co_await routing_.close();
    co_await connections_.close();
    co_await asio::co_spawn(transfer_executor_, forwarder_->async_stop(), asio::use_awaitable);
    state_ = State::Stopped;
    PROXY_INFO_PRINT("Agent stopped");
}

void RelayAgent::connection_ready(const NodeConnection &connection)
{
    control_.connection_ready(connection);
}

void RelayAgent::connection_closed(const NodeConnection &connection)
{
    control_.connection_closed(connection);
}

void RelayAgent::connection_finished(const NodeConnection &connection)
{
    control_.connection_finished(connection);
}

void RelayAgent::handle_control_message(const NodeConnection &connection, CtrlMessage message)
{
    control_.handle_control_message(connection, std::move(message));
}
