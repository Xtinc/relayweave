#include "relay_node.h"

namespace
{
void validate_server_status(const njson &params, std::string_view location)
{
    using namespace config;
    reject_unknown_fields(params, {"request_id", "node_id", "uptime_ms", "services"}, location);
    require_unsigned(params, "request_id", true);
    required_string(params, "node_id", location);
    require_unsigned(params, "uptime_ms");

    const auto services = params.find("services");
    if (services == params.end() || !services->is_array())
    {
        throw std::invalid_argument(std::string(location) + ".services must be an array");
    }
    for (const auto &service : *services)
    {
        if (!service.is_object())
        {
            throw std::invalid_argument(std::string(location) + ".services entries must be objects");
        }
        reject_unknown_fields(
            service,
            {"service", "protocol", "rx_bytes", "tx_bytes", "rx_bytes_per_second", "tx_bytes_per_second", "accessors"},
            location);
        message_service(service);
        message_protocol(service);
        require_unsigned(service, "rx_bytes");
        require_unsigned(service, "tx_bytes");
        require_unsigned(service, "rx_bytes_per_second");
        require_unsigned(service, "tx_bytes_per_second");
        const auto accessors = service.find("accessors");
        if (accessors == service.end() || !accessors->is_object())
        {
            throw std::invalid_argument(std::string(location) + ".services.accessors must be an object");
        }
        for (const auto &[client, connections] : accessors->items())
        {
            if (client.empty())
            {
                throw std::invalid_argument(std::string(location) + ".services.accessors client must not be empty");
            }
            if (!connections.is_number_unsigned() || connections.get<std::uint64_t>() == 0)
            {
                throw std::invalid_argument(std::string(location) + ".services.accessors.connections must be positive");
            }
        }
    }
}
} // namespace

// Public Node control state and queries require control_io.
void RelayNode::handle_control_message(SessionId id, const ControlSessionPtr &session, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    switch (message.type())
    {
    case CtrlCommand::ServiceRegister:
        register_service(id, session, params);
        break;
    case CtrlCommand::ServerIdentify:
        session->send(CtrlMessage{CtrlCommand::ServerIdentified, njson{{"node_id", config_.cluster.node_id}}});
        break;
    case CtrlCommand::NodeLookup:
        locate_node(id, session, params);
        break;
    case CtrlCommand::ServiceLookup:
        locate_service(id, session, params);
        break;
    case CtrlCommand::ServiceList:
        list_services(session, params);
        break;
    case CtrlCommand::ServerLoad:
        report_load(session, params);
        break;
    case CtrlCommand::ServerTraffic:
        report_traffic(session, params);
        break;
    case CtrlCommand::ServerCluster:
        report_cluster_status(id, session, params);
        break;
    case CtrlCommand::TopologyQuery:
        report_topology(id, session, params);
        break;
    case CtrlCommand::RelayOpen:
    case CtrlCommand::RelayReject:
    case CtrlCommand::RelayCancel:
        handle_relay(session, message);
        break;
    default:
        throw std::invalid_argument("Unknown control command: " + message.command);
    }
}

void RelayNode::handle_control_cluster_message(CtrlMessage message)
{
    switch (message.type())
    {
    case CtrlCommand::NodeLookup:
        handle_node_lookup(config::message_params(message));
        break;
    case CtrlCommand::NodeLocated:
    case CtrlCommand::NodeError:
        handle_node_location(std::move(message));
        break;
    case CtrlCommand::ServiceLookup:
        handle_cluster_lookup(config::message_params(message));
        break;
    case CtrlCommand::ServiceLocated:
        handle_cluster_location(std::move(message));
        break;
    case CtrlCommand::ServerStatusQuery:
        handle_cluster_status_query(config::message_params(message));
        break;
    case CtrlCommand::ServerStatusReport:
        handle_cluster_status_report(std::move(message));
        break;
    case CtrlCommand::TopologyQuery:
        handle_topology_query(config::message_params(message));
        break;
    case CtrlCommand::TopologySnapshot:
        handle_topology_snapshot(std::move(message));
        break;
    default:
        if (!cluster_messages_.try_send(asio::error_code{}, std::move(message)))
        {
            PROXY_ERROR_PRINT("Cluster receive rejected reason=queue full");
        }
        break;
    }
}

void RelayNode::broadcast_client_query(SessionId id, CtrlMessage message)
{
    static_cast<void>(config::message_params(message));
    (*message.params)["session_id"] = id;
    cluster_mgr_->broadcast(std::move(message));
}

void RelayNode::attach_cluster_reply_route(CtrlMessage &reply, const njson &query)
{
    auto &params = *reply.params;
    params.erase("node_id");
    params["requester_node"] = query.at("source");
    params["session_id"] = query.at("session_id");
    params["request_id"] = query.at("request_id");
}

ControlSessionPtr RelayNode::prepare_client_reply(CtrlMessage &reply, std::string_view location)
{
    const auto &params = config::message_params(reply);
    if (config::required_string(params, "requester_node", location) != config_.cluster.node_id)
    {
        return {};
    }
    auto session = registry_.find_session(config::require_unsigned(params, "session_id", true));
    if (!session)
    {
        return {};
    }
    (*reply.params)["node_id"] = config::required_string(params, "source", location);
    reply.params->erase("source");
    reply.params->erase("requester_node");
    reply.params->erase("session_id");
    return session;
}

void RelayNode::register_service(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    SRVTrafficPtr traffic;
    const auto result = registry_.register_service(id, service, protocol, traffic);
    if (result != RegistryMgr::Result::Registered)
    {
        PROXY_ERROR_PRINT("Service rejected service=%s/%s session_id=%llu reason=%.*s",
                          service.c_str(), relay_protocol_name(protocol).data(), static_cast<unsigned long long>(id),
                          static_cast<int>(RegistryMgr::result2string(result).size()), RegistryMgr::result2string(result).data());
        session->send(CtrlMessage{CtrlCommand::ServiceError, njson{{"request_id", request_id},
                                                                   {"service", service},
                                                                   {"protocol", relay_protocol_name(protocol)},
                                                                   {"reason", RegistryMgr::result2string(result)}}});
        return;
    }

    PROXY_INFO_PRINT("Service registered service=%s/%s session_id=%llu peer=%.*s", service.c_str(),
                     relay_protocol_name(protocol).data(), static_cast<unsigned long long>(id),
                     static_cast<int>(session->peer().size()), session->peer().data());
    session->send(CtrlMessage{
        CtrlCommand::ServiceOk,
        njson{{"request_id", request_id}, {"service", service}, {"protocol", relay_protocol_name(protocol)}}});

}

void RelayNode::locate_service(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    if (auto location = service_location(params))
    {
        session->send(std::move(*location));
        return;
    }
    broadcast_client_query(id, CtrlMessage{CtrlCommand::ServiceLookup, njson{{"request_id", params.at("request_id")},
                                                                             {"service", params.at("service")},
                                                                             {"protocol", params.at("protocol")}}});
}

void RelayNode::report_cluster_status(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    session->send(server_status_message(request_id));
    broadcast_client_query(id, CtrlMessage{CtrlCommand::ServerStatusQuery, njson{{"request_id", request_id}}});
}

void RelayNode::handle_cluster_lookup(const njson &params)
{
    const auto requester = config::required_string(params, "source", "service.lookup");
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    if (auto location = service_location(params))
    {
        attach_cluster_reply_route(*location, params);
        cluster_mgr_->send(requester, std::move(*location));
    }
}

void RelayNode::handle_cluster_location(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "service.located");
    if (!session)
    {
        return;
    }
    const auto &params = config::message_params(message);
    config::require_unsigned(params, "request_id", true);
    config::message_service(params);
    config::message_protocol(params);
    config::required_string(params, "address", "service.located");
    config::required_port(params, "port", "service.located");
    session->send(std::move(message));
}

void RelayNode::handle_cluster_status_query(const njson &params)
{
    const auto requester = config::required_string(params, "source", "server.status.query");
    if (requester == config_.cluster.node_id)
    {
        return;
    }
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    auto report = server_status_message(request_id);
    report.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReport));
    attach_cluster_reply_route(report, params);
    cluster_mgr_->send(requester, std::move(report));
}

void RelayNode::handle_cluster_status_report(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "server.status.report");
    if (!session)
    {
        return;
    }
    validate_server_status(config::message_params(message), "server.status.report");
    message.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReported));
    session->send(std::move(message));
}

void RelayNode::report_topology(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (topology_->is_master())
    {
        if (auto snapshot = topology_->snapshot_message(request_id))
        {
            session->send(std::move(*snapshot));
        }
        return;
    }
    broadcast_client_query(id, CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request_id}}});
}

void RelayNode::handle_topology_query(const njson &params)
{
    if (!topology_->is_master())
    {
        return;
    }
    const auto requester = config::required_string(params, "source", "topology.query");
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (auto snapshot = topology_->snapshot_message(request_id))
    {
        attach_cluster_reply_route(*snapshot, params);
        cluster_mgr_->send(requester, std::move(*snapshot));
    }
}

void RelayNode::handle_topology_snapshot(CtrlMessage message)
{
    if (config::required_string(config::message_params(message), "source", "topology.snapshot") !=
        topology_->master_id())
    {
        return;
    }
    auto session = prepare_client_reply(message, "topology.snapshot");
    if (!session)
    {
        return;
    }
    message.params->erase("node_id");
    Topology::validate_snapshot(config::message_params(message), "topology.snapshot");
    session->send(std::move(message));
}

std::optional<CtrlMessage> RelayNode::service_location(const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    const auto local = registry_.find_service(service);
    if (!local || local->protocol != protocol)
    {
        return std::nullopt;
    }
    return CtrlMessage{CtrlCommand::ServiceLocated, njson{{"request_id", request_id},
                                                          {"service", service},
                                                          {"protocol", relay_protocol_name(protocol)},
                                                          {"node_id", config_.cluster.node_id},
                                                          {"address", config_.control.advertise_address},
                                                          {"port", config_.control.port}}};
}

CtrlMessage RelayNode::server_status_message(std::uint64_t request_id)
{
    const auto uptime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at_).count();
    njson params{{"request_id", request_id}, {"node_id", config_.cluster.node_id}, {"uptime_ms", uptime < 0 ? 0 : uptime}};
    params["services"] = registry_.traffic_report();
    return CtrlMessage{CtrlCommand::ServerStatusReported, std::move(params)};
}

void RelayNode::list_services(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    njson names = njson::array();
    for (auto &service : registry_.service_names())
    {
        names.push_back(std::move(service));
    }
    session->send(
        CtrlMessage{CtrlCommand::ServiceListed, njson{{"request_id", request_id}, {"services", std::move(names)}}});
}

void RelayNode::report_load(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{
        CtrlCommand::ServerLoaded,
        njson{{"request_id", request_id},
              {"control_queue_delay_us", control_queue_delay_us_.load(std::memory_order_relaxed)},
              {"transfer_tcp_queue_delay_us", transfer_tcp_queue_delay_us_.load(std::memory_order_relaxed)},
              {"transfer_udp_queue_delay_us", transfer_udp_queue_delay_us_.load(std::memory_order_relaxed)}}});
}

void RelayNode::report_traffic(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{CtrlCommand::ServerTrafficReported,
                              njson{{"request_id", request_id}, {"services", registry_.traffic_report()}}});
}

RelayNode::SessionId RelayNode::allocate_session_id()
{
    SessionId id;
    do
    {
        id = next_session_id_++;
        if (next_session_id_ == 0)
        {
            next_session_id_ = 1;
        }
    } while (registry_.contains(id));
    return id;
}

void RelayNode::locate_node(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request = config::require_unsigned(params, "request_id", true);
    const auto node = config::required_string(params, "node_id", "node.lookup");
    if (!topology_->members().contains(node))
    {
        session->send(CtrlMessage(CtrlCommand::NodeError,
            njson{{"request_id", request}, {"node_id", node}, {"reason", "node unavailable"}}));
        return;
    }
    if (node == config_.cluster.node_id)
    {
        session->send(CtrlMessage(CtrlCommand::NodeLocated,
            njson{{"request_id", request}, {"node_id", node},
                  {"address", config_.control.advertise_address}, {"port", config_.control.port}}));
        return;
    }
    cluster_mgr_->send(node, CtrlMessage(CtrlCommand::NodeLookup,
        njson{{"request_id", request}, {"node_id", node}, {"session_id", id}}));
}

void RelayNode::handle_node_lookup(const njson &params)
{
    const auto source = config::required_string(params, "source", "node.lookup");
    if (config::required_string(params, "node_id", "node.lookup") != config_.cluster.node_id ||
        !topology_->members().contains(source))
    {
        return;
    }
    CtrlMessage reply(CtrlCommand::NodeLocated,
        njson{{"address", config_.control.advertise_address}, {"port", config_.control.port}});
    attach_cluster_reply_route(reply, params);
    cluster_mgr_->send(source, std::move(reply));
}

void RelayNode::handle_node_location(CtrlMessage message)
{
    if (auto session = prepare_client_reply(message, "node.located"))
    {
        const auto &params = config::message_params(message);
        config::require_unsigned(params, "request_id", true);
        if (message.type() == CtrlCommand::NodeLocated)
        {
            config::required_string(params, "address", "node.located");
            config::required_port(params, "port", "node.located");
        }
        session->send(std::move(message));
    }
}
