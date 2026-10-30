#include "node_control.h"
#include "flow_protocol.h"
#include "topology.h"
#include <algorithm>
#include <set>

static void validate_server_status(const njson &params, std::string_view location)
{
    using namespace config;
    reject_unknown_fields(params, {"request_id", "node_id", "uptime_ms", "services", "relays"}, location);
    require_unsigned(params, "request_id", true);
    required_string(params, "node_id", location);
    require_unsigned(params, "uptime_ms");

    {
        const auto relays = params.find("relays");
        if (relays == params.end() || !relays->is_array())
        {
            throw std::invalid_argument(std::string(location) + ".relays must be an array");
        }
        for (const auto &relay : *relays)
        {
            const auto mode = required_string(relay, "mode", location);
            message_service(relay);
            message_protocol(relay);
            if (mode == "single")
            {
                reject_unknown_fields(relay, {"mode", "service", "protocol", "uuid", "consumer_peer", "producer_peer"},
                                      location);
                require_unsigned(relay, "uuid", true);
                required_string(relay, "consumer_peer", location);
                required_string(relay, "producer_peer", location);
            }
            else if (mode == "multi")
            {
                require_unsigned(relay, "epoch", true);
                require_unsigned(relay, "flow_id", true);
                required_string(relay, "agent_peer", location);
                const auto role = required_string(relay, "role", location);
                if (role == "ingress")
                {
                    reject_unknown_fields(
                        relay, {"mode", "role", "service", "protocol", "epoch", "flow_id", "agent_peer", "path"},
                        location);
                    const auto &path = relay.at("path");
                    if (!path.is_array() || path.size() < 2 || path.size() > 8 || path.front() != params.at("node_id"))
                    {
                        throw std::invalid_argument(std::string(location) + ".relays contains an invalid path");
                    }
                    std::set<std::string> nodes;
                    for (const auto &node : path)
                    {
                        if (!node.is_string() || node.get_ref<const std::string &>().empty() ||
                            !nodes.insert(node.get<std::string>()).second)
                        {
                            throw std::invalid_argument(std::string(location) +
                                                        ".relays contains an invalid path node");
                        }
                    }
                }
                else if (role == "egress")
                {
                    reject_unknown_fields(
                        relay, {"mode", "role", "service", "protocol", "epoch", "flow_id", "agent_peer"}, location);
                }
                else
                {
                    throw std::invalid_argument(std::string(location) + ".relays contains an invalid role");
                }
            }
            else
            {
                throw std::invalid_argument(std::string(location) + ".relays contains an invalid mode");
            }
        }
    }

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

NodeControl::NodeControl(RegistryMgr &registry, ClusterMgr &cluster, const Topology &topology,
                         const NodeMetrics &metrics, const NodeSessionList &relays, std::string node_id,
                         std::string advertise_address)
    : registry_(registry), cluster_(cluster), topology_(topology), metrics_(metrics), relay_sessions_(relays),
      node_id_(std::move(node_id)), advertise_address_(std::move(advertise_address))
{
}

bool NodeControl::handle_client(SessionId id, const ControlSessionPtr &session, CtrlCommand command,
                                const njson &params)
{
    switch (command)
    {
    case CtrlCommand::ServiceRegister:
        register_service(id, session, params);
        break;
    case CtrlCommand::ServerIdentify:
        session->send(CtrlMessage{CtrlCommand::ServerIdentified, njson{{"node_id", node_id_}}});
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
    default:
        return false;
    }
    return true;
}

void NodeControl::handle_cluster(CtrlCommand command, CtrlMessage message)
{
    const auto &params = *message.params;
    switch (command)
    {
    case CtrlCommand::ServiceLookup:
        handle_cluster_lookup(params);
        break;
    case CtrlCommand::ServiceLocated:
        handle_cluster_location(std::move(message));
        break;
    case CtrlCommand::ServerStatusQuery:
        handle_cluster_status_query(params);
        break;
    case CtrlCommand::ServerStatusReport:
        handle_cluster_status_report(std::move(message));
        break;
    case CtrlCommand::TopologyQuery:
        handle_topology_query(params);
        break;
    case CtrlCommand::TopologySnapshot:
        handle_topology_snapshot(std::move(message));
        break;
    default:
        break;
    }
}

void NodeControl::broadcast_client_query(SessionId id, CtrlMessage message)
{
    (*message.params)["session_id"] = id;
    cluster_.broadcast(std::move(message));
}

void NodeControl::attach_cluster_reply_route(CtrlMessage &reply, const njson &query)
{
    auto &params = *reply.params;
    params.erase("node_id");
    params["requester_node"] = query.at("source");
    params["session_id"] = query.at("session_id");
    params["request_id"] = query.at("request_id");
}

ControlSessionPtr NodeControl::prepare_client_reply(CtrlMessage &reply, std::string_view location)
{
    auto &params = *reply.params;
    if (config::required_string(params, "requester_node", location) != node_id_)
    {
        return {};
    }
    auto session = registry_.find_session(config::require_unsigned(params, "session_id", true));
    if (!session)
    {
        return {};
    }
    params["node_id"] = params.at("source");
    params.erase("source");
    params.erase("requester_node");
    params.erase("session_id");
    return session;
}

void NodeControl::register_service(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    const auto result = registry_.register_service(id, service, protocol);
    if (result != RegistryMgr::Result::Registered)
    {
        PROXY_ERROR_PRINT("Service rejected service=%s/%s session_id=%llu reason=%.*s", service.c_str(),
                          relay_protocol_name(protocol).data(), static_cast<unsigned long long>(id),
                          static_cast<int>(RegistryMgr::result2string(result).size()),
                          RegistryMgr::result2string(result).data());
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

void NodeControl::locate_service(SessionId id, const ControlSessionPtr &session, const njson &params)
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

void NodeControl::report_cluster_status(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    session->send(server_status_message(request_id));
    broadcast_client_query(id, CtrlMessage{CtrlCommand::ServerStatusQuery, njson{{"request_id", request_id}}});
}

void NodeControl::handle_cluster_lookup(const njson &params)
{
    const auto &requester = params.at("source").get_ref<const std::string &>();
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    if (auto location = service_location(params))
    {
        attach_cluster_reply_route(*location, params);
        cluster_.send(requester, std::move(*location));
    }
}

void NodeControl::handle_cluster_location(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "service.located");
    if (!session)
    {
        return;
    }
    const auto &params = *message.params;
    config::require_unsigned(params, "request_id", true);
    config::message_service(params);
    config::message_protocol(params);
    config::required_string(params, "address", "service.located");
    session->send(std::move(message));
}

void NodeControl::handle_cluster_status_query(const njson &params)
{
    const auto &requester = params.at("source").get_ref<const std::string &>();
    if (requester == node_id_)
    {
        return;
    }
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    auto report = server_status_message(request_id);
    report.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReport));
    attach_cluster_reply_route(report, params);
    cluster_.send(requester, std::move(report));
}

void NodeControl::handle_cluster_status_report(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "server.status.report");
    if (!session)
    {
        return;
    }
    validate_server_status(*message.params, "server.status.report");
    message.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReported));
    session->send(std::move(message));
}

void NodeControl::report_topology(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (topology_.is_master())
    {
        if (auto snapshot = topology_.snapshot_message(request_id))
        {
            session->send(std::move(*snapshot));
        }
        return;
    }
    if (const auto master = topology_.master_id(); !master.empty())
    {
        cluster_.send(std::string(master),
                      CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request_id}, {"session_id", id}}});
    }
}

void NodeControl::handle_topology_query(const njson &params)
{
    if (!topology_.is_master())
    {
        return;
    }
    const auto &requester = params.at("source").get_ref<const std::string &>();
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (auto snapshot = topology_.snapshot_message(request_id))
    {
        attach_cluster_reply_route(*snapshot, params);
        cluster_.send(requester, std::move(*snapshot));
    }
}

void NodeControl::handle_topology_snapshot(CtrlMessage message)
{
    if (message.params->at("source") != topology_.master_id())
    {
        return;
    }
    auto session = prepare_client_reply(message, "topology.snapshot");
    if (!session)
    {
        return;
    }
    message.params->erase("node_id");
    Topology::validate_snapshot(*message.params, "topology.snapshot");
    session->send(std::move(message));
}

std::optional<CtrlMessage> NodeControl::service_location(const njson &params)
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
                                                          {"node_id", node_id_},
                                                          {"address", advertise_address_}}};
}

CtrlMessage NodeControl::server_status_message(std::uint64_t request_id)
{
    njson params{{"request_id", request_id}, {"node_id", node_id_}, {"uptime_ms", metrics_.uptime().count()}};
    params["services"] = registry_.traffic_report();
    params["relays"] = njson::array();
    for (const auto &relay : relay_sessions_)
    {
        if (auto report = relay->status_report())
        {
            params["relays"].push_back(std::move(*report));
        }
    }
    return CtrlMessage{CtrlCommand::ServerStatusReported, std::move(params)};
}

void NodeControl::list_services(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{CtrlCommand::ServiceListed,
                              njson{{"request_id", request_id}, {"services", registry_.service_names()}}});
}

void NodeControl::report_load(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    const auto delays = metrics_.queue_delays();
    session->send(CtrlMessage{CtrlCommand::ServerLoaded,
                              njson{{"request_id", request_id},
                                    {"control_queue_delay_us", delays.control_queue_delay_us},
                                    {"transfer_tcp_queue_delay_us", delays.transfer_tcp_queue_delay_us},
                                    {"transfer_udp_queue_delay_us", delays.transfer_udp_queue_delay_us}}});
}

void NodeControl::report_traffic(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{CtrlCommand::ServerTrafficReported,
                              njson{{"request_id", request_id}, {"services", registry_.traffic_report()}}});
}

NodeMetrics::NodeMetrics(asio::any_io_executor control, asio::any_io_executor tcp, asio::any_io_executor udp,
                         RegistryMgr &registry, std::uint64_t bandwidth_mbps)
    : registry_(registry), bandwidth_mbps_(bandwidth_mbps), control_probe_timer_(control),
      transfer_tcp_probe_timer_(tcp), transfer_udp_probe_timer_(udp)
{
}

void NodeMetrics::start(FlowProtocol &protocol, std::chrono::steady_clock::time_point started_at)
{
    protocol_ = &protocol;
    started_at_ = started_at;
    bandwidth_at_ = started_at;
}

void NodeMetrics::activate()
{
    running_.store(true);
    schedule_queue_probe(control_probe_timer_, control_queue_delay_us_);
    schedule_queue_probe(transfer_tcp_probe_timer_, transfer_tcp_queue_delay_us_);
    schedule_queue_probe(transfer_udp_probe_timer_, transfer_udp_queue_delay_us_);
}

void NodeMetrics::stop_control()
{
    running_.store(false);
    control_probe_timer_.cancel();
}

void NodeMetrics::stop_tcp()
{
    transfer_tcp_probe_timer_.cancel();
}

void NodeMetrics::stop_udp()
{
    transfer_udp_probe_timer_.cancel();
}

NodeMetrics::QueueDelays NodeMetrics::queue_delays() const noexcept
{
    return {control_queue_delay_us_.load(std::memory_order_relaxed),
            transfer_tcp_queue_delay_us_.load(std::memory_order_relaxed),
            transfer_udp_queue_delay_us_.load(std::memory_order_relaxed)};
}

double NodeMetrics::available() const noexcept
{
    return available_;
}

std::chrono::milliseconds NodeMetrics::uptime() const noexcept
{
    return std::max(std::chrono::milliseconds::zero(), std::chrono::duration_cast<std::chrono::milliseconds>(
                                                           std::chrono::steady_clock::now() - started_at_));
}

void NodeMetrics::schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us)
{
    static constexpr auto interval = std::chrono::seconds(1);
    static constexpr auto invalid = std::numeric_limits<std::uint32_t>::max();

    timer.expires_after(interval);
    const auto deadline = timer.expiry();
    timer.async_wait([self = shared_from_this(), &timer, &queue_delay_us, deadline](const asio::error_code &error) {
        if (error || !self->running_.load())
        {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = now - deadline;
        const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        const auto max_valid = static_cast<std::chrono::microseconds::rep>(invalid - 1);
        const auto delay = microseconds < 0 ? invalid : static_cast<std::uint32_t>(std::min(microseconds, max_valid));
        queue_delay_us.store(delay, std::memory_order_relaxed);
        if (&timer == &self->control_probe_timer_)
        {
            if (now - self->bandwidth_at_ >= std::chrono::seconds(5))
            {
                const double seconds = std::chrono::duration<double>(now - self->bandwidth_at_).count();
                const double used =
                    self->registry_.bytes_per_second() + self->protocol_->take_forwarded_bytes() / seconds;
                self->available_ = std::clamp(1.0 - used * 8.0 / (self->bandwidth_mbps_ * 1e6), 0.0, 1.0);
                self->bandwidth_at_ = now;
            }
            self->registry_.sample_traffic();
        }
        self->schedule_queue_probe(timer, queue_delay_us);
    });
}
