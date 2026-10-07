#include "control_router.h"
#include "topology.h"

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
        throw std::invalid_argument(std::string(location) + ".services must be an array");
    for (const auto &service : *services)
    {
        if (!service.is_object())
            throw std::invalid_argument(std::string(location) + ".services entries must be objects");
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
            throw std::invalid_argument(std::string(location) + ".services.accessors must be an object");
        for (const auto &[client, connections] : accessors->items())
        {
            if (client.empty())
                throw std::invalid_argument(std::string(location) + ".services.accessors client must not be empty");
            if (!connections.is_number_unsigned() || connections.get<std::uint64_t>() == 0)
                throw std::invalid_argument(std::string(location) + ".services.accessors.connections must be positive");
        }
    }
}
} // namespace

ControlRouter::ControlRouter(asio::any_io_executor transfer_tcp_executor, asio::any_io_executor transfer_udp_executor,
                             ControlRouterConfig config, std::shared_ptr<ClusterMgr> cluster_mgr,
                             std::shared_ptr<TcpPipeline> tcp_pipeline, std::shared_ptr<TlsPipeline> tls_pipeline,
                             std::shared_ptr<DatagramMgr> datagram_mgr, Topology &topology,
                             ClusterMessageChannel &cluster_messages,
                             const std::atomic<std::uint32_t> &control_queue_delay_us,
                             const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us,
                             const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us)
    : transfer_tcp_executor_(std::move(transfer_tcp_executor)),
      transfer_udp_executor_(std::move(transfer_udp_executor)), config_(std::move(config)),
      cluster_mgr_(std::move(cluster_mgr)), tcp_pipeline_(std::move(tcp_pipeline)),
      tls_pipeline_(std::move(tls_pipeline)), datagram_mgr_(std::move(datagram_mgr)), topology_(topology),
      cluster_messages_(cluster_messages), control_queue_delay_us_(control_queue_delay_us),
      transfer_tcp_queue_delay_us_(transfer_tcp_queue_delay_us),
      transfer_udp_queue_delay_us_(transfer_udp_queue_delay_us),
      registry_(config_.max_connections, config_.max_services, config_.max_services_per_session)
{
    if (!cluster_mgr_ || !tcp_pipeline_ || !tls_pipeline_ || !datagram_mgr_)
        throw std::invalid_argument("ControlRouter managers must not be null");
}

void ControlRouter::start()
{
    started_at_ = std::chrono::steady_clock::now();
}

void ControlRouter::stop()
{
    registry_.stop();
}

bool ControlRouter::full() noexcept
{
    return registry_.full();
}

std::size_t ControlRouter::session_count() noexcept
{
    return registry_.session_count();
}

ControlRouter::SessionId ControlRouter::add_session(const ControlSessionPtr &session)
{
    const auto id = allocate_session_id();
    registry_.add(id, session);
    return id;
}

void ControlRouter::remove_session(SessionId id, const ControlSessionPtr &session)
{
    registry_.remove(id);
    asio::post(transfer_tcp_executor_, [tcp = tcp_pipeline_, tls = tls_pipeline_, session]() {
        tcp->disconnect(session);
        tls->disconnect(session);
    });
    asio::post(transfer_udp_executor_, [datagram = datagram_mgr_, session]() { datagram->disconnect(session); });
}

void ControlRouter::handle(SessionId id, const ControlSessionPtr &session, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    switch (message.type())
    {
    case CtrlCommand::ServiceRegister:
        register_service(id, session, params);
        break;
    case CtrlCommand::ServerIdentify:
        session->send(CtrlMessage{CtrlCommand::ServerIdentified, njson{{"node_id", config_.node_id}}});
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
        open_relay(session, params);
        break;
    case CtrlCommand::RelayReject:
        reject_relay(session, params);
        break;
    case CtrlCommand::RelayCancel:
        cancel_relay(session, params);
        break;
    default:
        throw std::invalid_argument("Unknown control command: " + message.command);
    }
}

void ControlRouter::handle_cluster(CtrlMessage message)
{
    switch (message.type())
    {
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
            PROXY_ERROR_PRINT("Cluster receive rejected reason=queue full");
        break;
    }
}

void ControlRouter::sample_traffic()
{
    registry_.sample_traffic();
}

void ControlRouter::broadcast_client_query(SessionId id, CtrlMessage message)
{
    static_cast<void>(config::message_params(message));
    (*message.params)["session_id"] = id;
    cluster_mgr_->broadcast(std::move(message));
}

void ControlRouter::attach_cluster_reply_route(CtrlMessage &reply, const njson &query)
{
    auto &params = *reply.params;
    params.erase("node_id");
    params["requester_node"] = query.at("source");
    params["session_id"] = query.at("session_id");
    params["request_id"] = query.at("request_id");
}

ControlSessionPtr ControlRouter::prepare_client_reply(CtrlMessage &reply, std::string_view location)
{
    const auto &params = config::message_params(reply);
    if (config::required_string(params, "requester_node", location) != config_.node_id)
        return {};
    auto session = registry_.find_session(config::require_unsigned(params, "session_id", true));
    if (!session)
        return {};
    (*reply.params)["node_id"] = config::required_string(params, "source", location);
    reply.params->erase("source");
    reply.params->erase("requester_node");
    reply.params->erase("session_id");
    return session;
}

void ControlRouter::register_service(SessionId id, const ControlSessionPtr &session, const njson &params)
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
    if (protocol == RelayProtocol::Udp)
    {
        asio::post(transfer_udp_executor_,
                   [datagram = datagram_mgr_, session, service, traffic = std::move(traffic)]() {
                       datagram->attach_service(session, service, traffic);
                   });
    }
}

void ControlRouter::locate_service(SessionId id, const ControlSessionPtr &session, const njson &params)
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

void ControlRouter::report_cluster_status(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    session->send(server_status_message(request_id));
    broadcast_client_query(id, CtrlMessage{CtrlCommand::ServerStatusQuery, njson{{"request_id", request_id}}});
}

void ControlRouter::handle_cluster_lookup(const njson &params)
{
    const auto requester = config::required_string(params, "source", "service.lookup");
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    if (auto location = service_location(params))
    {
        attach_cluster_reply_route(*location, params);
        cluster_mgr_->send(requester, std::move(*location));
    }
}

void ControlRouter::handle_cluster_location(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "service.located");
    if (!session)
        return;
    const auto &params = config::message_params(message);
    config::require_unsigned(params, "request_id", true);
    config::message_service(params);
    config::message_protocol(params);
    config::required_string(params, "address", "service.located");
    config::required_port(params, "port", "service.located");
    session->send(std::move(message));
}

void ControlRouter::handle_cluster_status_query(const njson &params)
{
    const auto requester = config::required_string(params, "source", "server.status.query");
    if (requester == config_.node_id)
        return;
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    auto report = server_status_message(request_id);
    report.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReport));
    attach_cluster_reply_route(report, params);
    cluster_mgr_->send(requester, std::move(report));
}

void ControlRouter::handle_cluster_status_report(CtrlMessage message)
{
    auto session = prepare_client_reply(message, "server.status.report");
    if (!session)
        return;
    validate_server_status(config::message_params(message), "server.status.report");
    message.command = std::string(ctrl_command_name(CtrlCommand::ServerStatusReported));
    session->send(std::move(message));
}

void ControlRouter::report_topology(SessionId id, const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (topology_.is_master())
    {
        if (auto snapshot = topology_.snapshot_message(request_id))
            session->send(std::move(*snapshot));
        return;
    }
    broadcast_client_query(id, CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request_id}}});
}

void ControlRouter::handle_topology_query(const njson &params)
{
    if (!topology_.is_master())
    {
        return;
    }
    const auto requester = config::required_string(params, "source", "topology.query");
    static_cast<void>(config::require_unsigned(params, "session_id", true));
    const auto request_id = config::require_unsigned(params, "request_id", true);
    if (auto snapshot = topology_.snapshot_message(request_id))
    {
        attach_cluster_reply_route(*snapshot, params);
        cluster_mgr_->send(requester, std::move(*snapshot));
    }
}

void ControlRouter::handle_topology_snapshot(CtrlMessage message)
{
    if (config::required_string(config::message_params(message), "source", "topology.snapshot") !=
        topology_.master_id())
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

std::optional<CtrlMessage> ControlRouter::service_location(const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id", true);
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    const auto local = registry_.find_service(service);
    if (!local || local->protocol != protocol)
        return std::nullopt;
    return CtrlMessage{CtrlCommand::ServiceLocated, njson{{"request_id", request_id},
                                                          {"service", service},
                                                          {"protocol", relay_protocol_name(protocol)},
                                                          {"node_id", config_.node_id},
                                                          {"address", config_.advertise_address},
                                                          {"port", config_.control_port}}};
}

CtrlMessage ControlRouter::server_status_message(std::uint64_t request_id)
{
    const auto uptime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at_).count();
    njson params{{"request_id", request_id}, {"node_id", config_.node_id}, {"uptime_ms", uptime < 0 ? 0 : uptime}};
    params["services"] = registry_.traffic_report();
    return CtrlMessage{CtrlCommand::ServerStatusReported, std::move(params)};
}

void ControlRouter::list_services(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    njson names = njson::array();
    for (auto &service : registry_.service_names())
        names.push_back(std::move(service));
    session->send(
        CtrlMessage{CtrlCommand::ServiceListed, njson{{"request_id", request_id}, {"services", std::move(names)}}});
}

void ControlRouter::report_load(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{
        CtrlCommand::ServerLoaded,
        njson{{"request_id", request_id},
              {"control_queue_delay_us", control_queue_delay_us_.load(std::memory_order_relaxed)},
              {"transfer_tcp_queue_delay_us", transfer_tcp_queue_delay_us_.load(std::memory_order_relaxed)},
              {"transfer_udp_queue_delay_us", transfer_udp_queue_delay_us_.load(std::memory_order_relaxed)}}});
}

void ControlRouter::report_traffic(const ControlSessionPtr &session, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    session->send(CtrlMessage{CtrlCommand::ServerTrafficReported,
                              njson{{"request_id", request_id}, {"services", registry_.traffic_report()}}});
}

void ControlRouter::open_relay(const ControlSessionPtr &consumer, const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    auto producer = registry_.find_service(service);
    if ((!producer && protocol != RelayProtocol::Udp) || (producer && producer->protocol != protocol))
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError,
                                   njson{{"request_id", request_id},
                                         {"service", service},
                                         {"protocol", relay_protocol_name(protocol)},
                                         {"reason", !producer ? "service unavailable" : "service protocol mismatch"}}});
        return;
    }

    auto traffic = producer ? producer->traffic : SRVTrafficPtr{};
    if (protocol == RelayProtocol::Tcp)
    {
        asio::post(transfer_tcp_executor_, [pipeline = tcp_pipeline_, producer = producer->session, consumer, service,
                                            request_id, traffic = std::move(traffic)]() mutable {
            pipeline->open(producer, consumer, std::move(service), request_id, std::move(traffic));
        });
    }
    else if (protocol == RelayProtocol::Tls)
    {
        asio::post(transfer_tcp_executor_, [pipeline = tls_pipeline_, producer = producer->session, consumer, service,
                                            request_id, traffic = std::move(traffic)]() mutable {
            pipeline->open(producer, consumer, std::move(service), request_id, std::move(traffic));
        });
    }
    else
    {
        asio::post(transfer_udp_executor_, [datagram = datagram_mgr_,
                                            producer = producer ? producer->session : ControlSessionPtr{}, consumer,
                                            service, request_id, traffic = std::move(traffic)]() mutable {
            datagram->open(consumer, std::move(service), request_id, std::move(producer), std::move(traffic));
        });
    }
}

void ControlRouter::reject_relay(const ControlSessionPtr &session, const njson &params)
{
    const auto uuid = config::require_unsigned(params, "uuid", true);
    auto reason = config::optional_string(params, "reason", "producer rejected relay");
    asio::post(transfer_tcp_executor_,
               [tcp = tcp_pipeline_, tls = tls_pipeline_, datagram = datagram_mgr_,
                udp_executor = transfer_udp_executor_, session, uuid, reason = std::move(reason)]() mutable {
                   if (!tcp->reject(session, uuid, reason) && !tls->reject(session, uuid, reason))
                       asio::post(udp_executor, [datagram, session, uuid, reason = std::move(reason)]() mutable {
                           datagram->reject(session, uuid, std::move(reason));
                       });
               });
}

void ControlRouter::cancel_relay(const ControlSessionPtr &session, const njson &params)
{
    std::optional<std::uint64_t> uuid;
    if (params.contains("uuid"))
        uuid = config::require_unsigned(params, "uuid", true);
    const auto request_id = config::optional_unsigned(params, "request_id");
    asio::post(transfer_tcp_executor_, [tcp = tcp_pipeline_, tls = tls_pipeline_, datagram = datagram_mgr_,
                                        udp_executor = transfer_udp_executor_, session, uuid, request_id]() {
        if (!tcp->cancel(session, uuid, request_id) && !tls->cancel(session, uuid, request_id))
            asio::post(udp_executor,
                       [datagram, session, uuid, request_id]() { datagram->cancel(session, uuid, request_id); });
    });
}

ControlRouter::SessionId ControlRouter::allocate_session_id()
{
    SessionId id;
    do
    {
        id = next_session_id_++;
        if (next_session_id_ == 0)
            next_session_id_ = 1;
    } while (registry_.contains(id));
    return id;
}
