#include "relay_node.h"
#include "node_session.h"
#include <algorithm>
#include <cassert>
#include <future>
#include <set>

namespace
{
void validate_server_status(const njson &params, std::string_view location)
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
} // namespace

RelayNode::RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
                     asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config)
    : control_executor_(control_io.get_executor()), transfer_tcp_executor_(transfer_tcp_io.get_executor()),
      transfer_udp_executor_(transfer_udp_io.get_executor()), cluster_data_executor_(cluster_data_io.get_executor()),
      control_probe_timer_(control_executor_), transfer_tcp_probe_timer_(transfer_tcp_executor_),
      transfer_udp_probe_timer_(transfer_udp_executor_), traffic_sample_timer_(control_executor_),
      control_sessions_done_(control_executor_), control_acceptor_(control_executor_), ssl_context_(ssl_context),
      config_(std::move(config)),
      relay_id_allocator_(std::make_shared<RelayIdAllocator>()),
      tcp_pipeline_(std::make_shared<TcpPipeline>(
          transfer_tcp_executor_, ssl_context_, relay_id_allocator_, config_.tcp.address, config_.tcp.port,
          config_.tcp.max_setup_connections, config_.tcp.max_relays, config_.tcp.setup_timeout, config_.tcp.traffic)),
      tls_pipeline_(std::make_shared<TlsPipeline>(
          transfer_tcp_executor_, ssl_context_, relay_id_allocator_, config_.tls.address, config_.tls.port,
          config_.tls.max_setup_connections, config_.tls.max_relays, config_.tls.setup_timeout, config_.tls.traffic)),
      datagram_mgr_(std::make_shared<DatagramMgr>(
          transfer_udp_executor_, relay_id_allocator_,
          asio::ip::udp::endpoint(asio::ip::make_address(config_.datagram.address), config_.datagram.port),
          config_.datagram.max_relays, config_.datagram.traffic)),
      registry_(config_.control.max_connections, config_.control.max_services, config_.control.max_services_per_session),
      relays_done_(control_executor_)
{
    control_sessions_done_.expires_at(std::chrono::steady_clock::time_point::max());
    if (&control_io == &transfer_tcp_io || &control_io == &transfer_udp_io || &transfer_tcp_io == &transfer_udp_io ||
        &cluster_data_io == &control_io || &cluster_data_io == &transfer_tcp_io || &cluster_data_io == &transfer_udp_io)
    {
        throw std::invalid_argument(
            "Control, TCP transfer, UDP transfer, and cluster data require distinct io_context instances");
    }

    cluster_context_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                 asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                 asio::ssl::context::no_tlsv1_1);
    cluster_context_.use_certificate_chain_file(config_.certificate_chain.string());
    cluster_context_.use_private_key_file(config_.private_key.string(), asio::ssl::context::pem);
    if (SSL_CTX_check_private_key(cluster_context_.native_handle()) != 1)
    {
        throw std::invalid_argument("Cluster certificate and private key do not match");
    }
    cluster_context_.load_verify_file(config_.server_ca_file.string());

    config_.cluster.advertise_address = config_.control.advertise_address;
    cluster_mgr_ = std::make_shared<ClusterMgr>(*this, control_executor_, cluster_context_, config_.cluster,
                                                config_.channel, config_.control.max_connections);
    topology_ = std::make_unique<Topology>(
        control_io, config_.cluster.node_id, config_.cluster.role == ClusterConfig::Role::Master, *cluster_mgr_,
        control_queue_delay_us_, transfer_tcp_queue_delay_us_, transfer_udp_queue_delay_us_, available_);
    nodelink_mgr_ =
        std::make_unique<NodeLinkMgr>(control_executor_, cluster_data_executor_, config_.cluster, config_.tcp.address,
                                      config_.datagram.address, *cluster_mgr_, *topology_);

}

RelayNode::~RelayNode() = default;

void RelayNode::start()
{
    if (state_.load() != State::Created)
    {
        throw std::logic_error("RelayWeave Node can only be started once");
    }

    try
    {
        const tcp::endpoint endpoint(asio::ip::make_address(config_.control.address), config_.control.port);
        control_acceptor_.open(endpoint.protocol());
        control_acceptor_.set_option(asio::socket_base::reuse_address(true));
        control_acceptor_.bind(endpoint);
        control_acceptor_.listen();
        tcp_pipeline_->start();
        tls_pipeline_->start();
        datagram_mgr_->start();
        nodelink_mgr_->start();
        started_at_ = std::chrono::steady_clock::now();
        bandwidth_at_ = started_at_;
        cluster_mgr_->start();
        topology_->start();
        nodelink_mgr_->activate();
        state_.store(State::Running);
    }
    catch (...)
    {
        state_.store(State::Stopped);
        asio::error_code ignored;
        control_acceptor_.close(ignored);
        nodelink_mgr_->rollback();
        tcp_pipeline_->stop();
        tls_pipeline_->stop();
        datagram_mgr_->stop();
        throw;
    }

    PROXY_INFO_PRINT("Node listening control=%s:%u tcp=%s:%u tls=%s:%u udp=%s:%u", config_.control.address.c_str(),
                     static_cast<unsigned int>(config_.control.port), config_.tcp.address.c_str(),
                     static_cast<unsigned int>(config_.tcp.port), config_.tls.address.c_str(),
                     static_cast<unsigned int>(config_.tls.port), config_.datagram.address.c_str(),
                     static_cast<unsigned int>(config_.datagram.port));
    asio::co_spawn(
        control_executor_,
        [self = shared_from_this()] { return self->control_accept_loop(); },
        asio::detached);
    schedule_queue_probe(control_probe_timer_, control_queue_delay_us_);
    schedule_queue_probe(transfer_tcp_probe_timer_, transfer_tcp_queue_delay_us_);
    schedule_queue_probe(transfer_udp_probe_timer_, transfer_udp_queue_delay_us_);
    schedule_traffic_sample();
}

void RelayNode::stop()
{
    if (control_executor_.running_in_this_thread() || transfer_tcp_executor_.running_in_this_thread() ||
        transfer_udp_executor_.running_in_this_thread() || cluster_data_executor_.running_in_this_thread())
    {
        throw std::logic_error("Synchronous server stop must be called outside the server executors");
    }

    std::call_once(stop_once_, [this]() {
        const auto current = state_.load();
        if (current == State::Created)
        {
            state_.store(State::Stopped);
            PROXY_INFO_PRINT("Node stopped");
            return;
        }
        if (current != State::Running)
        {
            throw std::logic_error("Server is not running");
        }
        state_.store(State::Stopping);

        const auto save_stop_error = [this](auto &completion) {
            try
            {
                completion.get();
            }
            catch (...)
            {
                if (!stop_error_)
                {
                    stop_error_ = std::current_exception();
                }
            }
        };

        auto self = shared_from_this();
        auto control_stopped = asio::co_spawn(
            control_executor_,
            [self]() -> asio::awaitable<void> {
                self->control_probe_timer_.cancel();
                self->traffic_sample_timer_.cancel();
                asio::error_code ignored;
                self->control_acceptor_.close(ignored);
                self->registry_.stop();

                co_await self->stop_relays();
                co_await self->nodelink_mgr_->stop();

                std::exception_ptr topology_error;
                try
                {
                    co_await self->topology_->close();
                }
                catch (...)
                {
                    topology_error = std::current_exception();
                }

                std::exception_ptr cluster_error;
                try
                {
                    co_await self->cluster_mgr_->async_stop();
                }
                catch (...)
                {
                    cluster_error = std::current_exception();
                }

                if (self->active_control_sessions_ != 0)
                {
                    co_await self->control_sessions_done_.async_wait(use_nothrow_awaitable);
                }
                if (topology_error)
                {
                    std::rethrow_exception(topology_error);
                }
                if (cluster_error)
                {
                    std::rethrow_exception(cluster_error);
                }
            },
            asio::use_future);
        save_stop_error(control_stopped);

        auto transfer_tcp_stopped = asio::post(
            transfer_tcp_executor_,
            asio::use_future([self] {
                self->transfer_tcp_probe_timer_.cancel();
                self->tcp_pipeline_->stop();
                self->tls_pipeline_->stop();
            }));
        save_stop_error(transfer_tcp_stopped);
        tcp_pipeline_->wait_for_pending();
        tls_pipeline_->wait_for_pending();

        auto transfer_udp_stopped = asio::post(
            transfer_udp_executor_,
            asio::use_future([self] {
                self->transfer_udp_probe_timer_.cancel();
                self->datagram_mgr_->stop();
            }));
        save_stop_error(transfer_udp_stopped);

        state_.store(State::Stopped);
        PROXY_INFO_PRINT("Node stopped");
    });

    if (stop_error_)
    {
        std::rethrow_exception(stop_error_);
    }
}

asio::awaitable<void> RelayNode::control_accept_loop()
{
    for (;;)
    {
        auto [error, socket] = co_await control_acceptor_.async_accept(use_nothrow_awaitable);
        const auto state = state_.load();
        if (error)
        {
            if (state != State::Running || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Control accept failed reason=%s category=%s", error.message().c_str(),
                              error.category().name());
            continue;
        }
        if (state != State::Running)
        {
            co_return;
        }
        if (registry_.full())
        {
            PROXY_ERROR_PRINT("Control rejected active=%zu limit=%zu", registry_.session_count(),
                              config_.control.max_connections);
            continue;
        }

        auto session =
            std::make_shared<ControlSession>(std::move(socket), ssl_context_, TLSChannelRole::S, config_.channel);
        const auto session_id = allocate_session_id();
        registry_.add(session_id, session);
        ++active_control_sessions_;
        ScopeGuard task_rollback([this, session_id]() noexcept {
            registry_.remove(session_id);
            --active_control_sessions_;
        });
        asio::co_spawn(
            control_executor_,
            [self = shared_from_this(), session_id, session] {
                return self->run_control_session(session_id, session);
            },
            asio::detached);
        task_rollback.dismiss();
    }
}

asio::awaitable<void> RelayNode::run_control_session(SessionId id, ControlSessionPtr session)
{
    ScopeGuard session_done([this]() noexcept {
        assert(active_control_sessions_ != 0);
        --active_control_sessions_;
        if (state_.load() == State::Stopping && active_control_sessions_ == 0)
        {
            try
            {
                control_sessions_done_.cancel();
            }
            catch (...)
            {
            }
        }
    });
    std::string failure;
    bool connected = false;
    try
    {
        co_await session->start({});
        connected = true;
        PROXY_INFO_PRINT("Control [+] session_id=%llu peer=%.*s", static_cast<unsigned long long>(id),
                         static_cast<int>(session->peer().size()), session->peer().data());
        for (;;)
        {
            auto message = co_await session->async_receive();
            if (state_.load() == State::Running)
            {
                handle_control_message(id, session, std::move(message));
            }
        }
    }
    catch (const asio::system_error &exception)
    {
        if (exception.code() != asio::experimental::error::channel_cancelled &&
            exception.code() != asio::experimental::error::channel_closed)
        {
            failure = exception.what();
        }
    }
    catch (const std::exception &exception)
    {
        failure = exception.what();
    }
    catch (...)
    {
        failure = "unknown exception";
    }

    cancel_relays("control session disconnected", session);
    registry_.remove(id);
    co_await session->async_disconnect();
    if (connected)
    {
        PROXY_INFO_PRINT("Control [x] session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                         static_cast<int>(session->peer().size()), session->peer().data(),
                         state_.load() != State::Running ? "node stopping"
                         : failure.empty()               ? "channel closed"
                                                         : "error");
    }
    if (!failure.empty() && state_.load() == State::Running)
    {
        PROXY_ERROR_PRINT("Control failed session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                          static_cast<int>(session->peer().size()), session->peer().data(), failure.c_str());
    }
}

void RelayNode::handle_cluster_message(CtrlMessage message)
{
    if (message.type() == CtrlCommand::TopologyMembers)
    {
        topology_->update_members(config::message_params(message));
        if (state_.load() == State::Running)
        {
            nodelink_mgr_->members_changed();
            invalidate_relays();
        }
        return;
    }
    if (message.type() == CtrlCommand::TopologyReport)
    {
        topology_->update_report(config::message_params(message));
        return;
    }
    if (message.command.starts_with("link.") || message.command.starts_with("flow."))
    {
        // Drain existing acknowledgements while relay cleanup is running; reject new remote opens during stop.
        if (state_.load() == State::Running || message.command != "flow.open")
        {
            nodelink_mgr_->handle(std::move(message));
        }
        return;
    }
    if (message.type() == CtrlCommand::ClusterError)
    {
        nodelink_mgr_->control_failed(config::message_params(message).value("reason", "cluster control failed"));
        invalidate_relays("cluster control failed");
        return;
    }
    if (message.command.starts_with("relay.peer."))
    {
        if (state_.load() == State::Running)
        {
            handle_relay_peer(std::move(message));
        }
        return;
    }
    handle_control_cluster_message(std::move(message));
}

void RelayNode::schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us)
{
    static constexpr auto interval = std::chrono::seconds(1);
    static constexpr auto invalid = std::numeric_limits<std::uint32_t>::max();

    timer.expires_after(interval);
    const auto deadline = timer.expiry();
    timer.async_wait([self = shared_from_this(), &timer, &queue_delay_us, deadline](const asio::error_code &error) {
        if (error || self->state_.load() != State::Running)
        {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = now - deadline;
        const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        const auto max_valid = static_cast<std::chrono::microseconds::rep>(invalid - 1);
        const auto delay = microseconds < 0 ? invalid : static_cast<std::uint32_t>(std::min(microseconds, max_valid));
        queue_delay_us.store(delay, std::memory_order_relaxed);
        if (&timer == &self->control_probe_timer_ &&
            now - self->bandwidth_at_ >= std::chrono::seconds(5))
        {
            const double seconds = std::chrono::duration<double>(now - self->bandwidth_at_).count();
            const double used = self->registry_.bytes_per_second() +
                                self->nodelink_mgr_->channel().take_forwarded_bytes() / seconds;
            self->available_ = std::clamp(1.0 - used * 8.0 / (self->config_.bandwidth_mbps * 1e6), 0.0, 1.0);
            self->bandwidth_at_ = now;
        }
        self->schedule_queue_probe(timer, queue_delay_us);
    });
}

void RelayNode::schedule_traffic_sample()
{
    traffic_sample_timer_.expires_after(std::chrono::seconds(1));
    traffic_sample_timer_.async_wait([self = shared_from_this()](const asio::error_code &error) {
        if (error || self->state_.load() != State::Running)
        {
            return;
        }
        self->registry_.sample_traffic();
        self->schedule_traffic_sample();
    });
}

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
        break;
    }
}

void RelayNode::broadcast_client_query(SessionId id, CtrlMessage message)
{
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
    njson params{
        {"request_id", request_id}, {"node_id", config_.cluster.node_id}, {"uptime_ms", uptime < 0 ? 0 : uptime}};
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
        session->send(CtrlMessage(CtrlCommand::NodeLocated, njson{{"request_id", request},
                                                                  {"node_id", node},
                                                                  {"address", config_.control.advertise_address},
                                                                  {"port", config_.control.port}}));
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

void RelayNode::start_relay(std::shared_ptr<NodeSession> relay)
{
    relay_sessions_.push_back(relay);
    try
    {
        // This is the sole owned business task; its controller drains every child before returning.
        asio::co_spawn(control_executor_, relay->run(), asio::bind_cancellation_slot(
            relay->cancellation_.slot(), [self = shared_from_this(), relay](std::exception_ptr error) {
                if (error)
                    PROXY_ERROR_PRINT("Relay task failed reason=%s", exception_description(error).c_str());
                std::erase(self->relay_sessions_, relay);
                self->relays_done_.cancel();
            }));
    }
    catch (...)
    {
        std::erase(relay_sessions_, relay);
        throw;
    }
}

void RelayNode::handle_relay(const ControlSessionPtr &session, const CtrlMessage &message)
{
    const auto &params = config::message_params(message);
    if (message.type() != CtrlCommand::RelayOpen)
    {
        for (const auto &relay : relay_sessions_)
            if (relay->handle(session, message))
                return;
        return;
    }
    const auto request = config::require_unsigned(params, "request_id");
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    const bool multi = params.contains("path");
    try
    {
        njson values{{"request_id", request}, {"service", service}, {"protocol", relay_protocol_name(protocol)}};
        if (multi)
        {
            values["path"] = params.at("path");
            values["epoch"] = config::require_unsigned(params, "epoch", true);
            const auto path = values.at("path").get<std::vector<std::string>>();
            if (!request || path.size() < 2 || path.front() != config_.cluster.node_id ||
                values.at("epoch") != topology_->epoch())
                throw std::invalid_argument("invalid path or stale epoch");
            for (const auto &relay : relay_sessions_)
            {
                auto control = std::get_if<ControlRouterMulti>(&relay->control_);
                if (!control || !control->ingress_ || !control->matches(session, params))
                    continue;
                for (const auto *field : {"path", "epoch", "service", "protocol"})
                    if (control->params_.at(field) != values.at(field))
                        throw std::invalid_argument("relay request conflicts with existing request");
                if (control->progress_ >= ControlRouterMulti::Progress::AgentNotified)
                    control->notify_agent();
                return;
            }
            if (std::count_if(relay_sessions_.begin(), relay_sessions_.end(), [](const auto &relay) {
                    return std::holds_alternative<ControlRouterMulti>(relay->control_);
                }) >= static_cast<std::ptrdiff_t>(config_.control.max_connections))
                throw std::runtime_error("relay path capacity reached");
            start_relay(std::make_shared<NodeSession>(*this, session, std::move(values), true));
        }
        else
        {
            const auto producer = registry_.find_service(service);
            if (!producer || producer->protocol != protocol)
                throw std::runtime_error(!producer ? "service unavailable" : "service protocol mismatch");
            start_relay(std::make_shared<NodeSession>(*this, session, producer->session, std::move(values), producer->traffic));
        }
    }
    catch (const std::exception &error)
    {
        njson values{{"request_id", request}, {"service", service}, {"protocol", relay_protocol_name(protocol)}, {"reason", error.what()}};
        if (multi)
        {
            values["path"] = params.at("path");
            values["stage"] = "validate";
        }
        session->send(CtrlMessage(CtrlCommand::RelayError, std::move(values)));
    }
}

void RelayNode::handle_relay_peer(CtrlMessage message)
{
    const auto &params = config::message_params(message);
    try
    {
        const auto epoch = config::require_unsigned(params, "epoch", true);
        const auto id = config::require_unsigned(params, "flow_id", true);
        const auto source = config::required_string(params, "source", message.command);
        for (const auto &relay : relay_sessions_)
        {
            auto control = std::get_if<ControlRouterMulti>(&relay->control_);
            if (control && control->epoch_ == epoch && control->flow_id_ == id && control->peer_ == source)
            {
                control->handle_peer(message);
                return;
            }
        }
        if (message.command != "relay.peer.open")
            return;
        const auto protocol = config::message_protocol(params);
        if (!nodelink_mgr_->egress_ready(epoch, id, source, protocol == RelayProtocol::Udp ? RelayProtocol::Udp : RelayProtocol::Tcp))
            throw std::runtime_error("flow unavailable or wrong endpoint");
        const auto service = registry_.find_service(config::message_service(params));
        if (!service || service->protocol != protocol)
            throw std::runtime_error(!service ? "service unavailable" : "service protocol mismatch");
        if (std::count_if(relay_sessions_.begin(), relay_sessions_.end(), [](const auto &relay) {
                    return std::holds_alternative<ControlRouterMulti>(relay->control_);
                }) >= static_cast<std::ptrdiff_t>(config_.control.max_connections))
            throw std::runtime_error("relay path capacity reached");
        start_relay(std::make_shared<NodeSession>(*this, service->session, params, false, service->traffic));
    }
    catch (const std::exception &error)
    {
        PROXY_DEBUG_PRINT("Relay peer command=%s reason=%s", message.command.c_str(), error.what());
        if (message.command == "relay.peer.open")
            cluster_mgr_->send(params.at("source").get<std::string>(), CtrlMessage("relay.peer.close",
                njson{{"epoch", params.at("epoch")}, {"flow_id", params.at("flow_id")}, {"stage", "bind"}, {"reason", error.what()}}));
    }
}

void RelayNode::cancel_relays(std::string reason, const ControlSessionPtr &session)
{
    for (const auto &relay : relay_sessions_)
    {
        if (session && !relay->belongs_to(session))
            continue;
        const auto multi = std::get_if<ControlRouterMulti>(&relay->control_);
        relay->cancel("control", session && multi ? (multi->ingress_ ? "consumer control disconnected" : "producer control disconnected") : reason);
    }
}

void RelayNode::invalidate_relays(std::string reason)
{
    for (const auto &relay : relay_sessions_)
        if (auto control = std::get_if<ControlRouterMulti>(&relay->control_);
            control && (!reason.empty() || control->epoch_ != topology_->epoch()))
            control->cancel("control", reason.empty() ? "flow epoch changed" : reason);
}

asio::awaitable<void> RelayNode::stop_relays()
{
    cancel_relays("node stopping");
    while (!relay_sessions_.empty())
    {
        relays_done_.expires_at(std::chrono::steady_clock::time_point::max());
        co_await relays_done_.async_wait(use_nothrow_awaitable);
    }
}
