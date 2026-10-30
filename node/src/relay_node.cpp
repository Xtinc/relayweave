#include "relay_node.h"
#include "node_control.h"
#include <algorithm>
#include <cassert>
#include <future>

RelayNode::RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
                     asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config)
    : control_executor_(control_io.get_executor()), transfer_tcp_executor_(transfer_tcp_io.get_executor()),
      transfer_udp_executor_(transfer_udp_io.get_executor()), cluster_data_executor_(cluster_data_io.get_executor()),
      config_(std::move(config)), ssl_context_(ssl_context), control_acceptor_(control_executor_),
      control_sessions_done_(control_executor_), relays_done_(control_executor_),
      registry_(config_.control.max_connections, config_.control.max_services,
                config_.control.max_services_per_session),
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
          config_.datagram.max_relays, config_.datagram.traffic))
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
    metrics_ = std::make_shared<NodeMetrics>(control_executor_, transfer_tcp_executor_, transfer_udp_executor_,
                                             registry_, config_.bandwidth_mbps);
    topology_ =
        std::make_unique<Topology>(control_io, config_.cluster.node_id,
                                   config_.cluster.role == ClusterConfig::Role::Master, *cluster_mgr_, *metrics_);
    nodelink_mgr_ =
        std::make_unique<NodeLinkMgr>(control_executor_, cluster_data_executor_, config_.cluster, config_.tcp.address,
                                      config_.datagram.address, *cluster_mgr_, *topology_);
    control_ = std::make_unique<NodeControl>(registry_, *cluster_mgr_, *topology_, *metrics_, relay_sessions_,
                                             config_.cluster.node_id, config_.control.advertise_address);
}

RelayNode::~RelayNode() = default;

void RelayNode::start(std::function<void(std::string)> failure_handler)
{
    if (state_.load() != State::Created)
    {
        throw std::logic_error("RelayWeave Node can only be started once");
    }

    failure_handler_ = std::move(failure_handler);
    ScopeGuard rollback([this]() noexcept {
        state_.store(State::Stopping);
        asio::error_code ignored;
        control_acceptor_.close(ignored);
    });
    const tcp::endpoint endpoint(asio::ip::make_address(config_.control.address), config_.control.port);
    control_acceptor_.open(endpoint.protocol());
    control_acceptor_.set_option(asio::socket_base::reuse_address(true));
    control_acceptor_.bind(endpoint);
    control_acceptor_.listen();
    tcp_pipeline_->start();
    tls_pipeline_->start();
    datagram_mgr_->start();
    nodelink_mgr_->start();
    metrics_->start(nodelink_mgr_->protocol(), std::chrono::steady_clock::now());
    cluster_mgr_->start();
    topology_->start();
    nodelink_mgr_->activate();
    state_.store(State::Running);
    asio::co_spawn(
        control_executor_, [self = shared_from_this()] { return self->control_accept_loop(); }, asio::detached);
    metrics_->activate();
    rollback.dismiss();

    PROXY_INFO_PRINT("Node listening control=%s:%u tcp=%s:%u tls=%s:%u udp=%s:%u", config_.control.address.c_str(),
                     static_cast<unsigned int>(config_.control.port), config_.tcp.address.c_str(),
                     static_cast<unsigned int>(config_.tcp.port), config_.tls.address.c_str(),
                     static_cast<unsigned int>(config_.tls.port), config_.datagram.address.c_str(),
                     static_cast<unsigned int>(config_.datagram.port));
}

bool RelayNode::stop()
{
    const auto state = state_.load();
    if (state == State::Stopped)
    {
        return stop_ok_;
    }
    if (state != State::Created)
    {
        state_.store(State::Stopping);
        auto self = shared_from_this();
        const bool control_ok = asio::co_spawn(control_executor_, stop_control(), asio::use_future).get();
        stop_ok_ = control_ok && stop_ok_;

        asio::post(transfer_tcp_executor_, asio::use_future([self] {
                       self->metrics_->stop_tcp();
                       self->tcp_pipeline_->stop();
                       self->tls_pipeline_->stop();
                   }))
            .get();
        tcp_pipeline_->wait_for_pending();
        tls_pipeline_->wait_for_pending();

        asio::post(transfer_udp_executor_, asio::use_future([self] {
                       self->metrics_->stop_udp();
                       self->datagram_mgr_->stop();
                   }))
            .get();
    }

    state_.store(State::Stopped);
    PROXY_INFO_PRINT("Node stopped");
    return stop_ok_;
}

asio::awaitable<bool> RelayNode::stop_control()
{
    metrics_->stop_control();
    asio::error_code error;
    control_acceptor_.close(error);
    if (error)
    {
        PROXY_ERROR_PRINT("Control listener close failed reason=%s", error.message().c_str());
    }
    registry_.stop();

    co_await stop_relays();
    const bool links_ok = co_await nodelink_mgr_->stop();
    const bool topology_ok = co_await topology_->close();
    const bool cluster_ok = co_await cluster_mgr_->async_stop();
    if (active_control_sessions_ != 0)
    {
        co_await control_sessions_done_.async_wait(use_nothrow_awaitable);
    }
    co_return !error && links_ok && topology_ok && cluster_ok;
}

asio::awaitable<void> RelayNode::control_accept_loop()
{
    for (;;)
    {
        auto [error, socket] = co_await control_acceptor_.async_accept(use_nothrow_awaitable);
        const auto state = state_.load();
        if (state != State::Running || error == asio::error::operation_aborted)
        {
            co_return;
        }
        if (error)
        {
            PROXY_ERROR_PRINT("Control accept failed reason=%s category=%s", error.message().c_str(),
                              error.category().name());
            continue;
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
            [self = shared_from_this(), session_id, session] { return self->run_control_session(session_id, session); },
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
                handle_control_message(id, session, message);
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

    for (const auto &relay : relay_sessions_)
    {
        relay->control_disconnected(session);
    }
    registry_.remove(id);
    co_await session->async_disconnect();
    if (state_.load() == State::Running && topology_->members().size() > 1)
    {
        cluster_mgr_->broadcast(CtrlMessage(CtrlCommand::RelayDetach, njson{{"agent_session_id", id}}));
    }
    if (!failure.empty() && state_.load() == State::Running)
    {
        PROXY_ERROR_PRINT("Control failed session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                          static_cast<int>(session->peer().size()), session->peer().data(), failure.c_str());
    }
    else if (connected)
    {
        PROXY_INFO_PRINT("Control [x] session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                         static_cast<int>(session->peer().size()), session->peer().data(),
                         state_.load() != State::Running ? "node stopping" : "channel closed");
    }
}

void RelayNode::cluster_failed(std::string reason)
{
    const auto state = state_.load();
    if (state != State::Running && state != State::Stopping)
    {
        return;
    }
    state_.store(State::Stopping);
    // Wake Flow cleanup before stop_control() waits for the relay tasks to exit.
    nodelink_mgr_->control_failed(reason);
    invalidate_relays(reason);
    PROXY_ERROR_PRINT("Node failed reason=%s", reason.c_str());
    if (failure_handler_)
    {
        failure_handler_(std::move(reason));
    }
}

void RelayNode::handle_cluster_message(CtrlCommand command, CtrlMessage message)
{
    if (ctrl_command_group(command) == CtrlCommand::Relay && ctrl_command_subgroup(command) != CtrlCommand::RelayPeer)
    {
        handle_cluster_relay(command, std::move(message));
        return;
    }
    const auto group = ctrl_command_group(command);
    if (group == CtrlCommand::Link || group == CtrlCommand::Flow)
    {
        if (state_.load() == State::Running || command != CtrlCommand::FlowOpen)
        {
            nodelink_mgr_->handle(command, std::move(message));
        }
        return;
    }
    if (ctrl_command_subgroup(command) == CtrlCommand::RelayPeer)
    {
        if (state_.load() == State::Running)
        {
            handle_relay_peer(command, message);
        }
        return;
    }

    switch (command)
    {
    case CtrlCommand::TopologyMembers:
        topology_->update_members(config::message_params(message));
        if (state_.load() == State::Running)
        {
            nodelink_mgr_->members_changed();
            invalidate_relays();
        }
        break;
    case CtrlCommand::TopologyReport:
        topology_->update_report(config::message_params(message));
        break;
    case CtrlCommand::ClusterError:
        nodelink_mgr_->control_failed(config::message_params(message).value("reason", "cluster control failed"));
        invalidate_relays("cluster control failed");
        break;
    default:
        control_->handle_cluster(command, std::move(message));
        break;
    }
}

void RelayNode::handle_control_message(SessionId id, const ControlSessionPtr &session, const CtrlMessage &message)
{
    const auto &params = config::message_params(message);
    const auto command = message.type();
    switch (command)
    {
    case CtrlCommand::RelayOpen:
    case CtrlCommand::RelayReject:
    case CtrlCommand::RelayCancel:
        if (const auto target = config::optional_string(params, "node_id", config_.cluster.node_id);
            target != config_.cluster.node_id)
        {
            auto values = params;
            values["agent_session_id"] = id;
            if (command == CtrlCommand::RelayOpen)
            {
                values["accessor"] = std::string(session->peer());
            }
            if (cluster_mgr_->send(target, CtrlMessage(command, std::move(values))) !=
                    ClusterMgr::SendResult::Submitted &&
                command == CtrlCommand::RelayOpen)
            {
                auto error = params;
                error["reason"] = "entry unavailable";
                session->send(CtrlMessage(CtrlCommand::RelayError, std::move(error)));
            }
        }
        else
        {
            handle_relay(command, session, message);
        }
        return;
    default:
        if (!control_->handle_client(id, session, command, params))
        {
            throw std::invalid_argument("Unknown control command: " + message.command);
        }
    }
}

RelayNode::SessionId RelayNode::allocate_session_id()
{
    SessionId id;
    do
    {
        id = generate_random_id();
    } while (registry_.contains(id));
    return id;
}

void RelayNode::start_relay(std::shared_ptr<NodeSession> relay)
{
    if (relay->is_multi() && static_cast<std::size_t>(std::ranges::count_if(relay_sessions_, &NodeSession::is_multi)) >=
                                 config_.control.max_connections)
    {
        throw std::runtime_error("relay path capacity reached");
    }
    const auto protocol = relay->protocol();
    const auto capacity = protocol == RelayProtocol::Tcp   ? config_.tcp.max_relays
                          : protocol == RelayProtocol::Tls ? config_.tls.max_relays
                                                           : config_.datagram.max_relays;
    const auto count = std::ranges::count_if(relay_sessions_,
                                             [protocol](const auto &active) { return active->protocol() == protocol; });
    if (static_cast<std::size_t>(count) >= capacity)
    {
        throw std::runtime_error("relay capacity reached");
    }
    relay_sessions_.push_back(relay);
    ScopeGuard rollback([this, &relay]() noexcept { std::erase(relay_sessions_, relay); });
    asio::co_spawn(control_executor_, relay->run(),
                   asio::bind_cancellation_slot(
                       relay->cancellation_slot(), [self = shared_from_this(), relay](std::exception_ptr error) {
                           if (error)
                           {
                               PROXY_ERROR_PRINT("Relay task failed reason=%s", exception_description(error).c_str());
                           }
                           std::erase(self->relay_sessions_, relay);
                           if (self->relay_sessions_.empty())
                           {
                               self->relays_done_.cancel();
                           }
                       }));
    rollback.dismiss();
}

void RelayNode::handle_relay(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message)
{
    const auto &params = *message.params;
    if (command != CtrlCommand::RelayOpen)
    {
        for (const auto &relay : relay_sessions_)
        {
            if (relay->handle(command, session, message))
            {
                return;
            }
        }
        return;
    }
    const auto request = config::require_unsigned(params, "request_id");
    const auto service = config::message_service(params);
    const auto protocol = config::message_protocol(params);
    const bool multi = params.contains("path");
    try
    {
        RelayRequest values{service, protocol, request};
        if (multi)
        {
            values.path = params.at("path").get<std::vector<std::string>>();
            values.epoch = config::require_unsigned(params, "epoch", true);
            const auto &path = values.path;
            if (!request || path.size() < 2 || path.front() != config_.cluster.node_id ||
                values.epoch != topology_->epoch())
            {
                throw std::invalid_argument("invalid path or stale epoch");
            }
            for (const auto &relay : relay_sessions_)
            {
                if (relay->handle(command, session, message))
                {
                    return;
                }
            }
            start_relay(std::make_shared<NodeSession>(*this, session, std::move(values), true));
        }
        else
        {
            const auto producer = registry_.find_service(service);
            if (!producer || producer->protocol != protocol)
            {
                throw std::runtime_error(!producer ? "service unavailable" : "service protocol mismatch");
            }
            start_relay(
                std::make_shared<NodeSession>(*this, session, producer->session, std::move(values), producer->traffic));
        }
    }
    catch (const std::exception &error)
    {
        njson values{{"request_id", request},
                     {"service", service},
                     {"protocol", relay_protocol_name(protocol)},
                     {"reason", error.what()}};
        if (multi)
        {
            values["path"] = params.at("path");
            values["stage"] = "validate";
        }
        send_relay(session, CtrlMessage(CtrlCommand::RelayError, std::move(values)));
    }
}

void RelayNode::handle_relay_peer(CtrlCommand command, const CtrlMessage &message)
{
    const auto &params = *message.params;
    const auto epoch = config::require_unsigned(params, "epoch", true);
    const auto id = config::require_unsigned(params, "flow_id", true);
    const auto &source = params.at("source").get_ref<const std::string &>();
    for (const auto &relay : relay_sessions_)
    {
        if (relay->handle_peer(command, message))
        {
            return;
        }
    }
    if (command != CtrlCommand::RelayPeerOpen)
    {
        return;
    }
    try
    {
        const auto protocol = config::message_protocol(params);
        if (!nodelink_mgr_->egress_ready(epoch, id, source,
                                         protocol == RelayProtocol::Udp ? RelayProtocol::Udp : RelayProtocol::Tcp))
        {
            throw std::runtime_error("flow unavailable or wrong endpoint");
        }
        const auto service_name = config::message_service(params);
        const auto service = registry_.find_service(service_name);
        if (!service || service->protocol != protocol)
        {
            throw std::runtime_error(!service ? "service unavailable" : "service protocol mismatch");
        }
        RelayRequest request{service_name, protocol};
        request.epoch = epoch;
        request.flow_id = id;
        request.peer = source;
        request.accessor = config::optional_string(params, "accessor", "");
        start_relay(
            std::make_shared<NodeSession>(*this, service->session, std::move(request), false, service->traffic));
    }
    catch (const std::exception &error)
    {
        PROXY_DEBUG_PRINT("Relay peer command=%s reason=%s", message.command.c_str(), error.what());
        cluster_mgr_->send(
            source, CtrlMessage(CtrlCommand::RelayPeerClose,
                                njson{{"epoch", epoch}, {"flow_id", id}, {"stage", "bind"}, {"reason", error.what()}}));
    }
}

void RelayNode::invalidate_relays(std::string reason)
{
    for (const auto &relay : relay_sessions_)
    {
        relay->invalidate(topology_->epoch(), reason);
    }
}

asio::awaitable<void> RelayNode::stop_relays()
{
    for (const auto &relay : relay_sessions_)
    {
        relay->cancel("control", "node stopping");
    }
    while (!relay_sessions_.empty())
    {
        relays_done_.expires_at(std::chrono::steady_clock::time_point::max());
        co_await relays_done_.async_wait(use_nothrow_awaitable);
    }
}

bool RelayNode::send_relay(const AgentPeer &route, CtrlMessage message)
{
    auto &params = *message.params;
    params["node_id"] = config_.cluster.node_id;
    if (message.type() == CtrlCommand::RelayOpened || message.type() == CtrlCommand::RelayOffer)
    {
        params["data_address"] = config_.control.advertise_address;
    }
    if (route.node.empty())
    {
        const auto session = route.local.lock();
        if (!session)
        {
            return false;
        }
        session->send(std::move(message));
        return true;
    }
    params["agent_session_id"] = route.session_id;
    return cluster_mgr_->send(route.node, std::move(message)) == ClusterMgr::SendResult::Submitted;
}

void RelayNode::handle_cluster_relay(CtrlCommand command, CtrlMessage message)
{
    auto &params = *message.params;
    const auto source = params.at("source").get<std::string>();
    const auto id = config::require_unsigned(params, "agent_session_id", true);
    switch (command)
    {
    case CtrlCommand::RelayOpen:
    case CtrlCommand::RelayCancel:
    case CtrlCommand::RelayReject:
        if (state_.load() == State::Running || command != CtrlCommand::RelayOpen)
        {
            handle_relay(command, AgentPeer{source, id, config::optional_string(params, "accessor", "")}, message);
        }
        return;
    case CtrlCommand::RelayDetach:
        for (const auto &relay : relay_sessions_)
        {
            relay->control_disconnected(AgentPeer{source, id, {}});
        }
        return;
    case CtrlCommand::RelayOpened:
    case CtrlCommand::RelayReady:
    case CtrlCommand::RelayClosed:
    case CtrlCommand::RelayError:
    case CtrlCommand::RelayOffer:
        if (const auto session = registry_.find_session(id))
        {
            params["node_id"] = source;
            params.erase("source");
            params.erase("agent_session_id");
            params.erase("target");
            session->send(std::move(message));
        }
        return;
    default:
        return;
    }
}
