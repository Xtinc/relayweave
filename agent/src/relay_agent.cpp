#include "relay_agent.h"
#include "forwarder.h"
#include <algorithm>
#include <sstream>
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
            if (maximum > 16)
            {
                throw std::runtime_error("routing.max_nodes must be between 1 and 16");
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
        result.stream_open_timeout = optional_duration(*relay, "open_timeout_ms", result.stream_open_timeout, "relay");
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

class NodeConnection
{
    using tcp = asio::ip::tcp;

    struct Operations
    {
        explicit Operations(asio::any_io_executor executor) : resolver(executor), socket(executor), retry(executor)
        {
        }

        void cancel()
        {
            resolver.cancel();
            asio::error_code ignored;
            socket.close(ignored);
            retry.cancel();
        }

        tcp::resolver resolver;
        tcp::socket socket;
        asio::steady_timer retry;
    };

  public:
    NodeConnection(std::string node_id, std::string host, std::uint16_t port)
        : node_id_(std::move(node_id)), host_(std::move(host)), port_(port), id_(host_ + ":" + std::to_string(port_))
    {
    }

    asio::awaitable<void> run(RelayAgent &owner)
    {
        operations_.emplace(co_await asio::this_coro::executor);
        auto &operations = *operations_;
        auto reconnect_delay = owner.config_.reconnect_initial_delay;
        ScopeGuard clear_operations([this]() noexcept { operations_.reset(); });
        while (running_)
        {
            std::string failure;
            const char *stage = "resolve control endpoint";
            std::shared_ptr<TLSChannel> channel;
            try
            {
                PROXY_DEBUG_PRINT("Control connecting -> %s:%u", host_.c_str(), static_cast<unsigned int>(port_));
                auto endpoints = co_await operations.resolver.async_resolve(
                    host_, std::to_string(port_),
                    asio::cancel_after(owner.config_.connect_timeout, asio::use_awaitable));
                stage = "connect control endpoint";
                operations.socket = tcp::socket(operations.resolver.get_executor());
                const auto endpoint = co_await asio::async_connect(
                    operations.socket, endpoints,
                    asio::cancel_after(owner.config_.connect_timeout, asio::use_awaitable));
                if (!running_)
                {
                    break;
                }

                PROXY_DEBUG_PRINT("Control TCP connected -> %s:%u", endpoint.address().to_string().c_str(),
                                  static_cast<unsigned int>(endpoint.port()));
                stage = "start control TLS channel";
                channel = std::make_shared<TLSChannel>(std::move(operations.socket), owner.ssl_context_,
                                                       TLSChannelRole::C, owner.config_.channel);
                channel_ = channel;
                co_await channel->start(host_, owner.config_.server_name);
                if (!running_)
                {
                    break;
                }

                stage = "identify server node";
                channel->send(CtrlMessage{CtrlCommand::ServerIdentify, njson::object()});
                auto identified = co_await channel->async_receive(owner.config_.channel.handshake_timeout);
                if (identified.type() != CtrlCommand::ServerIdentified)
                {
                    throw std::invalid_argument("Expected server.identified");
                }

                const auto identified_node =
                    config::required_string(config::message_params(identified), "node_id", "server.identified");
                if (!node_id_.empty() && node_id_ != identified_node)
                {
                    throw std::invalid_argument("Connected server node_id does not match service location");
                }

                node_id_ = identified_node;
                connected_ = true;
                reconnect_delay = owner.config_.reconnect_initial_delay;
                owner.connection_ready(*this, channel);
                PROXY_INFO_PRINT("Control [+] node=%s peer=%s", node_id_.c_str(), id_.c_str());

                stage = "receive control message";
                while (running_)
                {
                    auto message = co_await channel->async_receive();
                    owner.handle_control_message(*this, std::move(message));
                }
            }
            catch (const asio::system_error &exception)
            {
                if (exception.code() != asio::experimental::error::channel_cancelled &&
                    exception.code() != asio::experimental::error::channel_closed)
                {
                    failure = std::string(stage) + " failed: " + exception.what();
                }
            }
            catch (const std::exception &exception)
            {
                failure = std::string(stage) + " failed: " + exception.what();
            }
            catch (...)
            {
                failure = std::string(stage) + " failed: unknown exception";
            }

            channel_.reset();
            if (connected_)
            {
                PROXY_INFO_PRINT("Control [x] node=%s peer=%s reason=%s", node_id_.c_str(), id_.c_str(),
                                 !running_ ? "agent stopping" : failure.empty() ? "channel closed" : "error");
                connected_ = false;
                if (running_)
                {
                    owner.connection_closed(*this);
                }
            }
            if (channel)
            {
                try
                {
                    co_await channel->async_disconnect();
                }
                catch (const std::exception &exception)
                {
                    PROXY_ERROR_PRINT("Control close failed peer=%s:%u reason=%s", host_.c_str(),
                                      static_cast<unsigned int>(port_), exception.what());
                }
            }
            if (!running_)
            {
                break;
            }

            if (!failure.empty())
            {
                PROXY_ERROR_PRINT("Control failed node=%s peer=%s reason=%s retry=%lldms", node_id_.c_str(), id_.c_str(),
                                  failure.c_str(), static_cast<long long>(
                                      std::chrono::duration_cast<std::chrono::milliseconds>(reconnect_delay).count()));
            }
            else
            {
                PROXY_DEBUG_PRINT("Control retry node=%s peer=%s delay=%lldms", node_id_.c_str(), id_.c_str(),
                                  static_cast<long long>(
                                      std::chrono::duration_cast<std::chrono::milliseconds>(reconnect_delay).count()));
            }
            operations.retry.expires_after(reconnect_delay);
            const auto [error] = co_await operations.retry.async_wait(use_nothrow_awaitable);
            if (error || !running_)
            {
                break;
            }

            reconnect_delay = std::min(reconnect_delay * 2, owner.config_.reconnect_max_delay);
        }
    }

    void stop()
    {
        running_ = false;
        if (operations_)
        {
            operations_->cancel();
        }

        if (auto channel = channel_.lock())
        {
            asio::co_spawn(channel->executor(), channel->async_disconnect(), asio::detached);
        }
    }

    void send(CtrlMessage message) const
    {
        if (!connected_)
        {
            return;
        }

        auto channel = channel_.lock();
        if (!channel)
        {
            return;
        }

        channel->send(std::move(message));
    }

    bool ready() const noexcept
    {
        return connected_ && !channel_.expired();
    }

    const std::string &id() const noexcept
    {
        return id_;
    }

    void set_node_id(std::string node_id)
    {
        if (node_id.empty())
        {
            return;
        }

        if (!node_id_.empty() && node_id_ != node_id)
        {
            throw std::invalid_argument("Server endpoint belongs to a different node_id");
        }

        node_id_ = std::move(node_id);
    }

    const std::string &node_id() const noexcept
    {
        return node_id_;
    }

    ServerRoute route() const
    {
        return ServerRoute{id_, host_, channel_};
    }

  private:
    std::string node_id_;
    std::string host_;
    std::uint16_t port_;
    std::string id_;
    std::weak_ptr<TLSChannel> channel_;
    std::optional<Operations> operations_;
    bool running_ = true;
    bool connected_ = false;
};

RelayAgent::RelayAgent(asio::io_context &control_io, asio::io_context &transfer_io, asio::ssl::context &ssl_context,
                       AgentConfig config)
    : control_executor_(control_io.get_executor()), transfer_executor_(transfer_io.get_executor()),
      ssl_context_(ssl_context), config_(std::move(config)),
      routing_(control_io, !config_.forwards.empty(), config_.routing_max_nodes), discovery_timer_(control_executor_),
      forwarder_(std::make_shared<Forwarder>(*this, transfer_executor_, ssl_context_, config_.server_name,
                                             config_.channel.handshake_timeout, config_.connect_timeout,
                                             config_.stream_open_timeout, config_.services, config_.forwards)),
      primary_connection_id_(config_.host + ":" + std::to_string(config_.port)), stopped_waiter_(control_executor_)
{
    stopped_waiter_.expires_at(std::chrono::steady_clock::time_point::max());
    if (&control_io == &transfer_io)
    {
        throw std::invalid_argument("Control and transfer channels require distinct io_context instances");
    }

    for (const auto &forward : config_.forwards)
    {
        service_routes_.insert(ServiceKey{forward.service, forward.protocol}, std::nullopt, std::uint64_t{0});
    }
}

void RelayAgent::start()
{
    if (state_ != State::Created)
    {
        PROXY_ERROR_PRINT("RelayWeave Agent can only be started once.");
        return;
    }

    forwarder_->start();
    state_ = State::Running;
    update_probe_targets();
    ensure_connection({}, config_.host, config_.port);
    ++active_tasks_;
    asio::co_spawn(
        control_executor_, [self = shared_from_this()]() -> asio::awaitable<void> { co_await self->discovery_loop(); },
        [self = shared_from_this()](std::exception_ptr failure) {
            if (failure && self->state_ == State::Running)
            {
                PROXY_ERROR_PRINT("Discovery stopped reason=%s", exception_description(failure).c_str());
            }
            --self->active_tasks_;
            self->try_stop_forwarder();
        });
}

asio::awaitable<void> RelayAgent::async_stop()
{
    auto self = shared_from_this();
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await asio::co_spawn(self->control_executor_, self->stop_on_control_executor(), asio::use_awaitable);
}

asio::awaitable<void> RelayAgent::stop_on_control_executor()
{
    if (state_ == State::Created)
    {
        state_ = State::StoppingControl;
        try_stop_forwarder();
    }
    else if (state_ == State::Running)
    {
        state_ = State::StoppingControl;
        PROXY_INFO_PRINT("Agent stopping");
        discovery_timer_.cancel();
        routing_.stop();
        connections_.for_each(
            [](const std::string &, std::shared_ptr<NodeConnection> &connection) { connection->stop(); });
        try_stop_forwarder();
    }

    if (state_ != State::Stopped)
    {
        co_await stopped_waiter_.async_wait(use_nothrow_awaitable);
    }
}

asio::awaitable<void> RelayAgent::discovery_loop()
{
    auto next_poll = AgentRouting::Clock::now();
    while (state_ == State::Running)
    {
        try
        {
            if (!co_await routing_.refresh_probes())
            {
                continue;
            }
        }
        catch (const std::exception &exception)
        {
            PROXY_ERROR_PRINT("Routing refresh failed reason=%s", exception.what());
        }
        const auto now = AgentRouting::Clock::now();
        if (now >= next_poll)
        {
            query_topology();
            query_services(true);
            next_poll = now + std::chrono::seconds(5);
        }
        if (state_ != State::Running)
        {
            break;
        }
        discovery_timer_.expires_at(next_poll);
        const auto [error] = co_await discovery_timer_.async_wait(use_nothrow_awaitable);
        if ((error && error != asio::error::operation_aborted) || state_ != State::Running)
        {
            break;
        }
    }
    co_await routing_.close();
}

void RelayAgent::query_topology()
{
    if (config_.forwards.empty() || state_ != State::Running)
    {
        return;
    }
    const auto primary = connections_.find_primary(primary_connection_id_);
    if (primary && (*primary)->ready() && !routing_.request_pending(AgentRouting::Clock::now()))
    {
        const auto request = allocate_request_id();
        routing_.begin_request(request, AgentRouting::Clock::now());
        (*primary)->send(CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request}}});
    }
}

void RelayAgent::update_probe_targets()
{
    const auto primary = connections_.find_primary(primary_connection_id_);
    std::map<std::string, std::string> targets{{primary ? (*primary)->node_id() : std::string{}, config_.host}};
    service_routes_.for_each([&](const ServiceKey &service, const std::uint64_t &) {
        if (const auto connection = service_connection(service))
        {
            targets.insert_or_assign(connection->node_id(), connection->route().host);
        }
    });
    if (routing_.set_required_targets(std::move(targets)))
    {
        path_cache_.clear();
    }
    discovery_timer_.cancel();
}

asio::awaitable<void> RelayAgent::calculate_service_paths(ServiceKey service, std::string connection_id)
{
    if (state_ != State::Running)
    {
        co_return;
    }

    const auto connection = connections_.find_primary(connection_id);
    if (!connection || !(*connection)->ready())
    {
        co_return;
    }

    const auto &destination = (*connection)->node_id();
    const auto now = AgentRouting::Clock::now();
    if (path_cache_.get(destination, now))
    {
        co_return;
    }

    auto candidates = routing_.candidate_paths(now);
    const auto alternatives = candidates.find(destination);
    // Candidates are diagnostics; business relays still connect directly to the service node.
    // Keep one complete log record per path so journald preserves level and service context.
    if (proxy_is_debug_enabled())
    {
        const auto count = alternatives == candidates.end() ? 0 : alternatives->second.size();
        PROXY_DEBUG_PRINT("Routes service=%s/%s -> %s candidates=%zu max_nodes=%zu%s",
                          service.service.c_str(), relay_protocol_name(service.protocol).data(), destination.c_str(),
                          count, config_.routing_max_nodes, count == 0 ? " cost=unavailable" : "");
        for (std::size_t index = 0; index < std::min(count, MAX_LOGGED_CANDIDATES); ++index)
        {
            const auto &candidate = alternatives->second[index];
            std::ostringstream path;
            path << "agent";
            for (const auto &node : candidate.nodes)
                path << " -> " << node;
            PROXY_DEBUG_PRINT("Route #%zu cost=%.3f service=%s/%s: %s%s", index + 1, candidate.cost,
                              service.service.c_str(), relay_protocol_name(service.protocol).data(),
                              path.str().c_str(), index == 0 ? " *" : "");
        }
    }

    path_cache_.put(destination, alternatives == candidates.end() ? std::vector<RouteGraph::Path>{}
                                                                  : std::move(alternatives->second));
}

std::shared_ptr<NodeConnection> RelayAgent::service_connection(const ServiceKey &service) const
{
    const auto server = service_routes_.secondary_key(service);
    const auto connection = server ? connections_.find_primary(*server) : nullptr;
    return connection ? *connection : nullptr;
}

std::shared_ptr<NodeConnection> RelayAgent::ensure_connection(std::string node_id, std::string host, std::uint16_t port)
{
    if (!node_id.empty())
    {
        if (const auto node = connections_.find_secondary(node_id))
        {
            return *node;
        }
    }

    const auto id = host + ":" + std::to_string(port);
    if (const auto existing = connections_.find_primary(id))
    {
        if (!node_id.empty())
        {
            if (!(*existing)->node_id().empty() && (*existing)->node_id() != node_id)
            {
                throw std::invalid_argument("Server endpoint belongs to a different node_id");
            }

            if (!connections_.set_secondary(id, node_id))
            {
                throw std::invalid_argument("Server node_id is already connected through another endpoint");
            }

            (*existing)->set_node_id(std::move(node_id));
        }
        return *existing;
    }

    auto secondary = node_id.empty() ? std::optional<std::string>{} : std::optional<std::string>{node_id};
    auto connection = std::make_shared<NodeConnection>(std::move(node_id), std::move(host), port);
    if (!connections_.insert(id, std::move(secondary), connection).second)
    {
        throw std::logic_error("Server connection indices are inconsistent");
    }

    ++active_tasks_;
    try
    {
        asio::co_spawn(control_executor_, connection->run(*this),
                       [self = shared_from_this(), keep_alive = connection](std::exception_ptr failure) {
                           static_cast<void>(keep_alive);
                           self->connection_finished(std::move(failure));
                       });
    }
    catch (...)
    {
        --active_tasks_;
        connections_.erase_primary(id);
        throw;
    }

    return connection;
}

void RelayAgent::connection_ready(NodeConnection &connection, const std::shared_ptr<TLSChannel> &channel)
{
    if (state_ != State::Running)
        return;

    if (!connections_.set_secondary(connection.id(), connection.node_id()))
    {
        connection.stop();
        connections_.erase_primary(connection.id());
        throw std::invalid_argument("Server node_id is already connected through another endpoint");
    }

    if (connection.id() == primary_connection_id_)
    {
        update_probe_targets();
        for (const auto &service : config_.services)
        {
            channel->send(
                CtrlMessage{CtrlCommand::ServiceRegister, njson{{"request_id", allocate_request_id()},
                                                                {"service", service.name},
                                                                {"protocol", relay_protocol_name(service.protocol)}}});
        }
        query_services();
        query_topology();
    }

    service_routes_.for_each_secondary(connection.id(), [&](const ServiceKey &service, std::uint64_t &request_id) {
        request_id = 0;
        auto route = connection.route();
        asio::post(transfer_executor_, [forwarder = forwarder_, service, route = std::move(route)]() mutable {
            forwarder->set_route(service.service, service.protocol, std::move(route));
        });
    });
}

void RelayAgent::connection_closed(const NodeConnection &connection)
{
    asio::post(transfer_executor_, [forwarder = forwarder_, id = connection.id()]() { forwarder->clear_server(id); });
    if (connection.id() == primary_connection_id_)
    {
        routing_.invalidate_snapshot();
        path_cache_.clear();
        update_probe_targets();
        service_routes_.for_each([](const ServiceKey &, std::uint64_t &request_id) { request_id = 0; });
    }

    if (state_ == State::Running)
    {
        query_services();
    }
}

void RelayAgent::connection_finished(std::exception_ptr failure)
{
    if (failure && state_ == State::Running)
    {
        PROXY_ERROR_PRINT("Control task stopped reason=%s", exception_description(failure).c_str());
    }

    if (active_tasks_ == 0)
    {
        PROXY_ERROR_PRINT("RelayAgent task accounting underflow.");
        return;
    }

    --active_tasks_;
    try_stop_forwarder();
}

void RelayAgent::release_unused_connections()
{
    std::vector<std::string> unused;
    connections_.for_each([&](const std::string &id, const std::shared_ptr<NodeConnection> &) {
        if (id != primary_connection_id_ && service_routes_.count_secondary(id) == 0)
        {
            unused.push_back(id);
        }
    });

    for (const auto &id : unused)
    {
        if (const auto connection = connections_.find_primary(id))
        {
            (*connection)->stop();
        }
        connections_.erase_primary(id);
    }
}

void RelayAgent::query_services(bool refresh)
{
    if (state_ != State::Running)
    {
        return;
    }

    const auto primary = connections_.find_primary(primary_connection_id_);
    if (!primary || !(*primary)->ready())
    {
        return;
    }

    service_routes_.for_each([&](const ServiceKey &service, std::uint64_t &request_id) {
        const auto server = service_routes_.secondary_key(service);
        const auto target = server ? connections_.find_primary(*server) : nullptr;
        if (target && (*target)->ready())
        {
            return;
        }

        if (request_id && !refresh)
        {
            return;
        }

        request_id = allocate_request_id();
        (*primary)->send(
            CtrlMessage{CtrlCommand::ServiceLookup, njson{{"request_id", request_id},
                                                          {"service", service.service},
                                                          {"protocol", relay_protocol_name(service.protocol)}}});
    });
}

void RelayAgent::locate_service(const njson &params)
{
    const auto request_id = config::require_unsigned(params, "request_id");
    ServiceKey service{config::message_service(params), config::message_protocol(params)};
    const auto desired = service_routes_.find_primary(service);
    if (!desired || !request_id || *desired != request_id)
    {
        return;
    }

    auto node_id = config::required_string(params, "node_id", "service.located");
    auto address = config::required_string(params, "address", "service.located");
    const auto port = config::required_port(params, "port", "service.located");
    auto connection = ensure_connection(std::move(node_id), std::move(address), port);
    *desired = 0;
    if (!service_routes_.set_secondary(service, connection->id()))
    {
        throw std::logic_error("Service route is not registered");
    }

    PROXY_INFO_PRINT("Service located service=%s/%s -> %s@%s", service.service.c_str(),
                     relay_protocol_name(service.protocol).data(), connection->node_id().c_str(),
                     connection->id().c_str());
    release_unused_connections();
    update_probe_targets();
    if (connection->ready())
    {
        auto route = connection->route();
        asio::post(transfer_executor_,
                   [forwarder = forwarder_, service = std::move(service), route = std::move(route)]() mutable {
                       forwarder->set_route(std::move(service.service), service.protocol, std::move(route));
                   });
    }
}

void RelayAgent::handle_control_message(const NodeConnection &connection, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    const auto command = message.type();
    auto route = connection.route();
    if (command == CtrlCommand::TopologySnapshot)
    {
        if (connection.id() == primary_connection_id_)
        {
            try
            {
                if (routing_.accept_snapshot(params, AgentRouting::Clock::now()))
                {
                    discovery_timer_.cancel();
                }
            }
            catch (const std::exception &exception)
            {
                PROXY_ERROR_PRINT("Topology rejected reason=%s", exception.what());
            }
        }
    }
    else if (command == CtrlCommand::RelayOffer)
    {
        auto service = config::message_service(params);
        const auto protocol = config::message_protocol(params);
        const auto uuid = config::require_unsigned(params, "uuid");
        const auto transfer_port = config::message_data_port(params);
        const auto ticket = config::require_unsigned(params, "ticket", true);
        if (protocol != RelayProtocol::Udp)
        {
            asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), protocol,
                                            service = std::move(service), uuid, ticket, transfer_port]() mutable {
                forwarder->stream_relay_offer(std::move(route), protocol, std::move(service), uuid, ticket,
                                              transfer_port);
            });
        }
        else
        {
            const auto session_id = config::require_unsigned(params, "session_id", true);
            asio::post(transfer_executor_,
                       [forwarder = forwarder_, route = std::move(route), service = std::move(service), uuid,
                        session_id, ticket, transfer_port]() mutable {
                           forwarder->datagram_relay_offer(std::move(route), std::move(service), uuid, session_id,
                                                           ticket, transfer_port);
                       });
        }
    }
    else if (command == CtrlCommand::RelayOpened)
    {
        const auto request_id = config::require_unsigned(params, "request_id");
        const auto protocol = config::message_protocol(params);
        const auto uuid = config::require_unsigned(params, "uuid");
        const auto transfer_port = config::message_data_port(params);
        const auto ticket = config::require_unsigned(params, "ticket", true);
        if (protocol != RelayProtocol::Udp)
        {
            asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), protocol, request_id,
                                            uuid, ticket, transfer_port]() mutable {
                forwarder->stream_relay_opened(std::move(route), protocol, request_id, uuid, ticket, transfer_port);
            });
        }
        else
        {
            const auto session_id = config::require_unsigned(params, "session_id", true);
            asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), request_id, uuid,
                                            session_id, ticket, transfer_port]() mutable {
                forwarder->datagram_relay_opened(std::move(route), request_id, uuid, session_id, ticket, transfer_port);
            });
        }
    }
    else if (command == CtrlCommand::ServiceOk)
    {
        const auto protocol = config::message_protocol(params);
        PROXY_INFO_PRINT("Service registered service=%s/%s -> %s", config::message_service(params).c_str(),
                         relay_protocol_name(protocol).data(), connection.node_id().c_str());
    }
    else if (command == CtrlCommand::ServiceLocated)
    {
        locate_service(params);
    }
    else if (command == CtrlCommand::ServiceError || command == CtrlCommand::RelayError)
    {
        if (command == CtrlCommand::RelayError)
        {
            if (const auto request_id = config::optional_unsigned(params, "request_id"))
            {
                const auto protocol = config::message_protocol(params);
                const auto uuid = config::optional_unsigned(params, "uuid", true);
                auto reason = config::optional_string(params, "reason", "relay failed");
                std::optional<ServiceKey> stale_service;
                if (reason == "service unavailable" || reason == "service protocol mismatch")
                {
                    stale_service.emplace(config::message_service(params), protocol);
                    const auto service_route = service_routes_.find_primary(*stale_service);
                    const auto server = service_routes_.secondary_key(*stale_service);
                    if (!service_route || !server || *server != connection.id())
                    {
                        stale_service.reset();
                    }
                    else
                    {
                        *service_route = 0;
                        service_routes_.set_secondary(*stale_service, std::nullopt);
                        update_probe_targets();
                    }
                }
                asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route),
                                                request_id = *request_id, protocol, uuid, reason = std::move(reason),
                                                stale_service]() mutable {
                    forwarder->relay_open_failed(std::move(route), request_id, protocol, uuid, std::move(reason));
                    if (stale_service)
                        forwarder->clear_route(stale_service->service, stale_service->protocol);
                });

                if (stale_service)
                {
                    release_unused_connections();
                    query_services();
                }
                // Forwarder reports the rejection once, with the pending relay context.
                return;
            }
        }
        PROXY_ERROR_PRINT("Server rejected node=%s command=%s service=%s request_id=%llu uuid=%llu reason=%s",
                          connection.node_id().c_str(), message.command.c_str(),
                          config::optional_string(params, "service", "-").c_str(),
                          static_cast<unsigned long long>(config::optional_unsigned(params, "request_id").value_or(0)),
                          static_cast<unsigned long long>(config::optional_unsigned(params, "uuid").value_or(0)),
                          config::optional_string(params, "reason", "unspecified").c_str());
    }
    else if (command == CtrlCommand::RelayReady)
    {
        const auto uuid = config::require_unsigned(params, "uuid", true);
        const auto protocol = config::message_protocol(params);
        asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), uuid, protocol]() mutable {
            forwarder->relay_ready(std::move(route), uuid, protocol);
        });
    }
    else if (command == CtrlCommand::RelayClosed)
    {
        const auto uuid = config::require_unsigned(params, "uuid", true);
        const auto protocol = config::message_protocol(params);
        auto reason = config::optional_string(params, "reason", "relay closed");
        asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), uuid, protocol,
                                        reason = std::move(reason)]() mutable {
            forwarder->relay_closed(std::move(route), uuid, protocol, std::move(reason));
        });
    }
    else if (command == CtrlCommand::ServiceListed)
    {
        PROXY_DEBUG_PRINT("Server event command=%s params=%s", message.command.c_str(), params.dump().c_str());
    }
    else
    {
        PROXY_ERROR_PRINT("Server command rejected command=%s", message.command.c_str());
    }
}

std::uint64_t RelayAgent::allocate_request_id()
{
    const auto request_id = next_request_id_++;
    if (next_request_id_ == 0)
    {
        next_request_id_ = 1;
    }
    return request_id;
}

void RelayAgent::try_stop_forwarder()
{
    if (state_ != State::StoppingControl || active_tasks_ != 0)
    {
        return;
    }

    state_ = State::StoppingForwarder;
    asio::co_spawn(
        control_executor_,
        [self = shared_from_this()]() -> asio::awaitable<void> {
            co_await asio::co_spawn(self->transfer_executor_, self->forwarder_->async_stop(), asio::use_awaitable);
            self->complete_stop();
            PROXY_INFO_PRINT("Agent stopped");
        },
        [self = shared_from_this()](std::exception_ptr failure) {
            if (!failure)
                return;
            PROXY_ERROR_PRINT("Agent stop failed reason=%s", exception_description(failure).c_str());
            self->complete_stop();
        });
}

void RelayAgent::complete_stop() noexcept
{
    connections_.clear();
    service_routes_.clear();
    state_ = State::Stopped;
    stopped_waiter_.cancel();
}
