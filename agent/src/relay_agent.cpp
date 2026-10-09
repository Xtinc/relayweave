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
                                             config_.relay_open_timeout, config_.services, config_.forwards)),
      primary_connection_id_(config_.host + ":" + std::to_string(config_.port)), stopped_waiter_(control_executor_)
{
    stopped_waiter_.expires_at(std::chrono::steady_clock::time_point::max());
    if (&control_io == &transfer_io)
    {
        throw std::invalid_argument("Control and transfer channels require distinct io_context instances");
    }

    for (const auto &forward : config_.forwards)
    {
        service_locations_.insert(ServiceKey{forward.service, forward.protocol}, std::nullopt, ServiceLocation{});
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
        invalidate_entries("agent stopping");
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
    service_locations_.for_each([&](const ServiceKey &service, const ServiceLocation &location) {
        if (const auto node = service_locations_.secondary_key(service))
        {
            targets.insert_or_assign(*node, location.address);
        }
    });
    if (routing_.set_required_targets(std::move(targets)))
    {
        path_cache_.clear();
    }
    discovery_timer_.cancel();
}

std::vector<std::string> RelayAgent::calculate_service_paths(const ServiceKey &service, const std::string &destination)
{
    if (state_ != State::Running)
    {
        return std::vector<std::string>{};
    }

    const auto now = AgentRouting::Clock::now();
    if (const auto cached = path_cache_.get(destination, now))
    {
        return cached->empty() ? std::vector<std::string>{} : cached->front().nodes;
    }

    auto candidates = routing_.candidate_paths(now);
    const auto alternatives = candidates.find(destination);
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
            {
                path << " -> " << node;
            }
            PROXY_DEBUG_PRINT("Route #%zu cost=%.3f service=%s/%s: %s%s", index + 1, candidate.cost,
                              service.service.c_str(), relay_protocol_name(service.protocol).data(),
                              path.str().c_str(), index == 0 ? " *" : "");
        }
    }

    auto best = alternatives == candidates.end() || alternatives->second.empty()
                    ? std::vector<std::string>{} : alternatives->second.front().nodes;
    path_cache_.put(destination, alternatives == candidates.end() ? std::vector<RouteGraph::Path>{}
                                                                  : std::move(alternatives->second));
    return best;
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

    for (auto &[id, wait] : entry_waits_)
    {
        if (wait->node == connection.node_id())
        {
            wait->changed.cancel();
        }
    }
}

void RelayAgent::connection_closed(const NodeConnection &connection)
{
    asio::post(transfer_executor_, [forwarder = forwarder_, id = connection.id()]() { forwarder->clear_server(id); });
    for (auto &[id, wait] : entry_waits_)
    {
        if (wait->connection == connection.id())
        {
            wait->reason = "entry control disconnected";
            wait->changed.cancel();
        }
    }
    if (connection.id() == primary_connection_id_)
    {
        invalidate_entries("primary control disconnected");
        routing_.invalidate_snapshot();
        path_cache_.clear();
        service_locations_.for_each([&](const ServiceKey &service, ServiceLocation &location) {
            location = ServiceLocation{};
            service_locations_.set_secondary(service, std::nullopt);
            asio::post(transfer_executor_, [forwarder = forwarder_, service] { forwarder->clear_service(service); });
        });
        update_probe_targets();
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
        if (id != primary_connection_id_ &&
            std::none_of(entry_waits_.begin(), entry_waits_.end(),
                         [&id](const auto &entry) { return entry.second->connection == id; }))
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

    service_locations_.for_each([&](const ServiceKey &service, ServiceLocation &location) {
        if (service_locations_.secondary_key(service) || (location.request && !refresh))
        {
            return;
        }
        location.request = allocate_request_id();
        (*primary)->send(CtrlMessage{CtrlCommand::ServiceLookup,
            njson{{"request_id", location.request}, {"service", service.service}, {"protocol", relay_protocol_name(service.protocol)}}});
    });
}

void RelayAgent::locate_service(const njson &params)
{
    const auto request = config::require_unsigned(params, "request_id");
    ServiceKey service{config::message_service(params), config::message_protocol(params)};
    const auto desired = service_locations_.find_primary(service);
    if (!desired || !request || desired->request != request)
    {
        return;
    }
    auto node = config::required_string(params, "node_id", "service.located");
    auto address = config::required_string(params, "address", "service.located");
    const auto port = config::required_port(params, "port", "service.located");
    const auto previous = service_locations_.secondary_key(service);
    if (previous && (*previous != node || desired->address != address || desired->port != port))
    {
        invalidate_entries("service destination changed", service);
    }
    *desired = ServiceLocation{0, std::move(address), port};
    service_locations_.set_secondary(service, node);
    PROXY_INFO_PRINT("Service located service=%s/%s -> %s@%s:%u", service.service.c_str(),
        relay_protocol_name(service.protocol).data(), node.c_str(), desired->address.c_str(), static_cast<unsigned int>(port));
    update_probe_targets();
    asio::post(transfer_executor_, [forwarder = forwarder_, service = std::move(service), node = std::move(node)]() mutable {
        forwarder->set_service(std::move(service), std::move(node));
    });
}

void RelayAgent::forget_service(const ServiceKey &service, std::string reason)
{
    if (const auto location = service_locations_.find_primary(service))
    {
        *location = ServiceLocation{};
        service_locations_.set_secondary(service, std::nullopt);
        invalidate_entries(std::move(reason), service);
        update_probe_targets();
        asio::post(transfer_executor_, [forwarder = forwarder_, service] { forwarder->clear_service(service); });
    }
}

void RelayAgent::handle_control_message(const NodeConnection &connection, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    const auto command = message.type();
    auto route = connection.route();
    if (command == CtrlCommand::NodeLocated || command == CtrlCommand::NodeError)
    {
        const auto request = config::require_unsigned(params, "request_id", true);
        const auto it = entry_waits_.find(request);
        if (connection.id() != primary_connection_id_ || it == entry_waits_.end() ||
            params.at("node_id") != it->second->node || !it->second->connection.empty() ||
            std::chrono::steady_clock::now() >= it->second->deadline || !it->second->reason.empty())
        {
            return;
        }
        if (command == CtrlCommand::NodeError)
        {
            it->second->reason = "node.lookup: " + config::optional_string(params, "reason", "node unavailable");
        }
        else
        {
            config::required_string(params, "address", "node.located");
            config::required_port(params, "port", "node.located");
            it->second->location = params;
        }
        it->second->changed.cancel();
        return;
    }
    if (command == CtrlCommand::RelayOffer || command == CtrlCommand::RelayOpened ||
        command == CtrlCommand::RelayReady || command == CtrlCommand::RelayClosed || command == CtrlCommand::RelayError)
    {
        if (command == CtrlCommand::RelayError)
        {
            const auto reason = config::optional_string(params, "reason", "relay failed");
            if (reason == "service unavailable" || reason == "service protocol mismatch")
            {
                const ServiceKey service{config::message_service(params), config::message_protocol(params)};
                const auto destination = service_locations_.secondary_key(service);
                auto failed_destination = connection.node_id();
                if (params.contains("path"))
                {
                    const auto path = params.at("path").get<std::vector<std::string>>();
                    failed_destination = path.empty() ? std::string{} : path.back();
                }
                if (destination && *destination == failed_destination)
                {
                    forget_service(service, reason);
                    query_services();
                }
            }
        }
        asio::post(transfer_executor_, [forwarder = forwarder_, route = std::move(route), message = std::move(message)]() mutable {
            forwarder->relay_message(std::move(route), std::move(message));
        });
        return;
    }
    if (command == CtrlCommand::TopologySnapshot)
    {
        if (connection.id() == primary_connection_id_)
        {
            try
            {
                const auto old_epoch = routing_.epoch();
                if (routing_.accept_snapshot(params, AgentRouting::Clock::now()))
                {
                    if (old_epoch != routing_.epoch())
                    {
                        path_cache_.clear();
                        invalidate_entries("topology epoch changed");
                    }
                    discovery_timer_.cancel();
                }
            }
            catch (const std::exception &exception)
            {
                PROXY_ERROR_PRINT("Topology rejected reason=%s", exception.what());
            }
        }
    }
    else if (command == CtrlCommand::ServiceOk)
    {
        const auto protocol = config::message_protocol(params);
        PROXY_INFO_PRINT("Service registered service=%s/%s -> %s", config::message_service(params).c_str(),
                         relay_protocol_name(protocol).data(), connection.node_id().c_str());
    }
    else if (command == CtrlCommand::ServiceLocated && connection.id() == primary_connection_id_)
    {
        locate_service(params);
    }
    else if (command == CtrlCommand::ServiceError && connection.id() == primary_connection_id_)
    {
        const auto reason = config::optional_string(params, "reason", "service unavailable");
        if (params.contains("service") && params.contains("protocol"))
        {
            const ServiceKey service{config::message_service(params), config::message_protocol(params)};
            const auto location = service_locations_.find_primary(service);
            const auto request = config::optional_unsigned(params, "request_id");
            if (location && request && location->request == *request)
            {
                forget_service(service, reason);
            }
        }
        PROXY_DEBUG_PRINT("Service discovery failed reason=%s", reason.c_str());
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
    service_locations_.clear();
    entry_waits_.clear();
    state_ = State::Stopped;
    stopped_waiter_.cancel();
}

RelayAgent::EntryWait::EntryWait(asio::any_io_executor executor, ServiceKey service, std::string node,
                               std::chrono::steady_clock::time_point deadline)
    : changed(executor), service(std::move(service)), node(std::move(node)), deadline(deadline)
{
}

asio::awaitable<RelaySelection> RelayAgent::select_relay(ServiceKey service, std::string destination,
                                                       std::chrono::steady_clock::time_point deadline)
{
    const auto current = service_locations_.secondary_key(service);
    const auto location = service_locations_.find_primary(service);
    if (state_ != State::Running || !current || *current != destination || !location)
    {
        throw std::runtime_error("service unavailable");
    }
    auto path = calculate_service_paths(service, destination);
    const auto entry = path.size() > 1 ? path.front() : destination;
    if (entry_waits_.size() >= 1000)
    {
        throw std::runtime_error("entry lookup capacity reached");
    }
    const auto epoch = routing_.epoch();
    const auto request = allocate_request_id();
    auto wait = std::make_shared<EntryWait>(control_executor_, service, entry, deadline);
    entry_waits_.emplace(request, wait);
    ScopeGuard rollback([this, request] { release_entry(request); });
    if (const auto existing = connections_.find_secondary(entry))
    {
        wait->connection = (*existing)->id();
    }
    else if (entry == destination)
    {
        wait->location = njson{{"address", location->address}, {"port", location->port}};
    }
    else
    {
        const auto primary = connections_.find_primary(primary_connection_id_);
        if (!primary || !(*primary)->ready())
        {
            throw std::runtime_error("node.lookup: primary control unavailable");
        }
        (*primary)->send(CtrlMessage(CtrlCommand::NodeLookup, njson{{"request_id", request}, {"node_id", entry}}));
    }
    for (;;)
    {
        if (state_ != State::Running || !wait->reason.empty())
        {
            throw std::runtime_error(wait->reason.empty() ? "agent stopping" : wait->reason);
        }
        const auto current_destination = service_locations_.secondary_key(service);
        if (routing_.epoch() != epoch || !current_destination || *current_destination != destination)
        {
            throw std::runtime_error("route epoch or service destination changed");
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            if (entry == destination)
            {
                forget_service(service, "service control connection timed out");
                query_services();
            }
            throw std::runtime_error("entry control connection timed out");
        }
        if (!wait->location.empty() && wait->connection.empty())
        {
            auto connection = ensure_connection(entry, config::required_string(wait->location, "address", "node.located"),
                                                 config::required_port(wait->location, "port", "node.located"));
            wait->connection = connection->id();
        }
        if (!wait->connection.empty())
        {
            if (const auto connection = connections_.find_primary(wait->connection); connection && (*connection)->ready())
            {
                rollback.dismiss();
                co_return RelaySelection{(*connection)->route(), std::move(path), epoch, request};
            }
        }
        wait->changed.expires_at(deadline);
        const auto [error] = co_await wait->changed.async_wait(use_nothrow_awaitable);
        if (error && error != asio::error::operation_aborted)
        {
            throw asio::system_error(error);
        }
        if ((co_await asio::this_coro::cancellation_state).cancelled() != asio::cancellation_type::none)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
    }
}

void RelayAgent::release_entry(std::uint64_t lease)
{
    entry_waits_.erase(lease);
    release_unused_connections();
}

void RelayAgent::invalidate_entries(std::string reason, const std::optional<ServiceKey> &service)
{
    for (auto &[id, wait] : entry_waits_)
    {
        if (!service || *service == wait->service)
        {
            wait->reason = reason;
            wait->changed.cancel();
        }
    }
    asio::post(transfer_executor_, [forwarder = forwarder_, reason = std::move(reason), service] {
        forwarder->invalidate_relays(reason, service);
    });
}
