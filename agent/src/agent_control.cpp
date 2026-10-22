#include "agent_control.h"
#include "agent_routing.h"
#include "node_connection_mgr.h"
#include "forwarder.h"

AgentControl::AgentControl(asio::any_io_executor control_executor, asio::any_io_executor transfer_executor,
                           const AgentConfig &config, NodeConnectionMgr &connections, AgentRouting &routing,
                           Forwarder &forwarder)
    : control_executor_(std::move(control_executor)), transfer_executor_(std::move(transfer_executor)),
      config_(config), routing_(routing), forwarder_(forwarder), connections_(connections),
      discovery_timer_(control_executor_), discovery_done_(control_executor_)
{
    for (const auto &forward : config_.forwards)
    {
        service_locations_.try_emplace(ServiceKey{forward.service, forward.protocol});
    }
}

void AgentControl::start(std::shared_ptr<RelayAgent> lifetime)
{
    running_ = true;
    if (config_.forwards.empty())
    {
        return;
    }
    update_probe_targets();
    discovery_active_ = true;
    try
    {
        asio::co_spawn(control_executor_, discovery_loop(),
                       [this, keep_alive = std::move(lifetime)](std::exception_ptr failure) {
                           if (failure && running_)
                           {
                               PROXY_ERROR_PRINT("Discovery stopped reason=%s", exception_description(failure).c_str());
                           }
                           discovery_active_ = false;
                           discovery_done_.notify_all();
                       });
    }
    catch (...)
    {
        discovery_active_ = false;
        throw;
    }
}

void AgentControl::stop()
{
    if (!running_)
    {
        return;
    }
    running_ = false;
    discovery_timer_.cancel();
    invalidate_entries("agent stopping");
}

asio::awaitable<void> AgentControl::close()
{
    stop();
    if (discovery_active_ && !co_await discovery_done_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}

void AgentControl::clear()
{
    service_locations_.clear();
}

void AgentControl::connection_finished(const NodeConnection &connection)
{
    // A selection that arrived during idle shutdown waits for task completion,
    // then reconnects at the same endpoint within its original deadline.
    for (auto &[id, wait] : entry_waits_)
    {
        if (wait.connection.get() == &connection)
        {
            wait.connection.reset();
            if (wait.reason.empty())
            {
                wait.location = EntryWait::Location{connection.host(), connection.port()};
                wait.changed.cancel();
            }
        }
    }
}

asio::awaitable<void> AgentControl::discovery_loop()
{
    auto next_poll = AgentRouting::Clock::now();
    while (running_)
    {
        const auto now = AgentRouting::Clock::now();
        if (now >= next_poll)
        {
            connections_.collect_idle_connections(now);
            query_topology();
            query_services(true);
            next_poll = now + std::chrono::seconds(5);
        }

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
        if (!running_)
        {
            break;
        }
        discovery_timer_.expires_at(next_poll);
        const auto [error] = co_await discovery_timer_.async_wait(use_nothrow_awaitable);
        if ((error && error != asio::error::operation_aborted) || !running_)
        {
            break;
        }
    }
}

void AgentControl::query_topology()
{
    const auto primary = connections_.primary();
    if (primary && primary->ready() && !routing_.request_pending(AgentRouting::Clock::now()))
    {
        const auto request = allocate_request_id();
        routing_.begin_request(request, AgentRouting::Clock::now());
        primary->send(CtrlMessage{CtrlCommand::TopologyQuery, njson{{"request_id", request}}});
    }
}

void AgentControl::update_probe_targets()
{
    const auto primary = connections_.primary();
    std::map<std::string, std::string> targets{{primary ? primary->node_id() : std::string{}, config_.host}};
    for (const auto &[service, location] : service_locations_)
    {
        if (!location.node.empty())
        {
            targets.insert_or_assign(location.node, location.address);
        }
    }

    if (routing_.set_required_targets(std::move(targets)))
    {
        discovery_timer_.cancel();
    }
}

void AgentControl::connection_ready(const NodeConnection &connection)
{
    if (connections_.is_primary(connection))
    {
        for (const auto &service : config_.services)
        {
            connection.send(
                CtrlMessage{CtrlCommand::ServiceRegister, njson{{"request_id", allocate_request_id()},
                                                                {"service", service.name},
                                                                {"protocol", relay_protocol_name(service.protocol)}}});
        }

        if (!config_.forwards.empty())
        {
            update_probe_targets();
            query_services();
            query_topology();
        }
    }

    for (auto &[id, wait] : entry_waits_)
    {
        if (wait.node == connection.node_id())
        {
            wait.changed.cancel();
        }
    }
}

void AgentControl::connection_closed(const NodeConnection &connection)
{
    asio::post(transfer_executor_, [forwarder = forwarder_.shared_from_this(), id = connection.id()] {
        forwarder->clear_server(id);
    });
    if (config_.forwards.empty())
    {
        return;
    }

    const bool primary = connections_.is_primary(connection);
    for (auto &[id, wait] : entry_waits_)
    {
        if (primary || wait.connection.get() == &connection)
        {
            wait.reason = primary ? "primary control disconnected" : "entry control disconnected";
            wait.changed.cancel();
        }
    }

    if (primary)
    {
        routing_.invalidate_snapshot();
        discovery_timer_.cancel();
        for (auto &[service, location] : service_locations_)
        {
            location = ServiceLocation{};
            asio::post(transfer_executor_, [forwarder = forwarder_.shared_from_this(), service] {
                forwarder->clear_service(service, "primary control disconnected");
            });
        }
        update_probe_targets();
    }
}

void AgentControl::query_services(bool refresh)
{
    const auto primary = connections_.primary();
    if (!primary || !primary->ready())
    {
        return;
    }

    for (auto &[service, location] : service_locations_)
    {
        if (!location.node.empty() || (location.request && !refresh))
        {
            continue;
        }
        location.request = allocate_request_id();
        primary->send(
            CtrlMessage{CtrlCommand::ServiceLookup, njson{{"request_id", location.request},
                                                          {"service", service.service},
                                                          {"protocol", relay_protocol_name(service.protocol)}}});
    }
}

void AgentControl::locate_service(const njson &params)
{
    const auto request = config::require_unsigned(params, "request_id");
    ServiceKey service{config::message_service(params), config::message_protocol(params)};
    const auto desired = service_locations_.find(service);
    if (desired == service_locations_.end() || !request || desired->second.request != request)
    {
        return;
    }
    auto node = config::required_string(params, "node_id", "service.located");
    auto address = config::required_string(params, "address", "service.located");
    const auto port = config::required_port(params, "port", "service.located");
    desired->second = ServiceLocation{0, node, std::move(address), port};
    PROXY_INFO_PRINT("Service located service=%s/%s -> %s@%s:%u", service.service.c_str(),
                     relay_protocol_name(service.protocol).data(), node.c_str(), desired->second.address.c_str(),
                     static_cast<unsigned int>(port));
    update_probe_targets();
    asio::post(transfer_executor_,
               [forwarder = forwarder_.shared_from_this(), service = std::move(service), node = std::move(node)]() mutable {
                   forwarder->set_service(std::move(service), std::move(node));
               });
}

void AgentControl::forget_service(const ServiceKey &service, std::string reason)
{
    if (const auto location = service_locations_.find(service); location != service_locations_.end())
    {
        location->second = ServiceLocation{};
        invalidate_entries(reason, service);
        update_probe_targets();
        asio::post(transfer_executor_, [forwarder = forwarder_.shared_from_this(), service, reason = std::move(reason)] {
            forwarder->clear_service(service, reason);
        });
    }
}

void AgentControl::handle_control_message(const NodeConnection &connection, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    const auto command = message.type();
    switch (command)
    {
    case CtrlCommand::NodeLocated:
    case CtrlCommand::NodeError: {
        const auto request = config::require_unsigned(params, "request_id", true);
        const auto it = entry_waits_.find(request);
        if (!connections_.is_primary(connection) || it == entry_waits_.end() ||
            params.at("node_id") != it->second.node || it->second.connection ||
            std::chrono::steady_clock::now() >= it->second.deadline || !it->second.reason.empty())
        {
            return;
        }

        if (command == CtrlCommand::NodeError)
        {
            it->second.reason = "node.lookup: " + config::optional_string(params, "reason", "node unavailable");
        }
        else
        {
            it->second.location = EntryWait::Location{config::required_string(params, "address", "node.located"),
                                                       config::required_port(params, "port", "node.located")};
        }
        it->second.changed.cancel();
        return;
    }

    case CtrlCommand::RelayError: {
        const auto reason = config::optional_string(params, "reason", "relay failed");
        if (reason == "service unavailable" || reason == "service protocol mismatch")
        {
            const ServiceKey service{config::message_service(params), config::message_protocol(params)};
            const auto destination = service_locations_.find(service);
            auto failed_destination = connection.node_id();
            if (params.contains("path"))
            {
                const auto path = params.at("path").get<std::vector<std::string>>();
                failed_destination = path.empty() ? std::string{} : path.back();
            }
            if (destination != service_locations_.end() && destination->second.node == failed_destination)
            {
                forget_service(service, reason);
                query_services();
            }
        }
        [[fallthrough]];
    }
    case CtrlCommand::RelayOffer:
    case CtrlCommand::RelayOpened:
    case CtrlCommand::RelayReady:
    case CtrlCommand::RelayClosed: {
        asio::post(transfer_executor_,
                   [forwarder = forwarder_.shared_from_this(), route = connection.route(), message = std::move(message)]() mutable {
                       forwarder->relay_message(std::move(route), std::move(message));
                   });
        return;
    }

    case CtrlCommand::TopologySnapshot: {
        if (!connections_.is_primary(connection))
        {
            return;
        }

        try
        {
            const auto old_epoch = routing_.epoch();
            if (routing_.accept_snapshot(params, AgentRouting::Clock::now()))
            {
                if (old_epoch != routing_.epoch())
                {
                    invalidate_entries("topology epoch changed");
                    asio::post(transfer_executor_,
                               [forwarder = forwarder_.shared_from_this()] {
                                   forwarder->invalidate_relays("topology epoch changed");
                               });
                }
                discovery_timer_.cancel();
            }
        }
        catch (const std::exception &exception)
        {
            PROXY_ERROR_PRINT("Topology rejected reason=%s", exception.what());
        }
        return;
    }

    case CtrlCommand::ServiceOk: {
        const auto protocol = config::message_protocol(params);
        PROXY_INFO_PRINT("Service registered service=%s/%s -> %s", config::message_service(params).c_str(),
                         relay_protocol_name(protocol).data(), connection.node_id().c_str());
        return;
    }

    case CtrlCommand::ServiceLocated:
        if (!connections_.is_primary(connection))
        {
            break;
        }

        locate_service(params);
        return;

    case CtrlCommand::ServiceError: {
        if (!connections_.is_primary(connection))
        {
            break;
        }

        const auto reason = config::optional_string(params, "reason", "service unavailable");
        if (params.contains("service") && params.contains("protocol"))
        {
            const ServiceKey service{config::message_service(params), config::message_protocol(params)};
            const auto location = service_locations_.find(service);
            const auto request = config::optional_unsigned(params, "request_id");
            if (location != service_locations_.end() && request && location->second.request == *request)
            {
                forget_service(service, reason);
            }
        }
        PROXY_DEBUG_PRINT("Service discovery failed reason=%s", reason.c_str());
        return;
    }

    default:
        break;
    }
    PROXY_ERROR_PRINT("Server command rejected command=%s", message.command.c_str());
}

std::uint64_t AgentControl::allocate_request_id()
{
    const auto request_id = next_request_id_++;
    if (next_request_id_ == 0)
    {
        next_request_id_ = 1;
    }
    return request_id;
}

AgentControl::EntryWait::EntryWait(asio::any_io_executor executor, ServiceKey service, std::string node,
                                 std::chrono::steady_clock::time_point deadline)
    : changed(executor), service(std::move(service)), node(std::move(node)), deadline(deadline)
{
}

asio::awaitable<RelaySelection> AgentControl::select_relay(ServiceKey service, std::string destination,
                                                         std::chrono::steady_clock::time_point deadline)
{
    const auto location = service_locations_.find(service);
    if (!running_ || location == service_locations_.end() || location->second.node.empty() ||
        location->second.node != destination)
    {
        throw std::runtime_error("service unavailable");
    }

    auto path = routing_.select_path(service, destination);
    const auto entry = path.size() > 1 ? path.front() : destination;
    if (entry_waits_.size() >= 1000)
    {
        throw std::runtime_error("entry lookup capacity reached");
    }
    if (std::chrono::steady_clock::now() >= deadline)
    {
        throw std::runtime_error("entry control connection timed out");
    }

    const auto epoch = routing_.epoch();
    auto connection = connections_.find_node(entry);
    if (connection && connection->ready())
    {
        co_return RelaySelection{connection->route(), std::move(path), epoch, std::move(connection)};
    }

    const auto request = allocate_request_id();
    auto &wait = entry_waits_.try_emplace(request, control_executor_, service, entry, deadline).first->second;
    ScopeGuard remove_wait([this, request] { entry_waits_.erase(request); });
    wait.connection = std::move(connection);

    if (!wait.connection && entry == destination)
    {
        wait.location = EntryWait::Location{location->second.address, location->second.port};
    }
    else if (!wait.connection)
    {
        const auto primary = connections_.primary();
        if (!primary || !primary->ready())
        {
            throw std::runtime_error("node.lookup: primary control unavailable");
        }
        primary->send(CtrlMessage(CtrlCommand::NodeLookup, njson{{"request_id", request}, {"node_id", entry}}));
    }

    for (;;)
    {
        if (!wait.reason.empty())
        {
            throw std::runtime_error(wait.reason);
        }

        if (routing_.epoch() != epoch)
        {
            throw std::runtime_error("route epoch changed");
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

        if (wait.location && !wait.connection)
        {
            wait.connection = connections_.ensure_connection(entry, wait.location->address, wait.location->port);
        }

        if (wait.connection && wait.connection->ready())
        {
            co_return RelaySelection{wait.connection->route(), std::move(path), epoch, wait.connection};
        }
        wait.changed.expires_at(deadline);
        const auto [error] = co_await wait.changed.async_wait(use_nothrow_awaitable);

        if (error && error != asio::error::operation_aborted)
        {
            throw asio::system_error(error);
        }
    }
}

void AgentControl::invalidate_entries(std::string reason, const std::optional<ServiceKey> &service)
{
    for (auto &[id, wait] : entry_waits_)
    {
        if (!service || *service == wait.service)
        {
            wait.reason = reason;
            wait.changed.cancel();
        }
    }
}
