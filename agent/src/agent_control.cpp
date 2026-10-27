#include "agent_control.h"
#include "agent_routing.h"
#include "forwarder.h"
#include "node_connection.h"
#include <map>
#include <unordered_set>

AgentControl::AgentControl(asio::any_io_executor control_executor, asio::any_io_executor transfer_executor,
                           const AgentConfig &config, NodeConnectionMgr &connections, AgentRouting &routing,
                           Forwarder &forwarder)
    : control_executor_(std::move(control_executor)), transfer_executor_(std::move(transfer_executor)), config_(config),
      routing_(routing), forwarder_(forwarder), connections_(connections), discovery_timer_(control_executor_),
      discovery_done_(control_executor_)
{
    for (const auto &forward : config_.forwards)
    {
        service_locations_.try_emplace(ServiceKey{forward.service, forward.protocol});
    }
}

void AgentControl::start()
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
        asio::co_spawn(control_executor_, discovery_loop(), [this](std::exception_ptr failure) {
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
    running_ = false;
    discovery_timer_.cancel();
}

asio::awaitable<void> AgentControl::close()
{
    if (discovery_active_ && !co_await discovery_done_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
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
        if (error && error != asio::error::operation_aborted)
        {
            break;
        }
    }
}

void AgentControl::query_topology()
{
    const auto primary = connections_.primary();
    const auto now = AgentRouting::Clock::now();
    if (primary && primary->ready() && !routing_.request_pending(now))
    {
        const auto request = allocate_request_id();
        routing_.begin_request(request, now);
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

void AgentControl::connection_closed()
{
    asio::post(transfer_executor_, [&forwarder = forwarder_] { forwarder.control_disconnected(); });
    if (config_.forwards.empty())
    {
        return;
    }

    routing_.invalidate_snapshot();
    discovery_timer_.cancel();
    for (auto &[service, location] : service_locations_)
    {
        location = ServiceLocation{};
    }
    update_probe_targets();
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
    desired->second = ServiceLocation{0, node, std::move(address)};
    PROXY_INFO_PRINT("Service located service=%s/%s -> %s@%s:%u", service.service.c_str(),
                     relay_protocol_name(service.protocol).data(), node.c_str(), desired->second.address.c_str(),
                     static_cast<unsigned int>(port));
    update_probe_targets();
    asio::post(transfer_executor_,
               [&forwarder = forwarder_, service = std::move(service), node = std::move(node)]() mutable {
                   forwarder.set_service(std::move(service), std::move(node));
               });
}

void AgentControl::forget_service(const ServiceKey &service, std::string reason)
{
    service_locations_.at(service) = ServiceLocation{};
    update_probe_targets();
    asio::post(transfer_executor_, [&forwarder = forwarder_, service, reason = std::move(reason)] {
        forwarder.clear_service(service, reason);
    });
}

void AgentControl::handle_control_message(const NodeConnection &connection, CtrlMessage message)
{
    const auto &params = config::message_params(message);
    const auto command = message.type();
    switch (command)
    {
    case CtrlCommand::RelayError: {
        const auto reason = config::optional_string(params, "reason", "relay failed");
        if (reason == "service unavailable" || reason == "service protocol mismatch")
        {
            const ServiceKey service{config::message_service(params), config::message_protocol(params)};
            const auto destination = service_locations_.find(service);
            auto failed_destination = params.at("node_id").get<std::string>();
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
        auto route = connection.route();
        route.node = params.at("node_id").get<std::string>();
        asio::post(transfer_executor_,
                   [&forwarder = forwarder_, route = std::move(route), message = std::move(message)]() mutable {
                       forwarder.relay_message(std::move(route), std::move(message));
                   });
        return;
    }

    case CtrlCommand::TopologySnapshot: {
        try
        {
            const auto old_epoch = routing_.epoch();
            if (routing_.accept_snapshot(params, AgentRouting::Clock::now()))
            {
                if (old_epoch != routing_.epoch())
                {
                    asio::post(transfer_executor_,
                               [&forwarder = forwarder_] { forwarder.invalidate_relays("topology epoch changed"); });
                }
                std::unordered_set<std::string> nodes;
                for (const auto &node : params.at("nodes"))
                {
                    nodes.insert(node.at("node_id").get<std::string>());
                }
                for (const auto &[service, location] : service_locations_)
                {
                    if (!location.node.empty() && !nodes.contains(location.node))
                    {
                        forget_service(service, "service node unavailable");
                    }
                }
                query_services();
                asio::post(transfer_executor_,
                           [&forwarder = forwarder_, nodes = std::move(nodes)] { forwarder.retain_nodes(nodes); });
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
        locate_service(params);
        return;

    case CtrlCommand::ServiceError: {
        const auto reason = config::optional_string(params, "reason", "service unavailable");
        if (params.contains("service") && params.contains("protocol"))
        {
            const ServiceKey service{config::message_service(params), config::message_protocol(params)};
            const auto location = service_locations_.find(service);
            const auto request = config::optional_unsigned(params, "request_id").value_or(0);
            if (location != service_locations_.end() && request && location->second.request == request)
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

asio::awaitable<RelaySelection> AgentControl::select_relay(ServiceKey service, std::string destination,
                                                           std::chrono::steady_clock::time_point deadline)
{
    const auto location = service_locations_.find(service);
    if (!running_ || location == service_locations_.end() || location->second.node != destination ||
        destination.empty())
    {
        throw std::runtime_error("service unavailable");
    }
    if (std::chrono::steady_clock::now() >= deadline)
    {
        throw std::runtime_error("relay establishment timed out");
    }
    const auto primary = connections_.primary();
    if (!primary || !primary->ready())
    {
        throw std::runtime_error("primary control unavailable");
    }
    auto path = routing_.select_path(service, destination);
    auto route = primary->route();
    route.node = path.size() > 1 ? path.front() : destination;
    co_return RelaySelection{std::move(route), std::move(path), routing_.epoch()};
}
