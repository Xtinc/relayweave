#include "relay_node.h"
#include "node_session.h"

using Clock = std::chrono::steady_clock;

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
        relays_done_.expires_at(Clock::time_point::max());
        co_await relays_done_.async_wait(use_nothrow_awaitable);
    }
}
