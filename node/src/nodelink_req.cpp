#include "nodelink_mgr.h"

void NodeLinkMgr::handle_flow_request(CtrlCommand command, CtrlMessage message)
{
    const auto &p = config::message_params(message);
    const auto id = config::require_unsigned(p, "flow_id", true);
    const auto epoch = config::require_unsigned(p, "epoch", true);
    const auto source = config::required_string(p, "source", message.command);
    if (command == CtrlCommand::FlowOpened)
    {
        const auto it = pending_opens_.find(id);
        if (source == topology_.master_id() && it != pending_opens_.end() && it->second->result.epoch == epoch)
        {
            it->second->result = {epoch, id, p.at("stage").get<std::string>(), p.at("reason").get<std::string>()};
            it->second->completed.notify_all();
        }
        return;
    }
    if (!topology_.is_master() || !topology_.members().contains(source))
    {
        return;
    }
    if (command == CtrlCommand::FlowCloseRequest)
    {
        const auto it = paths_.find(id);
        if (it != paths_.end() && it->second->result.epoch == epoch &&
            it->second->params.path.front() == source)
        {
            close_path(it->second, "close", "closed by caller");
        }
        return;
    }
    try
    {
        auto path = p.at("path").get<std::vector<std::string>>();
        if (epoch != topology_.epoch() || path.empty() || path.front() != source)
        {
            throw std::runtime_error("invalid flow requester or epoch");
        }
        auto request = create_path(std::move(path), parse_relay_protocol(p.at("transport").get<std::string>()), id);
        request->reply_to = source;
        start_path(request);
    }
    catch (const std::exception &error)
    {
        cluster_.send(source, CtrlMessage(CtrlCommand::FlowOpened, njson{{"epoch", epoch},
                                                                       {"flow_id", id},
                                                                       {"stage", "open"},
                                                                       {"reason", error.what()}}));
    }
}

bool NodeLinkMgr::egress_ready(std::uint64_t epoch, std::uint64_t id, const std::string &ingress,
                               RelayProtocol transport) const
{
    const auto it = local_flows_.find(id);
    if (it == local_flows_.end())
    {
        return false;
    }
    const auto &endpoint = it->second;
    const auto &p = endpoint.params;
    return p.epoch == epoch && endpoint.committed && p.path.back() == config_.node_id &&
           p.path.front() == ingress && p.transport == transport;
}

void NodeLinkMgr::notify_flow_closed(std::uint64_t id, std::string reason)
{
    const auto it = local_flows_.find(id);
    if (it != local_flows_.end() && it->second.closure)
    {
        auto watch = std::move(it->second.closure);
        watch->reason = reason.empty() ? "flow closed" : std::move(reason);
        watch->closed.notify_all();
    }
}

void NodeLinkMgr::fail_waiters(const std::string &reason)
{
    for (auto &[id, request] : pending_opens_)
    {
        request->result.reason = reason;
        request->completed.notify_all();
    }
    for (const auto &[id, flow] : local_flows_)
    {
        notify_flow_closed(id, reason);
    }
}
