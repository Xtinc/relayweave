#include "app_common.h"
#include "nodelink_mgr.h"

// Remote callers use the same master FlowRequest as local callers. These messages contain no Relay/Agent state.
asio::awaitable<FlowResult> NodeLinkMgr::request_flow(std::vector<std::string> path, RelayProtocol transport)
{
    if (path.size() < 2 || path.front() != config_.node_id || transport == RelayProtocol::Tls || !topology_.epoch() ||
        remote_flows_.size() >= 1000)
    {
        co_return FlowResult{0, 0, "open", "requires a current ingress and tcp/udp flow"};
    }
    auto id = generate_random_id();
    while (remote_flows_.contains(id) || flow_endpoints_.contains(id))
    {
        id = generate_random_id();
    }
    const auto epoch = topology_.epoch();
    auto request = std::make_shared<RemoteFlow>(control_);
    request->result = {epoch, id, "open", "flow result pending"};
    remote_flows_.emplace(id, request);
    bool accepted = false;
    ScopeGuard cleanup([this, id, epoch, &accepted] {
        remote_flows_.erase(id);
        if (!accepted)
        {
            cluster_.send(std::string(topology_.master_id()),
                          CtrlMessage("flow.close.request", njson{{"epoch", epoch}, {"flow_id", id}}));
        }
    });
    cluster_.send(std::string(topology_.master_id()),
                  CtrlMessage("flow.open", njson{{"epoch", epoch},
                                                 {"flow_id", id},
                                                 {"path", std::move(path)},
                                                 {"transport", relay_protocol_name(transport)}}));
    // Bound the remote reply as well as the existing master link/flow deadlines.
    if (!co_await asio::co_spawn(control_, request->completed.wait(),
                                 asio::cancel_after(std::chrono::seconds(30), asio::use_awaitable)))
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    accepted = static_cast<bool>(request->result);
    co_return request->result;
}

asio::awaitable<void> NodeLinkMgr::reply_flow(std::shared_ptr<FlowRequest> request, std::string target)
{
    try
    {
        co_await establish_flow(request);
    }
    catch (const std::exception &error)
    {
        if (!request->completed)
        {
            finish_flow(request, "open", error.what());
        }
    }
    const auto &result = request->result;
    cluster_.send(std::move(target), CtrlMessage("flow.opened", njson{{"epoch", result.epoch},
                                                                      {"flow_id", result.id},
                                                                      {"stage", result.stage},
                                                                      {"reason", result.reason}}));
}

void NodeLinkMgr::handle_flow_request(CtrlMessage message)
{
    const auto &p = config::message_params(message);
    try
    {
        const auto id = config::require_unsigned(p, "flow_id", true);
        const auto epoch = config::require_unsigned(p, "epoch", true);
        const auto source = config::required_string(p, "source", message.command);
        if (message.command == "flow.opened")
        {
            const auto it = remote_flows_.find(id);
            if (source == topology_.master_id() && it != remote_flows_.end() && it->second->result.epoch == epoch)
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
        if (message.command == "flow.close.request")
        {
            const auto it = flow_requests_.find(id);
            if (it != flow_requests_.end() && it->second->result.epoch == epoch &&
                it->second->params.at("path").front() == source)
            {
                finish_flow(it->second, "close", "closed by caller");
            }
            return;
        }
        const auto path = p.at("path").get<std::vector<std::string>>();
        if (epoch != topology_.epoch() || path.empty() || path.front() != source)
        {
            throw std::runtime_error("invalid flow requester or epoch");
        }
        // Reserve the existing FlowRequest synchronously, before a subsequent cancel can arrive.
        auto request = create_flow(path, parse_relay_protocol(p.at("transport").get<std::string>()), id);
        ++flow_tasks_;
        try
        {
            asio::co_spawn(control_, reply_flow(request, source), [this](std::exception_ptr error) {
                if (error)
                {
                    PROXY_ERROR_PRINT("Remote Flow request failed reason=%s", exception_description(error).c_str());
                }
                --flow_tasks_;
                done_.cancel();
            });
        }
        catch (...)
        {
            --flow_tasks_;
            finish_flow(request, "open", "cannot start flow request");
            throw;
        }
    }
    catch (const std::exception &error)
    {
        if (message.command == "flow.open")
        {
            cluster_.send(p.at("source").get<std::string>(),
                          CtrlMessage("flow.opened", njson{{"epoch", p.at("epoch")},
                                                           {"flow_id", p.at("flow_id")},
                                                           {"stage", "open"},
                                                           {"reason", error.what()}}));
        }
        else
        {
            PROXY_ERROR_PRINT("Invalid remote Flow command=%s reason=%s", message.command.c_str(), error.what());
        }
    }
}

bool NodeLinkMgr::egress_ready(std::uint64_t epoch, std::uint64_t id, const std::string &ingress,
                               RelayProtocol transport) const
{
    const auto it = flow_endpoints_.find(id);
    if (it == flow_endpoints_.end())
    {
        return false;
    }
    const auto &p = it->second;
    const auto &path = p.at("path");
    return p.at("epoch") == epoch && p.value("committed", false) && path.back() == config_.node_id &&
           path.front() == ingress && p.at("transport") == std::string(relay_protocol_name(transport));
}

void NodeLinkMgr::flow_closed(std::uint64_t id, std::string reason)
{
    const auto it = flow_watches_.find(id);
    if (it != flow_watches_.end())
    {
        auto watch = it->second;
        flow_watches_.erase(it);
        watch->reason = reason.empty() ? "flow closed" : std::move(reason);
        watch->closed.notify_all();
    }
}

void NodeLinkMgr::fail_remote_flows(const std::string &reason)
{
    for (auto &[id, request] : remote_flows_)
    {
        request->result.reason = reason;
        request->completed.notify_all();
    }
    while (!flow_watches_.empty())
    {
        flow_closed(flow_watches_.begin()->first, reason);
    }
}
