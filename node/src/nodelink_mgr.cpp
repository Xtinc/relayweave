#include "nodelink_mgr.h"
#include "node_common.h"
#include <algorithm>
#include <iterator>
#include <limits>
#include <utility>
using namespace std::chrono_literals;

namespace
{
constexpr auto link_setup_timeout = 10s;
constexpr auto flow_setup_timeout = 10s;
constexpr auto flow_close_timeout = 10s;
constexpr auto remote_open_timeout = 30s;
constexpr auto max_flow_ttl_ms = std::chrono::duration_cast<std::chrono::milliseconds>(flow_setup_timeout).count();
} // namespace

static_assert(NodeLinkMgr::max_flow_nodes <= std::numeric_limits<std::uint8_t>::digits);

static bool valid_flow_path(const std::vector<std::string> &path)
{
    if (path.size() < 2 || path.size() > NodeLinkMgr::max_flow_nodes)
    {
        return false;
    }

    for (auto it = path.begin(); it != path.end(); ++it)
    {
        if (std::find(path.begin(), it, *it) != it)
        {
            return false;
        }
    }
    return true;
}

njson NodeLinkMgr::LinkParams::to_params(std::string_view node) const
{
    njson p{{"id", id},
            {"epoch", epoch},
            {"master", master},
            {"left", left},
            {"right", right},
            {"transport", relay_protocol_name(transport)},
            {"token", token},
            {"left_address", left_address},
            {"right_address", right_address},
            {"tcp_port", tcp_port},
            {"udp_port", udp_port}};
    if (!node.empty())
    {
        p["peer"] = node == left ? right : left;
        p["peer_address"] = node == left ? right_address : left_address;
    }
    return p;
}

njson NodeLinkMgr::FlowParams::identity_params() const
{
    return {{"epoch", epoch}, {"master", master}, {"flow_id", id}, {"request_id", request_id}};
}

njson NodeLinkMgr::FlowParams::to_params() const
{
    auto p = identity_params();
    p["path"] = path;
    p["addresses"] = addresses;
    p["links"] = links;
    p["transport"] = relay_protocol_name(transport);
    return p;
}

NodeLinkMgr::NodeLinkMgr(asio::any_io_executor control, asio::any_io_executor data, ClusterConfig config,
                         std::string tcp_address, std::string udp_address, ClusterMgr &cluster, Topology &topology)
    : control_(control), config_(std::move(config)), cluster_(cluster), topology_(topology),
      protocol_(std::make_shared<FlowProtocol>(data, control, config_.node_id, std::move(tcp_address), config_.tcp_port,
                                               std::move(udp_address), config_.udp_port)),
      events_done_(control)
{
    events_done_.expires_at(Clock::time_point::max());
}

void NodeLinkMgr::start()
{
    protocol_->links().start();
}

void NodeLinkMgr::activate()
{
    running_ = true;
    protocol_->activate();
    asio::post(control_, [this] {
        if (!running_)
        {
            return;
        }
        events_running_ = true;
        asio::co_spawn(control_, receive_events(), [this](std::exception_ptr error) {
            if (error)
            {
                stop_ok_ = false;
                PROXY_ERROR_PRINT("Node data notifications: %s", exception_description(error).c_str());
            }
            events_running_ = false;
            events_done_.cancel();
        });
    });
}

void NodeLinkMgr::handle(CtrlCommand command, CtrlMessage message)
{
    if (!running_)
    {
        return;
    }

    switch (ctrl_command_group(command))
    {
    case CtrlCommand::Link:
        handle_link(command, std::move(message));
        break;
    case CtrlCommand::Flow:
        handle_flow(command, std::move(message));
        break;
    default:
        break;
    }
}
asio::awaitable<void> NodeLinkMgr::receive_events()
{
    try
    {
        for (;;)
        {
            auto event = co_await protocol_->receive_event();
            if (!running_)
            {
                continue;
            }
            switch (event.command)
            {
            case CtrlCommand::LinkPrepared:
            case CtrlCommand::LinkReady:
            case CtrlCommand::LinkError:
            case CtrlCommand::LinkClosed: {
                const auto id = event.id;
                const auto endpoint = local_links_.find(id);
                if (endpoint == local_links_.end() || endpoint->second.epoch != event.epoch)
                {
                    continue;
                }
                auto params = endpoint->second.to_params(config_.node_id);
                params["stage"] = std::move(event.stage);
                params["reason"] = std::move(event.reason);
                params.erase("token");
                cluster_.send(std::string(topology_.master_id()), CtrlMessage(event.command, std::move(params)));
                if (event.command == CtrlCommand::LinkError || event.command == CtrlCommand::LinkClosed)
                {
                    local_links_.erase(endpoint);
                }
                break;
            }
            case CtrlCommand::FlowPrepared:
            case CtrlCommand::FlowCommitted:
            case CtrlCommand::FlowError:
            case CtrlCommand::FlowClosed: {
                const auto id = event.id;
                const auto endpoint = local_flows_.find(id);
                if (endpoint == local_flows_.end() || endpoint->second.params.epoch != event.epoch)
                {
                    continue;
                }
                if (event.command == CtrlCommand::FlowCommitted)
                {
                    endpoint->second.committed = true;
                }
                if (event.command == CtrlCommand::FlowError || event.command == CtrlCommand::FlowClosed)
                {
                    notify_flow_closed(id, event.reason);
                }
                auto params = endpoint->second.params.identity_params();
                params["stage"] = std::move(event.stage);
                params["reason"] = std::move(event.reason);
                cluster_.send(std::string(topology_.master_id()), CtrlMessage(event.command, std::move(params)));
                if (event.command == CtrlCommand::FlowError || event.command == CtrlCommand::FlowClosed)
                {
                    local_flows_.erase(endpoint);
                }
                break;
            }
            default:
                break;
            }
        }
    }
    catch (...)
    {
        if (!running_)
        {
            co_return;
        }
        const auto reason = exception_description(std::current_exception());
        PROXY_ERROR_PRINT("Node data notifications failed reason=%s", reason.c_str());
        control_failed(reason);
    }
    co_await shutdown();
}
asio::awaitable<void> NodeLinkMgr::shutdown()
{
    running_ = false;
    fail_waiters("node stopping");
    while (!paths_.empty())
    {
        auto path = paths_.begin()->second;
        close_path(path, "stop", "node stopping");
        release_path(path, "node stopping");
    }
    while (!shared_links_.empty())
    {
        finish_link(shared_links_.begin()->second, "stop", "node stopping");
    }
    local_flows_.clear();
    local_links_.clear();
    const bool data_ok = co_await asio::co_spawn(protocol_->executor(), protocol_->stop(), asio::use_awaitable);
    stop_ok_ = data_ok && stop_ok_;
}

asio::awaitable<bool> NodeLinkMgr::stop()
{
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await shutdown();
    while (events_running_)
    {
        co_await events_done_.async_wait(use_nothrow_awaitable);
    }
    co_return stop_ok_;
}

NodeLinkMgr::SharedLink::SharedLink(asio::any_io_executor executor, LinkParams values)
    : params(std::move(values)), timeout(executor)
{
}

std::shared_ptr<NodeLinkMgr::SharedLink> NodeLinkMgr::start_link(std::string left, std::string right,
                                                                 RelayProtocol transport)
{
    if (!running_)
    {
        throw std::runtime_error("node links stopping or not started");
    }
    if (!topology_.is_master())
    {
        throw std::runtime_error("only master can ensure node links");
    }
    if (right < left)
    {
        std::swap(left, right);
    }

    const LinkKey pair{left, right, transport};
    if (auto found = shared_links_.find(pair); found != shared_links_.end())
    {
        return found->second;
    }
    if (shared_links_.size() >= 1000)
    {
        throw std::runtime_error("node link capacity exceeded");
    }
    const auto &members = topology_.members();
    LinkParams params;
    params.id = generate_random_id();
    params.epoch = topology_.epoch();
    params.master = topology_.master_id();
    params.left = left;
    params.right = right;
    params.transport = transport;
    params.token = std::to_string(generate_random_id()) + ":" + std::to_string(generate_random_id());
    params.left_address = members.at(left);
    params.right_address = members.at(right);
    params.tcp_port = config_.tcp_port;
    params.udp_port = config_.udp_port;
    const auto request = std::make_shared<SharedLink>(control_, std::move(params));
    shared_links_.emplace(pair, request);
    request->timeout.expires_after(link_setup_timeout);
    request->timeout.async_wait([this, request](asio::error_code error) {
        if (!error)
        {
            finish_link(request, "timeout", "node link establishment exceeded 10 seconds");
        }
    });
    for (auto &node : {left, right})
    {
        auto prepare = request->params.to_params(node);
        if (cluster_.send(node, CtrlMessage(CtrlCommand::LinkPrepare, std::move(prepare))) !=
            ClusterMgr::SendResult::Submitted)
        {
            finish_link(request, "control", "cluster control submission failed");
            break;
        }
    }
    return request;
}

bool NodeLinkMgr::send_both(const SharedLink &request, CtrlCommand command)
{
    auto params = request.params.to_params();
    return cluster_.send(request.params.left, CtrlMessage(command, params)) == ClusterMgr::SendResult::Submitted &&
           cluster_.send(request.params.right, CtrlMessage(command, std::move(params))) ==
               ClusterMgr::SendResult::Submitted;
}

void NodeLinkMgr::finish_link(std::shared_ptr<SharedLink> request, std::string stage, std::string reason)
{
    if (request->completed && reason.empty())
    {
        return;
    }
    request->completed = true;
    request->stage = std::move(stage);
    request->reason = std::move(reason);
    request->timeout.cancel();
    if (!request->reason.empty())
    {
        send_both(*request, CtrlCommand::LinkClose);
        auto found = shared_links_.find({request->params.left, request->params.right, request->params.transport});
        if (found != shared_links_.end() && found->second == request)
        {
            shared_links_.erase(found);
        }
    }
    for (const auto &[id, path] : paths_)
    {
        if (path->phase != PathPhase::Linking)
        {
            continue;
        }
        const auto edge = std::ranges::find(path->params.links, request->params.id);
        if (edge == path->params.links.end())
        {
            continue;
        }
        if (!request->reason.empty())
        {
            close_path(path, "links", request->stage + ": " + request->reason);
            continue;
        }
        path->acknowledged |= static_cast<std::uint8_t>(1u << std::distance(path->params.links.begin(), edge));
        prepare_path(path);
    }
}

void NodeLinkMgr::handle_link(CtrlCommand type, CtrlMessage message)
{
    try
    {
        const auto &p = config::message_params(message);
        const auto id = config::require_unsigned(p, "id", true);
        const auto epoch = config::require_unsigned(p, "epoch", true);
        const auto source = p.at("source").get<std::string>();
        if (epoch != topology_.epoch() || p.at("master").get<std::string>() != topology_.master_id())
        {
            return;
        }
        if (type == CtrlCommand::LinkPrepare || type == CtrlCommand::LinkConnect || type == CtrlCommand::LinkClose)
        {
            if (source == topology_.master_id())
            {
                handle_link_command(type, p, id, epoch, source);
            }
        }
        else if (topology_.is_master())
        {
            handle_link_reply(type, p, id, source);
        }
    }
    catch (const std::exception &e)
    {
        PROXY_ERROR_PRINT("Invalid node link control: %s", e.what());
    }
}

void NodeLinkMgr::handle_link_command(CtrlCommand type, const njson &p, std::uint64_t id,
                                      std::uint64_t epoch, const std::string &source)
{
    if (type == CtrlCommand::LinkPrepare)
    {
        if (local_links_.contains(id))
        {
            return;
        }
        auto left = p.at("left").get<std::string>();
        auto right = p.at("right").get<std::string>();
        auto peer = p.at("peer").get<std::string>();
        if (!(left < right) || (config_.node_id != left && config_.node_id != right) ||
            peer != (config_.node_id == left ? right : left) || !topology_.members().contains(peer) ||
            p.at("peer_address") != topology_.members().at(peer))
        {
            return;
        }
        const auto transport = parse_relay_protocol(p.at("transport").get<std::string>());
        auto token = p.at("token").get<std::string>();
        if (transport == RelayProtocol::Tls || token.empty())
        {
            return;
        }
        if (p.at("tcp_port") != config_.tcp_port || p.at("udp_port") != config_.udp_port)
        {
            auto values = p;
            values["stage"] = "prepare";
            values["reason"] = "cluster data ports differ between nodes";
            cluster_.send(source, CtrlMessage(CtrlCommand::LinkError, values));
            return;
        }
        if (local_links_.size() >= 1000)
        {
            throw std::runtime_error("node link capacity exceeded");
        }
        lnk::LinkSpec spec{id,
                           epoch,
                           transport,
                           std::move(peer),
                           p.at("peer_address").get<std::string>(),
                           token,
                           config_.tcp_port,
                           config_.udp_port,
                           config_.node_id == left};
        local_links_.emplace(
            id, LinkParams{id, epoch, std::string(topology_.master_id()), std::move(left), std::move(right),
                           transport, std::move(token), p.at("left_address").get<std::string>(),
                           p.at("right_address").get<std::string>(), config_.tcp_port, config_.udp_port});
        asio::post(protocol_->executor(), [data = protocol_, spec = std::move(spec)]() mutable {
            data->links().prepare(std::move(spec));
        });
    }
    else
    {
        auto found = local_links_.find(id);
        if (found == local_links_.end())
        {
            if (type == CtrlCommand::LinkClose)
            {
                cluster_.send(source, CtrlMessage(CtrlCommand::LinkClosed, p));
            }
            return;
        }
        const auto &authorized = found->second;
        if (p.at("token") != authorized.token || p.at("left") != authorized.left ||
            p.at("right") != authorized.right ||
            p.at("transport") != std::string(relay_protocol_name(authorized.transport)))
        {
            return;
        }
        if (type == CtrlCommand::LinkConnect)
        {
            asio::post(protocol_->executor(), [data = protocol_, id] { data->links().connect(id); });
        }
        else
        {
            asio::post(protocol_->executor(), [data = protocol_, id] { data->links().close(id); });
        }
    }
}

void NodeLinkMgr::handle_link_reply(CtrlCommand type, const njson &p, std::uint64_t id,
                                    const std::string &source)
{
    auto found = shared_links_.find({p.at("left").get<std::string>(), p.at("right").get<std::string>(),
                                     parse_relay_protocol(p.at("transport").get<std::string>())});
    if (found == shared_links_.end())
    {
        return;
    }

    auto request = found->second;
    if (request->params.id != id ||
        (source != request->params.left && source != request->params.right))
    {
        return;
    }

    if (type == CtrlCommand::LinkError || type == CtrlCommand::LinkClosed)
    {
        auto reason = p.value("reason", "");
        finish_link(request, p.value("stage", "remote"), reason.empty() ? "node link closed" : std::move(reason));
        return;
    }
    if (request->completed)
    {
        return;
    }
    const std::uint8_t bit = source == request->params.left ? 1 : 2;
    if (type == CtrlCommand::LinkPrepared)
    {
        if (request->prepared & bit)
        {
            return;
        }
        request->prepared |= bit;
        if (request->prepared == 3 && !send_both(*request, CtrlCommand::LinkConnect))
        {
            finish_link(request, "control", "cluster control submission failed");
        }
    }
    else if (type == CtrlCommand::LinkReady)
    {
        if (!(request->prepared & bit))
        {
            return;
        }
        request->ready |= bit;
        if (request->ready == 3)
        {
            finish_link(request, "ready", {});
        }
    }
}

void NodeLinkMgr::members_changed()
{
    for (auto &[id, request] : pending_opens_)
    {
        if (request->result.epoch != topology_.epoch())
        {
            request->result.reason = "flow epoch changed";
            request->completed.notify_all();
        }
    }

    if (!running_)
    {
        return;
    }
    const auto valid_link = [this](const LinkParams &p) {
        return p.epoch == topology_.epoch() && p.master == topology_.master_id() &&
               topology_.members().contains(p.left) && topology_.members().contains(p.right) &&
               p.left_address == topology_.members().at(p.left) && p.right_address == topology_.members().at(p.right);
    };
    for (auto it = local_links_.begin(); it != local_links_.end();)
    {
        if (valid_link(it->second))
        {
            ++it;
            continue;
        }
        auto id = it->first;
        asio::post(protocol_->executor(), [data = protocol_, id] { data->links().close(id); });
        it = local_links_.erase(it);
    }
    for (const auto &[id, path] : paths_)
    {
        if (!valid_flow(path->params))
        {
            close_path(path, "membership", "flow path membership changed");
        }
    }
    for (auto it = shared_links_.begin(); it != shared_links_.end();)
    {
        auto request = (it++)->second;
        if (!valid_link(request->params))
        {
            finish_link(std::move(request), "membership", "node offline or master epoch changed");
        }
    }

    for (auto it = local_flows_.begin(); it != local_flows_.end();)
    {
        if (valid_flow(it->second.params))
        {
            ++it;
            continue;
        }
        const auto id = it->first;
        notify_flow_closed(id, "flow path membership changed");
        asio::post(protocol_->executor(), [this, id] { protocol_->close_flow(id, "flow path membership changed"); });
        it = local_flows_.erase(it);
    }
}

NodeLinkMgr::MasterPath::MasterPath(asio::any_io_executor executor, FlowParams values)
    : params(std::move(values)), timeout(executor), opened(executor), released(executor)
{
    result.epoch = params.epoch;
    result.id = params.id;
    result.stage = "links";
}

bool NodeLinkMgr::valid_flow(const FlowParams &p) const
{
    if (p.epoch != topology_.epoch() || p.master != topology_.master_id())
    {
        return false;
    }
    const auto &nodes = p.path;
    const auto &addresses = p.addresses;
    if (!valid_flow_path(nodes) || addresses.size() != nodes.size())
    {
        return false;
    }
    for (std::size_t i = 0; i < nodes.size(); ++i)
    {
        auto member = topology_.members().find(nodes[i]);
        if (member == topology_.members().end() || member->second != addresses[i])
        {
            return false;
        }
    }
    return true;
}

void NodeLinkMgr::arm_path_timeout(const std::shared_ptr<MasterPath> &r)
{
    r->timeout.expires_after(flow_setup_timeout);
    r->timeout.async_wait([this, r](asio::error_code error) {
        if (!error && r->phase < PathPhase::Active && Clock::now() >= r->timeout.expiry())
        {
            close_path(r, r->result.stage, "flow establishment deadline expired");
        }
    });
}
asio::awaitable<FlowResult> NodeLinkMgr::open_flow(std::vector<std::string> path, RelayProtocol transport)
{
    if (!running_)
    {
        co_return FlowResult{0, 0, "open", "node links stopping or not started"};
    }
    if (!topology_.is_master())
    {
        if (path.size() < 2 || path.front() != config_.node_id || transport == RelayProtocol::Tls ||
            !topology_.epoch() || pending_opens_.size() >= lnk::max_flow_count)
        {
            co_return FlowResult{0, 0, "open", "requires a current ingress and tcp/udp flow"};
        }
        auto id = generate_random_id();
        while (pending_opens_.contains(id) || local_flows_.contains(id))
        {
            id = generate_random_id();
        }
        const auto epoch = topology_.epoch();
        PendingOpen request(control_);
        request.result = {epoch, id, "open", "flow result pending"};
        const auto submitted =
            cluster_.send(std::string(topology_.master_id()),
                          CtrlMessage(CtrlCommand::FlowOpen, njson{{"epoch", epoch},
                                                                   {"flow_id", id},
                                                                   {"path", std::move(path)},
                                                                   {"transport", relay_protocol_name(transport)}}));
        if (submitted != ClusterMgr::SendResult::Submitted)
        {
            co_return FlowResult{epoch, id, "open", "cluster control submission failed"};
        }
        pending_opens_.emplace(id, &request);
        bool accepted = false;
        ScopeGuard cleanup([this, id, epoch, &accepted] {
            pending_opens_.erase(id);
            if (!accepted)
            {
                cluster_.send(std::string(topology_.master_id()),
                              CtrlMessage(CtrlCommand::FlowCloseRequest, njson{{"epoch", epoch}, {"flow_id", id}}));
            }
        });
        if (!co_await asio::co_spawn(control_, request.completed.wait(),
                                    asio::cancel_after(remote_open_timeout, asio::use_awaitable)))
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        accepted = static_cast<bool>(request.result);
        co_return request.result;
    }
    std::shared_ptr<MasterPath> request;
    try
    {
        request = create_path(std::move(path), transport);
    }
    catch (const std::exception &error)
    {
        co_return FlowResult{0, 0, "open", error.what()};
    }
    try
    {
        start_path(request);
        if (!co_await request->opened.wait())
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
    }
    catch (...)
    {
        close_path(request, request->result.stage, "flow open interrupted");
        throw;
    }
    co_return request->result;
}

std::shared_ptr<NodeLinkMgr::MasterPath> NodeLinkMgr::create_path(std::vector<std::string> path,
                                                                  RelayProtocol transport, std::uint64_t requested_id)
{
    if (transport == RelayProtocol::Tls || !valid_flow_path(path))
    {
        throw std::runtime_error("requires an acyclic 2..8 Node path and tcp/udp");
    }

    std::vector<std::string> addresses;
    for (const auto &node : path)
    {
        auto member = topology_.members().find(node);
        if (member == topology_.members().end())
        {
            throw std::runtime_error("path node offline");
        }
        addresses.push_back(member->second);
    }
    if (!topology_.epoch() || paths_.size() >= lnk::max_flow_count)
    {
        throw std::runtime_error("membership unavailable or flow capacity exceeded");
    }
    const auto id = requested_id ? requested_id : generate_random_id();
    if (paths_.contains(id))
    {
        throw std::runtime_error("flow identity already exists");
    }
    FlowParams params;
    params.epoch = topology_.epoch();
    params.master = topology_.master_id();
    params.id = id;
    params.request_id = generate_random_id();
    params.links.resize(path.size() - 1);
    params.path = std::move(path);
    params.addresses = std::move(addresses);
    params.transport = transport;
    auto r = std::make_shared<MasterPath>(control_, std::move(params));
    paths_.emplace(id, r);
    arm_path_timeout(r);
    return r;
}

void NodeLinkMgr::start_path(const std::shared_ptr<MasterPath> &request)
{
    try
    {
        const auto &path = request->params.path;
        for (std::size_t i = 0; i + 1 < path.size(); ++i)
        {
            const auto link = start_link(path[i], path[i + 1], request->params.transport);
            request->params.links[i] = link->params.id;
            if (!link->completed)
            {
                continue;
            }
            if (!link->reason.empty())
            {
                close_path(request, "links", link->stage + ": " + link->reason);
                return;
            }
            request->acknowledged |= static_cast<std::uint8_t>(1u << i);
        }
    }
    catch (const std::exception &error)
    {
        close_path(request, "links", std::string("ensure: ") + error.what());
        return;
    }
    prepare_path(request);
}

void NodeLinkMgr::prepare_path(const std::shared_ptr<MasterPath> &request)
{
    const auto all = static_cast<std::uint8_t>((1u << request->params.links.size()) - 1);
    if (request->acknowledged != all)
    {
        return;
    }
    request->phase = PathPhase::Preparing;
    request->acknowledged = 0;
    request->result.stage = "prepare";
    arm_path_timeout(request);
    if (!send_all(*request, CtrlCommand::FlowPrepare))
    {
        close_path(request, "control", "cluster control submission failed");
    }
}

void NodeLinkMgr::finish_open(const std::shared_ptr<MasterPath> &request)
{
    request->opened.notify_all();
    if (request->reply_to.empty())
    {
        return;
    }
    auto target = std::exchange(request->reply_to, {});
    const auto &result = request->result;
    cluster_.send(std::move(target), CtrlMessage(CtrlCommand::FlowOpened, njson{{"epoch", result.epoch},
                                                                                {"flow_id", result.id},
                                                                                {"stage", result.stage},
                                                                                {"reason", result.reason}}));
}

bool NodeLinkMgr::send_all(const MasterPath &r, CtrlCommand command)
{
    auto p = command == CtrlCommand::FlowPrepare ? r.params.to_params() : r.params.identity_params();
    if (command == CtrlCommand::FlowPrepare)
    {
        p["ttl_ms"] = std::uint64_t(std::clamp<std::int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(r.timeout.expiry() - Clock::now()).count(), 1,
            max_flow_ttl_ms));
    }
    if (command == CtrlCommand::FlowClose)
    {
        p["stage"] = r.result.stage;
        p["reason"] = r.result.reason;
    }
    for (const auto &node : r.params.path)
    {
        if (cluster_.send(node, CtrlMessage(command, p)) != ClusterMgr::SendResult::Submitted)
        {
            return false;
        }
    }
    return true;
}

void NodeLinkMgr::close_path(std::shared_ptr<MasterPath> r, std::string stage, std::string reason)
{
    if (r->phase >= PathPhase::Closing)
    {
        return;
    }
    r->phase = PathPhase::Closing;
    r->acknowledged = 0;
    r->result.stage = std::move(stage);
    r->result.reason = std::move(reason);
    r->timeout.expires_after(flow_close_timeout);
    r->timeout.async_wait([this, r](asio::error_code error) {
        if (!error && r->phase == PathPhase::Closing)
        {
            release_path(r, "flow close acknowledgements exceeded 10 seconds");
        }
    });
    send_all(*r, CtrlCommand::FlowClose);
    finish_open(r);
}

void NodeLinkMgr::release_path(std::shared_ptr<MasterPath> r, std::string error)
{
    if (r->phase == PathPhase::Released)
    {
        return;
    }
    r->phase = PathPhase::Released;
    r->close_error = std::move(error);
    r->timeout.cancel();
    paths_.erase(r->result.id);
    r->released.notify_all();
}

void NodeLinkMgr::handle_flow(CtrlCommand type, CtrlMessage message)
{
    try
    {
        switch (type)
        {
        case CtrlCommand::FlowOpen:
        case CtrlCommand::FlowOpened:
        case CtrlCommand::FlowCloseRequest:
            handle_flow_request(type, std::move(message));
            return;
        default:
            break;
        }

        const auto &p = config::message_params(message);
        const auto id = config::require_unsigned(p, "flow_id", true);
        const auto epoch = config::require_unsigned(p, "epoch", true);
        const auto source = p.at("source").get<std::string>();
        if (epoch != topology_.epoch() || p.at("master").get<std::string>() != topology_.master_id())
        {
            return;
        }
        if (type == CtrlCommand::FlowPrepare || type == CtrlCommand::FlowCommit || type == CtrlCommand::FlowClose)
        {
            if (source == topology_.master_id())
            {
                handle_flow_command(type, p, id, epoch, source);
            }
        }
        else if (topology_.is_master())
        {
            handle_flow_reply(type, p, id, epoch, source);
        }
    }
    catch (const std::exception &e)
    {
        PROXY_ERROR_PRINT("Invalid flow control: %s", e.what());
    }
}

void NodeLinkMgr::handle_flow_command(CtrlCommand type, const njson &p, std::uint64_t id,
                                      std::uint64_t epoch, const std::string &source)
{
    if (type == CtrlCommand::FlowPrepare)
    {
        FlowParams params{epoch,
                          std::string(topology_.master_id()),
                          id,
                          config::require_unsigned(p, "request_id", true),
                          p.at("path").get<std::vector<std::string>>(),
                          p.at("addresses").get<std::vector<std::string>>(),
                          p.at("links").get<std::vector<std::uint64_t>>(),
                          parse_relay_protocol(p.at("transport").get<std::string>())};
        const auto reject = [&](std::string reason) {
            auto error = params.identity_params();
            error["stage"] = "prepare";
            error["reason"] = std::move(reason);
            cluster_.send(source, CtrlMessage(CtrlCommand::FlowError, std::move(error)));
        };
        const auto &path = params.path;
        const auto &edge_ids = params.links;
        const auto node = std::ranges::find(path, config_.node_id);
        const auto ttl = config::require_unsigned(p, "ttl_ms", true);
        if (!valid_flow(params) || node == path.end() ||
            ttl > std::uint64_t(max_flow_ttl_ms) ||
            edge_ids.size() + 1 != path.size() || std::ranges::find(edge_ids, 0) != edge_ids.end() ||
            params.transport == RelayProtocol::Tls)
        {
            reject("invalid flow path or identity");
            return;
        }
        if (auto it = local_flows_.find(id); it != local_flows_.end())
        {
            if (it->second.params != params)
            {
                reject("conflicting flow prepare");
                return;
            }
        }
        else if (local_flows_.size() >= lnk::max_flow_count)
        {
            reject("flow endpoint capacity exceeded");
            return;
        }
        const auto index = std::size_t(node - path.begin());
        lnk::FlowSpec spec{id, epoch, params.transport};
        spec.prepare_timeout = std::chrono::milliseconds(ttl);
        if (index)
        {
            spec.previous = edge_ids[index - 1];
            spec.previous_peer = path[index - 1];
        }
        if (index + 1 < path.size())
        {
            spec.next = edge_ids[index];
            spec.next_peer = path[index + 1];
        }
        local_flows_.try_emplace(id, LocalFlow{std::move(params)});
        asio::post(protocol_->executor(), [data = protocol_, spec = std::move(spec)]() mutable {
            try
            {
                data->prepare_flow(std::move(spec));
            }
            catch (const std::exception &e)
            {
                PROXY_ERROR_PRINT("NodeFlow prepare rejected: %s", e.what());
            }
        });
    }
    else
    {
        const auto request_id = config::require_unsigned(p, "request_id", true);
        auto it = local_flows_.find(id);
        if (it == local_flows_.end())
        {
            if (type == CtrlCommand::FlowClose)
            {
                cluster_.send(source, CtrlMessage(CtrlCommand::FlowClosed,
                                                  njson{{"master", std::string(topology_.master_id())},
                                                        {"epoch", epoch},
                                                        {"flow_id", id},
                                                        {"request_id", request_id}}));
            }
            return;
        }
        const auto &authorized = it->second.params;
        if (authorized.epoch != epoch || authorized.request_id != request_id)
        {
            return;
        }
        asio::post(protocol_->executor(), [this, id, type, reason = p.value("reason", "")] {
            if (type == CtrlCommand::FlowCommit)
            {
                protocol_->commit_flow(id);
            }
            else
            {
                protocol_->close_flow(id, reason);
            }
        });
    }
}

void NodeLinkMgr::handle_flow_reply(CtrlCommand type, const njson &p, std::uint64_t id,
                                    std::uint64_t epoch, const std::string &source)
{
    auto it = paths_.find(id);
    if (it == paths_.end())
    {
        return;
    }
    auto r = it->second;
    if (r->params.epoch != epoch || config::require_unsigned(p, "request_id", true) != r->params.request_id)
    {
        return;
    }
    const auto &path = r->params.path;
    const auto node = std::ranges::find(path, source);
    if (node == path.end())
    {
        return;
    }
    const auto bit = static_cast<std::uint8_t>(1u << std::distance(path.begin(), node));
    const auto all = static_cast<std::uint8_t>((1u << path.size()) - 1);
    if (r->phase == PathPhase::Closing)
    {
        if (type == CtrlCommand::FlowClosed)
        {
            r->acknowledged |= bit;
            if (r->acknowledged == all)
            {
                release_path(r);
            }
        }
        return;
    }
    if (type == CtrlCommand::FlowError || type == CtrlCommand::FlowClosed)
    {
        auto reason = p.value("reason", "remote error");
        close_path(r, p.value("stage", "remote"), reason.empty() ? "flow closed" : std::move(reason));
    }
    else if (r->phase == PathPhase::Preparing && type == CtrlCommand::FlowPrepared)
    {
        r->acknowledged |= bit;
        if (r->acknowledged == all)
        {
            r->phase = PathPhase::Committing;
            r->acknowledged = 0;
            r->result.stage = "commit";
            if (!send_all(*r, CtrlCommand::FlowCommit))
            {
                close_path(r, "control", "cluster control submission failed");
            }
        }
    }
    else if (r->phase == PathPhase::Committing && type == CtrlCommand::FlowCommitted)
    {
        r->acknowledged |= bit;
        if (r->acknowledged == all)
        {
            r->phase = PathPhase::Active;
            r->result.stage = "ready";
            r->timeout.cancel();
            finish_open(r);
        }
    }
}

asio::awaitable<void> NodeLinkMgr::close_flow(std::uint64_t epoch, std::uint64_t id)
{
    if (!topology_.is_master())
    {
        const auto submitted = cluster_.send(
            std::string(topology_.master_id()),
            CtrlMessage(CtrlCommand::FlowCloseRequest, njson{{"epoch", epoch}, {"flow_id", id}}));
        if (submitted != ClusterMgr::SendResult::Submitted)
        {
            throw std::runtime_error("flow close request submission failed (cluster send status=" +
                                     std::to_string(static_cast<int>(submitted)) + ")");
        }
        co_await asio::co_spawn(control_, wait_flow_closed(epoch, id),
                                asio::cancel_after(flow_close_timeout, asio::use_awaitable));
        co_return;
    }

    const auto it = paths_.find(id);
    if (it == paths_.end() || it->second->params.epoch != epoch)
    {
        co_return;
    }
    auto r = it->second;
    close_path(r, "close", "closed by caller");
    if (!co_await r->released.wait())
    {
        throw std::runtime_error("flow close cancelled");
    }
    if (!r->close_error.empty())
    {
        throw std::runtime_error(r->close_error);
    }
}

asio::awaitable<std::string> NodeLinkMgr::wait_flow_closed(std::uint64_t epoch, std::uint64_t id)
{
    const auto endpoint = local_flows_.find(id);
    if (endpoint == local_flows_.end() || endpoint->second.params.epoch != epoch)
    {
        co_return "flow closed";
    }
    auto watch = endpoint->second.closure;
    if (!watch)
    {
        watch = std::make_shared<CloseNotice>(control_);
        endpoint->second.closure = watch;
    }
    if (!co_await watch->closed.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    co_return watch->reason;
}

void NodeLinkMgr::control_failed(std::string reason)
{
    if (!running_)
    {
        return;
    }
    while (!paths_.empty())
    {
        auto path = paths_.begin()->second;
        close_path(path, "control", reason);
        release_path(path, reason);
    }
    for (auto it = shared_links_.begin(); it != shared_links_.end();)
    {
        auto request = (it++)->second;
        if (!request->completed)
        {
            finish_link(request, "control", reason);
        }
    }
    fail_waiters(reason);
    local_flows_.clear();
    asio::post(protocol_->executor(),
               [data = protocol_, reason = std::move(reason)] { data->invalidate_flows(reason); });
}

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
