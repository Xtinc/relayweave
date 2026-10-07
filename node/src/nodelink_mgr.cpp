#include "nodelink_mgr.h"
#include "app_common.h"
#include <algorithm>
#include <iterator>
#include <limits>
#include <utility>
using namespace std::chrono_literals;

namespace
{
static_assert(NodeLinkMgr::max_flow_nodes <= std::numeric_limits<std::uint8_t>::digits);

bool valid_flow_path(const std::vector<std::string> &path)
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
}

NodeLinkMgr::NodeLinkMgr(asio::any_io_executor control, asio::any_io_executor data, ClusterConfig config,
                         std::string tcp_address, std::string udp_address, ClusterMgr &cluster, Topology &topology)
    : control_(control), config_(std::move(config)), cluster_(cluster), topology_(topology),
      channel_(std::make_shared<LnkChannel>(data, config_.node_id, std::move(tcp_address), config_.tcp_port,
                                            std::move(udp_address), config_.udp_port)),
      done_(control)
{
    done_.expires_at(Clock::time_point::max());
}
void NodeLinkMgr::start()
{
    channel_->start();
}
void NodeLinkMgr::activate()
{
    // Startup publishes running_ before RelayNode's atomic Running state.
    running_ = true;
    channel_->activate();
    asio::post(control_, [this] {
        if (!running_)
        {
            return;
        }
        events_running_ = true;
        asio::co_spawn(control_, receive_events(), [this](std::exception_ptr error) {
            if (error)
            {
                PROXY_ERROR_PRINT("Node data notifications: %s", exception_description(error).c_str());
            }
            events_running_ = false;
            done_.cancel();
        });
    });
}
void NodeLinkMgr::rollback() noexcept
{
    running_ = false;
    channel_->rollback();
}
void NodeLinkMgr::handle(CtrlMessage message)
{
    if (!running_)
    {
        return;
    }
    if (message.command.starts_with("link."))
    {
        handle_link(std::move(message));
    }
    else if (message.command.starts_with("flow."))
    {
        handle_flow(std::move(message));
    }
}
// Runs on control_io; only receiving channel notifications crosses to cluster_data_io.
asio::awaitable<void> NodeLinkMgr::receive_events()
{
    try
    {
        for (;;)
        {
            auto message =
                co_await asio::co_spawn(channel_->executor(), channel_->receive_event(), asio::use_awaitable);
            if (!running_)
            {
                continue;
            }
            const bool link = message.command.starts_with("link.");
            auto &endpoints = link ? link_endpoints_ : flow_endpoints_;
            auto &params = message.params.value();
            const auto id = config::require_unsigned(params, link ? "id" : "flow_id", true);
            auto endpoint = endpoints.find(id);
            if (endpoint == endpoints.end())
            {
                continue;
            }
            const auto type = message.type();
            if (link)
            {
                if (type == CtrlCommand::LinkReady)
                {
                    endpoint->second["ready"] = true;
                }
                params.erase("token");
            }
            cluster_.send(std::string(topology_.master_id()), std::move(message));
            if (type == CtrlCommand::LinkError || type == CtrlCommand::LinkClosed || type == CtrlCommand::FlowError ||
                type == CtrlCommand::FlowClosed)
            {
                endpoints.erase(endpoint);
            }
        }
    }
    catch (const std::exception &e)
    {
        if (!running_)
        {
            co_return;
        }
        PROXY_ERROR_PRINT("Node data notifications failed reason=%s", e.what());
        control_failed(e.what());
    }
    // Notification loss is terminal; do not wait for this receiving task itself.
    co_await shutdown();
}
// Runs on control_io. Stop requests and await data tasks before control is disconnected.
asio::awaitable<void> NodeLinkMgr::shutdown()
{
    running_ = false;
    while (!flow_requests_.empty())
    {
        finish_flow(flow_requests_.begin()->second, "stop", "node stopping");
    }
    while (!closing_flows_.empty())
    {
        released(closing_flows_.begin()->second, "node stopping");
    }
    while (!link_requests_.empty())
    {
        finish_link(link_requests_.begin()->second, "stop", "node stopping");
    }
    flow_endpoints_.clear();
    link_endpoints_.clear();
    co_await asio::co_spawn(channel_->executor(), channel_->stop(), asio::use_awaitable);
}
asio::awaitable<void> NodeLinkMgr::stop()
{
    co_await shutdown();
    while (events_running_)
    {
        co_await done_.async_wait(use_nothrow_awaitable);
    }
}

NodeLinkMgr::StatusQuery::StatusQuery(asio::any_io_executor executor, std::uint64_t link_id)
    : id(generate_random_id()), result{{link_id, "status", {}}}, timeout(executor), completed_event(executor)
{
    timeout.expires_after(5s);
}

NodeLinkMgr::LinkRequest::LinkRequest(asio::any_io_executor executor, njson values)
    : params(std::move(values)), timeout(executor), completed_event(executor)
{
}

NodeLinkMgr::LinkKey NodeLinkMgr::link_key(const njson &p)
{
    return {p.at("left").get<std::string>(), p.at("right").get<std::string>(),
            parse_relay_protocol(p.at("transport").get_ref<const std::string &>())};
}

// Runs on control_io; callers crossing execution domains bind this chain at the RelayNode entry.
asio::awaitable<LinkResult> NodeLinkMgr::ensure_link(std::string left, std::string right, RelayProtocol transport)
{
    if (!running_)
    {
        co_return LinkResult{0, "ensure", "node links stopping or not started"};
    }
    if (!topology_.is_master())
    {
        co_return LinkResult{0, "ensure", "only master can ensure node links"};
    }
    if (left == right || transport == RelayProtocol::Tls)
    {
        co_return LinkResult{0, "ensure", "requires distinct nodes and tcp/udp transport"};
    }
    const auto &members = topology_.members();
    if (!topology_.epoch() || !members.contains(left) || !members.contains(right))
    {
        co_return LinkResult{0, "ensure", "node is offline or membership unavailable"};
    }
    if (right < left)
    {
        std::swap(left, right);
    }
    njson params{{"left", left}, {"right", right}, {"transport", relay_protocol_name(transport)}};
    const LinkKey pair{left, right, transport};
    std::shared_ptr<LinkRequest> request;
    if (auto found = link_requests_.find(pair); found != link_requests_.end())
    {
        request = found->second;
    }
    else
    {
        // IDs and credentials are fresh for every outer attempt, including failed attempts.
        if (link_requests_.size() >= 1000)
        {
            co_return LinkResult{0, "ensure", "node link capacity exceeded"};
        }
        auto id = generate_random_id();
        params["id"] = id;
        params["epoch"] = topology_.epoch();
        params["master"] = std::string(topology_.master_id());
        params["token"] = std::to_string(generate_random_id()) + ":" + std::to_string(generate_random_id());
        params["left_address"] = members.at(left);
        params["right_address"] = members.at(right);
        params["tcp_port"] = config_.tcp_port;
        params["udp_port"] = config_.udp_port;
        request = std::make_shared<LinkRequest>(control_, params);
        link_requests_.emplace(pair, request);
        request->timeout.expires_after(10s);
        request->timeout.async_wait([this, request](asio::error_code error) {
            if (!error)
            {
                finish_link(request, "timeout", "node link establishment exceeded 10 seconds");
            }
        });
        for (auto &node : {left, right})
        {
            auto prepare = params;
            const auto &peer = node == left ? right : left;
            prepare["peer"] = peer;
            prepare["peer_address"] = members.at(peer);
            cluster_.send(node, CtrlMessage(CtrlCommand::LinkPrepare, std::move(prepare)));
        }
    }
    if (request->completed)
    {
        co_return request->result;
    }
    co_await request->completed_event.wait();
    if (!request->completed)
    {
        co_return LinkResult{0, "ensure", "ensure cancelled"};
    }
    co_return request->result;
}

// Runs on control_io.
asio::awaitable<LinkStatus> NodeLinkMgr::link_status(std::uint64_t id)
{
    if (!running_ || !topology_.is_master())
    {
        co_return LinkStatus{{id, "status", "requires a running master"}};
    }
    std::shared_ptr<LinkRequest> request;
    for (const auto &[pair, candidate] : link_requests_)
    {
        if (candidate->params.at("id") == id && candidate->completed && candidate->result)
        {
            request = candidate;
            break;
        }
    }
    if (!request)
    {
        co_return LinkStatus{{id, "status", "node link is not Ready"}};
    }
    if (!request->status)
    {
        request->status = std::make_shared<StatusQuery>(control_, id);
        auto query = request->status;
        query->timeout.async_wait([this, request, query](asio::error_code error) {
            if (!error && request->status == query)
            {
                finish_status(*request, "node link status exceeded 5 seconds");
            }
        });
        auto params = request->params;
        params["request_id"] = request->status->id;
        params["response"] = false;
        cluster_.send(params.at("left"), CtrlMessage(CtrlCommand::LinkStatus, params));
        cluster_.send(params.at("right"), CtrlMessage(CtrlCommand::LinkStatus, params));
    }
    auto query = request->status;
    if (!co_await query->completed_event.wait())
    {
        auto result = query->result;
        result.result.reason = "status query cancelled";
        co_return result;
    }
    co_return query->result;
}

void NodeLinkMgr::finish_status(LinkRequest &request, std::string reason)
{
    if (auto query = std::exchange(request.status, {}))
    {
        query->result.result.reason = std::move(reason);
        query->timeout.cancel();
        query->completed_event.notify_all();
    }
}

void NodeLinkMgr::send_both(const LinkRequest &request, CtrlCommand command)
{
    cluster_.send(request.params.at("left"), CtrlMessage(command, request.params));
    cluster_.send(request.params.at("right"), CtrlMessage(command, request.params));
}

void NodeLinkMgr::finish_link(std::shared_ptr<LinkRequest> request, std::string stage, std::string reason)
{
    if (request->completed && reason.empty())
    {
        return;
    }
    request->completed = true;
    request->result = {request->params.at("id").get<std::uint64_t>(), std::move(stage), std::move(reason)};
    request->timeout.cancel();
    finish_status(*request, request->result.reason);
    if (!request->result)
    {
        send_both(*request, CtrlCommand::LinkClose);
        auto found = link_requests_.find(link_key(request->params));
        if (found != link_requests_.end() && found->second == request)
        {
            link_requests_.erase(found);
        }
    }
    request->completed_event.notify_all();
}

void NodeLinkMgr::handle_link(CtrlMessage message)
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
        const auto type = message.type();
        if (type == CtrlCommand::LinkPrepare || type == CtrlCommand::LinkConnect || type == CtrlCommand::LinkClose ||
            (type == CtrlCommand::LinkStatus && !p.value("response", false)))
        {
            if (source != topology_.master_id())
            {
                return;
            }
            if (type == CtrlCommand::LinkPrepare)
            {
                if (link_endpoints_.contains(id))
                {
                    return;
                }
                const auto left = p.at("left").get<std::string>();
                const auto right = p.at("right").get<std::string>();
                const auto peer = p.at("peer").get<std::string>();
                if (!(left < right) || (config_.node_id != left && config_.node_id != right) ||
                    peer != (config_.node_id == left ? right : left) || !topology_.members().contains(peer) ||
                    p.at("peer_address") != topology_.members().at(peer))
                {
                    return;
                }
                const auto transport = parse_relay_protocol(p.at("transport").get<std::string>());
                if (transport == RelayProtocol::Tls || p.at("token").get<std::string>().empty())
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
                if (link_endpoints_.size() >= 1000)
                {
                    throw std::runtime_error("node link capacity exceeded");
                }
                link_endpoints_[id] = p;
                asio::post(channel_->executor(),
                           [data = channel_, values = p]() mutable { data->prepare(std::move(values)); });
            }
            else
            {
                auto found = link_endpoints_.find(id);
                if (found == link_endpoints_.end())
                {
                    if (type == CtrlCommand::LinkClose)
                    {
                        cluster_.send(source, CtrlMessage(CtrlCommand::LinkClosed, p));
                    }
                    else if (type == CtrlCommand::LinkStatus)
                    {
                        auto status = p;
                        status["response"] = true;
                        status["ready"] = false;
                        status.erase("token");
                        cluster_.send(source, CtrlMessage(CtrlCommand::LinkStatus, std::move(status)));
                    }
                    return;
                }
                const auto &authorized = found->second;
                if (p.at("token") != authorized.at("token") || p.at("left") != authorized.at("left") ||
                    p.at("right") != authorized.at("right") || p.at("transport") != authorized.at("transport"))
                {
                    return;
                }
                if (type == CtrlCommand::LinkConnect)
                {
                    asio::post(channel_->executor(), [data = channel_, id] { data->connect(id); });
                }
                else if (type == CtrlCommand::LinkClose)
                {
                    asio::post(channel_->executor(), [data = channel_, id] { data->close(id); });
                    // The data executor acknowledges after closing the actual socket and queues.
                }
                else
                {
                    auto status = p;
                    status["response"] = true;
                    status["ready"] = authorized.value("ready", false);
                    status.erase("token");
                    cluster_.send(source, CtrlMessage(CtrlCommand::LinkStatus, std::move(status)));
                }
            }
            return;
        }
        if (!topology_.is_master())
        {
            return;
        }
        auto found = link_requests_.find(link_key(p));
        if (found == link_requests_.end())
        {
            return;
        }
        auto request = found->second;
        if (request->params.at("id") != id ||
            (source != p.at("left").get<std::string>() && source != p.at("right").get<std::string>()))
        {
            return;
        }
        const bool from_left = source == p.at("left").get<std::string>();
        const std::uint8_t bit = from_left ? 1 : 2;
        if (type == CtrlCommand::LinkStatus && p.value("response", false))
        {
            auto query = request->status;
            if (!query || p.at("request_id") != query->id)
            {
                return;
            }
            auto &node = from_left ? query->result.llink : query->result.rlink;
            auto &ready = from_left ? query->result.lready : query->result.rready;
            if (node.empty())
            {
                ready = p.at("ready").get<bool>();
                node = source;
            }
            if (query->result.complete())
            {
                finish_status(*request);
            }
        }
        else if (type == CtrlCommand::LinkError || type == CtrlCommand::LinkClosed)
        {
            auto reason = p.value("reason", "");
            finish_link(request, p.value("stage", "remote"), reason.empty() ? "node link closed" : std::move(reason));
        }
        else if (!request->completed && type == CtrlCommand::LinkPrepared)
        {
            if (!(request->prepared & bit))
            {
                request->prepared |= bit;
                if (request->prepared == 3)
                {
                    send_both(*request, CtrlCommand::LinkConnect);
                }
            }
        }
        else if (!request->completed && type == CtrlCommand::LinkReady)
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
    catch (const std::exception &e)
    {
        PROXY_ERROR_PRINT("Invalid node link control: %s", e.what());
    }
}

void NodeLinkMgr::members_changed()
{
    if (!running_)
    {
        return;
    }
    const auto valid_link = [this](const njson &p) {
        return p.at("epoch") == topology_.epoch() && p.at("master").get<std::string>() == topology_.master_id() &&
               topology_.members().contains(p.at("left").get<std::string>()) &&
               topology_.members().contains(p.at("right").get<std::string>()) &&
               p.at("left_address") == topology_.members().at(p.at("left").get<std::string>()) &&
               p.at("right_address") == topology_.members().at(p.at("right").get<std::string>());
    };
    for (auto it = link_endpoints_.begin(); it != link_endpoints_.end();)
    {
        if (valid_link(it->second))
        {
            ++it;
            continue;
        }
        auto id = it->first;
        asio::post(channel_->executor(), [data = channel_, id] { data->close(id); });
        it = link_endpoints_.erase(it);
    }
    for (auto it = link_requests_.begin(); it != link_requests_.end();)
    {
        auto request = (it++)->second;
        if (!valid_link(request->params))
        {
            finish_link(std::move(request), "membership", "node offline or master epoch changed");
        }
    }

    for (auto it = flow_requests_.begin(); it != flow_requests_.end();)
    {
        auto request = (it++)->second;
        if (!valid_flow(request->params))
        {
            finish_flow(std::move(request), "membership", "flow path membership changed");
        }
    }
    for (auto it = flow_endpoints_.begin(); it != flow_endpoints_.end();)
    {
        if (valid_flow(it->second))
        {
            ++it;
            continue;
        }
        const auto id = it->first;
        asio::post(channel_->executor(), [this, id] { channel_->close_flow(id, "flow path membership changed"); });
        it = flow_endpoints_.erase(it);
    }
}

asio::awaitable<void> NodeLinkMgr::close_link(std::uint64_t id)
{
    for (auto &[pair, request] : link_requests_)
    {
        if (request->params.at("id") == id)
        {
            finish_link(request, "close", "closed by caller");
            co_return;
        }
    }
}

NodeLinkMgr::FlowRequest::FlowRequest(asio::any_io_executor executor, njson values)
    : params(std::move(values)), timeout(executor), completed_event(executor), released_event(executor)
{
    result.epoch = params.at("epoch").get<std::uint64_t>();
    result.id = params.at("flow_id").get<std::uint64_t>();
    result.stage = "links";
}
bool NodeLinkMgr::valid_flow(const njson &p) const
{
    if (p.at("epoch") != topology_.epoch() || p.at("master").get<std::string>() != topology_.master_id())
    {
        return false;
    }
    const auto nodes = p.at("path").get<std::vector<std::string>>();
    const auto addresses = p.at("addresses").get<std::vector<std::string>>();
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
void NodeLinkMgr::arm_flow_timeout(const std::shared_ptr<FlowRequest> &r)
{
    r->timeout.expires_after(10s);
    r->timeout.async_wait([this, r](asio::error_code error) {
        if (!error && !r->completed && Clock::now() >= r->timeout.expiry())
        {
            finish_flow(r, r->result.stage, "flow establishment deadline expired");
        }
    });
}
// Runs on control_io; adjacent NodeLink requests below start independent concurrent chains.
asio::awaitable<FlowResult> NodeLinkMgr::open_flow(std::vector<std::string> path, RelayProtocol transport)
{
    if (!running_ || !topology_.is_master())
    {
        co_return FlowResult{0, 0, "open", "requires a running master"};
    }
    if (transport == RelayProtocol::Tls || !valid_flow_path(path))
    {
        co_return FlowResult{0, 0, "open", "requires an acyclic 2..8 Node path and tcp/udp"};
    }
    std::vector<std::string> addresses;
    for (const auto &node : path)
    {
        auto member = topology_.members().find(node);
        if (member == topology_.members().end())
        {
            co_return FlowResult{0, 0, "open", "path node offline"};
        }
        addresses.push_back(member->second);
    }
    if (!topology_.epoch() || flow_requests_.size() + closing_flows_.size() >= 1000)
    {
        co_return FlowResult{0, 0, "open", "membership unavailable or flow capacity exceeded"};
    }
    const auto id = generate_random_id();
    njson p{{"epoch", topology_.epoch()},
            {"master", std::string(topology_.master_id())},
            {"flow_id", id},
            {"request_id", generate_random_id()},
            {"path", path},
            {"addresses", addresses},
            {"transport", relay_protocol_name(transport)},
            {"links", std::vector<std::uint64_t>(path.size() - 1)}};
    auto r = std::make_shared<FlowRequest>(control_, std::move(p));
    flow_requests_.emplace(id, r);
    arm_flow_timeout(r);
    struct Edge
    {
        std::size_t index;
        LinkResult result;
    };
    auto edges = std::make_shared<asio::experimental::channel<void(asio::error_code, Edge)>>(control_, path.size() - 1);
    for (std::size_t i = 0; i + 1 < path.size(); ++i)
    {
        asio::co_spawn(control_, ensure_link(path[i], path[i + 1], transport),
                       [edges, i](std::exception_ptr error, LinkResult result) {
                           if (error)
                           {
                               result = {0, "links", exception_description(error)};
                           }
                           edges->try_send(asio::error_code{}, Edge{i, std::move(result)});
                       });
    }
    try
    {
        for (std::size_t count = 0; count + 1 < path.size(); ++count)
        {
            auto edge = co_await edges->async_receive(asio::use_awaitable);
            if (!edge.result && !r->completed)
            {
                finish_flow(r, "links", edge.result.stage + ": " + edge.result.reason);
            }
            if (!r->completed)
            {
                r->params["links"][edge.index] = edge.result.id;
            }
        }
    }
    catch (...)
    {
        if (!r->completed)
        {
            finish_flow(r, "links", "flow open interrupted");
        }
        throw;
    }
    if (r->completed)
    {
        co_return r->result;
    }
    if (!running_ || !valid_flow(r->params))
    {
        finish_flow(r, "membership", "path membership changed");
        co_return r->result;
    }
    r->result.stage = "prepare";
    arm_flow_timeout(r);
    send_all(*r, CtrlCommand::FlowPrepare);
    if (!r->completed)
    {
        co_await r->completed_event.wait();
        if (!r->completed)
        {
            finish_flow(r, "open", "flow open cancelled");
        }
    }
    co_return r->result;
}
void NodeLinkMgr::send_all(const FlowRequest &r, CtrlCommand command)
{
    auto p = r.params;
    if (command == CtrlCommand::FlowPrepare)
    {
        p["ttl_ms"] = std::uint64_t(std::clamp<std::int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(r.timeout.expiry() - Clock::now()).count(), 1, 10000));
    }
    if (command == CtrlCommand::FlowClose)
    {
        p["stage"] = r.result.stage;
        p["reason"] = r.result.reason;
    }
    for (const auto &node : p.at("path"))
    {
        cluster_.send(node.get<std::string>(), CtrlMessage(command, p));
    }
}
void NodeLinkMgr::finish_flow(std::shared_ptr<FlowRequest> r, std::string stage, std::string reason)
{
    if (r->completed && reason.empty())
    {
        return;
    }
    r->completed = true;
    r->result.stage = std::move(stage);
    r->result.reason = std::move(reason);
    r->timeout.cancel();
    if (!r->result)
    {
        flow_requests_.erase(r->result.id);
        closing_flows_.emplace(r->result.id, r);
        r->timeout.expires_after(10s);
        r->timeout.async_wait([this, r](asio::error_code error) {
            if (!error && !r->released)
            {
                released(r, "flow close acknowledgements exceeded 10 seconds");
            }
        });
        send_all(*r, CtrlCommand::FlowClose);
    }
    r->completed_event.notify_all();
}
void NodeLinkMgr::released(std::shared_ptr<FlowRequest> r, std::string error)
{
    r->released = true;
    r->close_error = std::move(error);
    r->timeout.cancel();
    closing_flows_.erase(r->result.id);
    r->released_event.notify_all();
}

void NodeLinkMgr::handle_flow(CtrlMessage message)
{
    try
    {
        const auto &p = config::message_params(message);
        const auto id = config::require_unsigned(p, "flow_id", true);
        const auto source = p.at("source").get<std::string>();
        if (p.at("epoch") != topology_.epoch() || p.at("master").get<std::string>() != topology_.master_id())
        {
            return;
        }
        const auto type = message.type();
        if (type == CtrlCommand::FlowPrepare || type == CtrlCommand::FlowCommit || type == CtrlCommand::FlowClose)
        {
            if (source != topology_.master_id())
            {
                return;
            }
            const auto reject = [&](std::string reason) {
                auto error = p;
                error["stage"] = "prepare";
                error["reason"] = std::move(reason);
                cluster_.send(source, CtrlMessage(CtrlCommand::FlowError, std::move(error)));
            };
            if (type == CtrlCommand::FlowPrepare)
            {
                const auto path = p.at("path").get<std::vector<std::string>>();
                const auto edge_ids = p.at("links").get<std::vector<std::uint64_t>>();
                const auto ttl = config::require_unsigned(p, "ttl_ms", true);
                config::require_unsigned(p, "request_id", true);
                if (!valid_flow(p) || std::ranges::find(path, config_.node_id) == path.end() || ttl > 10000 ||
                    edge_ids.size() + 1 != path.size() || std::ranges::find(edge_ids, 0) != edge_ids.end() ||
                    parse_relay_protocol(p.at("transport").get<std::string>()) == RelayProtocol::Tls)
                {
                    reject("invalid flow path or identity");
                    return;
                }
                if (auto it = flow_endpoints_.find(id); it != flow_endpoints_.end())
                {
                    if (it->second.at("request_id") != p.at("request_id") || it->second.at("path") != p.at("path") ||
                        it->second.at("links") != p.at("links") || it->second.at("transport") != p.at("transport"))
                    {
                        reject("conflicting flow prepare");
                        return;
                    }
                }
                else
                {
                    if (flow_endpoints_.size() >= 1000)
                    {
                        reject("flow endpoint capacity exceeded");
                        return;
                    }
                    flow_endpoints_.emplace(id, p);
                }
                asio::post(channel_->executor(), [this, values = p]() mutable {
                    try
                    {
                        channel_->prepare_flow(std::move(values));
                    }
                    catch (const std::exception &e)
                    {
                        PROXY_ERROR_PRINT("NodeFlow prepare rejected: %s", e.what());
                    }
                });
            }
            else
            {
                auto it = flow_endpoints_.find(id);
                if (it == flow_endpoints_.end())
                {
                    if (type == CtrlCommand::FlowClose)
                    {
                        cluster_.send(source, CtrlMessage(CtrlCommand::FlowClosed, p));
                    }
                    return;
                }
                for (const auto *field : {"request_id", "path", "links", "transport", "addresses"})
                {
                    if (p.at(field) != it->second.at(field))
                    {
                        return;
                    }
                }
                asio::post(channel_->executor(), [this, id, type, reason = p.value("reason", "")] {
                    if (type == CtrlCommand::FlowCommit)
                    {
                        channel_->commit_flow(id);
                    }
                    else
                    {
                        channel_->close_flow(id, reason);
                    }
                });
            }
            return;
        }
        if (!topology_.is_master())
        {
            return;
        }
        auto it = flow_requests_.find(id);
        auto closing = closing_flows_.find(id);
        if (it == flow_requests_.end() && closing == closing_flows_.end())
        {
            return;
        }
        auto r = it != flow_requests_.end() ? it->second : closing->second;
        if (p.at("request_id") != r->params.at("request_id") || p.at("path") != r->params.at("path") ||
            p.at("links") != r->params.at("links") || p.at("transport") != r->params.at("transport"))
        {
            return;
        }
        const auto path = r->params.at("path").get<std::vector<std::string>>();
        const auto node = std::ranges::find(path, source);
        if (node == path.end() || path.size() > max_flow_nodes)
        {
            return;
        }
        const auto bit = static_cast<std::uint8_t>(1u << std::distance(path.begin(), node));
        const auto all = static_cast<std::uint8_t>((1u << path.size()) - 1);
        if (closing != closing_flows_.end())
        {
            if (type == CtrlCommand::FlowClosed)
            {
                r->closed |= bit;
                if (r->closed == all)
                {
                    released(r);
                }
            }
            return;
        }
        if (type == CtrlCommand::FlowError || type == CtrlCommand::FlowClosed)
        {
            finish_flow(r, p.value("stage", "remote"),
                        p.value("reason", "flow closed").empty() ? "flow closed" : p.value("reason", "remote error"));
        }
        else if (!r->completed && type == CtrlCommand::FlowPrepared && r->result.stage == "prepare")
        {
            r->prepared |= bit;
            if (r->prepared == all)
            {
                r->result.stage = "commit";
                send_all(*r, CtrlCommand::FlowCommit);
            }
        }
        else if (!r->completed && type == CtrlCommand::FlowCommitted && r->result.stage == "commit" &&
                 (r->prepared & bit))
        {
            r->committed |= bit;
            if (r->committed == all)
            {
                finish_flow(r, "ready", {});
            }
        }
    }
    catch (const std::exception &e)
    {
        PROXY_ERROR_PRINT("Invalid flow control: %s", e.what());
    }
}
asio::awaitable<void> NodeLinkMgr::close_flow(std::uint64_t epoch, std::uint64_t id)
{
    std::shared_ptr<FlowRequest> r;
    if (auto it = flow_requests_.find(id); it != flow_requests_.end())
    {
        r = it->second;
    }
    else if (auto it = closing_flows_.find(id); it != closing_flows_.end())
    {
        r = it->second;
    }
    if (!r || r->result.epoch != epoch)
    {
        co_return;
    }
    if (!r->completed || r->result)
    {
        finish_flow(r, "close", "closed by caller");
    }
    if (!r->released)
    {
        co_await r->released_event.wait();
    }
    if (!r->released)
    {
        throw std::runtime_error("flow close cancelled");
    }
    if (!r->close_error.empty())
    {
        throw std::runtime_error(r->close_error);
    }
}

void NodeLinkMgr::control_failed(std::string reason)
{
    if (!running_)
    {
        return;
    }
    while (!flow_requests_.empty())
    {
        finish_flow(flow_requests_.begin()->second, "control", reason);
    }
    while (!closing_flows_.empty())
    {
        released(closing_flows_.begin()->second, reason);
    }
    // Drop authorization before data cleanup; its notifications must not target a lost control connection.
    flow_endpoints_.clear();
    asio::post(channel_->executor(), [data = channel_, reason = std::move(reason)] { data->invalidate_flows(reason); });
}
