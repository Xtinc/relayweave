#include "lnk_channel.h"
using namespace std::chrono_literals;

LnkChannel::NodeFlow::NodeFlow(asio::any_io_executor executor, njson values)
    : params(std::move(values)), epoch(params.at("epoch").get<std::uint64_t>()),
      transport(parse_relay_protocol(params.at("transport").get_ref<const std::string &>())),
      prepare_deadline(Clock::now() + std::chrono::milliseconds(params.at("ttl_ms").get<int>())), received(executor, 16)
{
}
void LnkChannel::notify_flow(const std::shared_ptr<NodeFlow> &s, CtrlCommand command, std::string stage,
                             std::string reason)
{
    auto p = s->params;
    p["stage"] = std::move(stage);
    p["reason"] = std::move(reason);
    emit(CtrlMessage(command, std::move(p)));
}
void LnkChannel::prepare_flow(njson p)
{
    const auto id = config::require_unsigned(p, "flow_id", true);
    if (stopping_)
    {
        return;
    }
    if (auto it = flows_.find(id); it != flows_.end())
    {
        if (it->second->params.at("request_id") != p.at("request_id") ||
            it->second->params.at("epoch") != p.at("epoch") || it->second->params.at("path") != p.at("path") ||
            it->second->params.at("links") != p.at("links") || it->second->params.at("transport") != p.at("transport"))
        {
            throw std::invalid_argument("conflicting flow prepare");
        }
        notify_flow(it->second, CtrlCommand::FlowPrepared, "prepare");
        return;
    }
    auto s = std::make_shared<NodeFlow>(executor_, std::move(p));
    try
    {
        if (flows_.size() + retired_.size() >= 1000 || retired_.contains(id))
        {
            throw std::runtime_error("flow capacity or retired identity");
        }
        const auto path = s->params.at("path").get<std::vector<std::string>>();
        auto found = std::ranges::find(path, node_id_);
        if (found == path.end())
        {
            throw std::invalid_argument("node not on flow path");
        }
        const auto index = std::size_t(found - path.begin());
        const auto links = s->params.at("links").get<std::vector<std::uint64_t>>();
        const auto check = [&](std::uint64_t link_id, const std::string &peer) {
            auto it = links_.find(link_id);
            if (it == links_.end() || !it->second->ready || it->second->epoch != s->epoch ||
                it->second->params.at("peer") != peer || it->second->transport != s->transport)
            {
                throw std::runtime_error("flow adjacent link is not Ready or identity differs");
            }
        };
        if (index)
        {
            s->previous = links.at(index - 1);
            check(s->previous, path.at(index - 1));
        }
        if (index + 1 < path.size())
        {
            s->next = links.at(index);
            check(s->next, path.at(index + 1));
        }
        flows_.emplace(id, s);
        notify_flow(s, CtrlCommand::FlowPrepared, "prepare");
    }
    catch (const std::exception &e)
    {
        notify_flow(s, CtrlCommand::FlowError, "prepare", e.what());
    }
}
void LnkChannel::commit_flow(std::uint64_t id)
{
    auto it = flows_.find(id);
    if (it == flows_.end() || stopping_)
    {
        return;
    }
    auto s = it->second;
    if (!s->active && Clock::now() >= s->prepare_deadline)
    {
        fail_flow(s, "commit", "flow preparation expired");
        return;
    }
    s->active = true;
    notify_flow(s, CtrlCommand::FlowCommitted, "commit");
}
void LnkChannel::close_flow(std::uint64_t id, std::string reason)
{
    auto it = flows_.find(id);
    if (it == flows_.end())
    {
        return;
    }
    auto s = it->second;
    s->closed = true;
    if (s->reason.empty())
    {
        s->reason = std::move(reason);
    }
    buffered_bytes_ -= s->bytes;
    s->bytes = 0;
    s->received.cancel();
    s->received.close();
    flows_.erase(it);
    retired_[id] = Clock::now() + 10s;
    notify_flow(s, CtrlCommand::FlowClosed, "close", s->reason);
}
void LnkChannel::fail_flow(std::shared_ptr<NodeFlow> s, std::string stage, std::string reason)
{
    if (s->closed)
    {
        return;
    }
    s->reason = reason;
    notify_flow(s, CtrlCommand::FlowError, std::move(stage), std::move(reason));
    close_flow(s->params.at("flow_id").get<std::uint64_t>());
}
std::size_t LnkChannel::charge(const FlowFrame &f)
{
    return 128 + f.payload.size() + f.reason.size();
}
FlowSendStatus LnkChannel::deliver(const std::shared_ptr<NodeFlow> &s, FlowFrame &f)
{
    const auto direction = std::size_t(f.reverse);
    if (s->fin[direction] && f.kind != LnkFrType::Reset)
    {
        return FlowSendStatus::Invalid;
    }
    if (s->transport == RelayProtocol::Udp && f.kind != LnkFrType::Data)
    {
        return FlowSendStatus::Invalid;
    }
    const auto output = f.reverse ? s->previous : s->next;
    if (output)
    {
        const auto link = links_.find(output);
        if (link == links_.end() || !link->second->ready)
        {
            return FlowSendStatus::Closed;
        }
        const LnkFrameHeader header{
            f.kind, f.reverse,
            static_cast<std::uint32_t>(f.kind == LnkFrType::Reset ? f.reason.size() : f.payload.size()), f.epoch,
            f.flow_id};
        // Local injection or the network decoder has already validated the frame format.
        const bool queued = f.kind == LnkFrType::Reset
                                ? enqueue(link->second, header, BytesBuf(f.reason.begin(), f.reason.end()))
                                : enqueue(link->second, header, std::move(f.payload));
        if (!queued)
        {
            return FlowSendStatus::WouldBlock;
        }
    }
    else
    {
        const auto bytes = charge(f);
        if (buffered_bytes_ + bytes > 8 * 1024 * 1024)
        {
            return FlowSendStatus::WouldBlock;
        }
        FlowFrame received{f.epoch, f.flow_id, f.reverse, f.kind, std::move(f.payload), f.reason};
        if (!s->received.try_send(asio::error_code{}, std::move(received)))
        {
            f.payload = std::move(received.payload);
            return FlowSendStatus::WouldBlock;
        }
        s->bytes += bytes;
        buffered_bytes_ += bytes;
    }
    if (f.kind == LnkFrType::Fin)
    {
        s->fin[direction] = true;
    }
    return FlowSendStatus::Queued;
}
FlowSendResult LnkChannel::send_flow(FlowFrame f)
{
    auto it = flows_.find(f.flow_id);
    if (stopping_ || it == flows_.end() || !it->second->active || it->second->epoch != f.epoch)
    {
        return {FlowSendStatus::Closed, std::move(f)};
    }
    auto s = it->second;
    if ((f.reverse ? s->next : s->previous) != 0)
    {
        return {FlowSendStatus::Invalid, std::move(f)};
    }
    try
    {
        f.validate();
    }
    catch (const std::exception &)
    {
        return {FlowSendStatus::Invalid, std::move(f)};
    }
    const auto status = deliver(s, f);
    if (status == FlowSendStatus::Queued)
    {
        if (f.kind == LnkFrType::Reset)
        {
            fail_flow(s, "reset", f.reason.empty() ? "flow reset" : f.reason);
        }
        return {status, {}};
    }
    if (status == FlowSendStatus::WouldBlock || status == FlowSendStatus::Closed)
    {
        fail_flow(s, "send", status == FlowSendStatus::WouldBlock ? "flow send capacity exceeded" : "flow link closed");
    }
    return {status, std::move(f)};
}
void LnkChannel::incoming_flow(const NodeLink &link, FlowFrame f)
{
    auto it = flows_.find(f.flow_id);
    if (stopping_ || it == flows_.end())
    {
        return;
    }
    auto s = it->second;
    // The transport read entry has already validated NodeLink readiness, epoch and authenticated peer.
    if (!s->active || s->epoch != f.epoch || (f.reverse ? s->next : s->previous) != link.id)
    {
        return;
    }
    const auto status = deliver(s, f);
    if (f.kind == LnkFrType::Reset && status == FlowSendStatus::Queued)
    {
        fail_flow(s, "reset", f.reason.empty() ? "flow reset" : f.reason);
    }
    else if (status == FlowSendStatus::WouldBlock || status == FlowSendStatus::Closed)
    {
        fail_flow(s, "forward",
                  status == FlowSendStatus::WouldBlock ? "flow receive/send capacity exceeded" : "flow link closed");
    }
}
asio::awaitable<FlowFrame> LnkChannel::receive_flow(std::uint64_t epoch, std::uint64_t id)
{
    auto it = flows_.find(id);
    if (it == flows_.end() || it->second->epoch != epoch || !it->second->active)
    {
        throw std::runtime_error("flow is not active");
    }
    auto s = it->second;
    auto [error, frame] = co_await s->received.async_receive(use_nothrow_awaitable);
    if (s->closed || error)
    {
        throw std::runtime_error(s->reason.empty() ? "flow closed" : s->reason);
    }
    s->bytes -= charge(frame);
    buffered_bytes_ -= charge(frame);
    co_return frame;
}
void LnkChannel::link_flows_closed(std::uint64_t id)
{
    for (auto it = flows_.begin(); it != flows_.end();)
    {
        auto flow = (it++)->second;
        if (flow->previous == id || flow->next == id)
        {
            fail_flow(flow, "link", "node link closed");
        }
    }
}
void LnkChannel::invalidate_flows(std::string reason)
{
    while (!flows_.empty())
    {
        fail_flow(flows_.begin()->second, "control", reason);
    }
}

void LnkChannel::expire_preparations(Clock::time_point now)
{
    for (auto it = flows_.begin(); it != flows_.end();)
    {
        auto flow = (it++)->second;
        if (!flow->active && now >= flow->prepare_deadline)
        {
            fail_flow(flow, "prepare", "flow preparation expired");
        }
    }
    std::erase_if(retired_, [now](const auto &v) { return v.second <= now; });
}
