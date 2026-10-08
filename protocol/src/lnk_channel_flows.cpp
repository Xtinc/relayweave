#include "lnk_channel.h"
#include <chrono>
#include <cstddef>

using lnk::Frame;
using lnk::NodeFlow;
using lnk::NodeLink;

static constexpr std::size_t receive_queue_capacity = 16;
static constexpr std::size_t max_flows = 1000;
static constexpr std::size_t max_buffered_bytes = 8 * 1024 * 1024;
static constexpr std::size_t frame_overhead = 128;
static constexpr auto retired_flow_ttl = std::chrono::seconds(10);

static std::size_t charge(const Frame &frame)
{
    return frame_overhead + frame.payload.allocation_size();
}

static bool accepts(const NodeFlow &flow, LnkFrType kind, bool reverse) noexcept
{
    return (!flow.fin[std::size_t(reverse)] || kind == LnkFrType::Reset) &&
           (flow.transport != RelayProtocol::Udp || kind == LnkFrType::Data);
}

lnk::NodeFlow::NodeFlow(asio::any_io_executor executor, njson values)
    : params(std::move(values)), epoch(params.at("epoch").get<std::uint64_t>()),
      transport(parse_relay_protocol(params.at("transport").get_ref<const std::string &>())),
      prepare_deadline(Clock::now() + std::chrono::milliseconds(params.at("ttl_ms").get<int>())),
      received(executor, receive_queue_capacity)
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
    if (stopping_)
    {
        return;
    }

    // NodeLinkMgr validates control parameters, path membership and conflicting prepares before posting here.
    const auto id = p.at("flow_id").get<std::uint64_t>();
    if (auto it = flows_.find(id); it != flows_.end())
    {
        notify_flow(it->second, CtrlCommand::FlowPrepared, "prepare");
        return;
    }

    auto s = std::make_shared<NodeFlow>(executor_, std::move(p));
    if (flows_.size() + retired_.size() >= max_flows || retired_.contains(id))
    {
        notify_flow(s, CtrlCommand::FlowError, "prepare", "flow capacity or retired identity");
        return;
    }

    const auto path = s->params.at("path").get<std::vector<std::string>>();
    const auto found = std::ranges::find(path, node_id_);
    assert(found != path.end());
    const auto index = std::size_t(found - path.begin());
    const auto links = s->params.at("links").get<std::vector<std::uint64_t>>();
    const auto check = [&](std::uint64_t link_id, const std::string &peer) {
        const auto it = links_.find(link_id);
        return it != links_.end() && it->second->state == NodeLink::State::Ready && it->second->epoch == s->epoch &&
               it->second->params.at("peer") == peer && it->second->transport == s->transport;
    };

    if (index)
    {
        s->previous = links[index - 1];
    }
    if (index + 1 < path.size())
    {
        s->next = links[index];
    }
    // Link state can change between the control-domain validation and this data-domain operation.
    if ((s->previous && !check(s->previous, path[index - 1])) ||
        (s->next && !check(s->next, path[index + 1])))
    {
        notify_flow(s, CtrlCommand::FlowError, "prepare", "flow adjacent link is not Ready or identity differs");
        return;
    }

    flows_.emplace(id, s);
    schedule_monitor(s->prepare_deadline);
    notify_flow(s, CtrlCommand::FlowPrepared, "prepare");
}

void LnkChannel::commit_flow(std::uint64_t id)
{
    auto it = flows_.find(id);
    if (it == flows_.end())
    {
        return;
    }

    auto s = it->second;
    if (s->state == NodeFlow::State::Prepared && Clock::now() >= s->prepare_deadline)
    {
        fail_flow(s, "commit", "flow preparation expired");
        return;
    }

    s->state = NodeFlow::State::Active;
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
    s->state = NodeFlow::State::Closed;
    if (s->reason.empty())
    {
        s->reason = std::move(reason);
    }

    buffered_bytes_ -= s->bytes;
    s->bytes = 0;
    s->received.cancel();
    s->received.close();
    flows_.erase(it);

    retired_[id] = Clock::now() + retired_flow_ttl;
    schedule_monitor(retired_.at(id));
    notify_flow(s, CtrlCommand::FlowClosed, "close", s->reason);
}

void LnkChannel::fail_flow(const std::shared_ptr<NodeFlow> &s, std::string stage, std::string reason)
{
    s->reason = reason;
    notify_flow(s, CtrlCommand::FlowError, std::move(stage), std::move(reason));
    // s may reference the erased map entry; do not access it after close_flow().
    close_flow(s->params.at("flow_id").get<std::uint64_t>());
}

bool LnkChannel::deliver(const std::shared_ptr<NodeFlow> &s, Frame &f)
{
    const auto direction = std::size_t(f.header.reverse);
    const auto kind = f.header.kind;
    const auto output = f.header.reverse ? s->previous : s->next;
    if (output)
    {
        const auto link = links_.find(output);
        // Closing a Link synchronously removes its Flows; no await separates lookup and delivery.
        assert(link != links_.end() && link->second->state == NodeLink::State::Ready);
        // Local injection or the network decoder has already validated the frame format.
        if (!enqueue(link->second, f.header, std::move(f.payload)))
        {
            return false;
        }
    }
    else if (kind != LnkFrType::Reset)
    {
        // RESET closes the flow in the caller and wakes receivers with its reason.
        // Queueing it here would only consume capacity before close_flow cancels the queue.
        const auto bytes = charge(f);
        if (buffered_bytes_ + bytes > max_buffered_bytes)
        {
            return false;
        }

        if (!s->received.try_send(asio::error_code{}, std::move(f)))
        {
            return false;
        }

        s->bytes += bytes;
        buffered_bytes_ += bytes;
    }

    if (kind == LnkFrType::Fin)
    {
        s->fin[direction] = true;
    }

    return true;
}

FlowSendStatus LnkChannel::send_flow(FlowFrame f)
{
    auto it = flows_.find(f.flow_id);
    if (it == flows_.end() || it->second->state != NodeFlow::State::Active || it->second->epoch != f.epoch)
    {
        return FlowSendStatus::Closed;
    }

    auto s = it->second;
    if ((f.reverse ? s->next : s->previous) != 0 || !f.validate() || !accepts(*s, f.kind, f.reverse))
    {
        return FlowSendStatus::Invalid;
    }

    // Public buffers may belong to another executor. Only internal Frames borrow the single-thread pool.
    const std::span<const std::uint8_t> bytes =
        f.kind == LnkFrType::Reset ? std::span(reinterpret_cast<const std::uint8_t *>(f.reason.data()), f.reason.size())
                                   : std::span<const std::uint8_t>(f.payload);
    Frame packet{{f.kind, f.reverse, static_cast<std::uint32_t>(bytes.size()), f.epoch, f.flow_id},
                 PooledBuffer(payload_pool_, bytes)};
    if (!deliver(s, packet))
    {
        fail_flow(s, "send", "flow send capacity exceeded");
        return FlowSendStatus::CapacityExceeded;
    }
    if (f.kind == LnkFrType::Reset)
    {
        fail_flow(s, "reset", f.reason.empty() ? "flow reset" : std::move(f.reason));
    }

    return FlowSendStatus::Queued;
}

void LnkChannel::incoming_flow(const NodeLink &link, Frame f)
{
    auto it = flows_.find(f.header.id);
    if (it == flows_.end())
    {
        return;
    }

    auto s = it->second;
    // The read entry validates Link epoch; prepare requires the Flow and its adjacent Links to share it.
    if (s->state != NodeFlow::State::Active || (f.header.reverse ? s->next : s->previous) != link.id ||
        !accepts(*s, f.header.kind, f.header.reverse))
    {
        return;
    }

    std::string reason;
    const bool reset = f.header.kind == LnkFrType::Reset;
    if (reset && f.payload.size())
    {
        reason.assign(reinterpret_cast<const char *>(f.payload.data()), f.payload.size());
    }

    if (!deliver(s, f))
    {
        fail_flow(s, "forward", "flow receive/send capacity exceeded");
    }
    else if (reset)
    {
        fail_flow(s, "reset", reason.empty() ? "flow reset" : std::move(reason));
    }
}

asio::awaitable<FlowFrame> LnkChannel::receive_flow(std::uint64_t epoch, std::uint64_t id)
{
    // cluster_data_io: retain the pool until the pending receive and its internal Frame are destroyed.
    auto self = shared_from_this();
    auto it = flows_.find(id);
    if (it == flows_.end() || it->second->epoch != epoch || it->second->state != NodeFlow::State::Active)
    {
        throw std::runtime_error("flow is not active");
    }

    auto s = it->second;
    auto [error, frame] = co_await s->received.async_receive(use_nothrow_awaitable);
    if (s->state == NodeFlow::State::Closed || error)
    {
        throw std::runtime_error(s->reason.empty() ? "flow closed" : s->reason);
    }

    const auto used = charge(frame);
    s->bytes -= used;
    buffered_bytes_ -= used;
    FlowFrame result{frame.header.epoch, frame.header.id, frame.header.reverse, frame.header.kind};
    if (frame.payload.size())
    {
        const auto bytes = frame.payload.bytes();
        result.payload.assign(bytes.begin(), bytes.end());
    }

    co_return result;
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
        if (flow->state == NodeFlow::State::Prepared && now >= flow->prepare_deadline)
        {
            fail_flow(flow, "prepare", "flow preparation expired");
        }
    }

    std::erase_if(retired_, [now](const auto &v) { return v.second <= now; });
}
