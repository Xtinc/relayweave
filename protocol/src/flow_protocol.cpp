#include "flow_protocol.h"
#include <asio/experimental/parallel_group.hpp>
#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>

using lnk::FlowState;
using lnk::Frame;
using lnk::NodeLink;
using lnk::PooledBuffer;

namespace
{
constexpr std::size_t max_flows = 1000;
constexpr std::size_t flow_window_bytes = 512 * 1024;
constexpr std::size_t flow_buffer_budget_bytes = 256 * 1024 * 1024;
constexpr std::size_t datagram_queue_capacity = 16;
constexpr std::size_t frame_overhead = Frame::buffer_overhead;
constexpr auto retired_flow_ttl = std::chrono::seconds(10);

std::size_t flow_reservation(bool stream)
{
    return stream ? 2 * (flow_window_bytes + frame_overhead)
                  : 2 * (datagram_queue_capacity * (65536 + frame_overhead) + frame_overhead);
}

} // namespace

// Endpoint I/O runs on the transfer executor.
lnk::FlowStream::FlowStream(asio::any_io_executor transfer, asio::any_io_executor data,
                            std::weak_ptr<FlowProtocol> channel, std::uint64_t flow_id, bool direction,
                            std::shared_ptr<PayloadPool> pool)
    : id_(flow_id), reverse_(direction), send_queue_(transfer, 0), receive_queue_(transfer, 1),
      receive_completed_(transfer, 1), send_space_(data, 1), protocol_(std::move(channel)), pool_(std::move(pool))
{
}

lnk::PooledBuffer lnk::FlowStream::copy_payload(std::span<const std::uint8_t> data)
{
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (data.size() > maximum_chunk_size)
    {
        throw std::invalid_argument("invalid stream chunk size");
    }
    return PooledBuffer(pool_, data);
}

asio::awaitable<void> lnk::FlowStream::send(PooledBuffer payload)
{
    const auto self = shared_from_this();
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (send_finished_)
    {
        throw std::runtime_error("stream sending has finished");
    }
    if (payload.size() > maximum_chunk_size)
    {
        throw std::invalid_argument("invalid stream chunk size");
    }
    const auto [error] = co_await send_queue_.async_send(
        asio::error_code{}, std::optional<PooledBuffer>(std::move(payload)), use_nothrow_awaitable);
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (error)
    {
        throw asio::system_error(error == asio::experimental::error::channel_cancelled ? asio::error::operation_aborted
                                                                                       : error);
    }
}

asio::awaitable<void> lnk::FlowStream::finish_send()
{
    const auto self = shared_from_this();
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (send_finished_)
    {
        throw std::runtime_error("stream sending has finished");
    }
    const auto [error] = co_await send_queue_.async_send(asio::error_code{}, std::nullopt, use_nothrow_awaitable);
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (error)
    {
        throw asio::system_error(error == asio::experimental::error::channel_cancelled ? asio::error::operation_aborted
                                                                                       : error);
    }
    send_finished_ = true;
}

asio::awaitable<std::optional<lnk::PooledBuffer>> lnk::FlowStream::receive()
{
    const auto self = shared_from_this();
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    auto [error, data] = co_await receive_queue_.async_receive(use_nothrow_awaitable);
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (error)
    {
        throw asio::system_error(error == asio::experimental::error::channel_cancelled ? asio::error::operation_aborted
                                                                                       : error);
    }
    co_return std::move(data);
}

void lnk::FlowStream::complete_receive()
{
    if (!closed_ && !receive_completed_.try_send(asio::error_code{}))
    {
        throw std::runtime_error("duplicate stream completion");
    }
}

void lnk::FlowStream::reset(std::string reset_reason)
{
    reset_reason.resize(std::min(reset_reason.size(), LnkFrameHeader::maximum_reason));
    if (auto channel = protocol_.lock())
    {
        asio::post(channel->executor(), [channel, port = shared_from_this(), reason = std::move(reset_reason)] {
            channel->stream_.reset_port(port, reason);
        });
    }
}

void lnk::FlowStream::close(std::string close_reason)
{
    if (closed_)
    {
        return;
    }
    closed_ = true;
    reason_ = close_reason.empty() ? "flow closed" : std::move(close_reason);
    // Queue Flow closure before cancelling its data tasks. Exact port identity rejects late closes.
    if (auto channel = protocol_.lock())
    {
        asio::post(channel->executor(), [channel, port = shared_from_this(), close_reason = reason_] {
            channel->stream_.close_port(port, close_reason);
        });
    }
    // Sending has a single transfer-executor producer: no new send can interleave reset/close here.
    send_queue_.reset();
    send_queue_.close();
    receive_queue_.reset();
    receive_queue_.close();
    receive_completed_.reset();
    receive_completed_.close();
}

lnk::FlowDatagram::FlowDatagram(asio::any_io_executor transfer, std::weak_ptr<FlowProtocol> channel,
                                std::uint64_t flow_id, bool direction, std::shared_ptr<Queue> queue,
                                std::shared_ptr<PayloadPool> pool)
    : id_(flow_id), reverse_(direction), send_queue_(transfer, datagram_queue_capacity),
      receive_queue_(std::move(queue)), protocol_(std::move(channel)), pool_(std::move(pool))
{
}

bool lnk::FlowDatagram::try_send(std::span<const std::uint8_t> payload)
{
    if (closed_)
    {
        return false;
    }
    if (payload.size() > LnkFrameHeader::maximum_payload)
    {
        throw std::invalid_argument("invalid datagram size");
    }
    try
    {
        if (send_queue_.try_send(asio::error_code{}, PooledBuffer(pool_, payload)))
        {
            return true;
        }
    }
    catch (const std::bad_alloc &)
    {
        // Pool exhaustion drops this packet without interrupting the shared UDP listener.
    }
    if (auto channel = protocol_.lock())
    {
        channel->dropped_datagrams_.fetch_add(1, std::memory_order_relaxed);
    }
    return false;
}

asio::awaitable<lnk::PooledBuffer> lnk::FlowDatagram::receive()
{
    const auto self = shared_from_this();
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    auto [error, frame] = co_await receive_queue_->async_receive(use_nothrow_awaitable);
    if (closed_)
    {
        throw std::runtime_error(reason_);
    }
    if (error)
    {
        throw asio::system_error(error == asio::experimental::error::channel_cancelled ? asio::error::operation_aborted
                                                                                       : error);
    }
    co_return std::move(frame);
}

void lnk::FlowDatagram::close(std::string close_reason)
{
    if (closed_)
    {
        return;
    }
    closed_ = true;
    reason_ = close_reason.empty() ? "flow closed" : std::move(close_reason);
    if (auto channel = protocol_.lock())
    {
        asio::post(channel->executor(), [channel, port = shared_from_this(), close_reason = reason_] {
            channel->datagram_.close_port(port, close_reason);
        });
    }
    send_queue_.reset();
    send_queue_.close();
    receive_queue_->reset();
    receive_queue_->close();
}

// Protocol service lifecycle and all Flow state run on the data executor.
FlowProtocol::FlowProtocol(asio::any_io_executor executor, asio::any_io_executor event_executor, std::string node_id,
                           std::string tcp_address, std::uint16_t tcp_port, std::string udp_address,
                           std::uint16_t udp_port)
    : executor_(executor), payload_pool_(std::make_shared<lnk::PayloadPool>(
          flow_buffer_budget_bytes + max_flows * std::size_t{65536})),
      links_(*this, executor, std::move(node_id), std::move(tcp_address), tcp_port, std::move(udp_address), udp_port),
      stream_(*this), datagram_(*this), events_(event_executor, 4096), monitor_timer_(executor), tasks_done_(executor)
{
    monitor_timer_.expires_at(Clock::time_point::max());
}
void FlowProtocol::emit(lnk::ProtocolEvent message)
{
    if (stopping_ || event_overflow_.load())
    {
        return;
    }
    if (!events_.try_send(asio::error_code{}, std::move(message)))
    {
        event_overflow_.store(true);
        events_.cancel();
        events_.close();
    }
}

asio::awaitable<lnk::ProtocolEvent> FlowProtocol::receive_event()
{
    auto self = shared_from_this();
    auto [error, message] = co_await self->events_.async_receive(use_nothrow_awaitable);
    if (self->event_overflow_.load())
    {
        throw std::runtime_error("Node control event queue capacity exceeded");
    }
    if (error)
    {
        throw asio::system_error(error);
    }
    co_return message;
}

void FlowProtocol::activate()
{
    asio::post(executor_, [self = shared_from_this()] {
        if (self->stopping_ || self->activated_)
        {
            return;
        }
        self->activated_ = true;
        self->spawn(self->run());
    });
}

asio::awaitable<void> FlowProtocol::run()
{
    // Data executor: any shared loop exit cancels its siblings and joins all four.
    auto [order, accept_error, read_error, write_error, monitor_error] =
        co_await asio::experimental::make_parallel_group(
            asio::co_spawn(executor_, links_.accept_loop(), asio::deferred),
            asio::co_spawn(executor_, links_.udp_read(), asio::deferred),
            asio::co_spawn(executor_, links_.udp_write(), asio::deferred),
            asio::co_spawn(executor_, monitor(), asio::deferred))
            .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

    if (stopping_)
    {
        co_return;
    }
    const std::array<std::exception_ptr, 4> errors{accept_error, read_error, write_error, monitor_error};
    if (errors[order[0]])
    {
        std::rethrow_exception(errors[order[0]]);
    }
    throw std::runtime_error("Node data loop exited unexpectedly");
}

void FlowProtocol::spawn(asio::awaitable<void> task)
{
    ++tasks_;
    asio::co_spawn(executor_, std::move(task), [self = shared_from_this()](std::exception_ptr error) {
        if (error && !self->stopping_)
        {
            const auto reason = "Node data task failed: " + exception_description(error);
            PROXY_ERROR_PRINT("%s", reason.c_str());
            self->request_stop(reason);
        }
        if (--self->tasks_ == 0 && self->stopping_)
        {
            self->tasks_done_.notify_all();
        }
    });
}

void FlowProtocol::schedule_monitor(Clock::time_point deadline)
{
    // cluster_data_io: only an earlier deadline needs to interrupt the current wait.
    if (!stopping_ && deadline < monitor_timer_.expiry())
    {
        monitor_timer_.expires_at(deadline);
    }
}

asio::awaitable<void> FlowProtocol::monitor()
{
    while (!stopping_)
    {
        const auto now = Clock::now();
        auto next_deadline = links_.poll(now);
        // A failed Link may have closed Flows and added retirement deadlines above.
        for (auto it = flows_.begin(); it != flows_.end();)
        {
            auto flow = (it++)->second;
            if (flow->phase == FlowState::Phase::Prepared)
            {
                if (now >= flow->prepare_deadline)
                {
                    fail_flow(flow, "prepare", "flow preparation expired");
                    continue;
                }
                next_deadline = std::min(next_deadline, flow->prepare_deadline);
            }
            else if (flow->phase == FlowState::Phase::Active && flow->transport == RelayProtocol::Tcp)
            {
                next_deadline = std::min(next_deadline, stream_.poll(flow, now));
            }
        }
        for (auto it = retired_.begin(); it != retired_.end();)
        {
            if (it->second <= now)
            {
                it = retired_.erase(it);
            }
            else
            {
                next_deadline = std::min(next_deadline, it->second);
                ++it;
            }
        }
        monitor_timer_.expires_at(next_deadline);
        auto [error] = co_await monitor_timer_.async_wait(use_nothrow_awaitable);
        // Deadline changes wake this wait without cancelling the coroutine.
        if (error == asio::error::operation_aborted &&
            (co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::none)
        {
            continue;
        }
        if (error)
        {
            throw asio::system_error(error, "Node link monitor timer failed");
        }
    }
}

void FlowProtocol::request_stop(std::string reason)
{
    if (stopping_)
    {
        return;
    }
    stopping_ = true;
    while (!flows_.empty())
    {
        close_flow(flows_.begin()->first, reason);
    }
    retired_.clear();
    events_.cancel();
    events_.close();
    links_.close_all();
    monitor_timer_.cancel();
    if (tasks_ == 0)
    {
        tasks_done_.notify_all();
    }
}

asio::awaitable<void> FlowProtocol::stop()
{
    // cluster_data_io: draining must finish even if the caller cancels its stop request.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    request_stop("node stopping");
    co_await tasks_done_.wait();
}

lnk::FlowState::Stream::Stream(asio::any_io_executor executor, bool endpoint)
    : receive_queue(executor, endpoint ? flow_window_bytes / frame_overhead + 2 : 0)
{
}

lnk::FlowState::Datagram::Datagram(asio::any_io_executor executor, bool endpoint)
    : receive_queue(endpoint ? std::make_shared<FlowDatagram::Queue>(executor, datagram_queue_capacity) : nullptr)
{
}

lnk::FlowState::FlowState(asio::any_io_executor executor, FlowSpec spec)
    : id(spec.id), epoch(spec.epoch), transport(spec.transport), previous(spec.previous), next(spec.next),
      prepare_deadline(Clock::now() + spec.prepare_timeout),
      datagram(executor, transport == RelayProtocol::Udp && (!previous || !next))
{
    if (transport == RelayProtocol::Tcp)
    {
        // Middle hops only forward; they do not need an endpoint receive buffer.
        stream.emplace(executor, !previous || !next);
    }
}

void FlowProtocol::notify_flow(const std::shared_ptr<FlowState> &flow, lnk::EventKind kind, std::string stage,
                               std::string reason)
{
    emit({kind, flow->epoch, flow->id, std::move(stage), std::move(reason)});
}

void FlowProtocol::prepare_flow(lnk::FlowSpec spec)
{
    if (stopping_)
    {
        return;
    }

    // NodeLinkMgr validates control parameters, path membership and conflicting prepares before posting here.
    const auto id = spec.id;
    if (auto it = flows_.find(id); it != flows_.end())
    {
        notify_flow(it->second, lnk::EventKind::FlowPrepared, "prepare");
        return;
    }

    const auto transport = spec.transport;
    const auto reservation = flow_reservation(transport == RelayProtocol::Tcp);
    auto s = std::make_shared<FlowState>(executor_, spec);
    if (flows_.size() + retired_.size() >= max_flows || retired_.contains(id))
    {
        notify_flow(s, lnk::EventKind::FlowError, "prepare", "flow capacity or retired identity");
        return;
    }

    const auto check = [&](std::uint64_t link_id, const std::string &peer) {
        const auto it = links_.connections_.find(link_id);
        return it != links_.connections_.end() && it->second->phase == NodeLink::Phase::Ready &&
               it->second->spec.epoch == s->epoch && it->second->spec.peer == peer &&
               it->second->spec.transport == transport;
    };
    // Neighbour state can change while the validated descriptor crosses from control to data.
    if ((s->previous && !check(s->previous, spec.previous_peer)) || (s->next && !check(s->next, spec.next_peer)))
    {
        notify_flow(s, lnk::EventKind::FlowError, "prepare", "flow adjacent link is not Ready or identity differs");
        return;
    }

    if (reservation > flow_buffer_budget_bytes - reserved_bytes_)
    {
        notify_flow(s, lnk::EventKind::FlowError, "prepare", "flow buffer reservation budget exceeded");
        return;
    }
    reserved_bytes_ += reservation;

    flows_.emplace(id, s);
    schedule_monitor(s->prepare_deadline);
    notify_flow(s, lnk::EventKind::FlowPrepared, "prepare");
}

void FlowProtocol::commit_flow(std::uint64_t id)
{
    auto it = flows_.find(id);
    if (it == flows_.end())
    {
        return;
    }

    const auto s = it->second;
    if (s->phase == FlowState::Phase::Prepared && Clock::now() >= s->prepare_deadline)
    {
        fail_flow(s, "commit", "flow preparation expired");
        return;
    }

    if (s->phase == FlowState::Phase::Prepared)
    {
        s->phase = FlowState::Phase::Active;
        if (s->transport == RelayProtocol::Tcp)
        {
            for (bool reverse : {false, true})
            {
                stream_.publish_credit(s, reverse);
                if (s->phase == FlowState::Phase::Closed)
                {
                    return;
                }
            }
        }
    }
    notify_flow(s, lnk::EventKind::FlowCommitted, "commit");
}

void FlowProtocol::close_flow(std::uint64_t id, std::string reason)
{
    auto it = flows_.find(id);
    if (it == flows_.end())
    {
        return;
    }

    auto s = it->second;
    s->phase = FlowState::Phase::Closed;
    if (s->reason.empty())
    {
        s->reason = std::move(reason);
    }

    if (s->transport == RelayProtocol::Tcp)
    {
        stream_.close(*s);
    }
    else
    {
        datagram_.close(*s);
    }
    for (const auto link_id : {s->previous, s->next})
    {
        links_.discard(link_id, s);
    }
    flows_.erase(it);
    reserved_bytes_ -= flow_reservation(s->transport == RelayProtocol::Tcp);

    retired_[id] = Clock::now() + retired_flow_ttl;
    schedule_monitor(retired_.at(id));
    notify_flow(s, lnk::EventKind::FlowClosed, "close", s->reason);
}

void FlowProtocol::fail_flow(std::shared_ptr<FlowState> s, std::string stage, std::string reason)
{
    if (s->phase == FlowState::Phase::Closed)
    {
        return;
    }
    s->reason = reason;
    notify_flow(s, lnk::EventKind::FlowError, std::move(stage), std::move(reason));
    // Keep the exact instance alive while removing it from the routing table.
    close_flow(s->id);
}

std::shared_ptr<FlowState> FlowProtocol::require_endpoint_flow(std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                               RelayProtocol transport)
{
    const auto it = flows_.find(id);
    if (it == flows_.end() || it->second->epoch != epoch || it->second->phase != FlowState::Phase::Active)
    {
        throw std::runtime_error("flow is not active");
    }
    const auto &flow = it->second;
    if (flow->transport != transport || (reverse ? flow->next : flow->previous))
    {
        throw std::invalid_argument("invalid or duplicate flow endpoint binding");
    }
    return flow;
}

void FlowProtocol::receive(const NodeLink &link, Frame f)
{
    if (f.header.kind == LnkFrType::Credit)
    {
        stream_.receive_credit(link, f.header);
        return;
    }
    auto it = flows_.find(f.header.id);
    if (it == flows_.end())
    {
        return;
    }

    const auto s = it->second;
    // The read entry validates Link epoch; prepare requires the Flow and its adjacent Links to share it.
    if (s->phase != FlowState::Phase::Active || (f.header.reverse ? s->next : s->previous) != link.spec.id)
    {
        return;
    }
    if (s->transport == RelayProtocol::Tcp)
    {
        stream_.receive(s, std::move(f));
    }
    else
    {
        datagram_.receive(s, std::move(f));
    }
}

void FlowProtocol::written(Frame frame)
{
    const auto used = frame.charge();
    frame.payload = {};
    if (frame.owner && frame.owner->transport == RelayProtocol::Tcp)
    {
        stream_.release_buffer(frame.owner, frame.header.reverse, used, frame.header.kind == LnkFrType::Data);
    }
}

void FlowProtocol::link_flows_closed(std::uint64_t id)
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

void FlowProtocol::invalidate_flows(std::string reason)
{
    while (!flows_.empty())
    {
        fail_flow(flows_.begin()->second, "control", reason);
    }
}

std::shared_ptr<lnk::FlowStream> FlowProtocol::bind_stream_flow(
    std::uint64_t epoch, std::uint64_t id, bool reverse, asio::any_io_executor transfer)
{
    auto flow = require_endpoint_flow(epoch, id, reverse, RelayProtocol::Tcp);
    return stream_.bind(flow, reverse, transfer);
}

std::shared_ptr<lnk::FlowDatagram> FlowProtocol::bind_datagram_flow(
    std::uint64_t epoch, std::uint64_t id, bool reverse, asio::any_io_executor transfer)
{
    auto flow = require_endpoint_flow(epoch, id, reverse, RelayProtocol::Udp);
    return datagram_.bind(flow, reverse, transfer);
}

// TCP protocol: credit, ordered DATA/FIN/RESET, and one joined endpoint task.

std::shared_ptr<lnk::FlowStream> FlowProtocol::Stream::bind(
    const std::shared_ptr<FlowState> &flow, bool reverse, asio::any_io_executor transfer)
{
    auto &stream = *flow->stream;
    if (stream.port)
    {
        throw std::invalid_argument("invalid or duplicate flow endpoint binding");
    }
    auto port =
        std::shared_ptr<lnk::FlowStream>(new lnk::FlowStream(transfer, protocol_.executor_, protocol_.weak_from_this(), flow->id, reverse, protocol_.payload_pool_));
    stream.port = port;
    port->send_finished_ = stream.directions[std::size_t(reverse)].finished;
    protocol_.spawn(run(flow));
    return port;
}

void FlowProtocol::Stream::close(FlowState &flow)
{
    auto &stream = *flow.stream;
    for (auto &direction : stream.directions)
    {
        direction.pending_frames.clear();
        direction.buffered_bytes = 0;
    }
    stream.receive_queue.reset();
    stream.receive_queue.close();
    if (stream.port)
    {
        stream.port->send_space_.close();
        asio::post(stream.port->executor(), [port = stream.port, reason = flow.reason] { port->close(reason); });
    }
}

bool FlowProtocol::Stream::route(const std::shared_ptr<FlowState> &s, Frame &f)
{
    const auto reverse = f.header.reverse;
    const auto kind = f.header.kind;
    const auto output = reverse ? s->previous : s->next;
    if (kind == LnkFrType::Reset)
    {
        // RESET bypasses DATA credit and closes the Flow in the caller.
        return !output || protocol_.links_.enqueue(*protocol_.links_.connections_.at(output), std::move(f));
    }
    const auto used = f.charge();
    auto &direction = s->stream->directions[std::size_t(reverse)];
    if (output)
    {
        direction.pending_frames.push_back(std::move(f));
        direction.buffered_bytes += used;
        flush(s, reverse);
    }
    else
    {
        if (!s->stream->receive_queue.try_send(asio::error_code{}, std::move(f)))
        {
            return false;
        }
        direction.buffered_bytes += used;
    }
    if (kind == LnkFrType::Fin)
    {
        s->stream->directions[std::size_t(reverse)].finished = true;
    }
    return true;
}

bool FlowProtocol::Stream::try_send(const std::shared_ptr<FlowState> &flow, bool reverse,
                                   std::optional<PooledBuffer> &data)
{
    assert(flow->phase == FlowState::Phase::Active && flow->transport == RelayProtocol::Tcp);
    auto &direction = flow->stream->directions[std::size_t(reverse)];
    if (data && lnk::PayloadPool::allocation_size(data->allocation_size()) + frame_overhead >
                    flow_window_bytes - std::min(direction.buffered_bytes, flow_window_bytes))
    {
        return false;
    }
    Frame packet{{data ? LnkFrType::Data : LnkFrType::Fin, reverse, static_cast<std::uint32_t>(data ? data->size() : 0),
                  flow->epoch, flow->id},
                 data ? std::move(*data) : PooledBuffer{}};
    if (!route(flow, packet))
    {
        throw std::runtime_error("flow violated reserved send capacity");
    }
    return true;
}

void FlowProtocol::Stream::reset(const std::shared_ptr<FlowState> &flow, bool reverse, std::string reason)
{
    // RESET has no credit wait and must also work after FIN or with a full DATA window.
    assert(flow->phase == FlowState::Phase::Active && flow->transport == RelayProtocol::Tcp);
    try
    {
        const auto bytes = std::span(reinterpret_cast<const std::uint8_t *>(reason.data()), reason.size());
        Frame packet{{LnkFrType::Reset, reverse, static_cast<std::uint32_t>(bytes.size()), flow->epoch, flow->id},
                     PooledBuffer(protocol_.payload_pool_, bytes)};
        if (!route(flow, packet))
        {
            protocol_.fail_flow(flow, "send", "node link control capacity violated");
            return;
        }
    }
    catch (const std::bad_alloc &)
    {
        // Pool exhaustion must still close this Flow; the control path propagates closure.
    }
    protocol_.fail_flow(flow, "reset", reason.empty() ? "flow reset" : std::move(reason));
}

void FlowProtocol::Stream::receive(const std::shared_ptr<FlowState> &flow, Frame frame)
{
    auto &direction = flow->stream->directions[std::size_t(frame.header.reverse)];
    const bool reset = frame.header.kind == LnkFrType::Reset;
    if (direction.finished && !reset)
    {
        return;
    }
    if (frame.header.kind == LnkFrType::Data)
    {
        const auto used = frame.charge();
        if (used > direction.receive_limit - direction.received_bytes)
        {
            protocol_.fail_flow(flow, "receive", "flow receive credit exceeded");
            return;
        }
        direction.received_bytes += used;
        protocol_.forwarded_bytes_.fetch_add(frame.payload.size(), std::memory_order_relaxed);
    }

    std::string reason;
    if (reset && frame.payload.size())
    {
        reason.assign(reinterpret_cast<const char *>(frame.payload.data()), frame.payload.size());
    }
    if (!route(flow, frame))
    {
        protocol_.fail_flow(flow, "forward", "flow violated reserved receive capacity");
    }
    else if (reset)
    {
        protocol_.fail_flow(flow, "reset", reason.empty() ? "flow reset" : std::move(reason));
    }
}

void FlowProtocol::Stream::flush(const std::shared_ptr<FlowState> &flow, bool reverse)
{
    if (flow->phase != FlowState::Phase::Active)
    {
        return;
    }
    const auto output = reverse ? flow->previous : flow->next;
    if (!output)
    {
        return;
    }
    auto &direction = flow->stream->directions[std::size_t(reverse)];
    const auto &link = protocol_.links_.connections_.at(output);
    while (!direction.pending_frames.empty())
    {
        auto &frame = direction.pending_frames.front();
        const auto used = frame.header.kind == LnkFrType::Data ? frame.charge() : 0;
        if (used > direction.send_limit - direction.sent_bytes)
        {
            return;
        }
        // Admission has already accounted for this buffer. NodeLink adds no second credit wait.
        frame.owner = flow;
        if (!protocol_.links_.enqueue(*link, std::move(frame)))
        {
            protocol_.fail_flow(flow, "send", "node link send capacity violated");
            return;
        }
        direction.sent_bytes += used;
        direction.pending_frames.pop_front();
    }
}

void FlowProtocol::Stream::receive_credit(const NodeLink &link, const LnkFrameHeader &header)
{
    const auto it = protocol_.flows_.find(header.id);
    if (it == protocol_.flows_.end() || link.spec.transport != RelayProtocol::Tcp)
    {
        return;
    }
    const auto &flow = it->second;
    if ((header.reverse ? flow->previous : flow->next) != link.spec.id)
    {
        return;
    }
    auto &direction = flow->stream->directions[std::size_t(header.reverse)];
    if (!direction.send_limit && header.sequence != flow_window_bytes)
    {
        protocol_.fail_flow(flow, "credit", "invalid initial flow credit window");
        return;
    }
    if (header.sequence <= direction.send_limit)
    {
        return;
    }
    if (header.sequence < direction.sent_bytes || header.sequence - direction.sent_bytes > flow_window_bytes)
    {
        protocol_.fail_flow(flow, "credit", "flow credit exceeds peer window");
        return;
    }
    direction.send_limit = header.sequence;
    flush(flow, header.reverse);
}

void FlowProtocol::Stream::publish_credit(const std::shared_ptr<FlowState> &flow, bool reverse)
{
    const auto input = reverse ? flow->next : flow->previous;
    // commit, the credit timer and release_buffer only publish for active TCP Flows.
    assert(flow->phase == FlowState::Phase::Active && flow->transport == RelayProtocol::Tcp);
    if (!input)
    {
        return;
    }
    auto &direction = flow->stream->directions[std::size_t(reverse)];
    // release_buffer checks overflow before advancing released; the initial limit is just window.
    const auto limit = direction.released_bytes + flow_window_bytes;
    const auto &link = protocol_.links_.connections_.at(input);
    // Link closure synchronously closes adjacent Flows before removing the Link.
    direction.receive_limit = limit;
    direction.credit_deadline = Clock::time_point::max();
    if (!protocol_.links_.enqueue(*link, Frame{{LnkFrType::Credit, reverse, 0, flow->epoch, flow->id, limit}}))
    {
        protocol_.fail_flow(flow, "credit", "node link control capacity violated");
    }
}

void FlowProtocol::Stream::release_buffer(const std::shared_ptr<FlowState> &flow, bool reverse, std::size_t bytes,
                                         bool data)
{
    if (flow->phase == FlowState::Phase::Closed)
    {
        return;
    }
    auto &direction = flow->stream->directions[std::size_t(reverse)];
    assert(direction.buffered_bytes >= bytes);
    direction.buffered_bytes -= bytes;
    if (data && (reverse ? flow->next : flow->previous))
    {
        if (bytes > std::numeric_limits<std::uint64_t>::max() - flow_window_bytes - direction.released_bytes)
        {
            protocol_.fail_flow(flow, "credit", "flow released counter overflow");
            return;
        }
        direction.released_bytes += bytes;
        if (direction.released_bytes + flow_window_bytes - direction.receive_limit >= flow_window_bytes / 4 ||
            direction.received_bytes == direction.receive_limit)
        {
            publish_credit(flow, reverse);
        }
        else if (direction.credit_deadline == Clock::time_point::max())
        {
            direction.credit_deadline = Clock::now() + std::chrono::milliseconds(2);
            protocol_.schedule_monitor(direction.credit_deadline);
        }
    }
    const auto &port = flow->stream->port;
    if (port && port->reverse_ == reverse)
    {
        port->send_space_.try_send(asio::error_code{});
    }
}

void FlowProtocol::Stream::close_port(const std::shared_ptr<lnk::FlowStream> &port, std::string reason)
{
    const auto it = protocol_.flows_.find(port->id_);
    if (it != protocol_.flows_.end() && it->second->transport == RelayProtocol::Tcp && it->second->stream->port == port)
    {
        protocol_.close_flow(port->id_, std::move(reason));
    }
}

void FlowProtocol::Stream::reset_port(const std::shared_ptr<lnk::FlowStream> &port, std::string reason)
{
    const auto it = protocol_.flows_.find(port->id_);
    if (it != protocol_.flows_.end() && it->second->transport == RelayProtocol::Tcp && it->second->stream->port == port)
    {
        reset(it->second, port->reverse_, std::move(reason));
    }
}

FlowProtocol::Clock::time_point FlowProtocol::Stream::poll(
    const std::shared_ptr<FlowState> &flow, Clock::time_point now)
{
    auto deadline = Clock::time_point::max();
    for (bool reverse : {false, true})
    {
        auto &direction = flow->stream->directions[std::size_t(reverse)];
        if (now >= direction.credit_deadline)
        {
            publish_credit(flow, reverse);
            if (flow->phase == FlowState::Phase::Closed)
            {
                return Clock::time_point::max();
            }
        }
        deadline = std::min(deadline, direction.credit_deadline);
    }
    return deadline;
}

asio::awaitable<void> FlowProtocol::Stream::run(std::shared_ptr<FlowState> flow)
{
    if (flow->phase == FlowState::Phase::Closed)
    {
        co_return;
    }
    auto &port = *flow->stream->port;
    // FIN completes only its direction. Errors cancel the peer and join it before Flow cleanup.
    auto [order, send_error, receive_error] = co_await asio::experimental::make_parallel_group(
        asio::co_spawn(protocol_.executor_, send_loop(flow, port), asio::deferred),
        asio::co_spawn(protocol_.executor_, receive_loop(flow, port), asio::deferred))
        .async_wait(asio::experimental::wait_for_one_error(), asio::use_awaitable);
    if (flow->phase == FlowState::Phase::Closed)
    {
        co_return;
    }
    const std::array<std::exception_ptr, 2> errors{send_error, receive_error};
    for (const auto index : order)
    {
        if (errors[index])
        {
            protocol_.fail_flow(flow, index == 0 ? "send" : "receive", exception_description(errors[index]));
            co_return;
        }
    }
}

asio::awaitable<void> FlowProtocol::Stream::send_loop(
    const std::shared_ptr<FlowState> &flow, lnk::FlowStream &port)
{
    // The local direction may have finished before binding; its state belongs to this executor.
    if (flow->stream->directions[std::size_t(port.reverse_)].finished)
    {
        co_return;
    }
    while (flow->phase == FlowState::Phase::Active)
    {
        auto data = co_await port.send_queue_.async_receive(asio::use_awaitable);
        if (flow->phase == FlowState::Phase::Closed)
        {
            co_return;
        }
        while (!try_send(flow, port.reverse_, data))
        {
            co_await port.send_space_.async_receive(asio::use_awaitable);
            if (flow->phase == FlowState::Phase::Closed)
            {
                co_return;
            }
        }
        if (!data)
        {
            port.send_queue_.close();
            co_return;
        }
    }
}

asio::awaitable<void> FlowProtocol::Stream::receive_loop(
    const std::shared_ptr<FlowState> &flow, lnk::FlowStream &port)
{
    while (flow->phase == FlowState::Phase::Active)
    {
        auto frame = co_await flow->stream->receive_queue.async_receive(asio::use_awaitable);
        if (flow->phase == FlowState::Phase::Closed)
        {
            co_return;
        }
        const auto used = frame.charge();
        const auto reverse = frame.header.reverse;
        const auto kind = frame.header.kind;
        std::optional<PooledBuffer> data;
        if (kind == LnkFrType::Data)
        {
            data.emplace(std::move(frame.payload));
        }
        if (!port.receive_queue_.try_send(asio::error_code{}, std::move(data)))
        {
            throw std::runtime_error("stream delivery capacity violated");
        }
        if (kind == LnkFrType::Data)
        {
            co_await port.receive_completed_.async_receive(asio::use_awaitable);
            if (flow->phase == FlowState::Phase::Closed)
            {
                co_return;
            }
        }
        release_buffer(flow, reverse, used, kind == LnkFrType::Data);
        if (kind == LnkFrType::Fin)
        {
            port.receive_queue_.close();
            co_return;
        }
    }
}

// UDP protocol: bounded packet admission and drops, without stream credit or acknowledgements.

std::shared_ptr<lnk::FlowDatagram> FlowProtocol::Datagram::bind(
    const std::shared_ptr<FlowState> &flow, bool reverse, asio::any_io_executor transfer)
{
    auto &datagram = flow->datagram;
    if (datagram.port)
    {
        throw std::invalid_argument("invalid or duplicate flow endpoint binding");
    }
    auto port = std::shared_ptr<lnk::FlowDatagram>(
        new lnk::FlowDatagram(transfer, protocol_.weak_from_this(), flow->id, reverse, datagram.receive_queue, protocol_.payload_pool_));
    datagram.port = port;
    protocol_.spawn(run(flow));
    return port;
}

void FlowProtocol::Datagram::close(FlowState &flow)
{
    if (flow.previous && flow.next)
    {
        return;
    }
    auto &datagram = flow.datagram;
    if (datagram.port)
    {
        asio::post(datagram.port->executor(), [port = datagram.port, reason = flow.reason] { port->close(reason); });
    }
    datagram.receive_queue->reset();
    datagram.receive_queue->close();
}

bool FlowProtocol::Datagram::route(const std::shared_ptr<FlowState> &s, Frame &f)
{
    const auto output = f.header.reverse ? s->previous : s->next;
    if (output)
    {
        f.owner = s;
        return protocol_.links_.enqueue(*protocol_.links_.connections_.at(output), std::move(f));
    }
    return s->datagram.receive_queue->try_send(asio::error_code{}, std::move(f.payload));
}

bool FlowProtocol::Datagram::try_send(const std::shared_ptr<FlowState> &flow, bool reverse,
                                     PooledBuffer &data)
{
    assert(flow->phase == FlowState::Phase::Active && flow->transport == RelayProtocol::Udp);
    Frame packet{{LnkFrType::Data, reverse, static_cast<std::uint32_t>(data.size()), flow->epoch, flow->id},
                 std::move(data)};
    if (!route(flow, packet))
    {
        protocol_.dropped_datagrams_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void FlowProtocol::Datagram::receive(const std::shared_ptr<FlowState> &flow, Frame frame)
{
    if (frame.header.kind != LnkFrType::Data)
    {
        return;
    }
    protocol_.forwarded_bytes_.fetch_add(frame.payload.size(), std::memory_order_relaxed);
    if (!route(flow, frame))
    {
        protocol_.dropped_datagrams_.fetch_add(1, std::memory_order_relaxed);
    }
}

void FlowProtocol::Datagram::close_port(const std::shared_ptr<lnk::FlowDatagram> &port, std::string reason)
{
    const auto it = protocol_.flows_.find(port->id_);
    if (it != protocol_.flows_.end() && it->second->datagram.port == port)
    {
        protocol_.close_flow(port->id_, std::move(reason));
    }
}

asio::awaitable<void> FlowProtocol::Datagram::run(std::shared_ptr<FlowState> flow)
{
    auto &port = *flow->datagram.port;
    try
    {
        while (flow->phase == FlowState::Phase::Active)
        {
            auto data = co_await port.send_queue_.async_receive(asio::use_awaitable);
            if (flow->phase == FlowState::Phase::Closed)
            {
                co_return;
            }
            try_send(flow, port.reverse_, data); // Full output drops this packet; never waits for space.
        }
    }
    catch (const std::exception &e)
    {
        protocol_.fail_flow(flow, "send", e.what());
    }
}
