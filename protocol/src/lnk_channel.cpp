#include "flow_protocol.h"
#include "frame_io.h"
#include <asio/experimental/parallel_group.hpp>
#include <cassert>
#include <chrono>
#include <cstddef>

lnk::PayloadPool::~PayloadPool()
{
    assert(live_ == 0);
    reclaim(0);
}

void lnk::PayloadPool::reclaim(std::size_t target)
{
    for (std::size_t index = free_.size(); index-- > 0 && retained_ > target;)
    {
        const auto size = std::size_t{256} << index;
        while (free_[index] && retained_ > target)
        {
            auto block = free_[index];
            free_[index] = *static_cast<void **>(block);
            std::pmr::new_delete_resource()->deallocate(block, size);
            retained_ -= size;
        }
    }
}

void *lnk::PayloadPool::do_allocate(std::size_t requested, std::size_t alignment)
{
    const std::lock_guard lock(mutex_);
    const auto size = allocation_size(requested);
    if (!size || size > 65536 || alignment > alignof(std::max_align_t))
    {
        throw std::bad_alloc();
    }
    const auto index = std::countr_zero(size) - 8;
    void *block;
    if (free_[index])
    {
        block = free_[index];
        free_[index] = *static_cast<void **>(block);
    }
    else
    {
        if (size > budget_)
        {
            throw std::bad_alloc();
        }
        reclaim(budget_ - size);
        if (retained_ > budget_ - size)
        {
            throw std::bad_alloc();
        }
        block = std::pmr::new_delete_resource()->allocate(size);
        retained_ += size;
    }
    live_ += size;
    return block;
}

void lnk::PayloadPool::do_deallocate(void *block, std::size_t requested, std::size_t)
{
    const std::lock_guard lock(mutex_);
    const auto size = allocation_size(requested);
    const auto index = std::countr_zero(size) - 8;
    live_ -= size;
    *static_cast<void **>(block) = free_[index];
    free_[index] = block;
}

bool lnk::PayloadPool::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}

namespace
{
using lnk::FlowState;
using lnk::Frame;
using lnk::NodeLink;
using lnk::PooledBuffer;
using LinkPhase = NodeLink::Phase;

constexpr std::size_t udp_send_queue_capacity = 100;
// Reserve admission capacity for control frames in the shared UDP send queue.
constexpr std::size_t reserved_control_frames = 4;
constexpr std::size_t control_queue_capacity = 4096;
constexpr std::size_t max_pending_accepts = 100;
constexpr std::size_t max_datagram_size =
    DatagramHeader::length + LnkFrameHeader::length + LnkFrameHeader::maximum_payload;
// One extra byte distinguishes an oversized, truncated datagram from a valid maximum-size frame.
constexpr std::size_t datagram_receive_size = max_datagram_size + 1;
constexpr auto link_setup_timeout = std::chrono::seconds(10);
constexpr auto heartbeat_interval = std::chrono::seconds(5);
constexpr auto heartbeat_timeout = std::chrono::seconds(20);
constexpr auto frame_read_timeout = std::chrono::seconds(20);

bool is_business(LnkFrType kind)
{
    return kind == LnkFrType::Data || kind == LnkFrType::Fin || kind == LnkFrType::Reset;
}

std::string_view link_stage(LinkPhase state)
{
    switch (state)
    {
    case LinkPhase::Preparing:
    case LinkPhase::Prepared:
    case LinkPhase::WaitingForPeer:
        return "prepare";
    case LinkPhase::Resolving:
        return "resolve";
    case LinkPhase::Connecting:
        return "connect";
    case LinkPhase::Attaching:
        return "attach";
    case LinkPhase::Ready:
        return "ready";
    case LinkPhase::Closed:
        return "close";
    }
    return {};
}
} // namespace

lnk::NodeLink::NodeLink(asio::any_io_executor executor, LinkSpec values)
    : spec(std::move(values)), resolver(executor), writable(executor, 1), deadline(Clock::now() + link_setup_timeout)
{
    if (spec.transport == RelayProtocol::Tcp)
    {
        socket.emplace(executor);
    }
}

LnkChannel::LnkChannel(FlowProtocol &protocol, asio::any_io_executor executor, std::string node_id,
                       std::string tcp_address, std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port)
    : protocol_(protocol), executor_(executor), node_id_(std::move(node_id)), acceptor_(executor),
      udp_socket_(executor), tcp_address_(std::move(tcp_address)), udp_address_(std::move(udp_address)),
      tcp_port_(tcp_port), udp_port_(udp_port), udp_writes_(executor, udp_send_queue_capacity)
{
}

void LnkChannel::start()
{
    try
    {
        if (!tcp_port_ || !udp_port_)
        {
            throw std::invalid_argument("cluster.tcp_port and cluster.udp_port must be nonzero");
        }
        const tcp::endpoint tcp_endpoint(asio::ip::make_address(tcp_address_), tcp_port_);
        const udp::endpoint udp_endpoint(asio::ip::make_address(udp_address_), udp_port_);
        acceptor_.open(tcp_endpoint.protocol());
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.bind(tcp_endpoint);
        acceptor_.listen();
        udp_socket_.open(udp_endpoint.protocol());
        udp_socket_.bind(udp_endpoint);
        PROXY_INFO_PRINT("Node data listening tcp=%s:%u udp=%s:%u", tcp_address_.c_str(),
                         static_cast<unsigned int>(tcp_port_), udp_address_.c_str(),
                         static_cast<unsigned int>(udp_port_));
    }
    catch (...)
    {
        rollback();
        throw;
    }
}

void LnkChannel::rollback() noexcept
{
    asio::error_code ignored;
    acceptor_.close(ignored);
    udp_socket_.close(ignored);
}

void LnkChannel::notify(const NodeLink &link, lnk::EventKind kind, std::string reason, std::string_view stage)
{
    protocol_.emit({kind, link.spec.epoch, link.spec.id, std::string(stage.empty() ? link_stage(link.phase) : stage),
                    std::move(reason)});
}

void LnkChannel::fail(NodeLink &link, std::string reason, std::string_view stage)
{
    if (link.phase == LinkPhase::Closed)
    {
        return;
    }
    notify(link, lnk::EventKind::LinkError, std::move(reason), stage);
    close(link.spec.id);
}

void LnkChannel::close(std::uint64_t id)
{
    auto found = connections_.find(id);
    if (found == connections_.end())
    {
        return;
    }
    auto link = found->second;
    const auto state = std::exchange(link->phase, LinkPhase::Closed);
    if (state == LinkPhase::Ready)
    {
        PROXY_INFO_PRINT("Node link [x] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(id),
                         relay_protocol_name(link->spec.transport).data(), link->spec.peer.c_str());
    }
    link->resolver.cancel();
    asio::error_code ignored;
    if (link->socket)
    {
        link->socket->close(ignored);
        link->writable.cancel();
        link->writable.close();
    }
    // Drain this Link once; Flow closure only needs to discard from surviving Links.
    connections_.erase(found);
    link->writes.clear();
    link->controls.clear();
    link->credits.clear();
    protocol_.link_flows_closed(id);
    notify(*link, lnk::EventKind::LinkClosed, {}, link_stage(state));
}

void LnkChannel::prepare(lnk::LinkSpec spec)
{
    // Control parameters have already been validated by NodeLinkMgr.
    const auto id = spec.id;
    if (protocol_.stopping_ || connections_.contains(id))
    {
        return;
    }
    auto link = std::make_shared<NodeLink>(executor_, std::move(spec));
    connections_.emplace(id, link);
    protocol_.schedule_monitor(link->deadline);
    if (link->spec.transport == RelayProtocol::Udp)
    {
        link->phase = LinkPhase::Resolving;
        protocol_.spawn(prepare_udp(link));
    }
    else
    {
        link->phase = LinkPhase::Prepared;
        notify(*link, lnk::EventKind::LinkPrepared);
    }
}

asio::awaitable<void> LnkChannel::prepare_udp(std::shared_ptr<NodeLink> link)
{
    if (link->phase == LinkPhase::Closed)
    {
        co_return;
    }
    try
    {
        const auto endpoints = co_await link->resolver.async_resolve(
            link->spec.peer_address, std::to_string(link->spec.udp_port),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->phase == LinkPhase::Closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            fail(*link, "resolver returned no endpoints");
            co_return;
        }
        link->endpoint = {endpoints.begin()->endpoint().address(), link->spec.udp_port};
        if (link->endpoint.protocol() != udp_socket_.local_endpoint().protocol())
        {
            fail(*link, "UDP peer address family differs from listener");
            co_return;
        }
        link->phase = LinkPhase::Prepared;
        notify(*link, lnk::EventKind::LinkPrepared);
    }
    catch (const std::exception &e)
    {
        fail(*link, e.what());
    }
}

void LnkChannel::connect(std::uint64_t id)
{
    const auto found = connections_.find(id);
    if (found == connections_.end() || found->second->phase != LinkPhase::Prepared)
    {
        return;
    }
    auto link = found->second;
    if (link->spec.transport == RelayProtocol::Udp || link->spec.initiator)
    {
        // Reserve the transition before spawning so duplicate connect requests cannot start another chain.
        link->phase = link->spec.transport == RelayProtocol::Udp ? LinkPhase::Attaching : LinkPhase::Resolving;
        protocol_.spawn(establish(link));
    }
    else
    {
        link->phase = LinkPhase::WaitingForPeer;
    }
}

CtrlMessage LnkChannel::attach_message(const NodeLink &link, CtrlCommand command) const
{
    return CtrlMessage(command, njson{{"id", link.spec.id},
                                      {"epoch", link.spec.epoch},
                                      {"node", node_id_},
                                      {"token", link.spec.token},
                                      {"data_version", LnkFrameHeader::version}});
}

bool LnkChannel::matches(const NodeLink &link, const CtrlMessage &message) const
{
    // CtrlMessage decoding already guarantees that present params are an object.
    if (!message.params)
    {
        return false;
    }
    const auto &p = *message.params;
    return p.value("id", std::uint64_t(0)) == link.spec.id && p.value("epoch", std::uint64_t(0)) == link.spec.epoch &&
           p.value("node", std::string{}) == link.spec.peer && p.value("token", std::string{}) == link.spec.token &&
           config::require_unsigned(p, "data_version", true) == LnkFrameHeader::version;
}

void LnkChannel::mark_ready(NodeLink &link)
{
    if (link.phase != LinkPhase::Attaching || !link.attached || !link.acknowledged)
    {
        return;
    }
    if (link.spec.transport == RelayProtocol::Tcp)
    {
        // Small CREDIT/FIN/RESET frames must not wait for delayed ACK and Nagle aggregation.
        link.socket->set_option(tcp::no_delay(true));
    }
    link.phase = LinkPhase::Ready;
    PROXY_INFO_PRINT("Node link [+] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(link.spec.id),
                     relay_protocol_name(link.spec.transport).data(), link.spec.peer.c_str());
    link.last_response = link.last_ping = Clock::now();
    protocol_.schedule_monitor(link.last_ping + heartbeat_interval);
    notify(link, lnk::EventKind::LinkReady);
}

asio::awaitable<void> LnkChannel::establish(std::shared_ptr<NodeLink> link)
{
    // This independently spawned chain may start after close() has removed the link.
    if (link->phase == LinkPhase::Closed)
    {
        co_return;
    }
    try
    {
        if (link->spec.transport == RelayProtocol::Udp)
        {
            auto payload = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
            if (payload.size() > LnkFrameHeader::maximum_payload)
            {
                fail(*link, "UDP attach exceeds frame body limit");
                co_return;
            }
            if (!enqueue(*link,
                         Frame{{LnkFrType::Attach, false, static_cast<std::uint32_t>(payload.size()), link->spec.epoch},
                               PooledBuffer(protocol_.payload_pool_, payload)}))
            {
                fail(*link, "UDP send queue full");
            }
            else
            {
                mark_ready(*link);
            }
            co_return;
        }
        const auto port = link->spec.tcp_port;
        // A single resolver operation and exactly one selected endpoint connect.
        const auto endpoints = co_await link->resolver.async_resolve(
            link->spec.peer_address, std::to_string(port),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->phase == LinkPhase::Closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            fail(*link, "resolver returned no endpoints");
            co_return;
        }
        auto endpoint = endpoints.begin()->endpoint();
        link->phase = LinkPhase::Connecting;
        co_await link->socket->async_connect(endpoint,
                                             asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->phase == LinkPhase::Closed)
        {
            co_return;
        }
        link->phase = LinkPhase::Attaching;
        auto bytes = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
        co_await asio::async_write(*link->socket, asio::buffer(bytes),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        auto response = co_await read_ctrl_frame(*link->socket, link->deadline);
        if (link->phase == LinkPhase::Closed)
        {
            co_return;
        }
        if (response.type() != CtrlCommand::LinkAttached || !matches(*link, response))
        {
            fail(*link, "TCP attach acknowledgement identity mismatch");
            co_return;
        }
        link->attached = link->acknowledged = true;
        mark_ready(*link);
        co_await tcp_run(*link);
    }
    catch (const std::exception &e)
    {
        fail(*link, e.what());
    }
}

asio::awaitable<void> LnkChannel::accept_loop()
{
    for (;;)
    {
        auto socket = co_await acceptor_.async_accept(asio::use_awaitable);
        // A successful accept can already be queued when stop closes the listener.
        if (protocol_.stopping_)
        {
            co_return;
        }
        if (accepting_.size() >= max_pending_accepts)
        {
            socket.close();
            continue;
        }
        auto pending = std::make_shared<tcp::socket>(std::move(socket));
        accepting_.insert(pending);
        protocol_.spawn(accept(pending));
    }
}

asio::awaitable<void> LnkChannel::accept(std::shared_ptr<tcp::socket> socket)
{
    std::shared_ptr<NodeLink> link;
    try
    {
        {
            // Only bootstrap owns the pending socket; the authenticated Link takes over its I/O.
            ScopeGuard cleanup([this, &socket]() { accepting_.erase(socket); });
            auto message = co_await read_ctrl_frame(*socket, Clock::now() + link_setup_timeout);
            if (message.type() != CtrlCommand::LinkAttach)
            {
                co_return;
            }
            auto id = config::require_unsigned(config::message_params(message), "id", true);
            auto found = connections_.find(id);
            if (found == connections_.end())
            {
                co_return;
            }
            link = found->second;
            if (link->spec.transport != RelayProtocol::Tcp || link->spec.initiator || link->attached ||
                !matches(*link, message))
            {
                co_return;
            }
            link->attached = true;
            link->phase = LinkPhase::Attaching;
            *link->socket = std::move(*socket);
        }
        socket.reset();
        auto response = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttached));
        co_await asio::async_write(*link->socket, asio::buffer(response),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->phase == LinkPhase::Closed)
        {
            co_return;
        }
        link->acknowledged = true;
        mark_ready(*link);
        co_await tcp_run(*link);
    }
    catch (const std::exception &e)
    {
        if (link && link->attached)
        {
            fail(*link, e.what());
        }
    }
}

bool LnkChannel::enqueue(NodeLink &link, Frame frame)
{
    // Caller has validated routing and Flow credit on the same data executor.
    assert(link.phase != LinkPhase::Closed && !protocol_.stopping_);
    assert(frame.payload.size() == frame.header.body_length);
    if (link.spec.transport == RelayProtocol::Tcp)
    {
        if (frame.header.kind == LnkFrType::Data || frame.header.kind == LnkFrType::Fin)
        {
            assert(frame.owner);
            const lnk::FlowWriteTag tag{frame.owner.get(), frame.header.reverse};
            link.writes.push(tag, std::move(frame));
        }
        else if (frame.header.kind == LnkFrType::Credit)
        {
            link.credits[{frame.header.id, frame.header.reverse}] = frame.header;
        }
        else
        {
            if (link.controls.size() >= control_queue_capacity)
            {
                return false;
            }
            link.controls.push_back(std::move(frame));
        }
        link.writable.try_send(asio::error_code{});
        return true;
    }
    const bool business = is_business(frame.header.kind);
    if (business && udp_pending_data_ >= udp_send_queue_capacity - reserved_control_frames)
    {
        return false;
    }
    if (!udp_writes_.try_send(asio::error_code{}, DatagramWrite{std::move(frame), link.spec.id}))
    {
        return false;
    }
    if (business)
    {
        ++udp_pending_data_;
    }
    return true;
}

void LnkChannel::process(NodeLink &link, const LnkFrameHeader &header, PooledBuffer payload)
{
    // The TCP/UDP read boundary has validated format, Link identity and readiness.
    switch (header.kind)
    {
    case LnkFrType::Credit:
        protocol_.receive(link, Frame{header, std::move(payload)});
        break;
    case LnkFrType::Ping:
        if (!enqueue(link, Frame{{LnkFrType::Pong, false, 0, header.epoch, 0, header.sequence}}))
        {
            fail(link, "send queue full");
        }
        break;
    case LnkFrType::Pong:
        // Delayed replies may acknowledge an earlier Ping; duplicates cannot renew liveness.
        if (header.sequence > link.last_ack_ping && header.sequence <= link.ping)
        {
            link.last_ack_ping = header.sequence;
            link.last_response = Clock::now();
        }
        break;
    case LnkFrType::Data:
    case LnkFrType::Fin:
    case LnkFrType::Reset: {
        protocol_.receive(link, Frame{header, std::move(payload)});
        break;
    }
    default:
        break;
    }
}

asio::awaitable<void> LnkChannel::tcp_run(NodeLink &link)
{
    if (link.phase == LinkPhase::Closed)
    {
        co_return;
    }
    // A physical Link has no business half-close: any loop exit cancels and joins its peer.
    auto [order, read_error, write_error] =
        co_await asio::experimental::make_parallel_group(asio::co_spawn(executor_, tcp_read(link), asio::deferred),
                                                         asio::co_spawn(executor_, tcp_write(link), asio::deferred))
            .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
    if (link.phase == LinkPhase::Closed)
    {
        co_return;
    }
    const std::array<std::exception_ptr, 2> errors{read_error, write_error};
    if (errors[order[0]])
    {
        std::rethrow_exception(errors[order[0]]);
    }
    throw std::runtime_error("Node link TCP loop exited unexpectedly");
}

asio::awaitable<void> LnkChannel::tcp_read(NodeLink &link)
{
    while (link.phase != LinkPhase::Closed)
    {
        // cluster_data_io: validate the bounded header before allocating the body.
        LnkFrameHeader::Buffer bytes{};
        const auto deadline = Clock::now() + frame_read_timeout;
        co_await asio::async_read(*link.socket, asio::buffer(bytes),
                                  asio::cancel_after(frame_read_timeout, asio::use_awaitable));
        const auto header = LnkFrameHeader::decode(bytes);
        if (header.kind == LnkFrType::Attach || header.kind == LnkFrType::Attached)
        {
            throw std::runtime_error("unexpected TCP bootstrap after attach");
        }
        PooledBuffer payload(protocol_.payload_pool_, header.body_length);
        if (payload.size())
        {
            co_await asio::async_read(*link.socket, asio::buffer(payload.data(), payload.size()),
                                      asio::cancel_after(deadline - Clock::now(), asio::use_awaitable));
        }
        if (link.phase == LinkPhase::Closed)
        {
            co_return;
        }
        if (header.epoch != link.spec.epoch)
        {
            continue;
        }
        process(link, header, std::move(payload));
    }
}

asio::awaitable<void> LnkChannel::tcp_write(NodeLink &link)
{
    std::size_t control_run = 0;
    while (link.phase != LinkPhase::Closed)
    {
        if (link.writes.empty() && link.controls.empty() && link.credits.empty())
        {
            co_await link.writable.async_receive(asio::use_awaitable);
            continue;
        }
        Frame queued;
        if (control_run < 16 || link.writes.empty())
        {
            if (!link.controls.empty())
            {
                queued = std::move(link.controls.front());
                link.controls.pop_front();
            }
            else if (!link.credits.empty())
            {
                queued.header = link.credits.begin()->second;
                link.credits.erase(link.credits.begin());
            }
        }
        if (queued.header.epoch)
        {
            ++control_run;
        }
        else
        {
            queued = std::move(link.writes.front());
            link.writes.pop();
            control_run = 0;
        }
        const auto header = queued.header.encode();
        const std::array<asio::const_buffer, 2> buffers{asio::buffer(header),
                                                        asio::buffer(queued.payload.data(), queued.payload.size())};
        co_await asio::async_write(*link.socket, buffers, asio::use_awaitable);
        protocol_.written(std::move(queued));
    }
}

asio::awaitable<void> LnkChannel::udp_read()
{
    PooledBuffer buffer(protocol_.payload_pool_, datagram_receive_size);
    for (;;)
    {
        if (!buffer.data())
        {
            buffer = PooledBuffer(protocol_.payload_pool_, datagram_receive_size);
        }
        udp::endpoint source;
        auto [error, size] = co_await udp_socket_.async_receive_from(asio::buffer(buffer.data(), buffer.size()), source,
                                                                     use_nothrow_awaitable);
        if (error)
        {
            if (error == asio::error::operation_aborted || error == asio::error::bad_descriptor)
            {
                throw asio::system_error(error, "Node link UDP receive failed");
            }
            continue;
        }
        if (size < DatagramHeader::length + LnkFrameHeader::length || size > max_datagram_size)
        {
            continue;
        }
        auto id = DatagramHeader::decode(buffer.bytes().first(DatagramHeader::length));
        if (!id)
        {
            continue;
        }
        auto found = connections_.find(*id);
        if (found == connections_.end())
        {
            continue;
        }
        auto link = found->second;
        if (link->spec.transport != RelayProtocol::Udp || link->endpoint != source)
        {
            continue;
        }
        try
        {
            const auto header =
                LnkFrameHeader::decode(buffer.bytes().subspan<DatagramHeader::length, LnkFrameHeader::length>());
            if (header.epoch != link->spec.epoch ||
                size != DatagramHeader::length + LnkFrameHeader::length + header.body_length)
            {
                continue;
            }
            const auto payload =
                buffer.bytes().subspan(DatagramHeader::length + LnkFrameHeader::length, header.body_length);
            if (header.kind == LnkFrType::Attach || header.kind == LnkFrType::Attached)
            {
                const auto message = decode_ctrl_datagram(payload);
                const auto command =
                    header.kind == LnkFrType::Attach ? CtrlCommand::LinkAttach : CtrlCommand::LinkAttached;
                if (link->phase == LinkPhase::Ready || message.type() != command || !matches(*link, message))
                {
                    continue;
                }
                if (header.kind == LnkFrType::Attach)
                {
                    if (link->attached)
                    {
                        continue;
                    }
                    link->attached = true;
                    auto response = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttached));
                    if (response.size() > LnkFrameHeader::maximum_payload)
                    {
                        fail(*link, "UDP acknowledgement exceeds frame body limit");
                        continue;
                    }
                    if (!enqueue(*link, Frame{{LnkFrType::Attached, false, static_cast<std::uint32_t>(response.size()),
                                               header.epoch},
                                              PooledBuffer(protocol_.payload_pool_, response)}))
                    {
                        fail(*link, "UDP send queue full");
                    }
                }
                else
                {
                    link->acknowledged = true;
                }
                mark_ready(*link);
            }
            else
            {
                if (link->phase != LinkPhase::Ready)
                {
                    continue;
                }
                if (header.body_length)
                {
                    buffer.slice(DatagramHeader::length + LnkFrameHeader::length, header.body_length);
                    process(*link, header, std::move(buffer));
                }
                else
                {
                    process(*link, header, {});
                }
            }
        }
        catch (const std::exception &)
        { /* Invalid datagrams never mutate the link. */
        }
    }
}

asio::awaitable<void> LnkChannel::udp_write()
{
    for (;;)
    {
        auto datagram = co_await udp_writes_.async_receive(asio::use_awaitable);
        // close_all() resets the pending count; late deliveries must not decrement it.
        if (protocol_.stopping_)
        {
            co_return;
        }
        if (is_business(datagram.frame.header.kind))
        {
            --udp_pending_data_;
        }
        if (datagram.frame.owner && datagram.frame.owner->phase == FlowState::Phase::Closed)
        {
            continue;
        }
        udp::endpoint endpoint;
        {
            const auto found = connections_.find(datagram.link_id);
            if (found == connections_.end() || found->second->spec.epoch != datagram.frame.header.epoch)
            {
                continue;
            }
            endpoint = found->second->endpoint;
        }
        const auto link_header = DatagramHeader::encode(datagram.link_id);
        const auto frame_header = datagram.frame.header.encode();
        const std::array<asio::const_buffer, 3> buffers{
            asio::buffer(link_header), asio::buffer(frame_header),
            asio::buffer(datagram.frame.payload.data(), datagram.frame.payload.size())};
        // The endpoint and buffers outlive the send; no link reference or table iterator crosses this await.
        auto [error, size] = co_await udp_socket_.async_send_to(buffers, endpoint, use_nothrow_awaitable);
        if (error)
        {
            const auto found = connections_.find(datagram.link_id);
            if (found != connections_.end() && found->second->spec.epoch == datagram.frame.header.epoch)
            {
                auto link = found->second;
                fail(*link, error.message());
            }
        }
    }
}

LnkChannel::Clock::time_point LnkChannel::poll(Clock::time_point now)
{
    auto next_deadline = Clock::time_point::max();
    for (auto it = connections_.begin(); it != connections_.end();)
    {
        // Advance before fail() erases the current link and its dependent flows.
        auto link = (it++)->second;
        if ((link->phase != LinkPhase::Ready && now >= link->deadline) ||
            (link->phase == LinkPhase::Ready && now - link->last_response >= heartbeat_timeout))
        {
            fail(*link, "node link deadline expired",
                 link->phase == LinkPhase::Ready ? "keepalive" : std::string_view{});
            continue;
        }
        else if (link->phase == LinkPhase::Ready && now - link->last_ping >= heartbeat_interval)
        {
            link->last_ping = now;
            ++link->ping;
            if (!enqueue(*link, Frame{{LnkFrType::Ping, false, 0, link->spec.epoch, 0, link->ping}}))
            {
                fail(*link, "send queue full");
                continue;
            }
        }
        const auto deadline = link->phase == LinkPhase::Ready ? std::min(link->last_ping + heartbeat_interval,
                                                                         link->last_response + heartbeat_timeout)
                                                              : link->deadline;
        next_deadline = std::min(next_deadline, deadline);
    }
    return next_deadline;
}

void LnkChannel::close_all()
{
    rollback();
    udp_writes_.reset();
    udp_writes_.close();
    udp_pending_data_ = 0;
    while (!connections_.empty())
    {
        close(connections_.begin()->first);
    }
    for (auto &socket : accepting_)
    {
        asio::error_code ignored;
        socket->close(ignored);
    }
}

void LnkChannel::discard(std::uint64_t link_id, const std::shared_ptr<FlowState> &flow)
{
    const auto found = connections_.find(link_id);
    if (found == connections_.end())
    {
        return;
    }
    auto &link = *found->second;
    link.writes.erase({flow.get(), false});
    link.writes.erase({flow.get(), true});
    link.credits.erase({flow->id, false});
    link.credits.erase({flow->id, true});
}
