#include "lnk_channel.h"
#include "frame_io.h"
#include <cassert>
#include <chrono>
#include <cstddef>

namespace
{
using lnk::Datagram;
using lnk::Frame;
using lnk::NodeFlow;
using lnk::NodeLink;
using LinkState = NodeLink::State;

constexpr std::size_t send_queue_capacity = 100;
constexpr std::size_t reserved_control_frames = 4;
// Reserve admission capacity for control frames; the shared FIFO preserves DATA/FIN order.
constexpr std::size_t business_queue_limit = send_queue_capacity - reserved_control_frames;
constexpr std::size_t event_queue_capacity = 4096;
constexpr std::size_t max_pending_accepts = 100;
constexpr std::size_t max_datagram_size =
    DatagramHeader::length + LnkFrameHeader::length + LnkFrameHeader::maximum_payload;
// One extra byte distinguishes an oversized, truncated datagram from a valid maximum-size frame.
constexpr std::size_t datagram_receive_size = max_datagram_size + 1;
constexpr std::size_t pool_blocks_per_chunk = 64;
constexpr auto link_setup_timeout = std::chrono::seconds(10);
constexpr auto heartbeat_interval = std::chrono::seconds(5);
constexpr auto heartbeat_timeout = std::chrono::seconds(20);
constexpr auto frame_read_timeout = std::chrono::seconds(20);

bool is_business(LnkFrType kind)
{
    return kind == LnkFrType::Data || kind == LnkFrType::Fin || kind == LnkFrType::Reset;
}

std::string_view link_stage(LinkState state)
{
    switch (state)
    {
    case LinkState::Preparing:
    case LinkState::Prepared:
    case LinkState::WaitingForPeer:
        return "prepare";
    case LinkState::Resolving:
        return "resolve";
    case LinkState::Connecting:
        return "connect";
    case LinkState::Attaching:
        return "attach";
    case LinkState::Ready:
        return "ready";
    case LinkState::Closed:
        return "close";
    }
    return {};
}
} // namespace

void LnkChannel::emit(CtrlMessage message)
{
    if (stopping_ || !event_error_.empty())
    {
        return;
    }
    if (!events_.try_send(asio::error_code{}, std::move(message)))
    {
        event_error_ = "Node control event queue capacity exceeded";
        events_.cancel();
        events_.close();
    }
}

asio::awaitable<CtrlMessage> LnkChannel::receive_event()
{
    auto [error, message] = co_await events_.async_receive(use_nothrow_awaitable);
    if (!event_error_.empty())
    {
        throw std::runtime_error(event_error_);
    }
    if (error)
    {
        throw asio::system_error(error);
    }
    co_return message;
}

lnk::NodeLink::NodeLink(asio::any_io_executor executor, njson values)
    : params(std::move(values)), id(params.at("id").get<std::uint64_t>()),
      epoch(params.at("epoch").get<std::uint64_t>()),
      transport(parse_relay_protocol(params.at("transport").get_ref<const std::string &>())), resolver(executor),
      deadline(Clock::now() + link_setup_timeout)
{
    if (transport == RelayProtocol::Tcp)
    {
        socket.emplace(executor);
        writes.emplace(executor, send_queue_capacity);
    }
}

LnkChannel::LnkChannel(asio::any_io_executor executor, std::string node_id, std::string tcp_address,
                       std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port)
    : executor_(executor), node_id_(std::move(node_id)), acceptor_(executor), udp_socket_(executor),
      tcp_address_(std::move(tcp_address)), udp_address_(std::move(udp_address)), tcp_port_(tcp_port),
      udp_port_(udp_port), payload_pool_({pool_blocks_per_chunk, datagram_receive_size}),
      udp_writes_(executor, send_queue_capacity), events_(executor, event_queue_capacity), monitor_timer_(executor),
      tasks_done_(executor)
{
    monitor_timer_.expires_at(Clock::time_point::max());
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

void LnkChannel::activate()
{
    asio::post(executor_, [self = shared_from_this()] {
        if (self->stopping_ || self->activated_)
        {
            return;
        }
        self->activated_ = true;
        self->spawn(self->accept_loop());
        self->spawn(self->udp_read());
        self->spawn(self->udp_write());
        self->spawn(self->monitor());
    });
}

void LnkChannel::rollback() noexcept
{
    asio::error_code ignored;
    acceptor_.close(ignored);
    udp_socket_.close(ignored);
}

void LnkChannel::spawn(asio::awaitable<void> task)
{
    ++tasks_;
    asio::co_spawn(executor_, std::move(task), [self = shared_from_this()](std::exception_ptr error) {
        if (error && !self->stopping_)
        {
            PROXY_ERROR_PRINT("Node data task failed: %s", exception_description(error).c_str());
        }
        if (--self->tasks_ == 0 && self->stopping_)
        {
            self->tasks_done_.notify_all();
        }
    });
}

void LnkChannel::notify(const std::shared_ptr<NodeLink> &link, CtrlCommand command, std::string reason,
                        std::string_view stage)
{
    auto values = link->params;
    values["stage"] = stage.empty() ? link_stage(link->state) : stage;
    values["reason"] = std::move(reason);
    emit(CtrlMessage(command, std::move(values)));
}

void LnkChannel::fail(const std::shared_ptr<NodeLink> &link, std::string reason, std::string_view stage)
{
    if (link->state == LinkState::Closed)
    {
        return;
    }
    notify(link, CtrlCommand::LinkError, std::move(reason), stage);
    close(link->id);
}

void LnkChannel::close(std::uint64_t id)
{
    auto found = links_.find(id);
    if (found == links_.end())
    {
        return;
    }
    auto link = found->second;
    const auto state = std::exchange(link->state, LinkState::Closed);
    if (state == LinkState::Ready)
    {
        PROXY_INFO_PRINT("Node link [x] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(id),
                         relay_protocol_name(link->transport).data(),
                         link->params.at("peer").get_ref<const std::string &>().c_str());
    }
    link->resolver.cancel();
    asio::error_code ignored;
    if (link->socket)
    {
        link->socket->close(ignored);
        link->writes->close();
    }
    links_.erase(found);
    link_flows_closed(id);
    notify(link, CtrlCommand::LinkClosed, {}, link_stage(state));
}

void LnkChannel::prepare(njson params)
{
    // Control parameters have already been validated by NodeLinkMgr.
    const auto id = params.at("id").get<std::uint64_t>();
    if (stopping_ || links_.contains(id))
    {
        return;
    }
    auto link = std::make_shared<NodeLink>(executor_, std::move(params));
    links_.emplace(id, link);
    schedule_monitor(link->deadline);
    if (link->transport == RelayProtocol::Udp)
    {
        link->state = LinkState::Resolving;
        spawn(prepare_udp(link));
    }
    else
    {
        link->state = LinkState::Prepared;
        notify(link, CtrlCommand::LinkPrepared);
    }
}

asio::awaitable<void> LnkChannel::prepare_udp(std::shared_ptr<NodeLink> link)
{
    if (link->state == LinkState::Closed)
    {
        co_return;
    }
    try
    {
        const auto endpoints = co_await link->resolver.async_resolve(
            link->params.at("peer_address").get<std::string>(),
            std::to_string(link->params.at("udp_port").get<std::uint16_t>()),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->state == LinkState::Closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            fail(link, "resolver returned no endpoints");
            co_return;
        }
        link->endpoint = {endpoints.begin()->endpoint().address(), link->params.at("udp_port").get<std::uint16_t>()};
        if (link->endpoint.protocol() != udp_socket_.local_endpoint().protocol())
        {
            fail(link, "UDP peer address family differs from listener");
            co_return;
        }
        link->state = LinkState::Prepared;
        notify(link, CtrlCommand::LinkPrepared);
    }
    catch (const std::exception &e)
    {
        fail(link, e.what());
    }
}

void LnkChannel::connect(std::uint64_t id)
{
    const auto found = links_.find(id);
    if (found == links_.end() || found->second->state != LinkState::Prepared)
    {
        return;
    }
    auto link = found->second;
    if (link->transport == RelayProtocol::Udp || node_id_ == link->params.at("left").get<std::string>())
    {
        // Reserve the transition before spawning so duplicate connect requests cannot start another chain.
        link->state = link->transport == RelayProtocol::Udp ? LinkState::Attaching : LinkState::Resolving;
        spawn(establish(link));
    }
    else
    {
        link->state = LinkState::WaitingForPeer;
    }
}

CtrlMessage LnkChannel::attach_message(const NodeLink &link, CtrlCommand command) const
{
    return CtrlMessage(command, njson{{"id", link.id},
                                      {"epoch", link.epoch},
                                      {"node", node_id_},
                                      {"token", link.params.at("token")},
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
    return p.value("id", std::uint64_t(0)) == link.id && p.value("epoch", std::uint64_t(0)) == link.epoch &&
           p.value("node", std::string{}) == link.params.at("peer").get<std::string>() &&
           p.value("token", std::string{}) == link.params.at("token").get<std::string>() &&
           config::require_unsigned(p, "data_version", true) == LnkFrameHeader::version;
}

void LnkChannel::mark_ready(const std::shared_ptr<NodeLink> &link)
{
    if (link->state != LinkState::Attaching || !link->attached || !link->acknowledged)
    {
        return;
    }
    link->state = LinkState::Ready;
    PROXY_INFO_PRINT("Node link [+] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(link->id),
                     relay_protocol_name(link->transport).data(),
                     link->params.at("peer").get_ref<const std::string &>().c_str());
    link->last_response = link->last_ping = Clock::now();
    schedule_monitor(link->last_ping + heartbeat_interval);
    notify(link, CtrlCommand::LinkReady);
}

asio::awaitable<void> LnkChannel::establish(std::shared_ptr<NodeLink> link)
{
    // This independently spawned chain may start after close() has removed the link.
    if (link->state == LinkState::Closed)
    {
        co_return;
    }
    try
    {
        if (link->transport == RelayProtocol::Udp)
        {
            auto payload = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
            if (payload.size() > LnkFrameHeader::maximum_payload)
            {
                fail(link, "UDP attach exceeds frame body limit");
                co_return;
            }
            if (!enqueue(link, {LnkFrType::Attach, false, static_cast<std::uint32_t>(payload.size()), link->epoch},
                         PooledBuffer(payload_pool_, payload)))
            {
                fail(link, "UDP send queue full");
            }
            else
            {
                mark_ready(link);
            }
            co_return;
        }
        const auto port = link->params.at("tcp_port").get<std::uint16_t>();
        // A single resolver operation and exactly one selected endpoint connect.
        const auto endpoints = co_await link->resolver.async_resolve(
            link->params.at("peer_address").get<std::string>(), std::to_string(port),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->state == LinkState::Closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            fail(link, "resolver returned no endpoints");
            co_return;
        }
        auto endpoint = endpoints.begin()->endpoint();
        link->state = LinkState::Connecting;
        co_await link->socket->async_connect(endpoint,
                                             asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->state == LinkState::Closed)
        {
            co_return;
        }
        link->state = LinkState::Attaching;
        auto bytes = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
        co_await asio::async_write(*link->socket, asio::buffer(bytes),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        auto response = co_await read_ctrl_frame(*link->socket, link->deadline);
        if (link->state == LinkState::Closed)
        {
            co_return;
        }
        if (response.type() != CtrlCommand::LinkAttached || !matches(*link, response))
        {
            fail(link, "TCP attach acknowledgement identity mismatch");
            co_return;
        }
        link->attached = link->acknowledged = true;
        mark_ready(link);
        spawn(tcp_write(link));
        co_await tcp_read(link);
    }
    catch (const std::exception &e)
    {
        fail(link, e.what());
    }
}

asio::awaitable<void> LnkChannel::accept_loop()
{
    while (!stopping_)
    {
        auto [error, socket] = co_await acceptor_.async_accept(use_nothrow_awaitable);
        if (error)
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
        spawn(accept(pending));
    }
}

asio::awaitable<void> LnkChannel::accept(std::shared_ptr<tcp::socket> socket)
{
    ScopeGuard cleanup([this, socket]() { accepting_.erase(socket); });
    std::shared_ptr<NodeLink> link;
    try
    {
        auto message = co_await read_ctrl_frame(*socket, Clock::now() + link_setup_timeout);
        if (message.type() != CtrlCommand::LinkAttach)
        {
            co_return;
        }
        auto id = config::require_unsigned(config::message_params(message), "id", true);
        auto found = links_.find(id);
        if (found == links_.end())
        {
            co_return;
        }
        link = found->second;
        // Invalid traffic must never close or occupy an authorized prepared slot.
        if (link->transport != RelayProtocol::Tcp || node_id_ != link->params.at("right").get<std::string>() ||
            link->attached || !matches(*link, message))
        {
            co_return;
        }
        link->attached = true;
        link->state = LinkState::Attaching;
        accepting_.erase(socket);
        *link->socket = std::move(*socket);
        auto response = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttached));
        co_await asio::async_write(*link->socket, asio::buffer(response),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->state == LinkState::Closed)
        {
            co_return;
        }
        link->acknowledged = true;
        mark_ready(link);
        spawn(tcp_write(link));
        co_await tcp_read(link);
    }
    catch (const std::exception &e)
    {
        if (link && link->attached)
        {
            fail(link, e.what());
        }
    }
}

bool LnkChannel::enqueue(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, PooledBuffer &&payload)
{
    // Callers hold a live link in this single-thread domain; none awaits before enqueue.
    assert(link->state != LinkState::Closed && !stopping_);
    const bool business = is_business(header.kind);
    const bool tcp = link->transport == RelayProtocol::Tcp;
    auto &pending = tcp ? link->pending_data : udp_pending_data_;
    if (business && pending >= business_queue_limit)
    {
        return false;
    }
    // Private enqueue receives only locally constructed or decoded, validated frames.
    assert(payload.size() == header.body_length);
    Frame queued{header, std::move(payload)};
    bool sent;
    if (tcp)
    {
        sent = link->writes->try_send(asio::error_code{}, std::move(queued));
    }
    else
    {
        Datagram packet{std::move(queued), link->id};
        sent = udp_writes_.try_send(asio::error_code{}, std::move(packet));
    }
    if (sent && business)
    {
        ++pending;
    }
    return sent;
}

void LnkChannel::process(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, PooledBuffer payload)
{
    // The TCP/UDP read boundary has validated format, Link identity and readiness.
    switch (header.kind)
    {
    case LnkFrType::Ping:
        if (!enqueue(link, {LnkFrType::Pong, false, 0, header.epoch, 0, header.sequence}))
        {
            fail(link, "send queue full");
        }
        break;
    case LnkFrType::Pong:
        // Delayed replies may acknowledge an earlier Ping; duplicates cannot renew liveness.
        if (header.sequence > link->last_ack_ping && header.sequence <= link->ping)
        {
            link->last_ack_ping = header.sequence;
            link->last_response = Clock::now();
        }
        break;
    case LnkFrType::Data:
    case LnkFrType::Fin:
    case LnkFrType::Reset: {
        incoming_flow(*link, Frame{header, std::move(payload)});
        break;
    }
    default:
        break;
    }
}

asio::awaitable<void> LnkChannel::tcp_read(std::shared_ptr<NodeLink> link)
{
    try
    {
        while (link->state != LinkState::Closed)
        {
            // cluster_data_io: validate the bounded header before allocating the body.
            LnkFrameHeader::Buffer bytes{};
            const auto deadline = Clock::now() + frame_read_timeout;
            co_await asio::async_read(*link->socket, asio::buffer(bytes),
                                      asio::cancel_after(frame_read_timeout, asio::use_awaitable));
            const auto header = LnkFrameHeader::decode(bytes);
            if (header.kind == LnkFrType::Attach || header.kind == LnkFrType::Attached)
            {
                fail(link, "unexpected TCP bootstrap after attach");
                co_return;
            }
            PooledBuffer payload(payload_pool_, header.body_length);
            if (payload.size())
            {
                co_await asio::async_read(*link->socket, asio::buffer(payload.data(), payload.size()),
                                          asio::cancel_after(deadline - Clock::now(), asio::use_awaitable));
            }
            if (link->state == LinkState::Closed)
            {
                co_return;
            }
            if (header.epoch != link->epoch)
            {
                continue;
            }
            process(link, header, std::move(payload));
        }
    }
    catch (const std::exception &e)
    {
        fail(link, e.what());
    }
}

asio::awaitable<void> LnkChannel::tcp_write(std::shared_ptr<NodeLink> link)
{
    try
    {
        while (link->state != LinkState::Closed)
        {
            auto queued = co_await link->writes->async_receive(asio::use_awaitable);
            if (is_business(queued.header.kind))
            {
                --link->pending_data;
            }
            const auto header = queued.header.encode();
            const std::array<asio::const_buffer, 2> buffers{asio::buffer(header),
                                                            asio::buffer(queued.payload.data(), queued.payload.size())};
            co_await asio::async_write(*link->socket, buffers, asio::use_awaitable);
        }
    }
    catch (const std::exception &e)
    {
        fail(link, e.what());
    }
}

asio::awaitable<void> LnkChannel::udp_read()
{
    PooledBuffer buffer(payload_pool_, datagram_receive_size);
    while (!stopping_)
    {
        if (!buffer.data())
        {
            buffer = PooledBuffer(payload_pool_, datagram_receive_size);
        }
        udp::endpoint source;
        auto [error, size] = co_await udp_socket_.async_receive_from(asio::buffer(buffer.data(), buffer.size()), source,
                                                                     use_nothrow_awaitable);
        if (error)
        {
            if (stopping_)
            {
                co_return;
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
        auto found = links_.find(*id);
        if (found == links_.end())
        {
            continue;
        }
        auto link = found->second;
        if (link->transport != RelayProtocol::Udp || link->endpoint != source)
        {
            continue;
        }
        try
        {
            const auto header =
                LnkFrameHeader::decode(buffer.bytes().subspan<DatagramHeader::length, LnkFrameHeader::length>());
            if (header.epoch != link->epoch ||
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
                if (link->state == LinkState::Ready || message.type() != command || !matches(*link, message))
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
                        fail(link, "UDP acknowledgement exceeds frame body limit");
                        continue;
                    }
                    if (!enqueue(
                            link,
                            {LnkFrType::Attached, false, static_cast<std::uint32_t>(response.size()), header.epoch},
                            PooledBuffer(payload_pool_, response)))
                    {
                        fail(link, "UDP send queue full");
                    }
                }
                else
                {
                    link->acknowledged = true;
                }
                mark_ready(link);
            }
            else
            {
                if (link->state != LinkState::Ready)
                {
                    continue;
                }
                if (header.body_length)
                {
                    buffer.slice(DatagramHeader::length + LnkFrameHeader::length, header.body_length);
                    process(link, header, std::move(buffer));
                }
                else
                {
                    process(link, header, {});
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
    while (!stopping_)
    {
        auto [queue_error, datagram] = co_await udp_writes_.async_receive(use_nothrow_awaitable);
        if (queue_error)
        {
            co_return;
        }
        if (is_business(datagram.frame.header.kind))
        {
            --udp_pending_data_;
        }
        udp::endpoint endpoint;
        {
            const auto found = links_.find(datagram.link_id);
            if (found == links_.end() || found->second->epoch != datagram.frame.header.epoch)
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
            const auto found = links_.find(datagram.link_id);
            if (found != links_.end() && found->second->epoch == datagram.frame.header.epoch)
            {
                auto link = found->second;
                fail(link, error.message());
            }
        }
    }
}

void LnkChannel::schedule_monitor(Clock::time_point deadline)
{
    // cluster_data_io: only an earlier deadline needs to interrupt the current wait.
    if (!stopping_ && deadline < monitor_timer_.expiry())
    {
        monitor_timer_.expires_at(deadline);
    }
}

asio::awaitable<void> LnkChannel::monitor()
{
    while (!stopping_)
    {
        const auto now = Clock::now();
        auto next_deadline = Clock::time_point::max();
        expire_preparations(now);
        for (auto it = links_.begin(); it != links_.end();)
        {
            // Advance before fail() erases the current link and its dependent flows.
            auto link = (it++)->second;
            if ((link->state != LinkState::Ready && now >= link->deadline) ||
                (link->state == LinkState::Ready && now - link->last_response >= heartbeat_timeout))
            {
                fail(link, "node link deadline expired",
                     link->state == LinkState::Ready ? "keepalive" : std::string_view{});
                continue;
            }
            else if (link->state == LinkState::Ready && now - link->last_ping >= heartbeat_interval)
            {
                link->last_ping = now;
                ++link->ping;
                if (!enqueue(link, {LnkFrType::Ping, false, 0, link->epoch, 0, link->ping}))
                {
                    fail(link, "send queue full");
                    continue;
                }
            }
            const auto deadline =
                link->state == LinkState::Ready
                    ? std::min(link->last_ping + heartbeat_interval, link->last_response + heartbeat_timeout)
                    : link->deadline;
            next_deadline = std::min(next_deadline, deadline);
        }
        // A failed Link may have closed Flows and added retirement deadlines above.
        for (const auto &[id, flow] : flows_)
        {
            if (flow->state == NodeFlow::State::Prepared)
            {
                next_deadline = std::min(next_deadline, flow->prepare_deadline);
            }
        }
        for (const auto &[id, deadline] : retired_)
        {
            next_deadline = std::min(next_deadline, deadline);
        }
        monitor_timer_.expires_at(next_deadline);
        auto [error] = co_await monitor_timer_.async_wait(use_nothrow_awaitable);
        if (error && error != asio::error::operation_aborted)
        {
            throw asio::system_error(error, "Node link monitor timer failed");
        }
    }
}

asio::awaitable<void> LnkChannel::stop()
{
    // cluster_data_io: draining must finish even if the caller cancels its stop request.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    stopping_ = true;
    while (!flows_.empty())
    {
        close_flow(flows_.begin()->first, "node stopping");
    }
    retired_.clear();
    events_.cancel();
    events_.close();
    rollback();
    monitor_timer_.cancel();
    udp_writes_.close();
    while (!links_.empty())
    {
        close(links_.begin()->first);
    }
    for (auto &socket : accepting_)
    {
        asio::error_code ignored;
        socket->close(ignored);
    }
    if (tasks_ == 0)
    {
        tasks_done_.notify_all();
    }
    co_await tasks_done_.wait();
}
