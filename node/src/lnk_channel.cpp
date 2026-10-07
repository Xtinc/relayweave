#include "lnk_channel.h"
#include "frame_io.h"
#include <cassert>
using namespace std::chrono_literals;

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

LnkChannel::NodeLink::NodeLink(asio::any_io_executor executor, njson values)
    : params(std::move(values)), id(params.at("id").get<std::uint64_t>()),
      epoch(params.at("epoch").get<std::uint64_t>()),
      transport(parse_relay_protocol(params.at("transport").get_ref<const std::string &>())), resolver(executor)
{
    if (transport == RelayProtocol::Tcp)
    {
        socket.emplace(executor);
        writes.emplace(executor, 100);
    }
}

LnkChannel::LnkChannel(asio::any_io_executor executor, std::string node_id, std::string tcp_address,
                       std::uint16_t tcp_port, std::string udp_address, std::uint16_t udp_port)
    : executor_(executor), node_id_(std::move(node_id)), acceptor_(executor), udp_socket_(executor),
      tcp_address_(std::move(tcp_address)), udp_address_(std::move(udp_address)), tcp_port_(tcp_port),
      udp_port_(udp_port), events_(executor, 4096), udp_writes_(executor, 100), monitor_timer_(executor),
      done_(executor)
{
    done_.expires_at(Clock::time_point::max());
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
        if (self->stopping_)
        {
            return;
        }
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
        if (--self->tasks_ == 0)
        {
            self->done_.cancel();
        }
    });
}

void LnkChannel::notify(const std::shared_ptr<NodeLink> &link, CtrlCommand command, std::string reason)
{
    auto values = link->params;
    values["stage"] = link->stage;
    values["reason"] = std::move(reason);
    emit(CtrlMessage(command, std::move(values)));
}

void LnkChannel::fail(const std::shared_ptr<NodeLink> &link, std::string reason)
{
    if (link->closed)
    {
        return;
    }
    notify(link, CtrlCommand::LinkError, std::move(reason));
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
    link->closed = true;
    if (link->ready)
    {
        PROXY_INFO_PRINT("Node link [x] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(id),
                         link->params.at("transport").get_ref<const std::string &>().c_str(),
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
    notify(link, CtrlCommand::LinkClosed);
}

void LnkChannel::prepare(njson params)
{
    const auto id = config::require_unsigned(params, "id", true);
    if (stopping_ || links_.contains(id))
    {
        return;
    }
    auto link = std::make_shared<NodeLink>(executor_, std::move(params));
    links_.emplace(id, link);
    if (link->transport == RelayProtocol::Udp)
    {
        spawn(prepare_udp(link));
    }
    else
    {
        notify(link, CtrlCommand::LinkPrepared);
    }
}

asio::awaitable<void> LnkChannel::prepare_udp(std::shared_ptr<NodeLink> link)
{
    try
    {
        link->stage = "resolve";
        const auto endpoints = co_await link->resolver.async_resolve(
            link->params.at("peer_address").get<std::string>(),
            std::to_string(link->params.at("udp_port").get<std::uint16_t>()),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            throw std::runtime_error("resolver returned no endpoints");
        }
        link->endpoint = {endpoints.begin()->endpoint().address(), link->params.at("udp_port").get<std::uint16_t>()};
        if (link->endpoint.protocol() != udp_socket_.local_endpoint().protocol())
        {
            throw std::runtime_error("UDP peer address family differs from listener");
        }
        link->stage = "prepare";
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
    if (stopping_ || found == links_.end() || found->second->connected)
    {
        return;
    }
    auto link = found->second;
    link->connected = true;
    if (link->transport == RelayProtocol::Udp || node_id_ == link->params.at("left").get<std::string>())
    {
        spawn(establish(link));
    }
}

CtrlMessage LnkChannel::attach_message(const NodeLink &link, CtrlCommand command) const
{
    return CtrlMessage(command, njson{{"id", link.params.at("id")},
                                      {"epoch", link.params.at("epoch")},
                                      {"node", node_id_},
                                      {"token", link.params.at("token")},
                                      {"data_version", LnkFrameHeader::version}});
}

bool LnkChannel::matches(const NodeLink &link, const CtrlMessage &message) const
{
    if (!message.params || !message.params->is_object())
    {
        return false;
    }
    const auto &p = *message.params;
    return p.value("id", std::uint64_t(0)) == link.params.at("id").get<std::uint64_t>() &&
           p.value("epoch", std::uint64_t(0)) == link.params.at("epoch").get<std::uint64_t>() &&
           p.value("node", std::string{}) == link.params.at("peer").get<std::string>() &&
           p.value("token", std::string{}) == link.params.at("token").get<std::string>() &&
           config::require_unsigned(p, "data_version", true) == LnkFrameHeader::version;
}

void LnkChannel::mark_ready(const std::shared_ptr<NodeLink> &link)
{
    if (link->ready || link->closed || !link->attached || !link->acknowledged)
    {
        return;
    }
    link->ready = true;
    PROXY_INFO_PRINT("Node link [+] id=%llu transport=%s peer=%s", static_cast<unsigned long long>(link->id),
                     link->params.at("transport").get_ref<const std::string &>().c_str(),
                     link->params.at("peer").get_ref<const std::string &>().c_str());
    link->last_response = link->last_ping = Clock::now();
    link->stage = "ready";
    notify(link, CtrlCommand::LinkReady);
}

asio::awaitable<void> LnkChannel::establish(std::shared_ptr<NodeLink> link)
{
    try
    {
        if (link->transport == RelayProtocol::Udp)
        {
            link->stage = "attach";
            auto payload = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
            if (payload.size() > LnkFrameHeader::maximum_payload)
            {
                throw std::invalid_argument("UDP attach exceeds frame body limit");
            }
            if (!enqueue(link, {LnkFrType::Attach, false, static_cast<std::uint32_t>(payload.size()), link->epoch},
                         std::move(payload)))
            {
                throw std::runtime_error("UDP send queue full");
            }
            co_return;
        }
        link->stage = "resolve";
        const auto port = link->params.at("tcp_port").get<std::uint16_t>();
        // A single resolver operation and exactly one selected endpoint connect.
        const auto endpoints = co_await link->resolver.async_resolve(
            link->params.at("peer_address").get<std::string>(), std::to_string(port),
            asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        if (link->closed)
        {
            co_return;
        }
        if (endpoints.empty())
        {
            throw std::runtime_error("resolver returned no endpoints");
        }
        auto endpoint = endpoints.begin()->endpoint();
        link->stage = "connect";
        co_await link->socket->async_connect(endpoint,
                                             asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        link->stage = "attach";
        auto bytes = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttach));
        co_await asio::async_write(*link->socket, asio::buffer(bytes),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        auto response = co_await read_ctrl_frame(*link->socket, link->deadline);
        if (response.type() != CtrlCommand::LinkAttached || !matches(*link, response))
        {
            throw std::runtime_error("TCP attach acknowledgement identity mismatch");
        }
        link->attached = link->acknowledged = true;
        mark_ready(link);
        spawn(write(link));
        co_await read(link);
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
        if (accepting_.size() >= 100)
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
        auto message = co_await read_ctrl_frame(*socket, Clock::now() + 10s);
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
            link->closed || link->attached || !matches(*link, message))
        {
            co_return;
        }
        link->attached = true;
        accepting_.erase(socket);
        *link->socket = std::move(*socket);
        link->stage = "attach";
        auto response = WireMessage::pack(attach_message(*link, CtrlCommand::LinkAttached));
        co_await asio::async_write(*link->socket, asio::buffer(response),
                                   asio::cancel_after(link->deadline - Clock::now(), asio::use_awaitable));
        link->acknowledged = true;
        mark_ready(link);
        spawn(write(link));
        co_await read(link);
    }
    catch (const std::exception &e)
    {
        if (link && link->attached)
        {
            fail(link, e.what());
        }
    }
}

bool LnkChannel::enqueue(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, BytesBuf &&payload)
{
    if (link->closed || stopping_)
    {
        return false;
    }
    const bool data =
        header.kind == LnkFrType::Data || header.kind == LnkFrType::Fin || header.kind == LnkFrType::Reset;
    const bool tcp = link->transport == RelayProtocol::Tcp;
    auto &pending = tcp ? link->pending_data : udp_pending_data_;
    if (data && pending >= 96)
    {
        return false;
    }
    // Private enqueue receives only locally constructed or decoded, validated frames.
    assert(payload.size() == header.body_length);
    QueuedFrame queued{header.encode(), std::move(payload), data};
    bool sent;
    if (tcp)
    {
        sent = link->writes->try_send(asio::error_code{}, std::move(queued));
        if (!sent)
        {
            payload = std::move(queued.payload);
        }
    }
    else
    {
        Datagram packet{std::move(queued), link};
        sent = udp_writes_.try_send(asio::error_code{}, std::move(packet));
        if (!sent)
        {
            payload = std::move(packet.frame.payload);
        }
    }
    if (sent && data)
    {
        ++pending;
    }
    return sent;
}

void LnkChannel::process(const std::shared_ptr<NodeLink> &link, const LnkFrameHeader &header, BytesBuf payload)
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
        if (header.sequence == link->ping)
        {
            link->last_response = Clock::now();
        }
        break;
    case LnkFrType::Data:
    case LnkFrType::Fin:
    case LnkFrType::Reset: {
        FlowFrame frame{header.epoch, header.id, header.reverse, header.kind};
        if (header.kind == LnkFrType::Reset)
        {
            frame.reason.assign(payload.begin(), payload.end());
        }
        else
        {
            frame.payload = std::move(payload);
        }
        incoming_flow(*link, std::move(frame));
        break;
    }
    default:
        break;
    }
}

asio::awaitable<void> LnkChannel::read(std::shared_ptr<NodeLink> link)
{
    try
    {
        while (!link->closed)
        {
            // cluster_data_io: validate the bounded header before allocating the body.
            LnkFrameHeader::Buffer bytes{};
            const auto deadline = Clock::now() + 20s;
            co_await asio::async_read(*link->socket, asio::buffer(bytes), asio::cancel_after(20s, asio::use_awaitable));
            const auto header = LnkFrameHeader::decode(bytes);
            if (header.kind == LnkFrType::Attach || header.kind == LnkFrType::Attached)
            {
                throw std::invalid_argument("unexpected TCP bootstrap after attach");
            }
            BytesBuf payload(header.body_length);
            co_await asio::async_read(*link->socket, asio::buffer(payload),
                                      asio::cancel_after(deadline - Clock::now(), asio::use_awaitable));
            if (link->closed)
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

asio::awaitable<void> LnkChannel::write(std::shared_ptr<NodeLink> link)
{
    try
    {
        while (!link->closed)
        {
            auto queued = co_await link->writes->async_receive(asio::use_awaitable);
            if (queued.data)
            {
                --link->pending_data;
            }
            const std::array buffers{asio::buffer(queued.header), asio::buffer(queued.payload)};
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
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload> buffer{};
    while (!stopping_)
    {
        udp::endpoint source;
        auto [error, size] =
            co_await udp_socket_.async_receive_from(asio::buffer(buffer), source, use_nothrow_awaitable);
        if (error)
        {
            if (stopping_)
            {
                co_return;
            }
            continue;
        }
        if (size < DatagramHeader::length + LnkFrameHeader::length)
        {
            continue;
        }
        auto id = DatagramHeader::decode(std::span(buffer).first(DatagramHeader::length));
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
        if (link->transport != RelayProtocol::Udp || link->endpoint.port() == 0 || link->endpoint != source)
        {
            continue;
        }
        try
        {
            const auto header =
                LnkFrameHeader::decode(std::span(buffer).subspan<DatagramHeader::length, LnkFrameHeader::length>());
            if (header.epoch != link->epoch ||
                size != DatagramHeader::length + LnkFrameHeader::length + header.body_length)
            {
                continue;
            }
            const auto payload =
                std::span(buffer).subspan(DatagramHeader::length + LnkFrameHeader::length, header.body_length);
            if (header.kind == LnkFrType::Attach || header.kind == LnkFrType::Attached)
            {
                const auto message = decode_ctrl_datagram(payload);
                const auto command =
                    header.kind == LnkFrType::Attach ? CtrlCommand::LinkAttach : CtrlCommand::LinkAttached;
                if (link->ready || message.type() != command || !matches(*link, message))
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
                        throw std::invalid_argument("UDP acknowledgement exceeds frame body limit");
                    }
                    if (!enqueue(
                            link,
                            {LnkFrType::Attached, false, static_cast<std::uint32_t>(response.size()), header.epoch},
                            std::move(response)))
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
                if (!link->ready)
                {
                    continue;
                }
                process(link, header, BytesBuf(payload.begin(), payload.end()));
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
        if (datagram.frame.data)
        {
            --udp_pending_data_;
        }
        auto &link = datagram.link;
        if (link->closed)
        {
            continue;
        }
        const auto link_header = DatagramHeader::encode(link->id);
        const std::array<asio::const_buffer, 3> buffers{asio::buffer(link_header), asio::buffer(datagram.frame.header),
                                                        asio::buffer(datagram.frame.payload)};
        auto [error, size] = co_await udp_socket_.async_send_to(buffers, link->endpoint, use_nothrow_awaitable);
        if (error)
        {
            fail(link, error.message());
        }
    }
}

asio::awaitable<void> LnkChannel::monitor()
{
    while (!stopping_)
    {
        monitor_timer_.expires_after(100ms);
        auto [error] = co_await monitor_timer_.async_wait(use_nothrow_awaitable);
        if (error)
        {
            co_return;
        }
        const auto now = Clock::now();
        expire_preparations(now);
        std::vector<std::pair<std::shared_ptr<NodeLink>, std::string>> expired;
        for (auto &[id, link] : links_)
        {
            if ((!link->ready && now >= link->deadline) || (link->ready && now - link->last_response >= 20s))
            {
                if (link->ready)
                {
                    link->stage = "keepalive";
                }
                expired.emplace_back(link, "node link deadline expired");
            }
            else if (link->ready && now - link->last_ping >= 5s)
            {
                link->last_ping = now;
                ++link->ping;
                if (!enqueue(link, {LnkFrType::Ping, false, 0, link->epoch, 0, link->ping}))
                {
                    expired.emplace_back(link, "send queue full");
                }
            }
        }
        for (auto &[link, reason] : expired)
        {
            fail(link, std::move(reason));
        }
    }
}

asio::awaitable<void> LnkChannel::stop()
{
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
    while (tasks_ != 0)
    {
        co_await done_.async_wait(use_nothrow_awaitable);
    }
}
