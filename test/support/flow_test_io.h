#ifndef RELAYWEAVE_TEST_FLOW_IO_H
#define RELAYWEAVE_TEST_FLOW_IO_H

#include "flow_test_state.h"
#include <variant>

TEST_MEMBER(FlowProtocol, stream_)
TEST_MEMBER(FlowProtocol, datagram_)
TEST_MEMBER(FlowProtocol, payload_pool_)
TEST_MEMBER(LnkChannel, node_id_)
using FlowStream = lnk::FlowStream;
using FlowDatagram = lnk::FlowDatagram;
TEST_MEMBER(FlowProtocol, require_endpoint_flow)

namespace test_flow
{
using Port = std::variant<std::shared_ptr<lnk::FlowStream>, std::shared_ptr<lnk::FlowDatagram>>;

inline void prepare_link(std::shared_ptr<FlowProtocol> protocol, const njson& p)
{
    protocol->links().prepare({p.at("id").get<std::uint64_t>(), p.at("epoch").get<std::uint64_t>(),
        parse_relay_protocol(p.at("transport").get<std::string>()), p.at("peer").get<std::string>(),
        p.at("peer_address").get<std::string>(), p.at("token").get<std::string>(),
        p.at("tcp_port").get<std::uint16_t>(), p.at("udp_port").get<std::uint16_t>(),
        test_access::LnkChannel_node_id_(protocol->links()) == p.at("left").get<std::string>()});
}

inline void prepare(std::shared_ptr<FlowProtocol> protocol, const njson& p)
{
    const auto path = p.at("path").get<std::vector<std::string>>();
    const auto links = p.at("links").get<std::vector<std::uint64_t>>();
    const auto index = std::size_t(std::ranges::find(path, test_access::LnkChannel_node_id_(protocol->links())) - path.begin());
    lnk::FlowSpec spec{p.at("flow_id").get<std::uint64_t>(), p.at("epoch").get<std::uint64_t>(),
                       parse_relay_protocol(p.at("transport").get<std::string>())};
    spec.prepare_timeout = std::chrono::milliseconds(p.at("ttl_ms").get<int>());
    if (index)
    {
        spec.previous = links.at(index - 1);
        spec.previous_peer = path.at(index - 1);
    }
    if (index + 1 < path.size())
    {
        spec.next = links.at(index);
        spec.next_peer = path.at(index + 1);
    }
    protocol->prepare_flow(std::move(spec));
}

inline asio::awaitable<CtrlMessage> event(std::shared_ptr<FlowProtocol> protocol)
{
    auto event = co_await protocol->receive_event();
    CtrlCommand command;
    switch (event.kind)
    {
    case lnk::EventKind::LinkPrepared: command = CtrlCommand::LinkPrepared; break;
    case lnk::EventKind::LinkReady: command = CtrlCommand::LinkReady; break;
    case lnk::EventKind::LinkError: command = CtrlCommand::LinkError; break;
    case lnk::EventKind::LinkClosed: command = CtrlCommand::LinkClosed; break;
    case lnk::EventKind::FlowPrepared: command = CtrlCommand::FlowPrepared; break;
    case lnk::EventKind::FlowCommitted: command = CtrlCommand::FlowCommitted; break;
    case lnk::EventKind::FlowError: command = CtrlCommand::FlowError; break;
    case lnk::EventKind::FlowClosed: command = CtrlCommand::FlowClosed; break;
    }
    co_return CtrlMessage(command, njson{{"epoch", event.epoch}, {"id", event.id}, {"flow_id", event.id},
                                         {"stage", event.stage}, {"reason", event.reason}});
}

// Fixtures cross once to retrieve/bind a synthetic endpoint; all actual traffic uses production ports.
inline asio::awaitable<Port> port(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch, std::uint64_t id,
                                  std::optional<bool> reverse = {})
{
    const auto transfer = co_await asio::this_coro::executor;
    co_return co_await asio::co_spawn(channel->executor(),
        [channel, epoch, id, reverse, transfer]() -> asio::awaitable<Port> {
            const auto &flows = test_access::FlowProtocol_flows_(*channel);
            const auto it = flows.find(id);
            const auto transport = it == flows.end() ? RelayProtocol::Tcp : it->second->transport;
            const bool direction = reverse.value_or(it != flows.end() && it->second->previous != 0);
            const auto flow = test_access::FlowProtocol_require_endpoint_flow(*channel, epoch, id, direction, transport);
            if (flow->transport == RelayProtocol::Tcp)
            {
                if (flow->stream->port)
                {
                    co_return flow->stream->port;
                }
                co_return channel->bind_stream_flow(epoch, id, direction, transfer);
            }
            if (flow->datagram.port)
            {
                co_return flow->datagram.port;
            }
            co_return channel->bind_datagram_flow(epoch, id, direction, transfer);
        }, asio::use_awaitable);
}

// Same-executor admission probes are only for tests that fill a window without yielding.
// They call production admission directly; endpoint validation belongs to bind_*_flow.
inline bool try_send_data(std::shared_ptr<FlowProtocol> channel, std::uint64_t id, bool reverse, BytesBuf payload)
{
    const auto flow = test_access::FlowProtocol_flows_(*channel).at(id);
    lnk::PooledBuffer data(test_access::FlowProtocol_payload_pool_(*channel), payload);
    if (flow->transport == RelayProtocol::Udp)
    {
        return test_access::FlowProtocol_datagram_(*channel).try_send(flow, reverse, data);
    }
    std::optional<lnk::PooledBuffer> chunk(std::move(data));
    return test_access::FlowProtocol_stream_(*channel).try_send(flow, reverse, chunk);
}

inline bool try_finish(std::shared_ptr<FlowProtocol> channel, std::uint64_t id, bool reverse)
{
    const auto flow = test_access::FlowProtocol_flows_(*channel).at(id);
    std::optional<lnk::PooledBuffer> eof;
    return test_access::FlowProtocol_stream_(*channel).try_send(flow, reverse, eof);
}

inline asio::awaitable<bool> send_data(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch,
                                      std::uint64_t id, bool reverse, BytesBuf payload)
{
    auto endpoint = co_await port(channel, epoch, id, reverse);
    if (auto *stream = std::get_if<std::shared_ptr<lnk::FlowStream>>(&endpoint))
    {
        co_await asio::co_spawn((*stream)->executor(),
            [stream = *stream, payload = std::move(payload)]() -> asio::awaitable<void> {
                co_await stream->send(lnk::PooledBuffer(stream->pool, payload));
            }, asio::use_awaitable);
        co_return true;
    }
    const auto datagram = std::get<std::shared_ptr<lnk::FlowDatagram>>(endpoint);
    co_return co_await asio::co_spawn(datagram->executor(),
        [datagram, payload = std::move(payload)]() -> asio::awaitable<bool> {
            co_return datagram->try_send(payload);
        }, asio::use_awaitable);
}

inline asio::awaitable<void> finish(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch,
                                   std::uint64_t id, bool reverse)
{
    const auto stream = std::get<std::shared_ptr<lnk::FlowStream>>(co_await port(channel, epoch, id, reverse));
    co_await asio::co_spawn(stream->executor(), stream->finish_send(), asio::use_awaitable);
}

inline asio::awaitable<void> reset(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch,
                                  std::uint64_t id, bool reverse, std::string reason = {})
{
    const auto stream = std::get<std::shared_ptr<lnk::FlowStream>>(co_await port(channel, epoch, id, reverse));
    co_await asio::co_spawn(stream->executor(),
        [stream, reason = std::move(reason)]() mutable -> asio::awaitable<void> {
            stream->reset(std::move(reason));
            co_return;
        }, asio::use_awaitable);
}

// Copy bytes for assertions and preserve the production distinction between empty DATA and EOF.
inline asio::awaitable<std::optional<BytesBuf>> receive(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch,
                                                       std::uint64_t id, bool complete = true)
{
    auto endpoint = co_await port(channel, epoch, id);
    if (auto *stream = std::get_if<std::shared_ptr<lnk::FlowStream>>(&endpoint))
    {
        co_return co_await asio::co_spawn((*stream)->executor(),
            [stream = *stream, complete]() -> asio::awaitable<std::optional<BytesBuf>> {
                auto data = co_await stream->receive();
                if (!data)
                {
                    co_return std::nullopt;
                }
                const auto bytes = data->bytes();
                BytesBuf payload(bytes.begin(), bytes.end());
                if (complete)
                {
                    data.reset();
                    stream->complete_receive();
                }
                co_return payload;
            }, asio::use_awaitable);
    }
    const auto datagram = std::get<std::shared_ptr<lnk::FlowDatagram>>(endpoint);
    co_return co_await asio::co_spawn(datagram->executor(),
        [datagram]() -> asio::awaitable<std::optional<BytesBuf>> {
            auto data = co_await datagram->receive();
            const auto bytes = data.bytes();
            co_return BytesBuf(bytes.begin(), bytes.end());
        }, asio::use_awaitable);
}

template <class Exception, class T>
asio::awaitable<bool> throws(asio::awaitable<T> operation)
{
    try
    {
        co_await std::move(operation);
    }
    catch (const Exception &)
    {
        co_return true;
    }
    co_return false;
}
} // namespace test_flow
#endif
