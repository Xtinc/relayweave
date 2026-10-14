#ifndef RELAYWEAVE_TEST_FLOW_IO_H
#define RELAYWEAVE_TEST_FLOW_IO_H

#include "flow_test_state.h"
#include <variant>

enum class FlowSendStatus
{
    Queued,
    WouldBlock,
    Dropped,
    Closed,
    Invalid
};

// Fixture-only request/result envelope; production endpoints carry bytes and EOF.
struct FlowFrame
{
    std::uint64_t epoch = 0;
    std::uint64_t flow_id = 0;
    bool reverse = false;
    LnkFrType kind = LnkFrType::Data;
    BytesBuf payload;
    std::string reason;
    [[nodiscard]] bool validate() const noexcept;
};

inline bool FlowFrame::validate() const noexcept
{
    if (!epoch || !flow_id)
    {
        return false;
    }
    switch (kind)
    {
    case LnkFrType::Data:
        return reason.empty() && payload.size() <= LnkFrameHeader::maximum_payload;
    case LnkFrType::Fin:
        return payload.empty() && reason.empty();
    case LnkFrType::Reset:
        return payload.empty() && reason.size() <= LnkFrameHeader::maximum_reason;
    default:
        return false;
    }
}

TEST_MEMBER(FlowProtocol, stream_)
TEST_MEMBER(FlowProtocol, datagram_)
TEST_MEMBER(FlowProtocol, payload_pool_)
TEST_MEMBER(LnkChannel, node_id_)
using FlowStream = lnk::FlowStream;
using FlowDatagram = lnk::FlowDatagram;
TEST_MEMBER(FlowStream, send_finished_)
TEST_MEMBER(FlowStream, reverse_)
TEST_MEMBER(FlowDatagram, reverse_)

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
            if (it == flows.end() || it->second->epoch != epoch || it->second->phase != lnk::FlowState::Phase::Active)
            {
                throw std::runtime_error("flow is not active");
            }
            const auto flow = it->second;
            const bool direction = reverse.value_or(flow->previous != 0);
            if ((direction ? flow->next : flow->previous) != 0)
            {
                throw std::invalid_argument("invalid endpoint direction");
            }
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

inline FlowSendStatus send(std::shared_ptr<FlowProtocol> channel, FlowFrame frame)
{
    const auto &flows = test_access::FlowProtocol_flows_(*channel);
    const auto it = flows.find(frame.flow_id);
    if (it == flows.end() || it->second->epoch != frame.epoch || it->second->phase != lnk::FlowState::Phase::Active)
    {
        return FlowSendStatus::Closed;
    }
    const auto flow = it->second;
    if (!frame.validate() || (frame.reverse ? flow->next : flow->previous) ||
        (flow->transport == RelayProtocol::Tcp ? (flow->stream->directions[std::size_t(frame.reverse)].finished && frame.kind != LnkFrType::Reset)
                      : frame.kind != LnkFrType::Data))
    {
        return FlowSendStatus::Invalid;
    }
    if (flow->transport == RelayProtocol::Udp)
    {
        lnk::PooledBuffer data(test_access::FlowProtocol_payload_pool_(*channel), frame.payload);
        return test_access::FlowProtocol_datagram_(*channel).try_send(flow, frame.reverse, data)
                   ? FlowSendStatus::Queued : FlowSendStatus::Dropped;
    }
    if (frame.kind == LnkFrType::Reset)
    {
        test_access::FlowProtocol_stream_(*channel).reset(flow, frame.reverse, std::move(frame.reason));
        return FlowSendStatus::Queued;
    }
    auto data = frame.kind == LnkFrType::Data
        ? std::optional<lnk::PooledBuffer>(lnk::PooledBuffer(test_access::FlowProtocol_payload_pool_(*channel), frame.payload))
        : std::nullopt;
    return test_access::FlowProtocol_stream_(*channel).try_send(flow, frame.reverse, data)
               ? FlowSendStatus::Queued : FlowSendStatus::WouldBlock;
}

inline asio::awaitable<FlowSendStatus> submit(std::shared_ptr<FlowProtocol> channel, FlowFrame frame)
{
    if (!frame.validate())
    {
        co_return FlowSendStatus::Invalid;
    }
    std::optional<Port> endpoint;
    try
    {
        endpoint = co_await port(channel, frame.epoch, frame.flow_id, frame.reverse);
    }
    catch (const std::invalid_argument &)
    {
        co_return FlowSendStatus::Invalid;
    }
    catch (const std::runtime_error &)
    {
        co_return FlowSendStatus::Closed;
    }
    if (auto *stream = std::get_if<std::shared_ptr<lnk::FlowStream>>(&*endpoint))
    {
        co_return co_await asio::co_spawn((*stream)->executor(),
            [stream = *stream, frame = std::move(frame)]() mutable -> asio::awaitable<FlowSendStatus> {
                if (frame.kind == LnkFrType::Reset)
                {
                    stream->reset(std::move(frame.reason));
                    co_return FlowSendStatus::Queued;
                }
                if (test_access::FlowStream_send_finished_(*stream))
                {
                    co_return FlowSendStatus::Invalid;
                }
                if (frame.kind == LnkFrType::Fin)
                {
                    co_await stream->finish_send();
                }
                else
                {
                co_await stream->send(stream->copy_payload(frame.payload));
                }
                co_return FlowSendStatus::Queued;
            }, asio::use_awaitable);
    }
    if (frame.kind != LnkFrType::Data)
    {
        co_return FlowSendStatus::Invalid;
    }
    const auto datagram = std::get<std::shared_ptr<lnk::FlowDatagram>>(*endpoint);
    co_return co_await asio::co_spawn(datagram->executor(),
        [datagram, payload = std::move(frame.payload)]() mutable -> asio::awaitable<FlowSendStatus> {
            co_return datagram->try_send(payload) ? FlowSendStatus::Queued : FlowSendStatus::Dropped;
        }, asio::use_awaitable);
}

inline asio::awaitable<FlowSendStatus> submit_data(std::shared_ptr<FlowProtocol> channel,
                                                 std::uint64_t epoch, std::uint64_t id, bool reverse,
                                                 std::span<const std::uint8_t> payload)
{
    co_return co_await submit(channel, {epoch, id, reverse, LnkFrType::Data, BytesBuf(payload.begin(), payload.end())});
}

inline asio::awaitable<FlowFrame> receive(std::shared_ptr<FlowProtocol> channel, std::uint64_t epoch,
                                         std::uint64_t id, bool complete = true)
{
    auto endpoint = co_await port(channel, epoch, id);
    if (auto *stream = std::get_if<std::shared_ptr<lnk::FlowStream>>(&endpoint))
    {
        co_return co_await asio::co_spawn((*stream)->executor(),
            [stream = *stream, complete, epoch, id]() -> asio::awaitable<FlowFrame> {
                auto data = co_await stream->receive();
                FlowFrame frame{epoch, id, !test_access::FlowStream_reverse_(*stream),
                                data ? LnkFrType::Data : LnkFrType::Fin};
                if (data)
                {
                    const auto bytes = data->bytes();
                    frame.payload.assign(bytes.begin(), bytes.end());
                }
                if (complete && data)
                {
                    data.reset();
                    stream->complete_receive();
                }
                co_return frame;
            }, asio::use_awaitable);
    }
    const auto datagram = std::get<std::shared_ptr<lnk::FlowDatagram>>(endpoint);
    co_return co_await asio::co_spawn(datagram->executor(),
        [datagram, epoch, id]() -> asio::awaitable<FlowFrame> {
            auto data = co_await datagram->receive();
            const auto bytes = data.bytes();
            co_return FlowFrame{epoch, id, !test_access::FlowDatagram_reverse_(*datagram),
                                LnkFrType::Data, BytesBuf(bytes.begin(), bytes.end())};
        }, asio::use_awaitable);
}
} // namespace test_flow
#endif
