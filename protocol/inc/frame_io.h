#ifndef RELAYWEAVE_FRAME_IO_H
#define RELAYWEAVE_FRAME_IO_H
#include "tls_channel.h"

template <typename Stream>
asio::awaitable<CtrlMessage> read_ctrl_frame(Stream &stream, std::chrono::steady_clock::time_point deadline)
{
    const auto remaining = [&]() {
        const auto value = deadline - std::chrono::steady_clock::now();
        if (value <= std::chrono::steady_clock::duration::zero())
        {
            throw asio::system_error(asio::error::timed_out, "frame read deadline");
        }
        return value;
    };
    WireMessage::Header header{};
    co_await asio::async_read(stream, asio::buffer(header), asio::cancel_after(remaining(), asio::use_awaitable));
    BytesBuf payload(WireMessage::decode_length(header));
    co_await asio::async_read(stream, asio::buffer(payload), asio::cancel_after(remaining(), asio::use_awaitable));
    co_return CtrlMessage::deserialize(payload);
}

inline CtrlMessage decode_ctrl_datagram(std::span<const std::uint8_t> datagram)
{
    if (datagram.size() < WireMessage::header_length)
    {
        throw std::invalid_argument("datagram frame header truncated");
    }
    const auto length = WireMessage::decode_length(datagram.first<WireMessage::header_length>());
    if (length != datagram.size() - WireMessage::header_length)
    {
        throw std::invalid_argument("datagram frame length mismatch");
    }
    return CtrlMessage::deserialize(datagram.subspan(WireMessage::header_length));
}

#endif
