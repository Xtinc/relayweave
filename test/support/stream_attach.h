#ifndef RELAYWEAVE_TEST_STREAM_ATTACH_H
#define RELAYWEAVE_TEST_STREAM_ATTACH_H
#include "tls_channel.h"

/** @brief Read the public greeting and authorize this socket through its actual control session. */
inline asio::awaitable<RelayAttach> attach_stream_control(asio::ip::tcp::socket &socket,
    const std::shared_ptr<TLSChannel> &control, RelayAttach request, std::string node = {})
{
    request.connection_id.emplace();
    co_await asio::async_read(socket, asio::buffer(*request.connection_id),
        asio::cancel_after(std::chrono::seconds(2), asio::use_awaitable));
    auto message = RelayAttach::to_msg(request);
    if (!node.empty())
    {
        (*message.params)["node_id"] = std::move(node);
    }
    control->send(std::move(message));
    co_return request;
}
#endif
