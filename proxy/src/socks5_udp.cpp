#include "socks5_udp.h"
#include "proxy_server.h"
#include "tls_channel.h"
#include "asio/experimental/parallel_group.hpp"
#include <algorithm>
#include <array>
#include <optional>
#include <span>
#include <vector>

namespace
{
using udp = asio::ip::udp;
constexpr std::size_t maximum_datagram = 65507;

/** @brief Represent the destination and payload offset of a validated standalone UDP request. */
struct DatagramTarget
{
    udp::endpoint destination;
    std::string host;
    std::size_t payload = 0;
};

/** @brief Normalize IPv4-mapped TCP peer addresses before checking client identity. */
asio::ip::address canonical_address(asio::ip::address address)
{
    if (address.is_v6() && address.to_v6().is_v4_mapped())
    {
        const auto bytes = address.to_v6().to_bytes();
        return asio::ip::address_v4({bytes[12], bytes[13], bytes[14], bytes[15]});
    }
    return address;
}

/** @brief Decode a bounded SOCKS UDP header; unsupported fragments and malformed packets are dropped. */
std::optional<DatagramTarget> parse_datagram(std::span<const std::uint8_t> packet)
{
    if (packet.size() < 4 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0)
    {
        return std::nullopt;
    }
    DatagramTarget target;
    std::size_t offset = 4;
    if (packet[3] == 0x01)
    {
        if (packet.size() < offset + 4 + 2)
        {
            return std::nullopt;
        }
        asio::ip::address_v4::bytes_type bytes;
        std::copy_n(packet.begin() + offset, bytes.size(), bytes.begin());
        target.destination.address(asio::ip::address_v4(bytes));
        offset += bytes.size();
    }
    else if (packet[3] == 0x04)
    {
        if (packet.size() < offset + 16 + 2)
        {
            return std::nullopt;
        }
        asio::ip::address_v6::bytes_type bytes;
        std::copy_n(packet.begin() + offset, bytes.size(), bytes.begin());
        target.destination.address(asio::ip::address_v6(bytes));
        offset += bytes.size();
    }
    else if (packet[3] == 0x03)
    {
        if (packet.size() < offset + 1)
        {
            return std::nullopt;
        }
        const auto length = packet[offset++];
        if (length == 0 || packet.size() < offset + length + 2)
        {
            return std::nullopt;
        }
        const auto name = packet.subspan(offset, length);
        if (std::ranges::any_of(name, [](std::uint8_t ch) { return ch <= 0x20 || ch >= 0x7f; }))
        {
            return std::nullopt;
        }
        target.host.assign(reinterpret_cast<const char *>(name.data()), name.size());
        offset += length;
    }
    else
    {
        return std::nullopt;
    }
    const auto port = static_cast<std::uint16_t>((static_cast<unsigned int>(packet[offset]) << 8) | packet[offset + 1]);
    if (port == 0)
    {
        return std::nullopt;
    }
    target.destination.port(port);
    target.payload = offset + 2;
    return target;
}

/** @brief Encode a reply's actual source address in the standard SOCKS UDP header. */
std::vector<std::uint8_t> reply_header(const udp::endpoint &source)
{
    std::vector<std::uint8_t> header{0, 0, 0};
    if (source.address().is_v4())
    {
        header.push_back(0x01);
        const auto bytes = source.address().to_v4().to_bytes();
        header.insert(header.end(), bytes.begin(), bytes.end());
    }
    else
    {
        header.push_back(0x04);
        const auto bytes = source.address().to_v6().to_bytes();
        header.insert(header.end(), bytes.begin(), bytes.end());
    }
    header.push_back(static_cast<std::uint8_t>(source.port() >> 8));
    header.push_back(static_cast<std::uint8_t>(source.port()));
    return header;
}

}

Socks5UdpAssociation::Socks5UdpAssociation(asio::ip::tcp::socket &control, const ProxyConfig &config)
    : control_(control), relay_(control.get_executor()), outbound_v4_(control.get_executor()),
      outbound_v6_(control.get_executor()), resolver_(control.get_executor()),
      client_(control.remote_endpoint().address(), 0), resolve_timeout_(config.connect_timeout),
      resolved_(config.udp_max_targets, config.udp_target_timeout),
      targets_(config.udp_max_targets, config.udp_target_timeout)
{
}

asio::awaitable<std::uint8_t> Socks5UdpAssociation::open(std::string host, std::uint16_t port)
{
    asio::error_code error;
    const auto expected_address = canonical_address(client_.address());
    const auto requested_address = asio::ip::make_address(host, error);
    if (!error)
    {
        if (!requested_address.is_unspecified() && canonical_address(requested_address) != expected_address)
        {
            co_return 0x02;
        }
    }
    else
    {
        const auto [resolve_error, endpoints] = co_await resolver_.async_resolve(
            host, "0", asio::cancel_after(resolve_timeout_, use_nothrow_awaitable));
        if (resolve_error || std::ranges::none_of(endpoints, [&expected_address](const auto &entry) {
                return canonical_address(entry.endpoint().address()) == expected_address;
            }))
        {
            co_return 0x02;
        }
    }
    if (!control_.is_open())
    {
        co_return 0x01;
    }
    client_.port(port);
    const udp::endpoint local(control_.local_endpoint().address(), 0);
    relay_.open(local.protocol(), error);
    if (!error)
    {
        relay_.bind(local, error);
    }
    if (error)
    {
        co_return 0x01;
    }
    outbound_v4_.open(udp::v4(), error);
    if (!error)
    {
        outbound_v4_.bind(udp::endpoint(udp::v4(), 0), error);
    }
    if (error)
    {
        co_return 0x01;
    }
    outbound_v6_.open(udp::v6(), error);
    if (!error)
    {
        outbound_v6_.bind(udp::endpoint(udp::v6(), 0), error);
    }
    if (error)
    {
        asio::error_code ignored;
        outbound_v6_.close(ignored);
    }
    co_return 0x00;
}

udp::endpoint Socks5UdpAssociation::endpoint() const
{
    return relay_.local_endpoint();
}

void Socks5UdpAssociation::cancel() noexcept
{
    resolver_.cancel();
    asio::error_code ignored;
    relay_.close(ignored);
    outbound_v4_.close(ignored);
    outbound_v6_.close(ignored);
}

asio::awaitable<void> Socks5UdpAssociation::watch_control()
{
    ScopeGuard close_datagrams([this] { cancel(); });
    std::array<std::uint8_t, 1> buffer;
    for (;;)
    {
        const auto [error, size] = co_await control_.async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
        if (error || size == 0)
        {
            co_return;
        }
    }
}

asio::awaitable<void> Socks5UdpAssociation::run()
{
    ScopeGuard close_datagrams([this] { cancel(); });
    const auto executor = control_.get_executor();
    using Receiver = decltype(asio::co_spawn(executor, watch_control(), asio::deferred));
    std::vector<Receiver> receivers;
    receivers.reserve(4);
    receivers.push_back(asio::co_spawn(executor, watch_control(), asio::deferred));
    receivers.push_back(asio::co_spawn(executor, receive_client(), asio::deferred));
    receivers.push_back(asio::co_spawn(executor, receive_target(outbound_v4_), asio::deferred));
    if (outbound_v6_.is_open())
    {
        receivers.push_back(asio::co_spawn(executor, receive_target(outbound_v6_), asio::deferred));
    }
    const auto [order, failures] = co_await asio::experimental::make_parallel_group(std::move(receivers))
        .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
    for (const auto index : order)
    {
        if (!failures[index])
        {
            continue;
        }
        try
        {
            std::rethrow_exception(failures[index]);
        }
        catch (const asio::system_error &failure)
        {
            if (failure.code() != asio::error::operation_aborted && failure.code() != asio::error::bad_descriptor)
            {
                throw;
            }
        }
    }
}

asio::awaitable<void> Socks5UdpAssociation::receive_client()
{
    std::array<std::uint8_t, maximum_datagram + 1> packet;
    for (;;)
    {
        udp::endpoint source;
        const auto [error, size] = co_await relay_.async_receive_from(asio::buffer(packet), source, use_nothrow_awaitable);
        if (error == asio::error::message_size)
        {
            continue;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        if (size > maximum_datagram || canonical_address(source.address()) != canonical_address(client_.address()) ||
            (client_.port() != 0 && source.port() != client_.port()))
        {
            continue;
        }
        const auto target = parse_datagram(std::span(packet).first(size));
        if (!target)
        {
            continue;
        }
        client_ = source;
        auto destination = target->destination;
        if (!target->host.empty())
        {
            const auto key = target->host + '\0' + std::to_string(destination.port());
            if (const auto cached = resolved_.get(key))
            {
                destination = *cached;
            }
            else
            {
                const auto [resolve_error, endpoints] = co_await resolver_.async_resolve(
                    target->host, std::to_string(destination.port()), asio::cancel_after(resolve_timeout_, use_nothrow_awaitable));
                if (resolve_error || endpoints.empty())
                {
                    continue;
                }
                auto chosen = std::ranges::find_if(endpoints, [](const auto &entry) {
                    return entry.endpoint().address().is_v4();
                });
                if (chosen == endpoints.end() && outbound_v6_.is_open())
                {
                    chosen = endpoints.begin();
                }
                if (chosen == endpoints.end())
                {
                    continue;
                }
                destination = chosen->endpoint();
                resolved_.put(key, destination);
            }
        }
        if (destination.address().is_unspecified() || destination.address().is_multicast())
        {
            continue;
        }
        auto &outbound = destination.address().is_v4() ? outbound_v4_ : outbound_v6_;
        if (!outbound.is_open())
        {
            continue;
        }
        targets_.put(destination, true);
        const auto [send_error, sent] = co_await outbound.async_send_to(
            asio::buffer(packet.data() + target->payload, size - target->payload), destination, use_nothrow_awaitable);
        if (send_error == asio::error::operation_aborted || send_error == asio::error::bad_descriptor)
        {
            co_return;
        }
    }
}

asio::awaitable<void> Socks5UdpAssociation::receive_target(udp::socket &socket)
{
    std::array<std::uint8_t, maximum_datagram + 1> payload;
    for (;;)
    {
        udp::endpoint source;
        const auto [error, size] = co_await socket.async_receive_from(asio::buffer(payload), source, use_nothrow_awaitable);
        if (error == asio::error::message_size || error == asio::error::connection_refused ||
            error == asio::error::connection_reset || error == asio::error::network_unreachable ||
            error == asio::error::host_unreachable)
        {
            continue;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        if (!targets_.get(source))
        {
            continue;
        }
        const auto header = reply_header(source);
        if (header.size() + size > maximum_datagram)
        {
            continue;
        }
        const std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(payload.data(), size)};
        const auto [send_error, sent] = co_await relay_.async_send_to(buffers, client_, use_nothrow_awaitable);
        if (send_error == asio::error::operation_aborted || send_error == asio::error::bad_descriptor)
        {
            co_return;
        }
    }
}
