#include "xfr_channel.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>

namespace
{
using namespace std::chrono_literals;
using udp = asio::ip::udp;

void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

struct Sockets
{
    asio::io_context io{1};
    const udp::endpoint loopback{asio::ip::address_v4::loopback(), 0};
    udp::socket target{io, loopback}, transfer{io, loopback};
    udp::socket target_peer{io, loopback}, transfer_peer{io, loopback};
    const DatagramHeader::Buffer header = DatagramHeader::encode(0x0102030405060708ULL);
    std::future<void> relay;

    Sockets()
    {
        target.connect(target_peer.local_endpoint());
        target_peer.connect(target.local_endpoint());
        transfer.connect(transfer_peer.local_endpoint());
        transfer_peer.connect(transfer.local_endpoint());
        relay = asio::co_spawn(io, relay_udp_connected(target, transfer, header), asio::use_future);
        io.poll(); // Both directions are waiting before injecting a packet/error.
    }

    ~Sockets()
    {
        asio::error_code ignored;
        target.close(ignored);
        transfer.close(ignored);
        target_peer.close(ignored);
        transfer_peer.close(ignored);
        io.restart();
        io.run_for(100ms);
    }

    template <class T> void wait(std::future<T> &result)
    {
        io.restart();
        const auto deadline = std::chrono::steady_clock::now() + 500ms;
        while (result.wait_for(0ms) != std::future_status::ready && std::chrono::steady_clock::now() < deadline)
            io.run_for(1ms);
        require(result.wait_for(0ms) == std::future_status::ready, "UDP operation did not complete");
    }

    BytesBuf receive(udp::socket &peer)
    {
        auto result = asio::co_spawn(io, [&peer]() -> asio::awaitable<BytesBuf> {
            BytesBuf bytes(DatagramHeader::maximum_wire_payload + 1);
            const auto size = co_await peer.async_receive(asio::buffer(bytes), asio::use_awaitable);
            bytes.resize(size);
            co_return bytes;
        }, asio::use_future);
        wait(result);
        return result.get();
    }

    BytesBuf frame(const BytesBuf &payload) const
    {
        BytesBuf result(header.begin(), header.end());
        result.insert(result.end(), payload.begin(), payload.end());
        return result;
    }

    void require_no_packet(udp::socket &peer)
    {
        peer.non_blocking(true);
        std::array<std::uint8_t, 1> buffer{};
        asio::error_code error;
        peer.receive(asio::buffer(buffer), 0, error);
        peer.non_blocking(false);
        require(error == asio::error::would_block || error == asio::error::try_again,
                "Dropped UDP packet was forwarded or duplicated");
    }
};

void payload_boundaries()
{
    Sockets sockets;
    for (const auto size : {std::size_t{0}, std::size_t{1}, std::size_t{4097},
                           DatagramHeader::maximum_user_payload})
    {
        BytesBuf payload(size);
        for (std::size_t i = 0; i < size; ++i)
            payload[i] = static_cast<std::uint8_t>(i * 31);
        const auto wire = sockets.frame(payload);
        sockets.target_peer.send(asio::buffer(payload));
        require(sockets.receive(sockets.transfer_peer) == wire, "UDP payload/header changed on send");
        sockets.transfer_peer.send(asio::buffer(wire));
        require(sockets.receive(sockets.target_peer) == payload, "UDP payload/header changed on receive");
    }
    require(sockets.relay.wait_for(0ms) != std::future_status::ready, "Valid packets terminated UDP relay");
}

void invalid_packets_do_not_stop_relay()
{
    Sockets sockets;
    const BytesBuf short_header(DatagramHeader::length - 1, 1);
    auto wrong_session = sockets.frame({11, 22});
    wrong_session[0] ^= 0xff;
    const BytesBuf zero_session(DatagramHeader::length, 0);
    for (const auto &invalid : {short_header, wrong_session, zero_session})
        sockets.transfer_peer.send(asio::buffer(invalid));
    // UDP preserves packet boundaries: the marker must be the first delivered
    // packet after invalid input, so no timeout-based drop assertion is needed.
    const BytesBuf marker{77, 88, 99};
    sockets.transfer_peer.send(asio::buffer(sockets.frame(marker)));
    require(sockets.receive(sockets.target_peer) == marker, "Invalid session/header was forwarded");
    sockets.require_no_packet(sockets.target_peer);

    const BytesBuf oversized(DatagramHeader::maximum_user_payload + 1, 42);
    sockets.target_peer.send(asio::buffer(oversized));
    sockets.target_peer.send(asio::buffer(marker));
    require(sockets.receive(sockets.transfer_peer) == sockets.frame(marker),
            "Oversized payload was forwarded or stopped the next packet");
    sockets.require_no_packet(sockets.transfer_peer);
    require(sockets.relay.wait_for(0ms) != std::future_status::ready, "Invalid packet terminated UDP relay");
}

void io_failure_drains_other_direction()
{
    for (const bool close_target : {false, true})
    {
        Sockets sockets;
        (close_target ? sockets.target : sockets.transfer).close();
        sockets.wait(sockets.relay);
        bool failed = false;
        try
        {
            sockets.relay.get();
        }
        catch (const asio::system_error &error)
        {
            failed = error.code() == asio::error::operation_aborted;
        }
        require(failed, "UDP relay did not preserve the original socket error");
    }
}
} // namespace

int main()
{
    try
    {
        payload_boundaries();
        invalid_packets_do_not_stop_relay();
        io_failure_drains_other_direction();
        std::cout << "[PASS] UDP payload boundaries, invalid packet recovery and bidirectional failure drain\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
