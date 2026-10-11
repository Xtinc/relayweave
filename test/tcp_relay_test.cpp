#include "xfr_channel.h"

#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

struct Sockets
{
    asio::io_context io{1};
    tcp::socket left{io}, right{io}, left_peer{io}, right_peer{io};

    Sockets()
    {
        tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
        left_peer.connect(acceptor.local_endpoint());
        acceptor.accept(left);
        right_peer.connect(acceptor.local_endpoint());
        acceptor.accept(right);
    }

    void poll()
    {
        io.restart();
        io.poll();
    }

    void require_failure(std::future<void> &result, asio::error_code expected)
    {
        io.restart();
        io.run_for(250ms);
        const bool drained = result.wait_for(0ms) == std::future_status::ready;
        // Close pending socket I/O even when the relay failed to cancel it.
        left.close();
        right.close();
        io.restart();
        io.run_for(250ms);
        require(drained, "TCP failure left the opposite direction waiting");
        bool failed = false;
        try
        {
            result.get();
        }
        catch (const asio::system_error &error)
        {
            failed = error.code() == expected;
        }
        require(failed, "TCP relay lost the original I/O error");
    }
};

void read_failure()
{
    for (const bool fail_left : {false, true})
    {
        Sockets sockets;
        auto result = asio::co_spawn(sockets.io, relay_tcp(sockets.left, sockets.right), asio::use_future);
        sockets.poll();
        auto &peer = fail_left ? sockets.left_peer : sockets.right_peer;
        peer.set_option(asio::socket_base::linger(true, 0));
        peer.close(); // RST, while the other direction is idle.
        sockets.require_failure(result, asio::error::connection_reset);
    }
}

void write_failure()
{
    for (const bool fail_left : {false, true})
    {
        Sockets sockets;
        auto result = asio::co_spawn(sockets.io, relay_tcp(sockets.left, sockets.right), asio::use_future);
        sockets.poll();
        (fail_left ? sockets.left : sockets.right).shutdown(tcp::socket::shutdown_send);
        asio::write(fail_left ? sockets.right_peer : sockets.left_peer, asio::buffer("request", 7));
        sockets.require_failure(result, asio::error::broken_pipe);
    }
}

void failure_during_limit_wait()
{
    for (const bool limit_left : {false, true})
    {
        Sockets sockets;
        TokenBucket limiter(1, 0);
        auto result = asio::co_spawn(sockets.io,
            relay_tcp(sockets.left, sockets.right, limit_left ? &limiter : nullptr,
                      limit_left ? nullptr : &limiter), asio::use_future);
        std::array<char, 60> payload{}; // One minute of token debt.
        asio::write(limit_left ? sockets.left_peer : sockets.right_peer, asio::buffer(payload));
        sockets.poll(); // The limited direction has entered its timer wait.
        auto &peer = limit_left ? sockets.right_peer : sockets.left_peer;
        peer.set_option(asio::socket_base::linger(true, 0));
        peer.close();
        sockets.require_failure(result, asio::error::connection_reset);
    }
}

void half_close()
{
    for (const bool request_from_left : {false, true})
    {
        Sockets sockets;
        TrafficCounter outgoing, incoming;
        auto result = asio::co_spawn(sockets.io,
            relay_tcp(sockets.left, sockets.right, nullptr, nullptr, &outgoing, &incoming), asio::use_future);
        auto &requester = request_from_left ? sockets.left_peer : sockets.right_peer;
        auto &responder = request_from_left ? sockets.right_peer : sockets.left_peer;
        asio::write(requester, asio::buffer("request", 7));
        requester.shutdown(tcp::socket::shutdown_send);
        sockets.poll();
        require(result.wait_for(0ms) != std::future_status::ready,
                "TCP EOF terminated the reverse direction");
        responder.non_blocking(true);
        std::array<char, 7> request{};
        asio::error_code error;
        const auto received = asio::read(responder, asio::buffer(request), error);
        require(!error && received == request.size() && std::string(request.data(), request.size()) == "request",
                "TCP request was not drained before FIN");
        char byte;
        responder.read_some(asio::buffer(&byte, 1), error);
        require(error == asio::error::eof, "TCP EOF did not half-close the destination");
        asio::write(responder, asio::buffer("reply", 5));
        responder.shutdown(tcp::socket::shutdown_send);
        sockets.poll();
        require(result.wait_for(0ms) == std::future_status::ready, "TCP did not drain both FIN directions");
        result.get();
        requester.non_blocking(true);
        std::array<char, 5> reply{};
        const auto replied = asio::read(requester, asio::buffer(reply), error);
        require(!error && replied == reply.size() && std::string(reply.data(), reply.size()) == "reply",
                "TCP response was lost after request EOF");
        requester.read_some(asio::buffer(&byte, 1), error);
        require(error == asio::error::eof, "TCP reverse FIN was not forwarded");
        require(outgoing.total() == (request_from_left ? 7u : 5u) &&
                incoming.total() == (request_from_left ? 5u : 7u), "TCP traffic accounting changed");
    }
}
}

int main()
{
    try
    {
        read_failure();
        write_failure();
        failure_during_limit_wait();
        half_close();
        std::cout << "[PASS] TCP I/O failures drain both directions; EOF preserves half-close\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
