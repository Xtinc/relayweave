#include "relay_agent.h"

#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

std::uint16_t unused_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
}

AgentConfig base_config(std::uint16_t control_port)
{
    AgentConfig config;
    config.host = "127.0.0.1";
    config.port = control_port;
    config.connect_timeout = 200ms;
    config.reconnect_initial_delay = 100ms;
    config.reconnect_max_delay = 200ms;
    config.relay_open_timeout = 200ms;
    return config;
}

void verify_listener_rollback(asio::ssl::context &ssl_context)
{
    asio::io_context control_io(1);
    asio::io_context transfer_io(1);
    const auto forward_port = unused_port(transfer_io);
    auto config = base_config(unused_port(control_io));
    config.forwards.push_back(AgentForwardConfig{"echo", "127.0.0.1", forward_port});
    config.forwards.push_back(AgentForwardConfig{"echo", "invalid-address", 12345});
    auto client = std::make_shared<RelayAgent>(control_io, transfer_io, ssl_context, std::move(config));

    bool failed = false;
    try
    {
        client->start();
    }
    catch (const std::exception &)
    {
        failed = true;
    }
    require(failed, "Client start unexpectedly succeeded with an invalid listener");

    tcp::acceptor probe(transfer_io, tcp::endpoint(asio::ip::address_v4::loopback(), forward_port));
}

void verify_stop_before_start(asio::ssl::context &ssl_context)
{
    asio::io_context control_io(1);
    asio::io_context transfer_io(1);
    auto control_work = asio::make_work_guard(control_io);
    auto transfer_work = asio::make_work_guard(transfer_io);
    auto client = std::make_shared<RelayAgent>(control_io, transfer_io, ssl_context,
                                               base_config(unused_port(control_io)));
    std::weak_ptr<RelayAgent> weak_client = client;

    auto first_stop = asio::co_spawn(control_io, client->async_stop(), asio::use_future);
    auto second_stop = asio::co_spawn(control_io, client->async_stop(), asio::use_future);
    std::thread control_thread([&control_io]() { control_io.run(); });
    std::thread transfer_thread([&transfer_io]() { transfer_io.run(); });

    first_stop.get();
    second_stop.get();
    client.reset();
    control_work.reset();
    transfer_work.reset();
    control_thread.join();
    transfer_thread.join();
    require(weak_client.expired(), "Agent stopped before start was retained by an asynchronous task");
}

asio::awaitable<void> require_disconnected_listener_closes(std::uint16_t forward_port)
{
    const auto executor = co_await asio::this_coro::executor;
    tcp::socket socket(executor);
    co_await socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(), forward_port),
                                  asio::cancel_after(1s, asio::use_awaitable));
    std::array<std::uint8_t, 1> byte{};
    auto [error, _] =
        co_await socket.async_read_some(asio::buffer(byte), asio::cancel_after(1s, use_nothrow_awaitable));
    require(error && error != asio::error::timed_out && error != asio::error::operation_aborted,
            "Disconnected local listener did not close the accepted socket");
}

void verify_stop_and_disconnected_listener(asio::ssl::context &ssl_context)
{
    asio::io_context control_io(1);
    asio::io_context transfer_io(1);
    asio::io_context probe_io(1);
    auto control_work = asio::make_work_guard(control_io);
    auto transfer_work = asio::make_work_guard(transfer_io);
    const auto control_port = unused_port(control_io);
    const auto forward_port = unused_port(transfer_io);
    auto config = base_config(control_port);
    config.forwards.push_back(AgentForwardConfig{"echo", "127.0.0.1", forward_port});
    auto client = std::make_shared<RelayAgent>(control_io, transfer_io, ssl_context, std::move(config));
    std::weak_ptr<RelayAgent> weak_client = client;

    client->start();
    client->start();

    std::thread control_thread([&control_io]() { control_io.run(); });
    std::thread transfer_thread([&transfer_io]() { transfer_io.run(); });
    auto rejected = asio::co_spawn(probe_io, require_disconnected_listener_closes(forward_port), asio::use_future);
    probe_io.run();
    rejected.get();

    // Hold the transfer executor so shutdown cannot finish before cancellation is tested.
    std::promise<void> transfer_blocked;
    std::promise<void> release_transfer;
    auto transfer_released = release_transfer.get_future().share();
    asio::post(transfer_io, [&transfer_blocked, transfer_released]() {
        transfer_blocked.set_value();
        transfer_released.wait();
    });
    transfer_blocked.get_future().get();

    const auto stop_started = std::chrono::steady_clock::now();
    asio::cancellation_signal cancellation;
    auto first_stop = asio::co_spawn(control_io, client->async_stop(),
                                    asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    auto second_stop = asio::co_spawn(control_io, client->async_stop(), asio::use_future);
    client.reset();
    std::promise<void> cancellation_sent;
    asio::post(control_io, [&]() {
        cancellation.emit(asio::cancellation_type::terminal);
        cancellation_sent.set_value();
    });
    cancellation_sent.get_future().get();
    const auto returned_early = first_stop.wait_for(50ms) == std::future_status::ready;
    release_transfer.set_value();
    bool stop_cancelled = false;
    try
    {
        first_stop.get();
    }
    catch (const asio::system_error &)
    {
        stop_cancelled = true;
    }
    second_stop.get();
    control_work.reset();
    transfer_work.reset();
    control_thread.join();
    transfer_thread.join();
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_started;
    require(!returned_early && !stop_cancelled, "Caller cancellation interrupted Agent shutdown");
    require(stop_elapsed < 2s, "Client stop did not promptly cancel its retry/listener operations");
    require(weak_client.expired(), "Stopped client was retained by an asynchronous task");
}
} // namespace

int main()
{
    try
    {
        asio::ssl::context ssl_context(asio::ssl::context::tls_client);
        verify_listener_rollback(ssl_context);
        verify_stop_before_start(ssl_context);
        verify_stop_and_disconnected_listener(ssl_context);
        std::cout << "[PASS] Client listener rollback, pre-start stop, disconnected behavior, and stop lifecycle\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
