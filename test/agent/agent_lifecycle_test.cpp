#include "relay_agent.h"
#include "forwarder.h"

#include "member_access.h"
TEST_MEMBER(RelayAgent, control_)
TEST_MEMBER(RelayAgent, forwarder_)
TEST_MEMBER(RelayAgent, state_)
TEST_MEMBER(AgentControl, discovery_active_)
TEST_MEMBER(AgentControl, discovery_done_)
TEST_MEMBER(AgentControl, service_locations_)
TEST_MEMBER(AsyncEvent, waiters_)
TEST_MEMBER(Forwarder, state_)

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

void verify_failed_start_cleanup(asio::ssl::context &ssl_context)
{
    asio::io_context control_io(1);
    asio::io_context transfer_io(1);
    const auto forward_port = unused_port(transfer_io);
    auto config = base_config(unused_port(control_io));
    config.forwards.push_back(AgentForwardConfig{"echo", "127.0.0.1", forward_port});
    config.forwards.push_back(AgentForwardConfig{"echo", "invalid-address", 12345});
    RelayAgent client(control_io, transfer_io, ssl_context, std::move(config));

    bool failed = false;
    try
    {
        client.start();
    }
    catch (const std::exception &)
    {
        failed = true;
    }
    require(failed, "Client start unexpectedly succeeded with an invalid listener");

    {
        auto control_work = asio::make_work_guard(control_io);
        std::thread control_thread([&] { control_io.run(); });
        std::thread transfer_thread([&] { transfer_io.run(); });
        ScopeGuard join_workers([&] {
            control_work.reset();
            control_thread.join();
            transfer_thread.join();
        });
        client.stop();
    }
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

    std::thread control_thread([&control_io]() { control_io.run(); });
    std::thread transfer_thread([&transfer_io]() { transfer_io.run(); });

    client->stop();
    control_work.reset();
    transfer_work.reset();
    control_thread.join();
    transfer_thread.join();
    client->stop();
    client.reset();
    require(weak_client.expired(), "Agent stopped before start was retained by an asynchronous task");
}

void verify_drain_failure(asio::ssl::context &ssl_context)
{
    asio::io_context control(1);
    asio::io_context transfer(1);
    auto config = base_config(0);
    config.forwards.push_back(AgentForwardConfig{"preserved", "127.0.0.1", 0});
    auto client = std::make_shared<RelayAgent>(control, transfer, ssl_context, std::move(config));
    auto &discovery = test_access::RelayAgent_control_(*client);
    test_access::AgentControl_discovery_active_(discovery) = true;
    auto control_work = asio::make_work_guard(control);
    auto first = std::async(std::launch::async, [client] { client->stop(); });
    auto &waiters = test_access::AsyncEvent_waiters_(test_access::AgentControl_discovery_done_(discovery));
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (waiters.empty())
    {
        require(std::chrono::steady_clock::now() < deadline, "Stop did not begin draining discovery");
        control.run_for(1ms);
    }
    require(waiters.size() == 1, "Stop did not reach the discovery drain");
    waiters.front()->notification.cancel();
    control.poll();
    require(first.wait_for(1s) == std::future_status::ready, "Drain failure did not return to the owner");
    bool failed = false;
    try
    {
        first.get();
    }
    catch (const asio::system_error &error)
    {
        failed = error.code() == asio::error::operation_aborted;
    }
    require(failed, "Drain failure was reported as a successful stop");
    using State = std::remove_cvref_t<decltype(test_access::RelayAgent_state_(*client))>;
    require(test_access::RelayAgent_state_(*client) == State::Stopping &&
                test_access::AgentControl_service_locations_(discovery).size() == 1,
            "Incomplete shutdown cleared metadata or marked the Agent stopped");
    auto *forwarder = test_access::RelayAgent_forwarder_(*client).get();
    using ForwarderState = std::remove_cvref_t<decltype(test_access::Forwarder_state_(*forwarder))>;
    require(test_access::Forwarder_state_(*forwarder) == ForwarderState::Created,
            "Failed control drain stopped a dependency that may still receive notifications");

    test_access::AgentControl_discovery_active_(discovery) = false;
    auto retry = std::async(std::launch::async, [client] { client->stop(); });
    const auto retry_deadline = std::chrono::steady_clock::now() + 1s;
    while (retry.wait_for(0ms) != std::future_status::ready)
    {
        require(std::chrono::steady_clock::now() < retry_deadline, "Stop retry did not drain the remaining modules");
        control.run_for(1ms);
        transfer.poll();
    }
    retry.get();
    require(test_access::RelayAgent_state_(*client) == State::Stopped &&
                test_access::Forwarder_state_(*forwarder) == ForwarderState::Stopped,
            "Successful stop retry did not finish cleanup");
    const std::weak_ptr<RelayAgent> weak_client = client;
    client.reset();
    require(weak_client.expired(), "Failed stop retained the Agent after test dependencies drained");
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

    // The owner must wait for the transfer executor to finish shutdown.
    std::promise<void> transfer_blocked;
    std::promise<void> release_transfer;
    auto transfer_released = release_transfer.get_future().share();
    asio::post(transfer_io, [&transfer_blocked, transfer_released]() {
        transfer_blocked.set_value();
        transfer_released.wait();
    });
    transfer_blocked.get_future().get();

    const auto stop_started = std::chrono::steady_clock::now();
    auto stopped = std::async(std::launch::async, [client] { client->stop(); });
    client.reset();
    const auto returned_early = stopped.wait_for(50ms) == std::future_status::ready;
    release_transfer.set_value();
    stopped.get();
    control_work.reset();
    transfer_work.reset();
    control_thread.join();
    transfer_thread.join();
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_started;
    require(!returned_early, "Agent stop returned before transfer shutdown completed");
    require(stop_elapsed < 2s, "Client stop did not promptly cancel its retry/listener operations");
    require(weak_client.expired(), "Stopped client was retained by an asynchronous task");
}
} // namespace

int main()
{
    try
    {
        asio::ssl::context ssl_context(asio::ssl::context::tls_client);
        verify_failed_start_cleanup(ssl_context);
        verify_stop_before_start(ssl_context);
        verify_drain_failure(ssl_context);
        verify_stop_and_disconnected_listener(ssl_context);
        std::cout << "[PASS] Client failed-start cleanup, pre-start stop, drain retry, disconnected behavior, and blocking stop\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
