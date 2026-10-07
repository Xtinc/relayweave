#include "async_event.h"
#include <asio.hpp>
#include <array>
#include <iostream>
#include <stdexcept>

namespace
{
void require(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
}

void broadcast_and_late_waiters()
{
    asio::io_context io;
    AsyncEvent event(io.get_executor());
    std::array<std::future<bool>, 4> results;
    for (auto &result : results)
    {
        result = asio::co_spawn(io, event.wait(), asio::use_future);
    }
    io.poll();
    require(!io.stopped(), "pending waiters did not keep the executor running");
    for (auto &result : results)
    {
        require(result.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
                "waiter completed before notification");
    }
    event.notify_all();
    event.notify_all();
    io.run();
    for (auto &result : results)
    {
        require(result.get(), "broadcast did not wake every waiter");
    }

    io.restart();
    auto late = asio::co_spawn(io, event.wait(), asio::use_future);
    io.run();
    require(late.get(), "notification was lost for a late waiter");
}

void individual_cancellation()
{
    asio::io_context io;
    AsyncEvent event(io.get_executor());
    asio::cancellation_signal cancellation;
    auto cancelled = asio::co_spawn(io, event.wait(),
                                   asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    auto pending = asio::co_spawn(io, event.wait(), asio::use_future);
    io.poll();
    cancellation.emit(asio::cancellation_type::all);
    io.poll();
    require(!cancelled.get(), "cancelled waiter did not report cancellation");
    require(pending.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
            "cancellation woke another waiter");
    // The cancelled coroutine has exited. Notification must not retain or use
    // its old waiter, and must still wake both existing and new waiters.
    auto later = asio::co_spawn(io, event.wait(), asio::use_future);
    io.poll();
    event.notify_all();
    io.run();
    require(pending.get() && later.get(), "cancellation affected other waiters or latched the event");

    io.restart();
    AsyncEvent notified(io.get_executor());
    auto result = asio::co_spawn(io, notified.wait(),
                                asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    io.poll();
    notified.notify_all();
    cancellation.emit(asio::cancellation_type::all);
    io.run();
    require(result.get(), "cancellation changed an already submitted notification");
}

void separate_completion_and_release()
{
    asio::io_context io;
    AsyncEvent completed(io.get_executor());
    AsyncEvent released(io.get_executor());
    auto opening = asio::co_spawn(io, completed.wait(), asio::use_future);
    auto closing = asio::co_spawn(io, released.wait(), asio::use_future);
    io.poll();
    completed.notify_all();
    io.poll();
    require(opening.get(), "completion did not wake the opening waiter");
    require(closing.wait_for(std::chrono::seconds(0)) != std::future_status::ready,
            "completion incorrectly woke the release waiter");
    released.notify_all();
    io.run();
    require(closing.get(), "release waiter did not resume");
}
} // namespace

int main()
{
    try
    {
        broadcast_and_late_waiters();
        individual_cancellation();
        separate_completion_and_release();
        std::cout << "[PASS] AsyncEvent broadcast, cancellation and separate flow stages\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
