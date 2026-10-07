#include "probe_set.h"
#include <asio/experimental/awaitable_operators.hpp>
#include <iostream>

namespace
{
void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

asio::awaitable<void> verify(asio::io_context &io)
{
    // Resolution and shutdown must work even when no raw socket can be opened.
    ProbeSet unresolved(io);
    unresolved.set_targets({{"unsupported", "::1"}});
    require(co_await unresolved.refresh(), "Failed resolution did not finish its refresh");
    require(unresolved.metrics().empty(), "Unresolved target supplied fabricated measurements");
    asio::steady_timer timer(io, std::chrono::milliseconds(5));
    co_await timer.async_wait(asio::use_awaitable);
    require(co_await unresolved.refresh(), "Resolution failure could not be retried");
    co_await unresolved.close();
    co_await unresolved.close();

    ProbeSet changing(io);
    changing.set_targets({{"obsolete", "localhost"}});
    asio::post(io, [&changing] { changing.set_targets({{"current", "::1"}}); });
    require(!co_await changing.refresh(), "DNS committed an obsolete configuration");
    require(co_await changing.refresh(), "The replacement configuration was not applied");
    co_await changing.close();

    ProbeSet closing(io);
    closing.set_targets({{"entry", "localhost"}});
    bool refresh_finished = false;
    std::exception_ptr failure;
    asio::co_spawn(io, closing.refresh(), [&](std::exception_ptr error, bool) {
        failure = error;
        refresh_finished = true;
    });
    co_await asio::post(io, asio::use_awaitable);
    using namespace asio::experimental::awaitable_operators;
    co_await (closing.close() && closing.close());
    require(refresh_finished && !failure, "Concurrent close returned before DNS refresh completed");
    require(closing.metrics().empty(), "Closed probes expose stale measurements");

    ProbeSet cancelled(io);
    cancelled.set_targets({{"entry", "localhost"}});
    bool cancelled_refresh_finished = false;
    bool cancelled_close_finished = false;
    std::exception_ptr cancelled_failure;
    asio::co_spawn(io, cancelled.refresh(), [&](std::exception_ptr error, bool) {
        cancelled_failure = error;
        cancelled_refresh_finished = true;
    });
    co_await asio::post(io, asio::use_awaitable);
    asio::cancellation_signal cancellation;
    asio::co_spawn(io, cancelled.close(), asio::bind_cancellation_slot(cancellation.slot(),
        [&](std::exception_ptr error) {
            if (error)
            {
                cancelled_failure = error;
            }
            cancelled_close_finished = true;
        }));
    cancellation.emit(asio::cancellation_type::terminal);
    co_await cancelled.close();
    co_await asio::post(io, asio::use_awaitable);
    require(cancelled_refresh_finished && cancelled_close_finished && !cancelled_failure,
            "Caller cancellation interrupted the shutdown drain");
}
}

int main()
{
    try
    {
        asio::io_context io(1);
        auto result = asio::co_spawn(io, verify(io), asio::use_future);
        io.run();
        result.get();
        std::cout << "[PASS] ProbeSet failed DNS retry, configuration revision and concurrent shutdown\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
