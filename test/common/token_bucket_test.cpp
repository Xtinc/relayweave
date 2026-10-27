#include "xfr_channel.h"

#include <chrono>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace
{
using namespace std::chrono_literals;

void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

void require_deadline(TokenBucket::TimePoint actual, TokenBucket::TimePoint expected, const char *message)
{
    // The duration calculation rounds fractional nanoseconds up. Long debts may
    // have tiny floating-point rounding differences, but must never wait less.
    require(actual >= expected && actual - expected <= 1us, message);
}

void fast_path_and_debt()
{
    // Freeze refill ahead of the real clock, without sleeps or test-only APIs.
    const auto now = TokenBucket::Clock::now() + 1h;
    TokenBucket burst(1, 10);
    require(burst.reserve(6, now) == now, "Initial burst unexpectedly waited");
    require(!burst.try_consume(5), "Insufficient tokens were accepted");
    require(burst.try_consume(4), "Rejected packet consumed the remaining tokens");
    require_deadline(burst.reserve(5, now), now + 5s, "Fast path charged bytes twice or lost consumed bytes");
    require(!burst.try_consume(1), "Fast path bypassed outstanding reservation debt");
    require_deadline(burst.reserve(11, now), now + 16s, "Large stream write lost preceding reservation debt");

    TokenBucket remaining(1, 10);
    require(remaining.reserve(6, now) == now, "Initial stream burst was rejected");
    require(!remaining.try_consume(5), "Insufficient stream burst was accepted");
    require_deadline(remaining.reserve(5, now), now + 1s, "Failed fast path consumed tokens before waiting");
}

void refill_and_burst_cap()
{
    const auto now = TokenBucket::Clock::now() + 1h;
    TokenBucket limiter(10, 10);
    require(limiter.reserve(8, now) == now, "Initial reservation unexpectedly waited");
    require(limiter.reserve(7, now + 500ms) == now + 500ms, "Elapsed time did not refill tokens");
    require(limiter.reserve(10, now + 4s) == now + 4s, "Idle bucket did not refill to its burst");
    require_deadline(limiter.reserve(11, now + 8s), now + 8100ms, "Idle bucket accumulated beyond the burst cap");

    TokenBucket no_burst(1, 0);
    require_deadline(no_burst.reserve(60, now), now + 60s, "Zero burst did not reserve stream debt");
    require(!no_burst.try_consume(1), "Zero burst accepted a datagram");
    require_deadline(no_burst.reserve(1, now + 120s), now + 121s, "Zero burst accumulated idle credit");
}

void unlimited_and_empty()
{
    const auto now = TokenBucket::Clock::now() + 1h;
    const auto maximum = std::numeric_limits<std::size_t>::max();
    TokenBucket unlimited;
    require(unlimited.try_consume(maximum) && unlimited.reserve(maximum, now) == now,
            "Disabled limiter rejected or delayed a transfer");

    TokenBucket limiter(1, 0);
    require_deadline(limiter.reserve(10, now), now + 10s, "Reservation did not establish debt");
    require(limiter.try_consume(0) && limiter.reserve(0, now) == now,
            "Empty payload was delayed by existing debt");
    require_deadline(limiter.reserve(1, now), now + 11s, "Empty payload changed outstanding debt");
}

void asynchronous_consumption()
{
    asio::io_context io;
    TokenBucket burst_limiter(1, 10);
    auto burst = asio::co_spawn(io, burst_limiter.consume(10), asio::use_future);
    io.poll();
    require(burst.wait_for(0s) == std::future_status::ready, "Available burst suspended consumption");
    burst.get();
    require(!burst_limiter.try_consume(10), "Async consumption did not deduct burst tokens");

    // Repeated waits on the same bucket exercise its reusable timer.
    TokenBucket limiter(1000, 10);
    for (int index = 0; index < 2; ++index)
    {
        const auto started = TokenBucket::Clock::now();
        auto result = asio::co_spawn(io, limiter.consume(30), asio::use_future);
        io.restart();
        io.poll();
        require(result.wait_for(0s) != std::future_status::ready, "Insufficient tokens bypassed the wait");
        io.run();
        result.get();
        // Even a refilled bucket has only 10 bytes of burst: at least 20 ms of debt.
        require(TokenBucket::Clock::now() - started >= 20ms, "Consumption resumed before its token deadline");
    }

    asio::cancellation_signal cancellation;
    auto cancelled = asio::co_spawn(io, limiter.consume(60000),
        asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    io.restart();
    io.poll();
    require(cancelled.wait_for(0s) != std::future_status::ready, "Cancellation test did not enter its wait");
    cancellation.emit(asio::cancellation_type::all);
    io.run();
    bool aborted = false;
    try
    {
        cancelled.get();
    }
    catch (const asio::system_error &error)
    {
        aborted = error.code() == asio::error::operation_aborted;
    }
    require(aborted, "Rate-limit wait did not propagate cancellation");

    auto empty = asio::co_spawn(io, limiter.consume(0), asio::use_future);
    io.restart();
    io.poll();
    require(empty.wait_for(0s) == std::future_status::ready, "Empty consumption waited on existing debt");
    empty.get();

    TokenBucket unlimited;
    auto unrestricted = asio::co_spawn(io, unlimited.consume(std::numeric_limits<std::size_t>::max()), asio::use_future);
    io.restart();
    io.poll();
    require(unrestricted.wait_for(0s) == std::future_status::ready, "Unlimited consumption suspended");
    unrestricted.get();
}
} // namespace

int main()
{
    try
    {
        fast_path_and_debt();
        refill_and_burst_cap();
        unlimited_and_empty();
        asynchronous_consumption();
        std::cout << "[PASS] TokenBucket reservations, consumption, reusable waits and cancellation\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
