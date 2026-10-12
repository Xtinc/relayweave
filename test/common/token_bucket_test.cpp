#include "xfr_channel.h"
#include "member_access.h"

#include <chrono>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>

TEST_MEMBER(TokenBucket, timer_)
TEST_MEMBER(TokenBucket, rate_)
TEST_MEMBER(TokenBucket, sequence_)
TEST_MEMBER(TokenBucket, reserve)
TEST_MEMBER(TokenBucket, deadline)

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
    require(test_access::TokenBucket_reserve(burst, 6, now) == now, "Initial burst unexpectedly waited");
    require(!burst.try_consume(5), "Insufficient tokens were accepted");
    require(burst.try_consume(4), "Rejected packet consumed the remaining tokens");
    require_deadline(test_access::TokenBucket_reserve(burst, 5, now), now + 5s, "Fast path charged bytes twice or lost consumed bytes");
    require(!burst.try_consume(1), "Fast path bypassed outstanding reservation debt");
    require_deadline(test_access::TokenBucket_reserve(burst, 11, now), now + 16s, "Large stream write lost preceding reservation debt");

    TokenBucket remaining(1, 10);
    require(test_access::TokenBucket_reserve(remaining, 6, now) == now, "Initial stream burst was rejected");
    require(!remaining.try_consume(5), "Insufficient stream burst was accepted");
    require_deadline(test_access::TokenBucket_reserve(remaining, 5, now), now + 1s, "Failed fast path consumed tokens before waiting");
}

void refill_and_burst_cap()
{
    const auto now = TokenBucket::Clock::now() + 1h;
    TokenBucket limiter(10, 10);
    require(test_access::TokenBucket_reserve(limiter, 8, now) == now, "Initial reservation unexpectedly waited");
    require(test_access::TokenBucket_reserve(limiter, 7, now + 500ms) == now + 500ms, "Elapsed time did not refill tokens");
    require(test_access::TokenBucket_reserve(limiter, 10, now + 4s) == now + 4s, "Idle bucket did not refill to its burst");
    require_deadline(test_access::TokenBucket_reserve(limiter, 11, now + 8s), now + 8100ms, "Idle bucket accumulated beyond the burst cap");

    TokenBucket no_burst(1, 0);
    require_deadline(test_access::TokenBucket_reserve(no_burst, 60, now), now + 60s, "Zero burst did not reserve stream debt");
    require(!no_burst.try_consume(1), "Zero burst accepted a datagram");
    require_deadline(test_access::TokenBucket_reserve(no_burst, 1, now + 120s), now + 121s, "Zero burst accumulated idle credit");
}

void unlimited_and_empty()
{
    const auto now = TokenBucket::Clock::now() + 1h;
    const auto maximum = std::numeric_limits<std::size_t>::max();
    TokenBucket unlimited;
    require(unlimited.try_consume(maximum) && test_access::TokenBucket_reserve(unlimited, maximum, now) == now,
            "Disabled limiter rejected or delayed a transfer");

    TokenBucket limiter(1, 0);
    require_deadline(test_access::TokenBucket_reserve(limiter, 10, now), now + 10s, "Reservation did not establish debt");
    require(limiter.try_consume(0) && test_access::TokenBucket_reserve(limiter, 0, now) == now,
            "Empty payload was delayed by existing debt");
    require_deadline(test_access::TokenBucket_reserve(limiter, 1, now), now + 11s, "Empty payload changed outstanding debt");
}

void dynamic_debt()
{
    const auto now = TokenBucket::Clock::now();
    TokenBucket limiter(100, 0);
    const auto original = test_access::TokenBucket_reserve(limiter, 10000, now);
    const auto before_down = TokenBucket::Clock::now();
    limiter.set_rate(50, 0);
    const auto after_down = TokenBucket::Clock::now();
    const auto down = test_access::TokenBucket_deadline(limiter, TokenBucket::Clock::now());
    require(down >= original + (original - after_down) && down <= original + (original - before_down) + 1us,
            "Lower rate lost unserved bytes");
    const auto before_up = TokenBucket::Clock::now();
    limiter.set_rate(200, 0);
    const auto after_up = TokenBucket::Clock::now();
    const auto up = test_access::TokenBucket_deadline(limiter, TokenBucket::Clock::now());
    require(up >= before_up + (down - before_up) / 4 - 1us && up <= after_up + (down - after_up) / 4 + 1us,
            "Higher rate did not shorten wait");
    require_deadline(test_access::TokenBucket_reserve(limiter, 50, after_up), up + 250ms, "Rate update charged debt twice");
    limiter.set_rate(0, 0);
    const auto unlimited_now = TokenBucket::Clock::now();
    require(test_access::TokenBucket_deadline(limiter, unlimited_now) == unlimited_now && limiter.try_consume(1000),
            "Unlimited update did not release debt");
    TokenBucket burst(100, 100);
    const auto before_burst = TokenBucket::Clock::now();
    burst.set_rate(10, 10);
    const auto after_burst = TokenBucket::Clock::now();
    require(!burst.try_consume(1), "Rate reduction retained accumulated burst");
    const auto due = test_access::TokenBucket_reserve(burst, 10, TokenBucket::Clock::now());
    require(due >= before_burst + 1s && due <= after_burst + 1s + 1us,
            "Clamped tokens retained excess burst");
}

void modes_and_fast_path()
{
    for (auto mode : {TokenBucket::Mode::Fixed, TokenBucket::Mode::Dynamic})
    {
        asio::io_context io;
        TokenBucket bucket(1024, 4096, mode);
        bool entered_wait = false;
        for (int i = 0; i < 32; ++i)
        {
            if (!bucket.try_consume(64))
            {
                entered_wait = true;
                asio::co_spawn(io, bucket.wait(64), asio::use_future);
            }
        }
        require(!entered_wait && !test_access::TokenBucket_timer_(bucket),
                "Uncongested frames entered wait or allocated a timer");
        bucket.feedback(1, FlowPressure::Pause);
        if (mode == TokenBucket::Mode::Fixed)
        {
            require(bucket.try_consume() && test_access::TokenBucket_sequence_(bucket) == 0 &&
                        test_access::TokenBucket_rate_(bucket) == 1024,
                    "Fixed mode applied dynamic feedback");
        }
        else
        {
            require(!bucket.try_consume() && !test_access::TokenBucket_timer_(bucket),
                    "Pause fast check bypassed pressure or allocated a timer");
            auto paused = asio::co_spawn(io, bucket.wait(), asio::use_future);
            io.poll();
            require(paused.wait_for(0s) != std::future_status::ready, "Dynamic pause did not wait");
            bucket.feedback(2, FlowPressure::Resume);
            io.restart();
            io.poll();
            paused.get();
            require(bucket.try_consume(), "Resume did not restore synchronous fast path");
        }

        TokenBucket limited(1, 0, mode);
        require(!limited.try_consume(10), "Slow bucket passed its fast path");
        auto pending = asio::co_spawn(io, limited.wait(10), asio::use_future);
        io.restart();
        io.poll();
        require(pending.wait_for(0s) != std::future_status::ready, "Rate wait did not block");
        limited.set_rate(64 * 1024, 0);
        io.restart();
        io.run_for(1s);
        require(pending.wait_for(0s) == std::future_status::ready, "Rate change did not wake the wait");
        pending.get();
        require(!limited.try_consume(1000000), "Slow packet skipped the debt check");
        auto closed = asio::co_spawn(io, limited.wait(1000000), asio::use_future);
        io.restart();
        io.poll();
        limited.close();
        require(!limited.try_consume(), "Closed bucket passed the fast path");
        io.restart();
        io.poll();
        require(closed.wait_for(0s) == std::future_status::ready, "Close retained a rate wait");
        try
        {
            closed.get();
            require(false, "Closed wait succeeded");
        }
        catch (const asio::system_error &error)
        {
            require(error.code() == asio::error::operation_aborted, "Wrong close error");
        }
    }
}
} // namespace

int main()
{
    try
    {
        fast_path_and_debt();
        refill_and_burst_cap();
        unlimited_and_empty();
        dynamic_debt();
        modes_and_fast_path();
        std::cout << "[PASS] TokenBucket fast path, reservation debt, refill and burst limits\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
