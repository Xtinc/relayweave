#include "member_access.h"
#include "pipeline_mgr.h"
#include <future>
#include <iostream>

TEST_MEMBER(TcpPipeline, remote_pairs_)
TEST_MEMBER(TokenBucket, rate_)
TEST_MEMBER(TokenBucket, sequence_)
TEST_MEMBER(TokenBucket, paused_)

using namespace std::chrono_literals;

static void check(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

struct Fixture
{
    asio::io_context io{1};
    asio::ssl::context ssl{asio::ssl::context::tls_server};
    std::shared_ptr<LnkChannel> channel = std::make_shared<LnkChannel>(io.get_executor(), io.get_executor(),
                                                                       "a", "127.0.0.1", 0, "127.0.0.1", 0);
    std::shared_ptr<TcpPipeline> pipeline;
    std::uint64_t uuid;

    Fixture(int role, TrafficLimitConfig limits = {})
        : pipeline(std::make_shared<TcpPipeline>(io.get_executor(), ssl, std::make_shared<RelayIdAllocator>(),
                                                 "127.0.0.1", 0, 16, 16, 10s, limits))
    {
        uuid = pipeline->install_remote_pair(role).at("uuid").get<std::uint64_t>();
        pipeline->bind_remote_pair(uuid, *channel, 7, 101, {}, {});
    }

    auto &endpoint()
    {
        return *test_access::TcpPipeline_remote_pairs_(*pipeline).at(uuid);
    }
    auto &limiter()
    {
        auto &pair = endpoint();
        return pair.role == RelayAttach::Producer ? pair.rx_limiter : pair.tx_limiter;
    }
    auto rate()
    {
        return test_access::TokenBucket_rate_(limiter());
    }

    void pump()
    {
        io.restart();
        io.poll();
    }
    void pressure(std::uint64_t sequence, FlowPressure action)
    {
        pipeline->pressure_remote_pair(uuid, 7, 101, endpoint().role == RelayAttach::Producer, sequence,
                                       action);
    }
    std::future<void> wait(std::size_t bytes = 0, asio::cancellation_slot slot = {})
    {
        auto pair = test_access::TcpPipeline_remote_pairs_(*pipeline).at(uuid);
        return asio::co_spawn(
            io, [pair, bytes]() -> asio::awaitable<void> {
                co_await (pair->role == RelayAttach::Producer ? pair->rx_limiter : pair->tx_limiter).wait(bytes);
            },
            asio::bind_cancellation_slot(slot, asio::use_future));
    }
};

static void failed(std::future<void> &result)
{
    check(result.wait_for(0s) == std::future_status::ready, "cancelled supply did not complete");
    try
    {
        result.get();
    }
    catch (const asio::system_error &error)
    {
        check(error.code() == asio::error::operation_aborted, "wrong cancellation error");
        return;
    }
    throw std::runtime_error("cancelled supply succeeded");
}

int main()
{
    try
    {
        for (int role : {RelayAttach::Consumer, RelayAttach::Producer})
        {
            Fixture f(role);
            f.pressure(1, FlowPressure::Decrease);
            check(f.rate() == 512 * 1024, "first decrease did not halve the finite seed");
            f.pressure(1, FlowPressure::Decrease);
            check(f.rate() == 512 * 1024, "duplicate decrease halved twice");
            f.pressure(2, FlowPressure::Decrease);
            check(f.rate() == 256 * 1024, "multiplicative decrease differs");
            f.pressure(3, FlowPressure::Increase);
            check(f.rate() == 320 * 1024, "increase did not add a fixed step");
            f.pipeline->pressure_remote_pair(f.uuid, 8, 101, role == RelayAttach::Producer, 100,
                                             FlowPressure::Pause);
            f.pipeline->pressure_remote_pair(f.uuid, 7, 102, role == RelayAttach::Producer, 100,
                                             FlowPressure::Pause);
            f.pipeline->pressure_remote_pair(f.uuid, 7, 101, role != RelayAttach::Producer, 100,
                                             FlowPressure::Pause);
            f.pressure(100, static_cast<FlowPressure>(5));
            check(test_access::TokenBucket_sequence_(f.limiter()) == 3 && !test_access::TokenBucket_paused_(f.limiter()),
                  "wrong identity, direction or action changed supply");
            f.pressure(4, FlowPressure::Pause);
            auto waiting = f.wait();
            f.pump();
            check(waiting.wait_for(0s) != std::future_status::ready, "pause did not block supply");
            f.pressure(3, FlowPressure::Resume);
            f.pump();
            check(test_access::TokenBucket_paused_(f.limiter()) && waiting.wait_for(0s) != std::future_status::ready,
                  "old feedback released pause");
            f.pressure(5, FlowPressure::Resume);
            f.pump();
            waiting.get();
            check(f.rate() == 320 * 1024, "resume changed the saved rate");

            std::uint64_t sequence = 5;
            while (f.rate() > 1)
                f.pressure(++sequence, FlowPressure::Decrease);
            auto rate_wait = f.wait(10);
            f.pump();
            check(rate_wait.wait_for(0s) != std::future_status::ready, "finite rate did not wait");
            f.pressure(++sequence, FlowPressure::Increase);
            f.io.run_for(1s);
            check(rate_wait.wait_for(0s) == std::future_status::ready,
                  "additive increase retained old deadline");
            rate_wait.get();

            asio::cancellation_signal rate_cancel;
            auto rate_cancelled = f.wait(1000000, rate_cancel.slot());
            f.pump();
            rate_cancel.emit(asio::cancellation_type::all);
            f.pump();
            failed(rate_cancelled);
            f.pressure(++sequence, FlowPressure::Pause);
            asio::cancellation_signal signal;
            auto cancelled = f.wait(10, signal.slot());
            f.pump();
            signal.emit(asio::cancellation_type::all);
            f.pump();
            failed(cancelled);
            auto closed = f.wait();
            f.pump();
            f.pipeline->close_remote_pair(f.uuid);
            f.pump();
            failed(closed);
        }
        {
            Fixture f(RelayAttach::Consumer);
            f.limiter().set_rate(1, 0);
            auto waiting = f.wait(10);
            f.pump();
            check(waiting.wait_for(0s) != std::future_status::ready, "rate wait did not block");
            f.limiter().set_rate(64 * 1024, 0);
            f.io.run_for(1s);
            check(waiting.wait_for(0s) == std::future_status::ready, "set_rate did not wake its own wait");
            waiting.get();
            f.pressure(1, FlowPressure::Pause);
            auto paused = f.wait();
            f.pump();
            f.limiter().set_rate(128 * 1024, 0);
            f.pump();
            check(paused.wait_for(0s) != std::future_status::ready, "set_rate released pressure pause");
            f.pressure(2, FlowPressure::Resume);
            f.pump();
            paused.get();
        }
        {
            TrafficLimitConfig limits;
            limits.tx_bytes_per_second = 100;
            Fixture f(RelayAttach::Consumer, limits);
            f.pressure(1, FlowPressure::Decrease);
            check(f.rate() == 50, "configured rate was not halved");
            f.pressure(2, FlowPressure::Increase);
            check(f.rate() == 100, "additive increase bypassed configured ceiling");
            f.pressure(3, FlowPressure::Increase);
            check(f.rate() == 100, "configured ceiling did not saturate");
            check(f.endpoint().rx_limiter.try_consume(100), "pressure changed opposite direction");
        }
        std::cout << "[PASS] AIMD, duplicate protection, dynamic wake, pause, cancel and close\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
