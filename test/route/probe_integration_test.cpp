#include "agent_routing.h"
#include "link_quality.h"
#include <asio/experimental/awaitable_operators.hpp>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <stdexcept>

namespace
{
using namespace std::chrono_literals;
void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

std::optional<RouteGraph::Path> best_candidate(const AgentRouting &routing, std::string_view destination)
{
    const auto paths = routing.candidate_paths(AgentRouting::Clock::now());
    const auto found = paths.find(std::string(destination));
    return found == paths.end() ? std::nullopt : std::optional(found->second.front());
}

asio::awaitable<void> wait(std::chrono::milliseconds duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor, duration);
    co_await timer.async_wait(asio::use_awaitable);
}

asio::awaitable<void> verify(asio::io_context &io)
{
    const auto loopback = asio::ip::make_address_v4("127.0.0.1");
    const auto other = asio::ip::make_address_v4("127.0.0.2");
    ICMP probes(io, 100ms, 80ms);
    ICMP concurrent(io, 100ms, 80ms);
    probes.run({loopback, other});
    concurrent.run({loopback});
    while (!std::ranges::all_of(probes.metrics(), [](const auto &metric) { return metric.assessment.total_received >= 3; }) ||
           concurrent.metrics().front().assessment.total_received < 3)
        co_await wait(1ms);
    co_await probes.close();
    co_await concurrent.close();
    require(concurrent.metrics().front().assessment.total_received >= 3,
            "Concurrent ICMP instances interfered with one another");
    for (const auto &metric : probes.metrics())
    {
        require(metric.transmitted >= 3 && metric.assessment.total_received >= 3 &&
                    metric.assessment.total_completed == metric.assessment.total_received,
                "Concurrent loopback probes did not complete successfully");
        require(metric.assessment.quality.usable && metric.assessment.quality.cost && *metric.assessment.quality.cost < 10.0,
                "Real replies did not produce usable quality");
    }

    const auto previous = probes.history().at(loopback);
    const auto previous_metrics = probes.metrics().front();
    ICMP resumed(io, 100ms, 80ms);
    resumed.run({loopback}, probes.history());
    while (resumed.metrics().front().assessment.total_received < previous_metrics.assessment.total_received + 2)
        co_await wait(1ms);
    co_await resumed.close();
    const auto restored = resumed.metrics().front();
    require(restored.transmitted >= previous.transmitted + 2 &&
                restored.assessment.total_completed >= previous_metrics.assessment.total_completed + 2 &&
                restored.assessment.total_received == restored.assessment.total_completed,
            "Rebuilding probes lost EMA history or reset completed measurements");

    // Preserve a send that was cancelled before completion: transmitted and
    // completed are independent counters when a probe engine is rebuilt.
    auto cancelled_send_history = probes.history();
    ++cancelled_send_history.at(loopback).transmitted;
    ICMP retained_sends(io, 100ms, 80ms);
    retained_sends.run({loopback}, cancelled_send_history);
    auto retained = retained_sends.metrics().front();
    require(retained.transmitted == previous.transmitted + 1 &&
                retained.assessment.total_completed == previous_metrics.assessment.total_completed,
            "Rebuilding probes replaced the actual send count with completed samples");
    while (retained_sends.metrics().front().assessment.total_received < previous_metrics.assessment.total_received + 1)
        co_await wait(1ms);
    co_await retained_sends.close();
    retained = retained_sends.metrics().front();
    require(retained.transmitted >= previous.transmitted + 2 &&
                retained.assessment.total_completed >= previous_metrics.assessment.total_completed + 1 &&
                retained.transmitted > retained.assessment.total_completed,
            "Resumed probes lost a cancelled send or counted it as a completed sample");

    ProbeSet shared(io);
    std::exception_ptr shared_failure;
    try
    {
        shared.set_targets({{"A", "127.0.0.1"}, {"alias", "127.0.0.1"}});
        require(co_await shared.refresh(), "Initial shared probes were not configured");
        co_await wait(200ms);
        auto metrics = shared.metrics();
        require(metrics.size() == 2 && metrics[0].assessment.total_received == 1 &&
                metrics[1].assessment.total_received == 1, "Aliases did not share a real probe");
        const auto completed = metrics[0].assessment.total_completed;
        shared.set_targets({{"renamed", "127.0.0.1"}, {"bad", "::1"}});
        require(shared.metrics().empty(), "Removed identities remained visible before DNS completed");
        require(co_await shared.refresh(), "Partial resolution did not complete");
        co_await wait(100ms);
        metrics = shared.metrics();
        require(metrics.size() == 1 && metrics[0].node_id == "renamed" &&
                metrics[0].assessment.total_completed == completed,
                "Identity update or failed DNS rebuilt the healthy probe");
        require(co_await shared.refresh(), "Failed DNS retry did not complete");
        co_await wait(100ms);
        require(shared.metrics()[0].assessment.total_completed == completed,
                "Retrying unresolved targets interrupted the healthy schedule");
        co_await wait(800ms);
        require(shared.metrics()[0].assessment.total_received >= 2, "Healthy probes stopped after partial DNS failure");
        const auto before_rebuild = shared.metrics()[0].assessment.total_completed;
        shared.set_targets({{"renamed", "127.0.0.1"}, {"new", "127.0.0.2"}});
        require(shared.metrics().size() == 1 && shared.metrics()[0].node_id == "renamed",
                "An unchanged identity lost its live measurements during target update");
        require(co_await shared.refresh(), "Changed address set did not apply");
        co_await wait(100ms);
        metrics = shared.metrics();
        for (const auto &metric : metrics)
        {
            if (metric.node_id == "renamed")
            {
                require(metric.assessment.total_completed >= before_rebuild, "Unchanged address lost its history");
            }
            else
            {
                require(metric.assessment.total_completed == 1, "New address inherited another link's history");
            }
        }
        shared.set_targets({{"renamed", "127.0.0.2"}, {"new", "127.0.0.2"}});
        metrics = shared.metrics();
        require(metrics.size() == 1 && metrics[0].node_id == "new",
                "Changed address retained measurements from the old destination");
    }
    catch (...)
    {
        shared_failure = std::current_exception();
    }
    co_await shared.close();
    require(shared.metrics().empty(), "Closed shared probes expose stale measurements");
    if (shared_failure)
    {
        std::rethrow_exception(shared_failure);
    }

    AgentRouting routing(io, false, 2);
    routing.set_required_targets({{"entry", "127.0.0.1"}});
    auto now = AgentRouting::Clock::now();
    routing.begin_request(1, now);
    LinkQuality::Summary quality;
    quality.cost = 10.0;
    quality.confidence = 1.0;
    quality.usable = true;
    using Json = nlohmann::json;
    const Json snapshot{
        {"request_id", 1u}, {"epoch", 1u}, {"snapshot_version", 1u}, {"created_age_ms", 0u},
        {"nodes", Json::array({{{"node_id", "entry"}, {"address", "127.0.0.1"}, {"report_age_ms", 0u}},
                               {{"node_id", "service"}, {"address", "127.0.0.2"}, {"report_age_ms", 0u}}})},
        {"links", Json::array({{{"source", "entry"}, {"destination", "service"},
                               {"age_ms", 0u}, {"quality", quality_json(quality)}}})}};
    routing.accept_snapshot(snapshot, now);
    std::exception_ptr failure;
    try
    {
        require(!best_candidate(routing, "service"), "Route appeared before any measurement");
        require(co_await routing.refresh_probes(), "Stable targets requested another refresh");
        co_await wait(200ms);
        auto path = best_candidate(routing, "service");
        require(path && path->nodes == std::vector<std::string>({"entry", "service"}),
                "Agent did not combine its real ICMP access edge with the cluster graph");

        routing.set_required_targets({{"entry", "127.0.0.1"}, {"service", "127.0.0.2"}});
        require(co_await routing.refresh_probes(), "Service discovery did not apply new targets");
        co_await wait(200ms);
        path = best_candidate(routing, "service");
        require(path && path->nodes == std::vector<std::string>({"service"}),
                "Agent did not recalculate after probing a new service destination");
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    co_await routing.close();
    if (failure)
    {
        std::rethrow_exception(failure);
    }
    require(!best_candidate(routing, "service"), "Closed Agent retained a usable local route");

    AgentRouting candidates(io, true, 2);
    now = AgentRouting::Clock::now();
    candidates.begin_request(1, now);
    candidates.accept_snapshot(snapshot, now);
    failure = nullptr;
    try
    {
        require(co_await candidates.refresh_probes(), "Topology targets did not apply");
        co_await wait(200ms);
        const auto path = best_candidate(candidates, "service");
        require(path && path->nodes == std::vector<std::string>({"service"}),
                "Agent did not probe candidates discovered in topology");
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    co_await candidates.close();
    if (failure)
    {
        std::rethrow_exception(failure);
    }

    auto changed = std::make_shared<AgentRouting>(io, false, 2);
    changed->set_required_targets({{"entry", "localhost"}});
    asio::post(io, [changed] { changed->set_required_targets({{"service", "127.0.0.1"}}); });
    failure = nullptr;
    try
    {
        require(!co_await changed->refresh_probes(), "DNS refresh did not detect a concurrent target change");
        require(co_await changed->refresh_probes(), "New discovery targets were not applied immediately");
        co_await wait(200ms);
        const auto path = best_candidate(*changed, "service");
        require(path && path->nodes == std::vector<std::string>({"service"}), "DNS committed an obsolete target set");
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    co_await changed->close();
    if (failure)
    {
        std::rethrow_exception(failure);
    }

    // close must wait for an in-progress DNS refresh, and concurrent closes share one drain.
    auto closing = std::make_shared<ProbeSet>(io);
    closing->set_targets({{"entry", "localhost"}});
    bool refresh_finished = false;
    asio::co_spawn(io, [closing, &refresh_finished]() -> asio::awaitable<void> {
        co_await closing->refresh();
        refresh_finished = true;
    }, asio::detached);
    co_await asio::post(io, asio::use_awaitable);
    using namespace asio::experimental::awaitable_operators;
    co_await (closing->close() && closing->close());
    require(refresh_finished && closing->metrics().empty(), "Close returned before DNS refresh drained");

    ICMP cancelled(io, 100ms, 80ms);
    cancelled.run({loopback});
    co_await asio::post(io, asio::use_awaitable);
    const auto before = cancelled.metrics().front().assessment.total_completed;
    co_await cancelled.close();
    require(!cancelled.active() && cancelled.metrics().front().assessment.total_completed == before,
            "Shutdown counted an unfinished probe as loss");
}
}

int main()
{
    try
    {
        // Only lack of raw socket permission skips this test; network failures must fail it.
        asio::io_context check_io(1);
        asio::ip::icmp::socket check(check_io);
        asio::error_code error;
        check.open(asio::ip::icmp::v4(), error);
        if (!error)
        {
            check.bind({asio::ip::address_v4::any(), 0}, error);
        }
        if (error == asio::error::access_denied || error == asio::error::no_permission)
        {
            std::cout << "[SKIP] probe_integration requires Windows administrator or Linux CAP_NET_RAW\n";
            return 77;
        }
        if (error)
        {
            throw asio::system_error(error);
        }
        asio::io_context io(1);
        auto result = asio::co_spawn(io, verify(io), asio::use_future);
        io.run();
        result.get();
        std::cout << "[PASS] Real ICMP, retained EMA history, Agent route updates and shutdown\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
