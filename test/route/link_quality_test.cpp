#include "link_quality.h"

#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <nlohmann/json.hpp>

namespace
{
void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}
auto at(int seconds)
{
    return LinkQuality::Clock::time_point(std::chrono::seconds(seconds));
}
void near(double actual, double expected, const char *reason)
{
    require(std::abs(actual - expected) < 1e-8, reason);
}
}

int main()
{
    try
    {
        LinkQuality quality;
        require(sizeof(LinkQuality) < 512, "EMA state is not constant and compact");
        require(!quality.assess(at(0)).quality.cost, "Empty history has a cost");
        LinkQuality::Summary summary{20.0, 1.0, true};
        const auto encoded = quality_json(summary);
        require(encoded.size() == 3 && !encoded.contains("score"), "Quality wire format carries a redundant score");
        near(*parse_quality(encoded).cost, 20.0, "Quality summary did not round trip");
        near(*routing_cost(summary, std::chrono::seconds(25)), 20.0, "Fresh cost was changed");
        near(*routing_cost(summary, std::chrono::seconds(35)), 22.5, "Freshness penalty is incorrect");
        require(!routing_cost(summary, std::chrono::seconds(45)), "Expired cost was accepted");
        require(!routing_cost(summary, std::chrono::milliseconds(-1)), "Negative age was accepted");
        summary.confidence = 0.0;
        near(*routing_cost(summary, std::chrono::seconds(0)), 25.0, "Cold-start penalty is incorrect");
        for (auto invalid : {nlohmann::json{{"cost", nullptr}, {"confidence", 0.0}, {"usable", true}},
                             nlohmann::json{{"cost", 1.0}, {"confidence", 2.0}, {"usable", true}},
                             nlohmann::json{{"cost", -1.0}, {"confidence", 1.0}, {"usable", true}}})
        {
            bool rejected = false;
            try { static_cast<void>(parse_quality(invalid)); }
            catch (const std::invalid_argument &) { rejected = true; }
            require(rejected, "Invalid quality summary was accepted");
        }
        for (int second = 0; second < 1800; ++second)
        {
            quality.record(true, 20.0, at(second));
        }
        auto result = quality.assess(at(1799));
        near(*result.quality.cost, 20.0, "Stable link cost is incorrect");
        require(result.quality.usable && result.quality.confidence > 0.99, "Mature link is not usable/confident");
        require(quality.assess(at(1813)).quality.usable && !quality.assess(at(1814)).quality.usable,
                "Completed-sample idle deadline is not exactly 15 seconds");
        auto old_success = quality;
        old_success.record(false, 0.0, at(1840));
        require(old_success.assess(at(1843)).quality.usable && !old_success.assess(at(1844)).quality.usable,
                "Last-success deadline is not exactly 45 seconds");
        LinkQuality short_loss;
        short_loss.record(true, 20.0, at(0));
        short_loss.record(false, 0.0, at(1));
        require(!short_loss.assess(at(1)).quality.usable, "Short-term loss gate did not disable a majority-loss link");
        quality.record(true, 400.0, at(1800));
        result = quality.assess(at(1800));
        require(*result.scales.back().rtt_ms < 21.0, "One spike erased long-term EMA");
        for (int second = 1801; second <= 1805; ++second)
        {
            quality.record(false, 0.0, at(second));
        }
        require(!quality.assess(at(1805)).quality.usable, "Long EMA masked consecutive failures");
        quality.record(true, 20.0, at(1806));
        require(quality.assess(at(1806)).quality.usable, "Reply did not recover link");
        require(!quality.assess(at(1900)).quality.usable, "Stale link stayed usable");
        quality.record(true, 10.0, at(4001));
        require(quality.assess(at(4001)).quality.confidence == 0.0, "Idle gap retained maturity");
        near(*quality.assess(at(4001)).quality.cost, 10.0, "Idle restart retained obsolete EMA");

        LinkQuality fast, slow;
        fast.record(true, 10.0, at(0));
        slow.record(true, 10.0, at(0));
        fast.record(true, 30.0, at(1));
        slow.record(true, 30.0, at(30));
        near(*slow.assess(at(30)).scales.front().rtt_ms, 70.0 / 3.0, "30s half-life decay is incorrect");
        require(*slow.assess(at(30)).scales.front().rtt_ms > *fast.assess(at(1)).scales.front().rtt_ms,
                "EMA ignored actual elapsed time");
        for (std::size_t index = 0; index < LinkQuality::half_lives.size(); ++index)
        {
            LinkQuality decay;
            const auto half_life = static_cast<int>(LinkQuality::half_lives[index].count());
            decay.record(true, 10.0, at(0));
            decay.record(true, 30.0, at(half_life / 2));
            decay.record(true, 30.0, at(half_life));
            // After one half-life, the first sample has weight 1/2 and the
            // midpoint sample has weight sqrt(1/2), independently of the scale.
            const auto midpoint_weight = std::sqrt(0.5);
            const auto expected = (5.0 + 30.0 * midpoint_weight + 30.0) / (0.5 + midpoint_weight + 1.0);
            near(*decay.assess(at(half_life)).scales[index].rtt_ms, expected,
                 "Configured half-life did not decay the evidence correctly");
        }
        fast.record(false, 0.0, at(2));
        result = fast.assess(at(2));
        near(*result.scales.back().jitter_ms, 20.0, "Jitter is incorrect");
        require(*result.scales.back().rtt_stddev_ms > 9.9, "RTT spread is incorrect");
        require(result.scales.back().loss_rate > 0.333 && result.scales.back().loss_rate < 0.334,
                "Failed sample did not enter loss EMA");
        require(!fast.record(true, -1.0, at(3)), "Negative RTT was accepted");
        require(!fast.record(true, std::numeric_limits<double>::quiet_NaN(), at(3)), "NaN was accepted");
        require(!fast.record(true, 10.0, at(0)), "Time regression was accepted");
        require(fast.assess(at(3)).total_completed == 3, "Invalid samples changed state");
        LinkQuality sparse;
        for (int second = 0; second <= 1800; second += 60)
        {
            sparse.record(true, 20.0, at(second));
        }
        require(sparse.assess(at(1800)).quality.confidence < 0.03, "Sparse history claimed maturity");

        // Equal timestamps isolate loss from decay, freshness and confidence.
        // A 20 ms link with 10% loss must lose to a stable 55 ms link.
        LinkQuality lossy, reliable;
        for (int sample = 0; sample < 9; ++sample)
        {
            lossy.record(true, 20.0, at(0));
        }
        lossy.record(false, 0.0, at(0));
        reliable.record(true, 55.0, at(0));
        const auto lossy_cost = routing_cost(lossy.assess(at(0)).quality, std::chrono::milliseconds(0));
        const auto reliable_cost = routing_cost(reliable.assess(at(0)).quality, std::chrono::milliseconds(0));
        require(lossy_cost && reliable_cost, "Loss preference test requires usable links");
        require(*lossy_cost > *reliable_cost, "Loss penalty still prefers a lossy low-latency link");

        LinkQuality down;
        down.record(false, 0.0, at(0));
        require(!down.assess(at(0)).quality.cost && !down.assess(at(0)).quality.usable, "All-loss link has a cost");
        for (int second = 1801; second < 20000; ++second)
        {
            quality.record(true, 20.0, at(second));
        }
        require(std::isfinite(*quality.assess(at(19999)).quality.cost), "Long runtime destabilized EMA");
        std::cout << "[PASS] time-aware multi-horizon EMA, state bytes=" << sizeof(LinkQuality) << '\n';
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
