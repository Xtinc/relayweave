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
        near(*result.quality.cost, 20.0, "Loss-free cost is incorrect");
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
        require(*result.metrics.rtt_ms < 30.0, "One spike replaced the weighted RTT history");
        for (int second = 1801; second <= 1805; ++second)
        {
            quality.record(false, 0.0, at(second));
        }
        require(!quality.assess(at(1805)).quality.usable, "RTT history masked consecutive failures");
        quality.record(true, 20.0, at(1806));
        require(quality.assess(at(1806)).quality.usable, "Reply did not recover link");
        require(!quality.assess(at(1900)).quality.usable, "Stale link stayed usable");
        quality.record(true, 10.0, at(4001));
        require(quality.assess(at(4001)).quality.confidence == 0.0, "Idle gap retained maturity");
        near(*quality.assess(at(4001)).quality.cost, 10.0, "Idle restart retained obsolete EMA");

        LinkQuality fast;
        LinkQuality slow;
        fast.record(true, 10.0, at(0));
        slow.record(true, 10.0, at(0));
        fast.record(true, 30.0, at(1));
        slow.record(true, 30.0, at(30));
        near(*slow.assess(at(30)).metrics.rtt_ms, 70.0 / 3.0, "30s half-life decay is incorrect");
        require(*slow.assess(at(30)).metrics.rtt_ms > *fast.assess(at(1)).metrics.rtt_ms,
                "EMA ignored actual elapsed time");
        {
            LinkQuality decay;
            const auto half_life = static_cast<int>(LinkQuality::half_life.count());
            decay.record(true, 10.0, at(0));
            decay.record(true, 30.0, at(half_life / 2));
            decay.record(true, 30.0, at(half_life));
            const auto midpoint_weight = std::sqrt(0.5);
            const auto expected = (5.0 + 30.0 * midpoint_weight + 30.0) / (0.5 + midpoint_weight + 1.0);
            near(*decay.assess(at(half_life)).metrics.rtt_ms, expected,
                 "Configured half-life did not decay the evidence correctly");
        }
        const auto previous_deviation = *fast.assess(at(1)).metrics.jitter_ms;
        fast.record(false, 0.0, at(2));
        result = fast.assess(at(2));
        near(*result.metrics.jitter_ms, previous_deviation, "Failed probe changed RTT standard deviation");
        LinkQuality spread;
        spread.record(true, 10.0, at(0));
        spread.record(true, 30.0, at(0));
        near(*spread.assess(at(0)).metrics.jitter_ms, 10.0, "Jitter must report RTT standard deviation");
        near(*spread.assess(at(0)).quality.cost, 38.906356760184394, "Lognormal P95 cost is incorrect");
        require(result.metrics.loss_rate > 0.341 && result.metrics.loss_rate < 0.342,
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
        require(sparse.assess(at(1800)).quality.confidence < 0.1, "Sparse history claimed maturity");

        LinkQuality lossy;
        LinkQuality reliable;
        for (int sample = 0; sample < 9; ++sample)
        {
            lossy.record(true, 20.0, at(0));
        }
        lossy.record(false, 0.0, at(0));
        reliable.record(true, 55.0, at(0));
        near(*lossy.assess(at(0)).quality.cost, 365.9383355319083,
             "A 10% loss rate saturated or used the wrong tail sensitivity");
        const auto lossy_cost = routing_cost(lossy.assess(at(0)).quality, std::chrono::milliseconds(0));
        const auto reliable_cost = routing_cost(reliable.assess(at(0)).quality, std::chrono::milliseconds(0));
        require(lossy_cost && reliable_cost, "Loss preference test requires usable links");
        require(*lossy_cost > *reliable_cost, "Loss estimate did not affect route preference");

        LinkQuality repeated;
        for (int sample = 0; sample < 1000; ++sample)
        {
            repeated.record(true, 10.0, at(0));
            repeated.record(true, 30.0, at(0));
        }
        near(*repeated.assess(at(0)).metrics.jitter_ms, 10.0,
             "RTT standard deviation shrank with sample count");
        repeated.record(false, 0.0, at(0));
        near(*repeated.assess(at(0)).quality.cost, 56.30515314154738,
             "Failure did not enter the equivalent scoring distribution");

        LinkQuality zero_delay;
        zero_delay.record(true, 0.0, at(0));
        near(*zero_delay.assess(at(0)).quality.cost, 0.0, "Zero RTT produced an invalid lognormal cost");

        LinkQuality fading_tail;
        fading_tail.record(true, 60000.0, at(0));
        for (int sample = 1; sample <= 20; ++sample)
        {
            const auto now = at(sample * 1799); // Shorter than the full-history reset interval.
            fading_tail.record(true, 0.0, now);
            const auto fading = fading_tail.assess(now);
            require(fading.quality.cost && std::isfinite(*fading.quality.cost),
                    "Decaying RTT history produced a non-finite cost");
            require(parse_quality(quality_json(fading.quality)).cost.has_value(),
                    "Extreme RTT history broke the quality wire format");
        }

        LinkQuality doubled;
        doubled.record(true, 20.0, at(0));
        doubled.record(true, 60.0, at(0));
        near(*doubled.assess(at(0)).quality.cost, 2.0 * *spread.assess(at(0)).quality.cost,
             "Lognormal cost depends on the RTT unit scale");

        LinkQuality spike;
        for (int second = 0; second < 14400; ++second)
        {
            spike.record(true, 70.0, at(second));
        }
        spike.record(true, 700.0, at(14400));
        require(*spike.assess(at(14400)).quality.cost > 200.0,
                "A recent 700 ms spike did not outweigh a stable 200 ms alternative");

        LinkQuality severe_loss;
        severe_loss.record(true, 20.0, at(0));
        for (int sample = 0; sample < 99; ++sample)
        {
            severe_loss.record(false, 0.0, at(0));
        }
        near(*severe_loss.assess(at(0)).quality.cost, 800.0, "High-loss estimate escaped the cap");
        require(!severe_loss.assess(at(0)).quality.usable, "Near-total loss remained usable");

        auto boundary_success = repeated;
        auto boundary_failure = repeated;
        boundary_success.record(true, 800.0, at(1));
        boundary_failure.record(false, 0.0, at(1));
        near(*boundary_success.assess(at(1)).quality.cost, *boundary_failure.assess(at(1)).quality.cost,
             "A boundary reply and a timeout have different quality costs");
        require(*boundary_success.assess(at(1)).quality.cost < 800.0,
                "Boundary equivalence was hidden by score saturation");
        require(*boundary_success.assess(at(1)).metrics.rtt_ms > *boundary_failure.assess(at(1)).metrics.rtt_ms,
                "Timeout substitution leaked into successful RTT diagnostics");

        LinkQuality overshoot;
        overshoot.record(true, 5000.0, at(0));
        near(*overshoot.assess(at(0)).quality.cost, 800.0, "Successful scoring sample escaped the deadline cap");
        near(*overshoot.assess(at(0)).metrics.rtt_ms, 5000.0, "Scoring cap altered real RTT diagnostics");

        for (double rtt : {0.001, 1.0, 20.0, 70.0, 200.0, 700.0, 799.0})
        {
            double previous_cost = -1.0;
            for (int failures = 0; failures < 100; ++failures)
            {
                LinkQuality distribution;
                for (int sample = failures; sample < 100; ++sample)
                {
                    distribution.record(true, rtt, at(0));
                    distribution.record(true, rtt / 2.0, at(0));
                }
                for (int sample = 0; sample < 2 * failures; ++sample)
                {
                    distribution.record(false, 0.0, at(0));
                }
                const auto cost = *distribution.assess(at(0)).quality.cost;
                require(cost >= previous_cost - 1e-8 && cost <= 800.0,
                        "Increasing loss lowered the bounded score for a fixed successful RTT distribution");
                previous_cost = cost;
            }
        }

        for (double rtt : {1.0, 20.0, 70.0, 200.0, 500.0, 799.0})
        {
            for (int failures : {20, 31})
            {
                LinkQuality calibrated;
                for (int sample = 0; sample < 100; ++sample)
                {
                    calibrated.record(sample >= failures, rtt, at(0));
                }
                const auto cost = *calibrated.assess(at(0)).quality.cost;
                require(failures == 20 ? cost < 800.0 : cost == 800.0,
                        "Stable-link saturation fell outside the calibrated 20%-31% loss range");
            }
        }

        LinkQuality tiny_rtt;
        for (int second = 0; second < 1800; ++second)
        {
            tiny_rtt.record(true, 0.001, at(second));
        }
        tiny_rtt.record(false, 0.0, at(1800));
        double previous_cost = *tiny_rtt.assess(at(1800)).quality.cost;
        for (int second = 1801; second < 4800; ++second)
        {
            tiny_rtt.record(true, 0.001, at(second));
            const auto cost = *tiny_rtt.assess(at(second)).quality.cost;
            require(cost <= previous_cost + 1e-8,
                    "An extreme lognormal shape rewarded higher loss during recovery");
            previous_cost = cost;
        }

        LinkQuality down;
        down.record(false, 0.0, at(0));
        require(!down.assess(at(0)).quality.cost && !down.assess(at(0)).quality.usable, "All-loss link has a cost");
        for (int second = 4002; second < 20000; ++second)
        {
            quality.record(true, 20.0, at(second));
        }
        require(std::isfinite(*quality.assess(at(19999)).quality.cost), "Long runtime destabilized EMA");
        std::cout << "[PASS] single-scale bounded lognormal EMA, state bytes=" << sizeof(LinkQuality) << '\n';
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
