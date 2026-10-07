#include "link_quality.h"

#include <algorithm>
#include <cmath>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace
{
constexpr double ln2 = 0.6931471805599453;
constexpr std::array<double, 3> weights{0.15, 0.35, 0.50};
}

nlohmann::json quality_json(const LinkQuality::Summary &quality)
{
    return {{"cost", quality.cost ? nlohmann::json(*quality.cost) : nlohmann::json(nullptr)},
            {"confidence", quality.confidence}, {"usable", quality.usable}};
}

LinkQuality::Summary parse_quality(const nlohmann::json &value)
{
    if (!value.is_object() || value.size() != 3 || !value.contains("cost") ||
        !value.contains("confidence") || !value.contains("usable") || !value.at("usable").is_boolean())
    {
        throw std::invalid_argument("Invalid link quality object");
    }
    const auto number = [](const nlohmann::json &field, double maximum) {
        if (!field.is_number())
        {
            throw std::invalid_argument("Link quality requires a number");
        }
        const auto result = field.get<double>();
        if (!std::isfinite(result) || result < 0.0 || result > maximum)
        {
            throw std::invalid_argument("Link quality number is out of range");
        }
        return result;
    };
    LinkQuality::Summary result;
    result.confidence = number(value.at("confidence"), 1.0);
    result.usable = value.at("usable").get<bool>();
    if (!value.at("cost").is_null())
    {
        result.cost = number(value.at("cost"), 1e9);
    }
    else if (result.usable)
    {
        throw std::invalid_argument("Usable link quality requires a cost");
    }
    return result;
}

std::optional<double> routing_cost(const LinkQuality::Summary &quality, std::chrono::milliseconds age) noexcept
{
    using namespace std::chrono_literals;
    if (!quality.usable || !quality.cost || !std::isfinite(*quality.cost) || *quality.cost < 0.0 ||
        !std::isfinite(quality.confidence) || quality.confidence < 0.0 || quality.confidence > 1.0 ||
        age < 0ms || age >= 45s)
    {
        return std::nullopt;
    }
    double freshness = 1.0;
    if (age > 25s)
    {
        const double aging = std::chrono::duration<double>(age - 25s).count() / 20.0;
        freshness += 0.5 * aging * aging;
    }
    const double cost = *quality.cost * freshness + 5.0 * (1.0 - quality.confidence);
    return std::isfinite(cost) ? std::optional(cost) : std::nullopt;
}

bool LinkQuality::record(bool success, double rtt_ms, Clock::time_point now) noexcept
{
    if ((success && (!std::isfinite(rtt_ms) || rtt_ms < 0.0 || rtt_ms > 60000.0)) ||
        (last_sample_ && now < *last_sample_))
    {
        return false;
    }
    const auto elapsed = last_sample_ ? std::chrono::duration<double>(now - *last_sample_).count() : 0.0;
    if (!last_sample_ || elapsed >= half_lives.back().count())
    {
        first_sample_ = now;
        if (last_sample_)
        {
            ema_ = {};
            previous_rtt_.reset();
            consecutive_failures_ = 0;
        }
    }
    const auto jitter = success && previous_rtt_ && elapsed < half_lives.front().count()
                            ? std::optional(std::abs(rtt_ms - *previous_rtt_)) : std::nullopt;
    for (std::size_t index = 0; index < ema_.size(); ++index)
    {
        auto &ema = ema_[index];
        const auto decay = std::exp(-ln2 * elapsed / half_lives[index].count());
        ema.completed_weight = decay * ema.completed_weight + 1.0;
        ema.received_weight = decay * ema.received_weight + (success ? 1.0 : 0.0);
        ema.rtt_sum = decay * ema.rtt_sum + (success ? rtt_ms : 0.0);
        ema.rtt_square_sum = decay * ema.rtt_square_sum + (success ? rtt_ms * rtt_ms : 0.0);
        ema.jitter_weight = decay * ema.jitter_weight + (jitter ? 1.0 : 0.0);
        ema.jitter_sum = decay * ema.jitter_sum + jitter.value_or(0.0);
    }
    last_sample_ = now;
    ++completed_;
    if (!success)
    {
        ++consecutive_failures_;
        previous_rtt_.reset();
        return true;
    }
    ++received_;
    consecutive_failures_ = 0;
    last_success_ = now;
    previous_rtt_ = rtt_ms;
    return true;
}

LinkQuality::Assessment LinkQuality::assess(Clock::time_point now) const noexcept
{
    Assessment result;
    result.total_completed = completed_;
    result.total_received = received_;
    if (last_success_ && now >= *last_success_)
    {
        result.last_success_age = now - *last_success_;
    }
    if (!last_sample_ || now < *last_sample_)
    {
        return result;
    }
    double weighted_cost = 0.0;
    double weight_sum = 0.0;
    const auto idle = std::chrono::duration<double>(now - *last_sample_).count();
    for (std::size_t index = 0; index < ema_.size(); ++index)
    {
        const auto &ema = ema_[index];
        auto &metrics = result.scales[index];
        metrics.sample_weight = ema.completed_weight * std::exp(-ln2 * idle / half_lives[index].count());
        if (ema.completed_weight == 0.0)
        {
            continue;
        }
        metrics.loss_rate = std::clamp(1.0 - ema.received_weight / ema.completed_weight, 0.0, 1.0);
        if (ema.received_weight < 1e-9 || metrics.loss_rate >= 1.0)
        {
            continue;
        }
        const auto mean = ema.rtt_sum / ema.received_weight;
        const auto jitter = ema.jitter_weight > 1e-9 ? ema.jitter_sum / ema.jitter_weight : 0.0;
        const auto deviation = std::sqrt(std::max(0.0, ema.rtt_square_sum / ema.received_weight - mean * mean));
        metrics.rtt_ms = mean;
        metrics.jitter_ms = jitter;
        metrics.rtt_stddev_ms = deviation;
        const auto cost = mean + 0.5 * jitter + 0.25 * deviation - LOSS_PENALTY * std::log1p(-metrics.loss_rate);
        weighted_cost += weights[index] * cost;
        weight_sum += weights[index];
    }
    if (weight_sum == 0.0)
    {
        return result;
    }
    const auto coverage = std::chrono::duration<double>(*last_sample_ - first_sample_).count();
    const auto time_confidence = std::clamp(coverage / half_lives.back().count(), 0.0, 1.0);
    // 80% of the effective evidence expected after one long half-life at 1 Hz.
    const auto evidence_target = 0.8 * 0.5 * half_lives.back().count() / ln2;
    result.quality.confidence = std::min(time_confidence, std::min(1.0, result.scales.back().sample_weight / evidence_target));
    result.quality.cost = weighted_cost / weight_sum;
    result.quality.usable = result.scales.front().rtt_ms.has_value() && result.scales.front().loss_rate < 0.5 &&
                            consecutive_failures_ < 5 && result.last_success_age < std::chrono::seconds(45) && idle < 15.0;
    return result;
}
