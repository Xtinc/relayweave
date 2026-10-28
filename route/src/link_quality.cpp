#include "link_quality.h"

#include <algorithm>
#include <cmath>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace
{
constexpr double ln2 = 0.6931471805599453;
constexpr double normal_quantile_95 = 1.6448536269514715;
constexpr auto history_reset_interval = std::chrono::minutes(30);
} // namespace

nlohmann::json quality_json(const LinkQuality::Summary &quality)
{
    return {{"cost", quality.cost ? nlohmann::json(*quality.cost) : nlohmann::json(nullptr)},
            {"confidence", quality.confidence},
            {"usable", quality.usable}};
}

LinkQuality::Summary parse_quality(const nlohmann::json &value)
{
    if (!value.is_object() || value.size() != 3 || !value.contains("cost") || !value.contains("confidence") ||
        !value.contains("usable") || !value.at("usable").is_boolean())
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
        !std::isfinite(quality.confidence) || quality.confidence < 0.0 || quality.confidence > 1.0 || age < 0ms ||
        age >= 45s)
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
    if (!last_sample_ || elapsed >= std::chrono::duration<double>(history_reset_interval).count())
    {
        first_sample_ = now;
        ema_ = {};
        consecutive_failures_ = 0;
    }
    const auto decay = std::exp(-ln2 * elapsed / half_life.count());
    const auto score = success ? std::min(rtt_ms, static_cast<double>(scoring_timeout.count()))
                               : static_cast<double>(scoring_timeout.count());
    ema_.completed_weight = decay * ema_.completed_weight + 1.0;
    ema_.received_weight = decay * ema_.received_weight + (success ? 1.0 : 0.0);
    ema_.rtt_sum = decay * ema_.rtt_sum + (success ? rtt_ms : 0.0);
    ema_.rtt_square_sum = decay * ema_.rtt_square_sum + (success ? rtt_ms * rtt_ms : 0.0);
    ema_.score_sum = decay * ema_.score_sum + score;
    ema_.score_square_sum = decay * ema_.score_square_sum + score * score;
    last_sample_ = now;
    ++completed_;
    if (!success)
    {
        ++consecutive_failures_;
        return true;
    }
    ++received_;
    consecutive_failures_ = 0;
    last_success_ = now;
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
    auto &metrics = result.metrics;
    const auto idle = std::chrono::duration<double>(now - *last_sample_).count();
    metrics.loss_rate = std::clamp(1.0 - ema_.received_weight / ema_.completed_weight, 0.0, 1.0);
    if (ema_.received_weight < 1e-9 || metrics.loss_rate >= 1.0)
    {
        return result;
    }
    const auto mean = ema_.rtt_sum / ema_.received_weight;
    metrics.rtt_ms = mean;
    metrics.jitter_ms = std::sqrt(std::max(0.0, ema_.rtt_square_sum / ema_.received_weight - mean * mean));

    const auto maximum = static_cast<double>(scoring_timeout.count());
    const auto score_mean = std::clamp(ema_.score_sum / ema_.completed_weight, 0.0, maximum);
    const auto score_variance = std::max(0.0, ema_.score_square_sum / ema_.completed_weight - score_mean * score_mean);
    double cost = score_mean;
    if (score_mean > 0.0 && score_variance > 0.0)
    {
        static const auto maximum_variation = std::sqrt(std::expm1(normal_quantile_95 * normal_quantile_95));
        const auto variation = std::min(std::sqrt(score_variance) / score_mean, maximum_variation);
        const auto log_deviation = std::sqrt(std::log1p(variation * variation));
        cost = std::min(
            maximum, score_mean * std::exp(normal_quantile_95 * log_deviation - 0.5 * log_deviation * log_deviation));
    }
    const auto coverage = std::chrono::duration<double>(*last_sample_ - first_sample_).count();
    const auto time_confidence = std::clamp(coverage / half_life.count(), 0.0, 1.0);
    const auto evidence_target = 0.8 * 0.5 * half_life.count() / ln2;
    const auto sample_weight = ema_.completed_weight * std::exp(-ln2 * idle / half_life.count());
    result.quality.confidence = std::min(time_confidence, sample_weight / evidence_target);
    result.quality.cost = cost;
    result.quality.usable = metrics.loss_rate < 0.5 && consecutive_failures_ < 5 &&
                            result.last_success_age < std::chrono::seconds(45) && idle < 15.0;
    return result;
}
