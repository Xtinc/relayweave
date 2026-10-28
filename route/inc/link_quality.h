#ifndef RELAYWEAVE_LINK_QUALITY_H
#define RELAYWEAVE_LINK_QUALITY_H

#include <chrono>
#include <cstddef>
#include <optional>
#include <nlohmann/json_fwd.hpp>

/**
 * @brief Owns constant-memory probe statistics on the caller's serial executor.
 * Successful RTT diagnostics exclude failures. Scoring caps successful samples and
 * substitutes failures at scoring_timeout, using one half-life for all statistics.
 */
class LinkQuality
{
  public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::chrono::seconds half_life{30};
    static constexpr std::chrono::milliseconds scoring_timeout{800};

    /** @brief Exponentially weighted successful RTT moments and completed-probe loss. */
    struct EmaMetrics
    {
        std::optional<double> rtt_ms;
        std::optional<double> jitter_ms;
        double loss_rate = 1.0;
    };

    /** @brief Bounded quality cost in milliseconds, confidence and route eligibility. */
    struct Summary
    {
        std::optional<double> cost;
        double confidence = 0.0;
        bool usable = false;
    };

    /** @brief Quality, single-scale diagnostics and lifetime completion counters. */
    struct Assessment
    {
        Summary quality;
        EmaMetrics metrics;
        std::size_t total_completed = 0;
        std::size_t total_received = 0;
        Clock::duration last_success_age = Clock::duration::max();
    };

    /**
     * @brief Records one completed probe; invalid samples leave history unchanged.
     * @param success Whether the probe received a valid reply before its deadline.
     * @param rtt_ms Successful RTT in milliseconds, finite and within [0, 60000]; ignored on failure.
     * @param now Completion time, no earlier than the previous completion.
     * @return Whether the sample was accepted.
     */
    bool record(bool success, double rtt_ms, Clock::time_point now) noexcept;
    /**
     * @brief Evaluates decayed evidence, bounded lognormal quality and availability.
     * @param now Evaluation time, no earlier than the latest completion.
     * @return Diagnostics and quality; an empty history or invalid time has no cost.
     */
    [[nodiscard]] Assessment assess(Clock::time_point now) const noexcept;

  private:
    /** @brief Owns decayed evidence, successful RTT moments and capped scoring moments. */
    struct Ema
    {
        double completed_weight = 0.0;
        double received_weight = 0.0;
        double rtt_sum = 0.0;
        double rtt_square_sum = 0.0;
        double score_sum = 0.0;
        double score_square_sum = 0.0;
    };

    Ema ema_;
    Clock::time_point first_sample_;
    std::optional<Clock::time_point> last_sample_;
    std::optional<Clock::time_point> last_success_;
    std::size_t completed_ = 0;
    std::size_t received_ = 0;
    std::size_t consecutive_failures_ = 0;
};

// Shared wire representation for Node reports and Agent topology snapshots.
nlohmann::json quality_json(const LinkQuality::Summary &quality);
LinkQuality::Summary parse_quality(const nlohmann::json &value);

// Converts a fresh, usable quality summary to the final nonnegative route edge weight.
std::optional<double> routing_cost(const LinkQuality::Summary &quality, std::chrono::milliseconds age) noexcept;

#endif
