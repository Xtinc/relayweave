#ifndef RELAYWEAVE_LINK_QUALITY_H
#define RELAYWEAVE_LINK_QUALITY_H

#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <nlohmann/json_fwd.hpp>

// Normalized exponential statistics of completed probes. Constant memory per link.
class LinkQuality
{
  public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::array<std::chrono::seconds, 3> half_lives{
        std::chrono::seconds(30), std::chrono::seconds(300), std::chrono::seconds(1800)};

    struct EmaMetrics
    {
        double sample_weight = 0.0;
        std::optional<double> rtt_ms;
        std::optional<double> jitter_ms;
        std::optional<double> rtt_stddev_ms;
        double loss_rate = 1.0;
    };

    struct Summary
    {
        std::optional<double> cost;
        double confidence = 0.0;
        bool usable = false;
    };

    struct Assessment
    {
        Summary quality;
        std::array<EmaMetrics, 3> scales;
        std::size_t total_completed = 0;
        std::size_t total_received = 0;
        Clock::duration last_success_age = Clock::duration::max();
    };

    // Invalid RTTs and out-of-order timestamps are rejected without altering history.
    bool record(bool success, double rtt_ms, Clock::time_point now) noexcept;
    [[nodiscard]] Assessment assess(Clock::time_point now) const noexcept;

  private:
    static constexpr double LOSS_PENALTY = 400.0;

    struct Ema
    {
        double completed_weight = 0.0;
        double received_weight = 0.0;
        double jitter_weight = 0.0;
        double rtt_sum = 0.0;
        double rtt_square_sum = 0.0;
        double jitter_sum = 0.0;
    };

    std::array<Ema, 3> ema_;
    Clock::time_point first_sample_;
    std::optional<Clock::time_point> last_sample_;
    std::optional<Clock::time_point> last_success_;
    std::optional<double> previous_rtt_;
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
