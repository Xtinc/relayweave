#ifndef RELAYWEAVE_ICMP_H
#define RELAYWEAVE_ICMP_H

#include "asio.hpp"
#include "link_quality.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <vector>

class ICMP
{
  public:
    using Clock = std::chrono::steady_clock;
    using Duration = Clock::duration;

    struct Metrics
    {
        asio::ip::address_v4 destination;
        std::size_t transmitted = 0;
        LinkQuality::Assessment assessment;
    };

    ICMP(asio::io_context &io_context, Duration interval, Duration timeout,
         std::optional<std::size_t> count = std::nullopt);
    ~ICMP() = default;

    ICMP(const ICMP &) = delete;
    ICMP &operator=(const ICMP &) = delete;
    ICMP(ICMP &&) = delete;
    ICMP &operator=(ICMP &&) = delete;

    // Persistent probe state. Cancelled sends still count as transmitted, but
    // do not enter the completed-sample statistics.
    struct ProbeState
    {
        std::size_t transmitted = 0;
        LinkQuality quality;
    };

    using History = std::map<asio::ip::address_v4, ProbeState>;
    // Call run, metrics and history on the supplied single-threaded io_context.
    // Once run has started, close must drain all coroutines before destruction.
    void run(std::vector<asio::ip::address_v4> destinations, const History &history = {});
    History history() const;
    bool active() const noexcept
    {
        return running_probes_ != 0 && !stopping_;
    }

    std::vector<Metrics> metrics() const;
    asio::awaitable<void> close();

  private:
    struct Session
    {
        explicit Session(asio::io_context &io_context);

        Session(const Session &) = delete;
        Session &operator=(const Session &) = delete;

        ProbeState state;
        asio::steady_timer timer;
        Clock::time_point sent_at;
        std::uint16_t sequence = 0;
        bool in_flight = false;
    };

    asio::awaitable<void> probe_loop(asio::ip::address_v4 destination);
    asio::awaitable<void> receive_loop();
    asio::awaitable<void> close_on_executor();
    void handle_packet(std::span<const std::uint8_t> packet, asio::ip::address_v4 source) noexcept;
    std::optional<std::uint16_t> parse_reply(std::span<const std::uint8_t> packet) const noexcept;
    void probe_finished() noexcept;
    void stop() noexcept;
    void notify_finished() noexcept;

    asio::io_context &io_context_;
    const Duration interval_;
    const Duration timeout_;
    const std::optional<std::size_t> count_;
    asio::ip::icmp::socket socket_;
    std::uint64_t instance_ = 0;
    asio::steady_timer finished_wait_;
    std::map<asio::ip::address_v4, Session> sessions_;
    std::size_t running_probes_ = 0;
    bool receiving_ = false;
    bool stopping_ = false;
};

#endif // RELAYWEAVE_ICMP_H
