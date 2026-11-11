#ifndef RELAYWEAVE_PEER_BLACKLIST_H
#define RELAYWEAVE_PEER_BLACKLIST_H

#include <asio/ip/address_v4.hpp>
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

/**
 * @brief Own bounded IPv4 admission penalties for one TCP listener on its single-threaded executor.
 * @details Access advances one shared minute clock and ages the whole table. Entries contain only
 * counters, never individual timestamps. IPv4 loopback (127.0.0.0/8) is never tracked or blocked.
 * No tasks or external dependencies are retained.
 */
class PeerBlacklist
{
  public:
    using Clock = std::chrono::steady_clock;
    static constexpr auto round_duration = std::chrono::minutes(1);
    static constexpr std::uint8_t failure_limit = 5;
    static constexpr std::uint16_t initial_rounds = 30;
    static constexpr std::uint16_t maximum_rounds = 720;
    static constexpr std::size_t capacity = 16384;

    /**
     * @brief Initialize the listener's common aging clock without starting a timer.
     * @param listener Listener name copied for ban and capacity diagnostics.
     * @param now Initial steady-clock round boundary; injectable for deterministic tests.
     */
    explicit PeerBlacklist(std::string_view listener, Clock::time_point now = Clock::now());

    /**
     * @brief Age all entries, then reject and extend an active ban by one round, capped at 720.
     * @param address IPv4 source of a newly accepted TCP socket, independent of its source port.
     * @param now Current steady-clock time; must not precede previous calls.
     * @return True when the socket must be rejected; false for loopback, an allowed or expired source.
     * @pre Call exactly once per accepted socket, on this listener's executor.
     */
    bool blocked(asio::ip::address_v4 address, Clock::time_point now = Clock::now());

    /**
     * @brief Record one connection ending before authentication or authorization, banning on the fifth this round.
     * @param address IPv4 source captured before transferring ownership of the accepted socket.
     * @param reason Diagnostic reason used only when a new ban is installed.
     * @param now Current steady-clock time; must not precede previous calls.
     * @return True only for a newly installed ban; false for loopback, below threshold, while banned, or at capacity.
     * @pre Call at most once per failed connection, on this listener's executor.
     */
    bool record_failure(asio::ip::address_v4 address, std::string_view reason,
                        Clock::time_point now = Clock::now());

  private:
    struct Entry
    {
        std::uint8_t failures = 0;
        std::uint16_t rounds = 0;
    };

    /** @brief Clear all suspicion counters and subtract elapsed rounds from bans in one bounded scan. */
    void age(Clock::time_point now);

    std::string listener_;
    Clock::time_point round_at_;
    std::map<std::uint32_t, Entry> entries_;
    bool capacity_reported_ = false;
};

#endif
