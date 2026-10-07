#ifndef RELAYWEAVE_PROBE_SET_H
#define RELAYWEAVE_PROBE_SET_H

#include "icmp.h"
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

// Owns address resolution and probe reconstruction for a single control io_context.
// An empty ID is allowed for the configured Agent ingress before server.identify completes.
// set_targets and metrics are executor-confined; close drains refresh before destroying ICMP.
class ProbeSet
{
  public:
    using Targets = std::map<std::string, std::string>;
    struct Metric
    {
        std::string node_id;
        std::size_t transmitted;
        LinkQuality::Assessment assessment;
    };

    explicit ProbeSet(asio::io_context &io);
    void set_targets(Targets targets);
    // False means targets changed during an await; refresh again immediately.
    asio::awaitable<bool> refresh();
    std::vector<Metric> metrics() const;
    void stop();
    asio::awaitable<void> close();

  private:
    enum class State { Open, Stopping, Closing, Closed };
    asio::awaitable<bool> refresh_on_executor();
    asio::awaitable<void> close_on_executor();
    asio::awaitable<bool> refresh_targets();
    void notify_finished() noexcept;

    asio::io_context &io_;
    asio::ip::tcp::resolver resolver_;
    asio::steady_timer finished_wait_;
    Targets targets_;
    std::map<asio::ip::address_v4, std::vector<std::string>> resolved_;
    std::optional<ICMP> icmp_;
    ICMP::History history_;
    std::uint64_t revision_ = 0;
    std::uint64_t applied_revision_ = 0;
    bool retry_resolution_ = false;
    bool refreshing_ = false;
    State state_ = State::Open;
};

#endif
