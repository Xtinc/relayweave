#ifndef RELAYWEAVE_TOPOLOGY_H
#define RELAYWEAVE_TOPOLOGY_H

#include "cluster_mgr.h"
#include "probe_set.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

class Topology
{
  public:
    Topology(asio::io_context &io_context, std::string node_id, bool master, ClusterMgr &cluster_mgr,
             const std::atomic<std::uint32_t> &control_queue_delay_us,
             const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us,
             const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us);

    void start();
    asio::awaitable<void> close();
    void update_members(const njson &params);
    void update_report(const njson &params);
    std::optional<CtrlMessage> snapshot_message(std::uint64_t request_id) const;
    bool is_master() const noexcept;
    std::string_view master_id() const noexcept;
    const std::map<std::string, std::string> &members() const noexcept
    {
        return members_;
    }
    std::uint64_t epoch() const noexcept
    {
        return epoch_;
    }
    static void validate_snapshot(const njson &params, std::string_view location);

  private:
    using Clock = std::chrono::steady_clock;

    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };

    struct Link
    {
        std::string destination;
        std::optional<double> rtt_ms;
        std::optional<double> jitter_ms;
        double loss_rate = 0.0;
        std::size_t transmitted = 0;
        std::size_t received = 0;
        std::size_t completed = 0;
        std::optional<std::chrono::milliseconds> age;
        LinkQuality::Summary quality;
    };

    struct Report
    {
        std::uint64_t sequence = 0;
        Clock::time_point received_at;
        std::uint32_t control_queue_delay_us = 0;
        std::uint32_t transfer_tcp_queue_delay_us = 0;
        std::uint32_t transfer_udp_queue_delay_us = 0;
        std::vector<Link> links;
    };

    struct Snapshot
    {
        std::uint64_t version = 0;
        Clock::time_point created_at;
        njson nodes = njson::array();
        njson links = njson::array();
    };

    asio::awaitable<void> run();
    void send_report();
    void rebuild_snapshot();
    void wake() noexcept;

    static constexpr auto report_interval = std::chrono::seconds(5);
    static constexpr auto report_expiry = std::chrono::seconds(15);

    asio::io_context &io_context_;
    std::string node_id_;
    bool master_;
    ClusterMgr &cluster_mgr_;
    const std::atomic<std::uint32_t> &control_queue_delay_us_;
    const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us_;
    const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us_;
    asio::steady_timer wake_timer_;
    asio::steady_timer finished_wait_;
    ProbeSet probes_;
    std::map<std::string, std::string> members_;
    std::map<std::string, Report> reports_;
    Snapshot snapshot_;
    std::string master_id_;
    std::uint64_t epoch_ = 0;
    std::uint64_t members_version_ = 0;
    std::uint64_t report_sequence_ = 0;
    State state_ = State::Created;
};

#endif
