#ifndef RELAYWEAVE_TOPOLOGY_H
#define RELAYWEAVE_TOPOLOGY_H

#include "cluster_mgr.h"
#include "async_event.h"
#include "probe_set.h"
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

class NodeMetrics;

/**
 * @brief Maintain cluster membership, link probes, reports, and master snapshots on control_io.
 * @details Owns ProbeSet and the reporting task; borrows ClusterMgr and NodeMetrics.
 * Member epochs authorize NodeLink transactions. All instance entry points require control_io.
 */
class Topology
{
  public:
    /**
     * @brief Construct local topology state and probes without starting the reporting task.
     * @param io_context Single-threaded control context owning probes, reports, and snapshot state.
     * @param node_id Unique nonempty cluster Node identity.
     * @param master True when this Node aggregates member reports as cluster master.
     * @param cluster_mgr Node-owned cluster manager borrowed for membership and report exchange.
     * @param metrics Node-owned metrics provider borrowed for control-side snapshots.
     */
    Topology(asio::io_context &io_context, std::string node_id, bool master, ClusterMgr &cluster_mgr,
             const NodeMetrics &metrics);

    /** @brief Start the reporting task once during bootstrap or on control_io. */
    void start();
    /**
     * @brief Stop and drain probes and reporting; every normal return guarantees completion.
     * @return True after successful probe and reporting drainage.
     */
    asio::awaitable<bool> close();
    /**
     * @brief Validate and apply a newer master membership list, updating probe targets.
     * @param params Master membership payload with authenticated source, epoch, version, master ID, and
     * member addresses.
     */
    void update_members(const njson &params);
    /**
     * @brief Validate and retain a member quality report on the master.
     * @param params Authenticated member report containing epoch, sequence, queue delays, availability, and
     * link quality.
     */
    void update_report(const njson &params);
    /**
     * @brief Build a correlated snapshot reply on the master, or return no message on a slave.
     * @param request_id Request identifier echoed in the snapshot response.
     * @return Correlated topology.snapshot on the master, or std::nullopt when no master snapshot can be
     * supplied.
     */
    std::optional<CtrlMessage> snapshot_message(std::uint64_t request_id) const;
    /**
     * @brief Return whether this topology belongs to the cluster master.
     * @return True for the configured cluster master, false for a slave.
     */
    bool is_master() const noexcept;
    /**
     * @brief Borrow the current master ID until the next membership update.
     * @return View of the current master identity; invalidated by membership updates or destruction.
     */
    std::string_view master_id() const noexcept;
    /**
     * @brief Borrow the current node-to-advertised-address membership map.
     * @return Read-only reference to the current Node ID to advertised-address map; do not retain across
     * asynchronous waits.
     */
    const std::map<std::string, std::string> &members() const noexcept
    {
        return members_;
    }
    /**
     * @brief Return the current membership epoch, or zero before membership is known.
     * @return Current master incarnation, or zero before the first membership update.
     */
    std::uint64_t epoch() const noexcept
    {
        return epoch_;
    }
    /**
     * @brief Validate snapshot fields and quality ranges; throw with the supplied diagnostic location.
     * @param params Topology snapshot reply with request ID, epoch, snapshot version, nodes, and links.
     * @param location Field path or command name included in validation error messages.
     * @throws std::invalid_argument If required fields, payload shapes, or quality ranges are invalid.
     */
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

    /** @brief Store one reported directed link with measured quality and sample age. */
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

    /** @brief Retain a member's latest sequence, receive time, queue delays, and link measurements. */
    struct Report
    {
        std::uint64_t sequence = 0;
        Clock::time_point received_at;
        std::uint32_t control_queue_delay_us = 0;
        std::uint32_t transfer_tcp_queue_delay_us = 0;
        std::uint32_t transfer_udp_queue_delay_us = 0;
        double available = 1.0;
        std::vector<Link> links;
    };

    /** @brief Own the master's versioned, serialized node and link snapshot. */
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
    const NodeMetrics &metrics_;
    asio::steady_timer wake_timer_;
    AsyncEvent finished_;
    std::exception_ptr drain_error_;
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
