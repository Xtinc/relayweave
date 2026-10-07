#include "topology.h"
#include "link_quality.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>

namespace
{
std::uint32_t queue_delay(const njson &params, std::string_view name, std::string_view location)
{
    const auto value = config::require_unsigned(params, name);
    if (value > std::numeric_limits<std::uint32_t>::max())
    {
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " is out of range");
    }
    return static_cast<std::uint32_t>(value);
}

double finite_number(const njson &parent, std::string_view name, double minimum, double maximum,
                     std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end() || !iterator->is_number())
    {
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " must be a number");
    }
    const auto value = iterator->get<double>();
    if (!std::isfinite(value) || value < minimum || value > maximum)
    {
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " is out of range");
    }
    return value;
}

std::optional<double> optional_number(const njson &parent, std::string_view name, std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end())
    {
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " is required");
    }
    if (iterator->is_null())
    {
        return std::nullopt;
    }
    return finite_number(parent, name, 0.0, std::numeric_limits<double>::max(), location);
}

std::optional<std::chrono::milliseconds> optional_age(const njson &parent, std::string_view name,
                                                      std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end())
    {
        throw std::invalid_argument(std::string(location) + "." + std::string(name) + " is required");
    }
    if (iterator->is_null())
    {
        return std::nullopt;
    }
    const auto value = config::require_unsigned(parent, name);
    const auto maximum = static_cast<std::uint64_t>(std::chrono::milliseconds::max().count());
    return std::chrono::milliseconds(std::min(value, maximum));
}

std::uint64_t milliseconds(std::chrono::steady_clock::duration duration) noexcept
{
    if (duration <= std::chrono::steady_clock::duration::zero())
    {
        return 0;
    }
    const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return static_cast<std::uint64_t>(value);
}

njson nullable(std::optional<double> value)
{
    return value ? njson(*value) : njson(nullptr);
}

njson nullable(std::optional<std::chrono::milliseconds> value)
{
    return value ? njson(static_cast<std::uint64_t>(value->count())) : njson(nullptr);
}

void validate_snapshot_link(const njson &link, std::string_view location)
{
    if (!link.is_object())
    {
        throw std::invalid_argument(std::string(location) + " link must be an object");
    }
    config::required_string(link, "source", location);
    config::required_string(link, "destination", location);
    static_cast<void>(optional_number(link, "rtt_ms", location));
    static_cast<void>(optional_number(link, "jitter_ms", location));
    static_cast<void>(finite_number(link, "loss_rate", 0.0, 1.0, location));
    config::require_unsigned(link, "transmitted");
    config::require_unsigned(link, "received");
    config::require_unsigned(link, "completed");
    static_cast<void>(optional_age(link, "age_ms", location));
    static_cast<void>(parse_quality(link.at("quality")));
}
} // namespace

void Topology::validate_snapshot(const njson &params, std::string_view location)
{
    config::require_unsigned(params, "request_id", true);
    config::require_unsigned(params, "epoch", true);
    config::require_unsigned(params, "snapshot_version", true);
    config::require_unsigned(params, "created_age_ms");
    const auto nodes = params.find("nodes");
    const auto links = params.find("links");
    if (nodes == params.end() || !nodes->is_array() || links == params.end() || !links->is_array())
    {
        throw std::invalid_argument(std::string(location) + ".nodes and links must be arrays");
    }
    for (const auto &node : *nodes)
    {
        config::required_string(node, "node_id", location);
        config::required_string(node, "address", location);
        const auto report_age = node.find("report_age_ms");
        if (report_age == node.end() || (!report_age->is_null() && !report_age->is_number_unsigned()))
        {
            throw std::invalid_argument(std::string(location) + ".nodes.report_age_ms is invalid");
        }
        for (const auto field :
             {"control_queue_delay_us", "transfer_tcp_queue_delay_us", "transfer_udp_queue_delay_us"})
        {
            const auto value = node.find(field);
            if (value == node.end() || (!value->is_null() && !value->is_number_unsigned()))
            {
                throw std::invalid_argument(std::string(location) + ".nodes queue delay is invalid");
            }
        }
    }
    for (const auto &link : *links)
    {
        validate_snapshot_link(link, location);
    }
}

Topology::Topology(asio::io_context &io_context, std::string node_id, bool master, ClusterMgr &cluster_mgr,
                   const std::atomic<std::uint32_t> &control_queue_delay_us,
                   const std::atomic<std::uint32_t> &transfer_tcp_queue_delay_us,
                   const std::atomic<std::uint32_t> &transfer_udp_queue_delay_us)
    : io_context_(io_context), node_id_(std::move(node_id)), master_(master), cluster_mgr_(cluster_mgr),
      control_queue_delay_us_(control_queue_delay_us), transfer_tcp_queue_delay_us_(transfer_tcp_queue_delay_us),
      transfer_udp_queue_delay_us_(transfer_udp_queue_delay_us), wake_timer_(io_context), finished_wait_(io_context),
      probes_(io_context)
{
    if (node_id_.empty())
    {
        throw std::invalid_argument("Topology requires a node ID");
    }
    finished_wait_.expires_at(Clock::time_point::max());
}

void Topology::start()
{
    if (state_ != State::Created)
    {
        throw std::logic_error("Topology can only be started once");
    }
    state_ = State::Running;
    try
    {
        asio::co_spawn(io_context_, run(), asio::detached);
    }
    catch (...)
    {
        state_ = State::Stopped;
        throw;
    }
}

asio::awaitable<void> Topology::close()
{
    co_await asio::dispatch(io_context_, asio::use_awaitable);
    if (state_ == State::Created)
    {
        state_ = State::Stopped;
        co_return;
    }
    if (state_ == State::Stopped)
    {
        co_return;
    }
    state_ = State::Stopping;
    probes_.stop();
    wake();
    auto [error] = co_await finished_wait_.async_wait(use_nothrow_awaitable);
    if (error && error != asio::error::operation_aborted)
    {
        throw asio::system_error(error, "Topology close wait failed");
    }
}

bool Topology::is_master() const noexcept
{
    return master_;
}

std::string_view Topology::master_id() const noexcept
{
    return master_id_;
}

void Topology::update_members(const njson &params)
{
    const auto source = config::required_string(params, "source", "topology.members");
    const auto master = config::required_string(params, "master", "topology.members");
    if (source != master)
    {
        throw std::invalid_argument("topology.members source does not match master");
    }
    if ((master_ && master != node_id_) || (!master_id_.empty() && master != master_id_))
    {
        throw std::invalid_argument("topology.members changed the cluster master");
    }
    const auto epoch = config::require_unsigned(params, "epoch", true);
    const auto version = config::require_unsigned(params, "version", true);
    if (epoch == epoch_ && version <= members_version_)
    {
        return;
    }

    const auto raw_members = params.find("members");
    if (raw_members == params.end() || !raw_members->is_array() || raw_members->empty())
    {
        throw std::invalid_argument("topology.members.members must be a non-empty array");
    }
    std::map<std::string, std::string> members;
    for (const auto &entry : *raw_members)
    {
        const auto id = config::required_string(entry, "node_id", "topology.members");
        auto address = config::required_string(entry, "address", "topology.members");
        if (!members.emplace(id, std::move(address)).second)
        {
            throw std::invalid_argument("topology.members contains a duplicate node_id");
        }
    }
    if (!members.contains(node_id_) || !members.contains(master))
    {
        throw std::invalid_argument("topology.members does not contain this node and master");
    }

    epoch_ = epoch;
    members_version_ = version;
    master_id_ = master;
    members_ = std::move(members);
    reports_.clear();
    report_sequence_ = 0;
    auto targets = members_;
    targets.erase(node_id_);
    probes_.set_targets(std::move(targets));
    if (master_)
    {
        rebuild_snapshot();
    }
    wake();
}

void Topology::update_report(const njson &params)
{
    if (!master_)
    {
        return;
    }
    const auto source = config::required_string(params, "source", "topology.report");
    if (!members_.contains(source) || config::require_unsigned(params, "epoch", true) != epoch_ ||
        config::require_unsigned(params, "members_version", true) != members_version_)
    {
        return;
    }
    const auto sequence = config::require_unsigned(params, "sequence", true);
    const auto previous = reports_.find(source);
    if (previous != reports_.end() && sequence <= previous->second.sequence)
    {
        return;
    }

    Report report;
    report.sequence = sequence;
    report.received_at = Clock::now();
    report.control_queue_delay_us = queue_delay(params, "control_queue_delay_us", "topology.report");
    report.transfer_tcp_queue_delay_us = queue_delay(params, "transfer_tcp_queue_delay_us", "topology.report");
    report.transfer_udp_queue_delay_us = queue_delay(params, "transfer_udp_queue_delay_us", "topology.report");

    const auto raw_links = params.find("links");
    if (raw_links == params.end() || !raw_links->is_array())
    {
        throw std::invalid_argument("topology.report.links must be an array");
    }
    std::set<std::string> destinations;
    report.links.reserve(raw_links->size());
    for (const auto &entry : *raw_links)
    {
        auto destination = config::required_string(entry, "destination", "topology.report");
        if (destination == source || !members_.contains(destination) || !destinations.insert(destination).second)
        {
            throw std::invalid_argument("topology.report contains an invalid destination");
        }
        report.links.push_back(Link{std::move(destination), optional_number(entry, "rtt_ms", "topology.report"),
                                    optional_number(entry, "jitter_ms", "topology.report"),
                                    finite_number(entry, "loss_rate", 0.0, 1.0, "topology.report"),
                                    static_cast<std::size_t>(config::require_unsigned(entry, "transmitted")),
                                    static_cast<std::size_t>(config::require_unsigned(entry, "received")),
                                    static_cast<std::size_t>(config::require_unsigned(entry, "completed")),
                                    optional_age(entry, "age_ms", "topology.report"),
                                    parse_quality(entry.at("quality"))});
    }
    reports_.insert_or_assign(source, std::move(report));
    rebuild_snapshot();
}

asio::awaitable<void> Topology::run()
{
    auto next_report = Clock::now();
    try
    {
        while (state_ == State::Running)
        {
            if (!co_await probes_.refresh())
            {
                continue;
            }
            if (state_ != State::Running)
            {
                break;
            }

            const auto now = Clock::now();
            if (now >= next_report)
            {
                send_report();
                if (master_)
                {
                    rebuild_snapshot();
                }
                next_report = now + report_interval;
            }
            wake_timer_.expires_at(next_report);
            auto [error] = co_await wake_timer_.async_wait(use_nothrow_awaitable);
            if (error && error != asio::error::operation_aborted)
            {
                throw asio::system_error(error, "Topology timer failed");
            }
        }
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("Topology failed reason=%s", exception.what());
    }
    catch (...)
    {
        PROXY_ERROR_PRINT("Topology failed reason=unknown exception");
    }

    try
    {
        co_await probes_.close();
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("Topology close failed reason=%s", exception.what());
    }
    state_ = State::Stopped;
    finished_wait_.cancel();
}

void Topology::send_report()
{
    if (epoch_ == 0 || members_version_ == 0 || master_id_.empty())
    {
        return;
    }

    njson links = njson::array();
    for (const auto &metric : probes_.metrics())
    {
        const auto &assessment = metric.assessment;
        const auto &long_ema = assessment.scales.back();
        const auto age =
            assessment.last_success_age == ICMP::Duration::max()
                ? std::optional<std::chrono::milliseconds>{}
                : std::optional(std::chrono::duration_cast<std::chrono::milliseconds>(assessment.last_success_age));
        links.push_back(njson{{"destination", metric.node_id},
                              {"rtt_ms", nullable(long_ema.rtt_ms)},
                              {"jitter_ms", nullable(long_ema.jitter_ms)},
                              {"loss_rate", long_ema.loss_rate},
                              {"transmitted", metric.transmitted},
                              {"received", assessment.total_received},
                              {"completed", assessment.total_completed},
                              {"age_ms", nullable(age)},
                              {"quality", quality_json(assessment.quality)}});
    }
    ++report_sequence_;
    cluster_mgr_.send(
        master_id_,
        CtrlMessage{CtrlCommand::TopologyReport,
                    njson{{"epoch", epoch_},
                          {"members_version", members_version_},
                          {"sequence", report_sequence_},
                          {"control_queue_delay_us", control_queue_delay_us_.load(std::memory_order_relaxed)},
                          {"transfer_tcp_queue_delay_us", transfer_tcp_queue_delay_us_.load(std::memory_order_relaxed)},
                          {"transfer_udp_queue_delay_us", transfer_udp_queue_delay_us_.load(std::memory_order_relaxed)},
                          {"links", std::move(links)}}});
}

void Topology::rebuild_snapshot()
{
    if (!master_ || epoch_ == 0)
    {
        return;
    }
    const auto now = Clock::now();
    Snapshot next;
    next.version = snapshot_.version + 1;
    next.created_at = now;

    for (const auto &[id, address] : members_)
    {
        njson node{{"node_id", id}, {"address", address}};
        const auto report = reports_.find(id);
        if (report == reports_.end() || now - report->second.received_at >= report_expiry)
        {
            node["report_age_ms"] = nullptr;
            node["control_queue_delay_us"] = nullptr;
            node["transfer_tcp_queue_delay_us"] = nullptr;
            node["transfer_udp_queue_delay_us"] = nullptr;
        }
        else
        {
            node["report_age_ms"] = milliseconds(now - report->second.received_at);
            node["control_queue_delay_us"] = report->second.control_queue_delay_us;
            node["transfer_tcp_queue_delay_us"] = report->second.transfer_tcp_queue_delay_us;
            node["transfer_udp_queue_delay_us"] = report->second.transfer_udp_queue_delay_us;
            for (const auto &link : report->second.links)
            {
                auto age = link.age;
                if (age)
                {
                    const auto elapsed =
                        std::chrono::duration_cast<std::chrono::milliseconds>(now - report->second.received_at);
                    *age += std::min(elapsed, std::chrono::milliseconds::max() - *age);
                }
                next.links.push_back(njson{{"source", id},
                                           {"destination", link.destination},
                                           {"rtt_ms", nullable(link.rtt_ms)},
                                           {"jitter_ms", nullable(link.jitter_ms)},
                                           {"loss_rate", link.loss_rate},
                                           {"transmitted", link.transmitted},
                                           {"received", link.received},
                                           {"completed", link.completed},
                                           {"age_ms", nullable(age)},
                                           {"quality", quality_json(link.quality)}});
            }
        }
        next.nodes.push_back(std::move(node));
    }
    snapshot_ = std::move(next);
}

std::optional<CtrlMessage> Topology::snapshot_message(std::uint64_t request_id) const
{
    if (!master_ || snapshot_.version == 0)
        return std::nullopt;
    return CtrlMessage{CtrlCommand::TopologySnapshot,
                       njson{{"request_id", request_id},
                             {"epoch", epoch_},
                             {"snapshot_version", snapshot_.version},
                             {"created_age_ms", milliseconds(Clock::now() - snapshot_.created_at)},
                             {"nodes", snapshot_.nodes},
                             {"links", snapshot_.links}}};
}

void Topology::wake() noexcept
{
    try
    {
        wake_timer_.cancel();
    }
    catch (...)
    {
    }
}
