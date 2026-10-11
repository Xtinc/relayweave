#include "probe_set.h"

#include "message.h"
#include <algorithm>

ProbeSet::ProbeSet(asio::io_context &io) : io_(io), resolver_(io), finished_wait_(io)
{
    finished_wait_.expires_at(ICMP::Clock::time_point::max());
}

void ProbeSet::set_targets(Targets targets)
{
    if (targets != targets_)
    {
        // Hide removed or changed identities immediately, while DNS is still pending.
        // Keep the IP keys so an identity-only update can reuse the running probes.
        for (auto &[_, ids] : resolved_)
        {
            std::erase_if(ids, [&](const std::string &id) {
                const auto previous = targets_.find(id);
                const auto next = targets.find(id);
                return previous == targets_.end() || next == targets.end() || previous->second != next->second;
            });
        }
        targets_ = std::move(targets);
        ++revision_;
    }
}

asio::awaitable<bool> ProbeSet::refresh()
{
    return asio::co_spawn(io_, refresh_on_executor(), asio::use_awaitable);
}

asio::awaitable<bool> ProbeSet::refresh_on_executor()
{
    if (refreshing_)
    {
        throw std::logic_error("ProbeSet refresh must not overlap another refresh");
    }
    refreshing_ = true;
    bool result;
    try
    {
        result = co_await refresh_targets();
    }
    catch (...)
    {
        refreshing_ = false;
        notify_finished();
        throw;
    }
    refreshing_ = false;
    notify_finished();
    co_return result;
}

asio::awaitable<bool> ProbeSet::refresh_targets()
{
    using namespace std::chrono_literals;
    if (state_ != State::Open)
    {
        co_return true;
    }
    if (revision_ == applied_revision_ && !retry_resolution_ && (targets_.empty() || (icmp_ && icmp_->active())))
    {
        co_return true;
    }
    const auto revision = revision_;
    const auto targets = targets_; // DNS may suspend while set_targets replaces the input.
    std::map<asio::ip::address_v4, std::vector<std::string>> resolved;
    bool retry = false;
    for (const auto &[id, host] : targets)
    {
        asio::error_code error;
        auto address = asio::ip::make_address_v4(host, error);
        if (error)
        {
            const auto [resolve_error, endpoints] = co_await resolver_.async_resolve(
                asio::ip::tcp::v4(), host, "0", asio::cancel_after(1s, asio::as_tuple(asio::use_awaitable)));
            if (state_ != State::Open || revision != revision_)
            {
                co_return state_ != State::Open;
            }
            if (resolve_error || endpoints.empty())
            {
                retry = true;
                PROXY_ERROR_PRINT("Probe resolve failed target=%s reason=%s retry=next poll", host.c_str(),
                                  resolve_error ? resolve_error.message().c_str() : "no IPv4 address");
                continue;
            }
            address = endpoints.begin()->endpoint().address().to_v4();
        }
        if (address.is_unspecified() || address.is_multicast() || address.to_uint() == 0xffffffffU)
        {
            retry = true;
            PROXY_ERROR_PRINT("Probe rejected target=%s reason=not unicast IPv4", host.c_str());
            continue;
        }
        resolved[address].push_back(id);
    }

    const bool same_addresses = resolved.size() == resolved_.size() &&
                                std::equal(resolved.begin(), resolved.end(), resolved_.begin(),
                                           [](const auto &a, const auto &b) { return a.first == b.first; });
    if (!same_addresses || !icmp_ || !icmp_->active())
    {
        if (icmp_)
        {
            co_await icmp_->close();
            history_ = icmp_->history();
            icmp_.reset();
        }
        if (state_ != State::Open || revision != revision_)
        {
            co_return state_ != State::Open;
        }
        std::erase_if(history_, [&](const auto &entry) { return !resolved.contains(entry.first); });
        if (!resolved.empty())
        {
            std::vector<asio::ip::address_v4> addresses;
            for (const auto &[address, _] : resolved)
            {
                addresses.push_back(address);
            }
            icmp_.emplace(io_, 1s, 800ms);
            bool started = false;
            try
            {
                icmp_->run(std::move(addresses), history_);
                started = true;
            }
            catch (const std::exception &exception)
            {
                const auto *error = dynamic_cast<const asio::system_error *>(&exception);
                if (error && (error->code() == asio::error::access_denied ||
                              error->code() == std::errc::operation_not_permitted))
                    PROXY_DEBUG_PRINT("Probe unavailable ICMP reason=%s retry=next poll", exception.what());
                else
                    PROXY_ERROR_PRINT("Probe failed ICMP reason=%s retry=next poll", exception.what());
            }
            if (!started)
            {
                // run may have started some coroutines; always drain before destroying their owner.
                co_await icmp_->close();
                icmp_.reset();
            }
            else
            {
                history_.clear();
            }
        }
    }
    resolved_ = std::move(resolved);
    applied_revision_ = revision;
    retry_resolution_ = retry;
    co_return state_ != State::Open || revision == revision_;
}

std::vector<ProbeSet::Metric> ProbeSet::metrics() const
{
    std::vector<Metric> result;
    if (state_ != State::Open || !icmp_)
    {
        return result;
    }
    for (const auto &metric : icmp_->metrics())
    {
        const auto found = resolved_.find(metric.destination);
        if (found != resolved_.end())
        {
            for (const auto &id : found->second)
            {
                result.push_back({id, metric.transmitted, metric.assessment});
            }
        }
    }
    return result;
}

void ProbeSet::stop()
{
    if (state_ == State::Open)
    {
        state_ = State::Stopping;
    }
    resolver_.cancel();
}

asio::awaitable<void> ProbeSet::close()
{
    // Owner destruction must wait for the drain even if the caller is cancelled.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await asio::co_spawn(io_, close_on_executor(), asio::use_awaitable);
}

asio::awaitable<void> ProbeSet::close_on_executor()
{
    if (state_ == State::Closed)
    {
        co_return;
    }
    if (state_ == State::Closing)
    {
        while (state_ == State::Closing)
        {
            const auto [error] = co_await finished_wait_.async_wait(asio::as_tuple(asio::use_awaitable));
            if (error && error != asio::error::operation_aborted)
            {
                throw asio::system_error(error, "ProbeSet close wait failed");
            }
        }
        co_return;
    }
    stop();
    state_ = State::Closing;
    while (refreshing_)
    {
        const auto [error] = co_await finished_wait_.async_wait(asio::as_tuple(asio::use_awaitable));
        if (error && error != asio::error::operation_aborted)
        {
            throw asio::system_error(error, "ProbeSet refresh shutdown wait failed");
        }
    }
    if (icmp_)
    {
        co_await icmp_->close();
        icmp_.reset();
    }
    history_.clear();
    state_ = State::Closed;
    notify_finished();
}

void ProbeSet::notify_finished() noexcept
{
    try
    {
        finished_wait_.cancel();
    }
    catch (...)
    {
    }
}
