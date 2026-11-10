#include "peer_blacklist.h"
#include "message.h"

PeerBlacklist::PeerBlacklist(std::string_view listener, Clock::time_point now) : listener_(listener), round_at_(now)
{
}

void PeerBlacklist::age(Clock::time_point now)
{
    const auto elapsed = (now - round_at_) / round_duration;
    if (elapsed <= 0)
    {
        return;
    }
    for (auto it = entries_.begin(); it != entries_.end();)
    {
        if (it->second.rounds <= elapsed)
        {
            it = entries_.erase(it);
        }
        else
        {
            it->second.rounds -= static_cast<std::uint16_t>(elapsed);
            ++it;
        }
    }
    round_at_ += round_duration * elapsed;
    capacity_reported_ = false;
}

bool PeerBlacklist::blocked(asio::ip::address_v4 address, Clock::time_point now)
{
    age(now);
    const auto found = entries_.find(address.to_uint());
    if (found == entries_.end() || found->second.rounds == 0)
    {
        return false;
    }
    if (found->second.rounds < maximum_rounds)
    {
        ++found->second.rounds;
    }
    return true;
}

bool PeerBlacklist::record_failure(asio::ip::address_v4 address, std::string_view reason, Clock::time_point now)
{
    age(now);
    auto found = entries_.find(address.to_uint());
    if (found == entries_.end())
    {
        if (entries_.size() >= capacity)
        {
            if (!capacity_reported_)
            {
                capacity_reported_ = true;
                PROXY_ERROR_PRINT("Blacklist full listener=%s limit=%zu", listener_.c_str(), capacity);
            }
            return false;
        }
        found = entries_.try_emplace(address.to_uint()).first;
    }
    auto &entry = found->second;
    if (entry.rounds != 0 || ++entry.failures < failure_limit)
    {
        return false;
    }
    entry.failures = 0;
    entry.rounds = initial_rounds;
    PROXY_INFO_PRINT("Blacklist ban listener=%s ip=%s failures=%u rounds=%u reason=%.*s", listener_.c_str(),
                     address.to_string().c_str(), static_cast<unsigned int>(failure_limit),
                     static_cast<unsigned int>(initial_rounds), static_cast<int>(reason.size()), reason.data());
    return true;
}
