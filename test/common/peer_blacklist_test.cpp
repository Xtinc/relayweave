#include "peer_blacklist.h"
#include <iostream>
#include <stdexcept>

namespace
{
using namespace std::chrono_literals;
using Clock = PeerBlacklist::Clock;
const auto address = asio::ip::make_address_v4("192.0.2.1");
const auto other = asio::ip::make_address_v4("192.0.2.2");

void require(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
}

void ban(PeerBlacklist &list, asio::ip::address_v4 ip, Clock::time_point now)
{
    for (int i = 1; i <= 5; ++i)
    {
        require(list.record_failure(ip, "test violation", now) == (i == 5), "Incorrect failure threshold");
    }
}

void rounds_and_expiry()
{
    const auto start = Clock::time_point{};
    PeerBlacklist list("test", start);
    for (int i = 0; i < 4; ++i)
    {
        require(!list.record_failure(address, "test", start + 59s), "Banned before threshold");
        require(!list.blocked(address, start + 59s), "Allowed access changed the counter");
    }
    require(!list.record_failure(address, "test", start + 60s), "Previous round's failures leaked");
    require(!list.blocked(other, start + 90s), "Another IP was blocked");
    for (int i = 0; i < 3; ++i)
    {
        require(!list.record_failure(address, "test", start + 119s), "A query shifted the round boundary");
    }
    require(!list.record_failure(address, "test", start + 120s), "Round boundary drifted");
    require(!list.record_failure(address, "test", start + 48h), "Long idle retained suspicion");

    PeerBlacklist untouched("test", start);
    ban(untouched, address, start + 59s);
    require(!untouched.blocked(address, start + 30min), "Initial ban survived 30 common rounds");
    ban(untouched, address, start + 30min);

    PeerBlacklist active("test", start);
    ban(active, address, start);
    require(active.blocked(address, start + 29min + 59s), "Ban expired too soon");
    require(!active.blocked(address, start + 31min), "One rejected connection did not add exactly one round");
}

void renewal_and_cap()
{
    const auto start = Clock::time_point{};
    PeerBlacklist list("test", start);
    ban(list, address, start);
    for (int i = 0; i < 1000; ++i)
    {
        require(list.blocked(address, start), "Repeated access removed a ban");
    }
    require(!list.blocked(address, start + 720min), "Ban exceeded the 720-round remaining cap");

    PeerBlacklist continuous("test", start);
    ban(continuous, address, start);
    for (int i = 1; i <= 1000; ++i)
    {
        require(continuous.blocked(address, start + std::chrono::minutes(i)), "Continuous access lost its ban");
    }
    require(!continuous.blocked(address, start + 1030min), "Renewal failed to age after access stopped");

    PeerBlacklist pending("test", start);
    ban(pending, address, start);
    require(!pending.record_failure(address, "late failed handshake", start), "Duplicate ban transition");
    require(!pending.blocked(address, start + 30min), "Already accepted failures extended the ban");
}

void capacity_and_isolation()
{
    const auto start = Clock::time_point{};
    PeerBlacklist first("first", start);
    PeerBlacklist second("second", start);
    ban(first, address, start);
    require(!second.blocked(address, start), "Listeners shared a ban");
    for (std::uint32_t i = 1; i < PeerBlacklist::capacity; ++i)
    {
        require(!first.record_failure(asio::ip::address_v4(i), "test", start), "New source started banned");
    }
    for (int i = 0; i < 5; ++i)
    {
        require(!first.record_failure(other, "test", start), "Capacity limit admitted a new record");
    }
    require(first.blocked(address, start), "Capacity pressure evicted a ban");
    require(!first.blocked(other, start + 1min), "Dropped source was banned");
    ban(first, other, start + 1min);
    require(first.blocked(address, start + 1min), "Batch aging removed an active ban");
}

} // namespace

int main()
{
    try
    {
        rounds_and_expiry();
        renewal_and_cap();
        capacity_and_isolation();
        std::cout << "peer_blacklist_test passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
