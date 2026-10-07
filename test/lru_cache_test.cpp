#include "lru_cache.h"

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

struct CollisionHash
{
    std::size_t operator()(int) const noexcept { return 0; }
};

void verify_lru()
{
    const auto now = Clock::time_point{};
    LRUCache<int, std::string, CollisionHash> cache(2, 5s);
    require(!cache.get(1, now), "Empty cache returned a value");
    cache.put(1, "one", now);
    cache.put(2, "two", now);
    const auto *one = cache.get(1, now + 1s);
    require(one && *one == "one", "Cache hit lost the value");
    cache.put(3, "three", now + 1s);
    require(cache.size() == 2 && !cache.get(2, now + 1s), "Hit did not promote the LRU entry");
    require(cache.get(1, now + 1s) == one, "Splicing or insertion invalidated a live pointer");
    cache.put(1, "updated", now + 2s);
    cache.put(4, "four", now + 2s);
    require(cache.size() == 2 && !cache.get(3, now + 2s), "Overwrite did not promote the entry");
    require(*cache.get(1, now + 2s) == "updated", "Overwrite did not replace the value");
    require(!cache.erase(9), "Missing erase reported success");
    require(cache.erase(1) && !cache.get(1, now + 2s) && cache.size() == 1, "Erase left an indexed entry");
    cache.clear();
    require(cache.size() == 0 && !cache.get(4, now + 2s), "Clear left an entry");
    cache.put(5, "five", now + 3s);
    require(*cache.get(5, now + 3s) == "five", "Cache cannot be reused after clear");
}

void verify_ttl()
{
    const auto now = Clock::time_point{};
    LRUCache<int, int> cache(2, 5s);
    cache.put(1, 10, now);
    require(cache.get(1, now + 5s - 1ns), "Value expired before the TTL boundary");
    require(!cache.get(1, now + 5s) && cache.size() == 0, "Read renewed TTL or boundary was not expired");

    cache.put(1, 10, now);
    cache.put(1, 20, now + 4s);
    require(*cache.get(1, now + 5s) == 20, "Overwrite did not reset TTL");
    require(!cache.get(1, now + 9s), "Updated value survived its TTL boundary");

    cache.put(1, 10, now);
    cache.put(1, 30, now + 6s);
    require(cache.size() == 1 && *cache.get(1, now + 6s) == 30, "Expired overwrite duplicated or lost the entry");
    require(!cache.get(1, now + 11s), "Expired overwrite did not start a new TTL");

    cache.put(1, 10, now);
    cache.put(2, 20, now);
    require(cache.size() == 2, "Stored size is incorrect");
    require(!cache.get(2, now + 10s) && cache.size() == 1, "Expiry did not lazily erase the requested entry");
    require(!cache.get(1, now + 10s) && cache.size() == 0, "Expired entries remained accessible");
}

void verify_options_and_values()
{
    const auto now = Clock::time_point{};
    LRUCache<int, int> disabled(0, 5s);
    disabled.put(1, 10, now);
    require(disabled.size() == 0 && !disabled.get(1, now), "Zero capacity did not disable caching");
    for (auto ttl : {0s, -1s})
    {
        bool rejected = false;
        try
        {
            LRUCache<int, int> invalid(0, ttl);
        }
        catch (const std::invalid_argument &)
        {
            rejected = true;
        }
        require(rejected, "Nonpositive TTL was accepted");
    }
    LRUCache<std::string, std::vector<int>> empty_result(1, 5s);
    empty_result.put("destination", {}, now);
    const auto *result = empty_result.get("destination", now + 1s);
    require(result && result->empty(), "Empty result was mistaken for a cache miss");

    LRUCache<int, std::unique_ptr<int>> move_only(1, 5s);
    move_only.put(1, std::make_unique<int>(42), now);
    require(**move_only.get(1, now) == 42, "Move-only value was lost");

    LRUCache<int, int> real_time(1, 1h);
    real_time.put(1, 42);
    require(*real_time.get(1) == 42, "Default clock arguments failed");
}
}

int main()
{
    try
    {
        verify_lru();
        verify_ttl();
        verify_options_and_values();
        std::cout << "[PASS] LRU capacity, recency and fixed TTL\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
