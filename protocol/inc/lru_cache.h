#ifndef RELAYWEAVE_LRU_CACHE_H
#define RELAYWEAVE_LRU_CACHE_H

#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>

// Caller must serialize access. Hits update recency, but never renew the TTL.
// Expired entries are removed lazily on get(); size() counts stored entries.
template <class Key, class Value, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
class LRUCache
{
  public:
    using Clock = std::chrono::steady_clock;

    LRUCache(std::size_t capacity, Clock::duration ttl) : capacity_(capacity), ttl_(ttl)
    {
        if (ttl <= Clock::duration::zero())
        {
            throw std::invalid_argument("LRUCache TTL must be positive");
        }
    }

    // The index contains list iterators; copying it would refer to the original list.
    LRUCache(const LRUCache &) = delete;
    LRUCache &operator=(const LRUCache &) = delete;
    LRUCache(LRUCache &&) = delete;
    LRUCache &operator=(LRUCache &&) = delete;

    // Returned pointers survive recency changes, but not replacement, erasure or eviction.
    const Value *get(const Key &key, Clock::time_point now = Clock::now())
    {
        const auto found = index_.find(key);
        if (found == index_.end())
        {
            return nullptr;
        }
        const auto entry = found->second;
        if (now < entry->written_at || now - entry->written_at >= ttl_)
        {
            entries_.erase(entry);
            index_.erase(found);
            return nullptr;
        }
        entries_.splice(entries_.begin(), entries_, entry);
        return &entry->value;
    }

    void put(Key key, Value value, Clock::time_point now = Clock::now())
    {
        if (capacity_ == 0)
        {
            return;
        }
        if (const auto found = index_.find(key); found != index_.end())
        {
            const auto entry = found->second;
            entry->value = std::move(value);
            entry->written_at = now;
            entries_.splice(entries_.begin(), entries_, entry);
            return;
        }

        entries_.push_front(Entry{std::move(key), std::move(value), now});
        try
        {
            index_.emplace(entries_.front().key, entries_.begin());
        }
        catch (...)
        {
            entries_.pop_front();
            throw;
        }
        if (index_.size() > capacity_)
        {
            index_.erase(entries_.back().key);
            entries_.pop_back();
        }
    }

    bool erase(const Key &key)
    {
        const auto found = index_.find(key);
        if (found == index_.end())
        {
            return false;
        }
        entries_.erase(found->second);
        index_.erase(found);
        return true;
    }

    void clear() noexcept
    {
        index_.clear();
        entries_.clear();
    }

    std::size_t size() const noexcept
    {
        return index_.size();
    }

  private:
    struct Entry
    {
        Key key;
        Value value;
        Clock::time_point written_at;
    };

    const std::size_t capacity_;
    const Clock::duration ttl_;
    std::list<Entry> entries_;
    std::unordered_map<Key, typename std::list<Entry>::iterator, Hash, KeyEqual> index_;
};

#endif
