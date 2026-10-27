#ifndef RELAYWEAVE_FAIR_QUEUE_H
#define RELAYWEAVE_FAIR_QUEUE_H

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <list>
#include <unordered_set>
#include <utility>

// Approximate frame fairness by interleaving tags at insertion time. The caller
// serializes access. The insertion hint is only a lookup cache, not a scheduling
// cursor. No byte weights or persistent scheduling rounds are used.
template <class Tag, class T, class Hash = std::hash<Tag>> class FairQueue
{
    struct Entry
    {
        Tag tag;
        T value;
    };

  public:
    FairQueue() = default;
    FairQueue(const FairQueue &) = delete;
    FairQueue &operator=(const FairQueue &) = delete;
    FairQueue(FairQueue &&) = delete;
    FairQueue &operator=(FairQueue &&) = delete;

    void push(Tag tag, T value)
    {
        auto position = entries_.end();
        if (last_inserted_ != entries_.end() && last_inserted_->tag == tag)
        {
            position = std::next(last_inserted_);
        }
        else
        {
            const auto previous = std::find_if(entries_.rbegin(), entries_.rend(),
                [&](const Entry &entry) { return entry.tag == tag; });
            position = previous == entries_.rend() ? entries_.begin() : previous.base();
        }
        seen_.clear();
        while (position != entries_.end() && seen_.insert(position->tag).second)
        {
            ++position;
        }
        last_inserted_ = entries_.insert(position, Entry{std::move(tag), std::move(value)});
    }

    T &front()
    {
        assert(!empty());
        return entries_.front().value;
    }
    const T &front() const
    {
        assert(!empty());
        return entries_.front().value;
    }
    void pop()
    {
        assert(!empty());
        if (last_inserted_ == entries_.begin())
        {
            last_inserted_ = entries_.end();
        }
        entries_.pop_front();
    }
    std::size_t erase(const Tag &tag)
    {
        last_inserted_ = entries_.end();
        return std::erase_if(entries_, [&](const Entry &entry) { return entry.tag == tag; });
    }
    void clear() noexcept
    {
        last_inserted_ = entries_.end();
        entries_.clear();
        seen_.clear();
    }
    bool empty() const noexcept
    {
        return entries_.empty();
    }
    std::size_t size() const noexcept
    {
        return entries_.size();
    }

  private:
    std::list<Entry> entries_;
    typename std::list<Entry>::iterator last_inserted_ = entries_.end();
    std::unordered_set<Tag, Hash> seen_; // Reuse buckets across insertion scans.
};

#endif
