#ifndef RELAYWEAVE_TEST_FAIR_QUEUE_CANDIDATES_H
#define RELAYWEAVE_TEST_FAIR_QUEUE_CANDIDATES_H

#include <algorithm>
#include <cassert>
#include <deque>
#include <functional>
#include <iterator>
#include <list>
#include <memory>
#include <unordered_set>
#include <utility>

// Equivalent experimental implementations, excluded from the production library.
template <class Tag, class T, class Hash, template <class...> class Storage> class StandardFairQueue
{
    struct Entry
    {
        Tag tag;
        T value;
    };

  public:
    StandardFairQueue() = default;
    StandardFairQueue(const StandardFairQueue &) = delete;
    StandardFairQueue &operator=(const StandardFairQueue &) = delete;
    StandardFairQueue(StandardFairQueue &&) = delete;
    StandardFairQueue &operator=(StandardFairQueue &&) = delete;

    void push(Tag tag, T value)
    {
        auto position = entries_.end();
        if (cached_ && last_inserted_->tag == tag)
        {
            position = std::next(last_inserted_);
        }
        else
        {
            const auto previous = std::find_if(entries_.rbegin(), entries_.rend(),
                [&](const Entry &entry) { return entry.tag == tag; });
            position = previous == entries_.rend() ? entries_.begin() : previous.base();
        }
        std::unordered_set<Tag, Hash> seen;
        while (position != entries_.end() && seen.insert(position->tag).second)
        {
            ++position;
        }
        // Insertion may invalidate deque iterators, including on allocation failure.
        cached_ = false;
        last_inserted_ = entries_.insert(position, Entry{std::move(tag), std::move(value)});
        cached_ = true;
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
        if (cached_ && last_inserted_ == entries_.begin())
        {
            cached_ = false;
        }
        entries_.pop_front();
    }
    std::size_t erase(const Tag &tag)
    {
        cached_ = false;
        return std::erase_if(entries_, [&](const Entry &entry) { return entry.tag == tag; });
    }
    void clear() noexcept
    {
        cached_ = false;
        entries_.clear();
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
    Storage<Entry> entries_;
    typename Storage<Entry>::iterator last_inserted_{};
    bool cached_ = false;
};

template <class Tag, class T, class Hash = std::hash<Tag>>
using DequeFairQueue = StandardFairQueue<Tag, T, Hash, std::deque>;
template <class Tag, class T, class Hash = std::hash<Tag>>
using ListFairQueue = StandardFairQueue<Tag, T, Hash, std::list>;

template <class Tag, class T, class Hash = std::hash<Tag>> class LinkedFairQueue
{
    struct Node
    {
        // Keep traversal links beside the tag, instead of beyond a large payload.
        Node *previous = nullptr;
        Node *next = nullptr;
        Tag tag;
        T value;

        Node(Tag tag, T value) : tag(std::move(tag)), value(std::move(value)) {}
    };

  public:
    LinkedFairQueue() = default;
    LinkedFairQueue(const LinkedFairQueue &) = delete;
    LinkedFairQueue &operator=(const LinkedFairQueue &) = delete;
    LinkedFairQueue(LinkedFairQueue &&) = delete;
    LinkedFairQueue &operator=(LinkedFairQueue &&) = delete;
    ~LinkedFairQueue()
    {
        clear();
    }

    void push(Tag tag, T value)
    {
        auto previous = last_inserted_ && last_inserted_->tag == tag ? last_inserted_ : tail_;
        while (previous && !(previous->tag == tag))
        {
            previous = previous->previous;
        }
        auto position = previous ? previous->next : head_;
        std::unordered_set<Tag, Hash> seen;
        while (position && seen.insert(position->tag).second)
        {
            position = position->next;
        }
        auto candidate = std::make_unique<Node>(std::move(tag), std::move(value));
        candidate->previous = position ? position->previous : tail_;
        candidate->next = position;
        if (candidate->previous)
        {
            candidate->previous->next = candidate.get();
        }
        else
        {
            head_ = candidate.get();
        }
        if (position)
        {
            position->previous = candidate.get();
        }
        else
        {
            tail_ = candidate.get();
        }
        last_inserted_ = candidate.release();
        ++size_;
    }

    T &front()
    {
        assert(!empty());
        return head_->value;
    }
    const T &front() const
    {
        assert(!empty());
        return head_->value;
    }
    void pop()
    {
        assert(!empty());
        unlink(head_);
    }
    std::size_t erase(const Tag &tag)
    {
        std::size_t removed = 0;
        auto current = head_;
        while (current)
        {
            const auto next = current->next;
            if (current->tag == tag)
            {
                unlink(current);
                ++removed;
            }
            current = next;
        }
        return removed;
    }
    void clear() noexcept
    {
        while (head_)
        {
            unlink(head_);
        }
    }
    bool empty() const noexcept
    {
        return head_ == nullptr;
    }
    std::size_t size() const noexcept
    {
        return size_;
    }

  private:
    void unlink(Node *node) noexcept
    {
        if (node == last_inserted_)
        {
            last_inserted_ = nullptr;
        }
        if (node->previous)
        {
            node->previous->next = node->next;
        }
        else
        {
            head_ = node->next;
        }
        if (node->next)
        {
            node->next->previous = node->previous;
        }
        else
        {
            tail_ = node->previous;
        }
        delete node;
        --size_;
    }

    Node *head_ = nullptr;
    Node *tail_ = nullptr;
    Node *last_inserted_ = nullptr;
    std::size_t size_ = 0;
};

#endif
