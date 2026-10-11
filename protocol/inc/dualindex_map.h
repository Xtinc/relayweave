#ifndef PROXY_DUAL_INDEX_MAP_H
#define PROXY_DUAL_INDEX_MAP_H

#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

enum class SecondaryKeyMode
{
    Unique,
    NonUnique,
};

template <class PrimaryKey, class SecondaryKey, class Value, SecondaryKeyMode Mode = SecondaryKeyMode::NonUnique>
class DualIndexMap
{
    struct Node
    {
        Node(PrimaryKey primary, std::optional<SecondaryKey> secondary, Value value, std::size_t primary_hash,
             std::size_t secondary_hash)
            : primary(std::move(primary)), secondary(std::move(secondary)), value(std::move(value)),
              primary_hash(primary_hash), secondary_hash(secondary_hash)
        {
        }

        PrimaryKey primary;
        std::optional<SecondaryKey> secondary;
        Value value;
        std::size_t primary_hash;
        std::size_t secondary_hash;
        Node *primary_next = nullptr;
        Node **primary_previous_link = nullptr;
        Node *secondary_next = nullptr;
        Node **secondary_previous_link = nullptr;
    };

    static constexpr std::size_t initial_bucket_count = 16;

  public:
    DualIndexMap() = default;
    DualIndexMap(const DualIndexMap &) = delete;
    DualIndexMap &operator=(const DualIndexMap &) = delete;
    DualIndexMap(DualIndexMap &&) = delete;
    DualIndexMap &operator=(DualIndexMap &&) = delete;

    ~DualIndexMap()
    {
        clear();
    }

    std::pair<Value *, bool> insert(PrimaryKey primary, std::optional<SecondaryKey> secondary, Value value)
    {
        const auto primary_hash = hash_primary(primary);
        if (auto *existing = find_primary_node(primary, primary_hash))
            return {&existing->value, false};

        std::size_t secondary_hash = 0;
        if (secondary)
        {
            secondary_hash = hash_secondary(*secondary);
            if constexpr (Mode == SecondaryKeyMode::Unique)
            {
                if (auto *existing = find_secondary_node(*secondary, secondary_hash))
                    return {&existing->value, false};
            }
        }

        auto candidate = std::make_unique<Node>(std::move(primary), std::move(secondary), std::move(value),
                                                primary_hash, secondary_hash);
        ensure_capacity(size_ + 1);
        link_primary(candidate.get());
        if (candidate->secondary)
        {
            link_secondary(candidate.get());
        }

        auto *inserted = candidate.release();
        ++size_;
        return {&inserted->value, true};
    }

    Value *find_primary(const PrimaryKey &primary)
    {
        const auto hash = hash_primary(primary);
        const auto node = find_primary_node(primary, hash);
        return node ? &node->value : nullptr;
    }

    const Value *find_primary(const PrimaryKey &primary) const
    {
        const auto hash = hash_primary(primary);
        const auto node = find_primary_node(primary, hash);
        return node ? &node->value : nullptr;
    }

    Value *find_secondary(const SecondaryKey &secondary)
        requires(Mode == SecondaryKeyMode::Unique)
    {
        const auto node = find_secondary_node(secondary, hash_secondary(secondary));
        return node ? &node->value : nullptr;
    }

    const Value *find_secondary(const SecondaryKey &secondary) const
        requires(Mode == SecondaryKeyMode::Unique)
    {
        const auto node = find_secondary_node(secondary, hash_secondary(secondary));
        return node ? &node->value : nullptr;
    }

    const SecondaryKey *secondary_key(const PrimaryKey &primary) const
    {
        const auto hash = hash_primary(primary);
        const auto node = find_primary_node(primary, hash);
        return node && node->secondary ? &*node->secondary : nullptr;
    }

    bool set_secondary(const PrimaryKey &primary, std::optional<SecondaryKey> secondary)
    {
        static_assert(std::is_nothrow_swappable_v<std::optional<SecondaryKey>>,
                      "SecondaryKey must be nothrow swappable");

        const auto primary_hash = hash_primary(primary);
        auto *node = find_primary_node(primary, primary_hash);
        if (!node)
            return false;
        if (secondary_equal(node->secondary, secondary))
            return true;

        std::size_t secondary_hash = 0;
        if (secondary)
        {
            secondary_hash = hash_secondary(*secondary);
            if constexpr (Mode == SecondaryKeyMode::Unique)
            {
                if (const auto existing = find_secondary_node(*secondary, secondary_hash); existing && existing != node)
                    return false;
            }
        }

        if (node->secondary)
            unlink_secondary(node);
        node->secondary.swap(secondary);
        node->secondary_hash = secondary_hash;
        if (node->secondary)
            link_secondary(node);
        return true;
    }

    std::size_t count_secondary(const SecondaryKey &secondary) const
    {
        if (!bucket_count_)
            return 0;
        const auto hash = hash_secondary(secondary);
        std::size_t count = 0;
        for (auto *node = secondary_buckets_[bucket_index(hash)]; node; node = node->secondary_next)
        {
            if (node->secondary_hash == hash && secondary_equal(*node->secondary, secondary))
                ++count;
        }
        return count;
    }

    bool erase_primary(const PrimaryKey &primary)
    {
        const auto hash = hash_primary(primary);
        auto *node = find_primary_node(primary, hash);
        if (!node)
            return false;
        erase_node(node);
        return true;
    }

    std::size_t erase_secondary(const SecondaryKey &secondary)
    {
        if (!bucket_count_)
            return 0;
        const auto hash = hash_secondary(secondary);
        std::size_t erased = 0;
        auto *node = secondary_buckets_[bucket_index(hash)];
        while (node)
        {
            auto *next = node->secondary_next;
            if (node->secondary_hash == hash && secondary_equal(*node->secondary, secondary))
            {
                erase_node(node);
                ++erased;
            }
            node = next;
        }
        return erased;
    }

    template <class Function> void for_each(Function &&function)
    {
        for (std::size_t bucket = 0; bucket < bucket_count_; ++bucket)
        {
            for (auto *node = primary_buckets_[bucket]; node; node = node->primary_next)
            {
                function(node->primary, node->value);
            }
        }
    }

    std::size_t size() const noexcept
    {
        return size_;
    }

    void clear() noexcept
    {
        for (std::size_t bucket = 0; bucket < bucket_count_; ++bucket)
        {
            auto *node = primary_buckets_[bucket];
            while (node)
            {
                auto *next = node->primary_next;
                delete node;
                node = next;
            }
            primary_buckets_[bucket] = nullptr;
            secondary_buckets_[bucket] = nullptr;
        }
        size_ = 0;
    }

  private:
    static std::size_t hash_primary(const PrimaryKey &primary)
    {
        return std::hash<PrimaryKey>{}(primary);
    }

    static std::size_t hash_secondary(const SecondaryKey &secondary)
    {
        return std::hash<SecondaryKey>{}(secondary);
    }

    static bool primary_equal(const PrimaryKey &left, const PrimaryKey &right)
    {
        return std::equal_to<PrimaryKey>{}(left, right);
    }

    static bool secondary_equal(const SecondaryKey &left, const SecondaryKey &right)
    {
        return std::equal_to<SecondaryKey>{}(left, right);
    }

    static bool secondary_equal(const std::optional<SecondaryKey> &left, const std::optional<SecondaryKey> &right)
    {
        if (left.has_value() != right.has_value())
            return false;
        return !left || secondary_equal(*left, *right);
    }

    std::size_t bucket_index(std::size_t hash) const noexcept
    {
        return hash & (bucket_count_ - 1);
    }

    Node *find_primary_node(const PrimaryKey &primary, std::size_t hash)
    {
        return const_cast<Node *>(std::as_const(*this).find_primary_node(primary, hash));
    }

    const Node *find_primary_node(const PrimaryKey &primary, std::size_t hash) const
    {
        if (!bucket_count_)
        {
            return nullptr;
        }
        for (auto *node = primary_buckets_[bucket_index(hash)]; node; node = node->primary_next)
        {
            if (node->primary_hash == hash && primary_equal(node->primary, primary))
            {
                return node;
            }
        }
        return nullptr;
    }

    Node *find_secondary_node(const SecondaryKey &secondary, std::size_t hash)
    {
        return const_cast<Node *>(std::as_const(*this).find_secondary_node(secondary, hash));
    }

    const Node *find_secondary_node(const SecondaryKey &secondary, std::size_t hash) const
    {
        if (!bucket_count_)
        {
            return nullptr;
        }
        for (auto *node = secondary_buckets_[bucket_index(hash)]; node; node = node->secondary_next)
        {
            if (node->secondary_hash == hash && secondary_equal(*node->secondary, secondary))
            {
                return node;
            }
        }
        return nullptr;
    }

    static void link_primary_to(Node *node, Node **buckets, std::size_t bucket_count) noexcept
    {
        auto &head = buckets[node->primary_hash & (bucket_count - 1)];
        node->primary_next = head;
        node->primary_previous_link = &head;
        if (head)
        {
            head->primary_previous_link = &node->primary_next;
        }
        head = node;
    }

    static void link_secondary_to(Node *node, Node **buckets, std::size_t bucket_count) noexcept
    {
        auto &head = buckets[node->secondary_hash & (bucket_count - 1)];
        node->secondary_next = head;
        node->secondary_previous_link = &head;
        if (head)
        {
            head->secondary_previous_link = &node->secondary_next;
        }
        head = node;
    }

    void link_primary(Node *node) noexcept
    {
        link_primary_to(node, primary_buckets_.get(), bucket_count_);
    }

    void link_secondary(Node *node) noexcept
    {
        link_secondary_to(node, secondary_buckets_.get(), bucket_count_);
    }

    static void unlink_primary(Node *node) noexcept
    {
        *node->primary_previous_link = node->primary_next;
        if (node->primary_next)
        {
            node->primary_next->primary_previous_link = node->primary_previous_link;
        }
        node->primary_next = nullptr;
        node->primary_previous_link = nullptr;
    }

    static void unlink_secondary(Node *node) noexcept
    {
        *node->secondary_previous_link = node->secondary_next;
        if (node->secondary_next)
        {
            node->secondary_next->secondary_previous_link = node->secondary_previous_link;
        }
        node->secondary_next = nullptr;
        node->secondary_previous_link = nullptr;
    }

    void erase_node(Node *node) noexcept
    {
        if (node->secondary)
        {
            unlink_secondary(node);
        }
        unlink_primary(node);
        delete node;
        --size_;
    }

    void ensure_capacity(std::size_t requested)
    {
        auto new_count = bucket_count_ ? bucket_count_ : initial_bucket_count;
        while (requested > new_count - new_count / 4)
        {
            if (new_count > std::numeric_limits<std::size_t>::max() / 2)
            {
                throw std::length_error("DualIndexMap capacity overflow");
            }
            new_count *= 2;
        }
        if (new_count != bucket_count_)
        {
            rehash(new_count);
        }
    }

    void rehash(std::size_t new_count)
    {
        auto primary = std::make_unique<Node *[]>(new_count);
        auto secondary = std::make_unique<Node *[]>(new_count);

        for (std::size_t bucket = 0; bucket < bucket_count_; ++bucket)
        {
            auto *node = primary_buckets_[bucket];
            while (node)
            {
                auto *next = node->primary_next;
                link_primary_to(node, primary.get(), new_count);
                if (node->secondary)
                {
                    link_secondary_to(node, secondary.get(), new_count);
                }
                node = next;
            }
        }

        primary_buckets_ = std::move(primary);
        secondary_buckets_ = std::move(secondary);
        bucket_count_ = new_count;
    }

    std::unique_ptr<Node *[]> primary_buckets_;
    std::unique_ptr<Node *[]> secondary_buckets_;
    std::size_t bucket_count_ = 0;
    std::size_t size_ = 0;
};

#endif // PROXY_DUAL_INDEX_MAP_H
