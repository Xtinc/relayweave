#include "dualindex_map.h"

template <class PrimaryKey, class SecondaryKey, class Value, SecondaryKeyMode Mode = SecondaryKeyMode::NonUnique>
class StdDualIndexMap
{
    struct Record
    {
        std::optional<SecondaryKey> secondary;
        Value value;
    };

    using PrimaryMap = std::map<PrimaryKey, Record>;
    using SecondaryMap = std::conditional_t<Mode == SecondaryKeyMode::Unique, std::map<SecondaryKey, PrimaryKey>,
                                            std::multimap<SecondaryKey, PrimaryKey>>;

  public:
    struct SecondaryEntry
    {
        const PrimaryKey *primary = nullptr;
        Value *value = nullptr;

        explicit operator bool() const noexcept
        {
            return value != nullptr;
        }
    };

    struct ConstSecondaryEntry
    {
        const PrimaryKey *primary = nullptr;
        const Value *value = nullptr;

        explicit operator bool() const noexcept
        {
            return value != nullptr;
        }
    };

    std::pair<Value *, bool> insert(PrimaryKey primary, std::optional<SecondaryKey> secondary, Value value)
    {
        if (const auto existing = primary_.find(primary); existing != primary_.end())
            return {&existing->second.value, false};

        if constexpr (Mode == SecondaryKeyMode::Unique)
        {
            if (secondary)
            {
                const auto existing = secondary_.find(*secondary);
                if (existing != secondary_.end())
                    return {find_primary(existing->second), false};
            }
        }

        auto [entry, inserted] = primary_.emplace(std::move(primary), Record{std::move(secondary), std::move(value)});
        if (!inserted)
            return {&entry->second.value, false};

        try
        {
            if (entry->second.secondary && !insert_secondary(*entry->second.secondary, entry->first))
            {
                primary_.erase(entry);
                return {nullptr, false};
            }
        }
        catch (...)
        {
            primary_.erase(entry);
            throw;
        }
        return {&entry->second.value, true};
    }

    Value *find_primary(const PrimaryKey &primary)
    {
        const auto entry = primary_.find(primary);
        return entry == primary_.end() ? nullptr : &entry->second.value;
    }

    const Value *find_primary(const PrimaryKey &primary) const
    {
        const auto entry = primary_.find(primary);
        return entry == primary_.end() ? nullptr : &entry->second.value;
    }

    Value *find_secondary(const SecondaryKey &secondary)
        requires(Mode == SecondaryKeyMode::Unique)
    {
        return find_secondary_entry(secondary).value;
    }

    const Value *find_secondary(const SecondaryKey &secondary) const
        requires(Mode == SecondaryKeyMode::Unique)
    {
        return find_secondary_entry(secondary).value;
    }

    SecondaryEntry find_secondary_entry(const SecondaryKey &secondary)
        requires(Mode == SecondaryKeyMode::Unique)
    {
        const auto entry = secondary_.find(secondary);
        if (entry == secondary_.end())
            return {};
        const auto primary = primary_.find(entry->second);
        return primary == primary_.end() ? SecondaryEntry{} : SecondaryEntry{&primary->first, &primary->second.value};
    }

    ConstSecondaryEntry find_secondary_entry(const SecondaryKey &secondary) const
        requires(Mode == SecondaryKeyMode::Unique)
    {
        const auto entry = secondary_.find(secondary);
        if (entry == secondary_.end())
            return {};
        const auto primary = primary_.find(entry->second);
        return primary == primary_.end() ? ConstSecondaryEntry{}
                                         : ConstSecondaryEntry{&primary->first, &primary->second.value};
    }

    const SecondaryKey *secondary_key(const PrimaryKey &primary) const
    {
        const auto entry = primary_.find(primary);
        if (entry == primary_.end() || !entry->second.secondary)
            return nullptr;
        return &*entry->second.secondary;
    }

    bool set_secondary(const PrimaryKey &primary, std::optional<SecondaryKey> secondary)
    {
        static_assert(std::is_nothrow_swappable_v<std::optional<SecondaryKey>>,
                      "SecondaryKey must be nothrow swappable");

        const auto entry = primary_.find(primary);
        if (entry == primary_.end())
            return false;
        if (secondary_equal(entry->second.secondary, secondary))
            return true;

        if constexpr (Mode == SecondaryKeyMode::Unique)
        {
            if (secondary)
            {
                const auto existing = secondary_.find(*secondary);
                if (existing != secondary_.end() && !primary_equal(existing->second, primary))
                    return false;
            }
        }

        if (secondary && !insert_secondary(*secondary, entry->first))
            return false;

        using std::swap;
        swap(entry->second.secondary, secondary);
        if (secondary)
            erase_secondary_link(*secondary, entry->first);
        return true;
    }

    std::size_t count_secondary(const SecondaryKey &secondary) const
    {
        return secondary_.count(secondary);
    }

    bool erase_primary(const PrimaryKey &primary)
    {
        const auto entry = primary_.find(primary);
        if (entry == primary_.end())
            return false;
        if (entry->second.secondary)
            erase_secondary_link(*entry->second.secondary, entry->first);
        primary_.erase(entry);
        return true;
    }

    std::size_t erase_secondary(const SecondaryKey &secondary)
    {
        std::size_t erased = 0;
        for (;;)
        {
            const auto secondary_entry = secondary_.find(secondary);
            if (secondary_entry == secondary_.end())
                return erased;
            const auto primary_entry = primary_.find(secondary_entry->second);
            secondary_.erase(secondary_entry);
            if (primary_entry != primary_.end())
                primary_.erase(primary_entry);
            ++erased;
        }
    }

    template <class Function> void for_each(Function &&function)
    {
        for (auto &[primary, record] : primary_)
            function(primary, record.value);
    }

    template <class Function> void for_each_secondary(const SecondaryKey &secondary, Function &&function)
    {
        const auto [first, last] = secondary_.equal_range(secondary);
        for (auto entry = first; entry != last; ++entry)
        {
            const auto primary = primary_.find(entry->second);
            if (primary != primary_.end())
                function(primary->first, primary->second.value);
        }
    }

    std::size_t size() const noexcept
    {
        return primary_.size();
    }

    void clear() noexcept
    {
        secondary_.clear();
        primary_.clear();
    }

  private:
    bool insert_secondary(const SecondaryKey &secondary, const PrimaryKey &primary)
    {
        if constexpr (Mode == SecondaryKeyMode::Unique)
            return secondary_.emplace(secondary, primary).second;
        else
        {
            secondary_.emplace(secondary, primary);
            return true;
        }
    }

    void erase_secondary_link(const SecondaryKey &secondary, const PrimaryKey &primary)
    {
        const auto [first, last] = secondary_.equal_range(secondary);
        for (auto entry = first; entry != last; ++entry)
        {
            if (primary_equal(entry->second, primary))
            {
                secondary_.erase(entry);
                return;
            }
        }
    }

    bool primary_equal(const PrimaryKey &left, const PrimaryKey &right) const
    {
        const auto compare = primary_.key_comp();
        return !compare(left, right) && !compare(right, left);
    }

    bool secondary_equal(const std::optional<SecondaryKey> &left, const std::optional<SecondaryKey> &right) const
    {
        if (left.has_value() != right.has_value())
            return false;
        if (!left)
            return true;
        const auto compare = secondary_.key_comp();
        return !compare(*left, *right) && !compare(*right, *left);
    }

    PrimaryMap primary_;
    SecondaryMap secondary_;
};
