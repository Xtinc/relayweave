#include "dualindex_map.h"
#include "std_dual_index_map.h"

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

struct CollisionKey
{
    int value;
    auto operator<=>(const CollisionKey &) const = default;
};

struct ThrowingHashKey
{
    int value;
    static inline int rejected = -1;
    auto operator<=>(const ThrowingHashKey &) const = default;
};

template <> struct std::hash<CollisionKey>
{
    std::size_t operator()(const CollisionKey &) const noexcept
    {
        return 1;
    }
};

template <> struct std::hash<ThrowingHashKey>
{
    std::size_t operator()(const ThrowingHashKey &key) const
    {
        if (key.value == ThrowingHashKey::rejected)
            throw std::runtime_error("requested hash failure");
        return std::hash<int>{}(key.value);
    }
};

namespace
{
void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

template <class Map> void test_unique_secondary()
{
    Map values;
    auto [first, inserted] = values.insert("first", 1, "one");
    require(inserted && first && *first == "one", "Failed to insert unique entry");

    auto [duplicate_primary, primary_inserted] = values.insert("first", 2, "replacement");
    require(!primary_inserted && duplicate_primary == first, "Duplicate primary key was not rejected");

    auto [duplicate_secondary, secondary_inserted] = values.insert("second", 1, "two");
    require(!secondary_inserted && duplicate_secondary == first, "Duplicate unique secondary key was not rejected");
    require(values.find_secondary(1) == first, "Unique secondary lookup failed");
    require(!values.find_secondary(99), "Missing secondary lookup returned a value");
    require(std::as_const(values).find_secondary(1) == first, "Const secondary lookup failed");
    auto [unindexed, unindexed_inserted] = values.insert("second", std::nullopt, "two");
    require(unindexed_inserted && unindexed && values.secondary_key("second") == nullptr,
            "Failed to insert unindexed entry");
    require(values.set_secondary("second", 2), "Failed to bind secondary key");
    require(values.find_secondary(2) == unindexed, "Bound secondary key was not indexed");
    require(!values.set_secondary("second", 1), "Conflicting secondary key update was accepted");
    require(values.find_secondary(2) == unindexed, "Failed update changed the existing secondary key");

    require(values.set_secondary("second", std::nullopt), "Failed to clear secondary key");
    require(values.find_secondary(2) == nullptr, "Cleared secondary key remains indexed");
    require(values.erase_primary("first"), "Failed to erase primary key");
    require(values.find_secondary(1) == nullptr && values.size() == 1, "Primary erase left a secondary index");
}

template <class Map> void test_non_unique_secondary()
{
    Map values;
    require(values.insert("first", 7, 1).second, "Failed to insert first shared secondary key");
    require(values.insert("second", 7, 2).second, "Failed to insert second shared secondary key");
    require(values.insert("third", 8, 3).second, "Failed to insert distinct secondary key");
    require(values.count_secondary(7) == 2, "Incorrect non-unique secondary count");

    int sum = 0;
    values.for_each([&](const std::string &primary, int &value) {
        if (*values.secondary_key(primary) == 7) sum += value;
    });
    require(sum == 3, "Non-unique secondary traversal failed");

    require(values.set_secondary("third", 7), "Failed to replace secondary key");
    require(values.count_secondary(7) == 3 && values.count_secondary(8) == 0,
            "Secondary key replacement left inconsistent indices");
    require(values.erase_secondary(7) == 3, "Secondary erase returned the wrong count");
    require(values.size() == 0, "Secondary erase left primary entries");
}

template <class Map> std::map<int, std::pair<std::optional<int>, int>> snapshot(Map &values)
{
    std::map<int, std::pair<std::optional<int>, int>> result;
    values.for_each([&](const int &primary, int &value) {
        const auto secondary = values.secondary_key(primary);
        result.emplace(primary, std::pair{secondary ? std::optional<int>{*secondary} : std::nullopt, value});
    });
    return result;
}

template <SecondaryKeyMode Mode, class Left, class Right> void compare_maps(Left &left, Right &right)
{
    require(left.size() == right.size(), "Differential size mismatch");
    require(snapshot(left) == snapshot(right), "Differential primary index mismatch");

    for (int secondary = 0; secondary < 32; ++secondary)
    {
        require(left.count_secondary(secondary) == right.count_secondary(secondary),
                "Differential secondary count mismatch");
        std::map<int, int> left_values;
        std::map<int, int> right_values;
        const auto collect = [secondary](auto &map, auto &result) {
            map.for_each([&](const int &primary, int &value) {
                const auto key = map.secondary_key(primary);
                if (key && *key == secondary) result.emplace(primary, value);
            });
        };
        collect(left, left_values);
        collect(right, right_values);
        require(left_values == right_values, "Differential secondary traversal mismatch");

        if constexpr (Mode == SecondaryKeyMode::Unique)
        {
            const auto left_value = left.find_secondary(secondary);
            const auto right_value = right.find_secondary(secondary);
            require((left_value == nullptr) == (right_value == nullptr), "Differential unique lookup mismatch");
            require(!left_value || *left_value == *right_value, "Differential unique value mismatch");
        }
    }
}

template <SecondaryKeyMode Mode> void test_random_differential()
{
    StdDualIndexMap<int, int, int, Mode> reference;
    DualIndexMap<int, int, int, Mode> candidate;
    std::mt19937 random(0x5a17u + static_cast<unsigned int>(Mode));

    for (int step = 0; step < 20000; ++step)
    {
        const auto primary = static_cast<int>(random() % 256);
        const auto secondary = static_cast<int>(random() % 32);
        const auto operation = random() % 7;
        const auto optional_secondary = (random() % 5 == 0) ? std::optional<int>{} : std::optional<int>{secondary};

        if (operation <= 1)
        {
            const auto reference_result = reference.insert(primary, optional_secondary, step);
            const auto candidate_result = candidate.insert(primary, optional_secondary, step);
            require(reference_result.second == candidate_result.second, "Differential insert result mismatch");
            require((reference_result.first == nullptr) == (candidate_result.first == nullptr),
                    "Differential insert pointer mismatch");
            require(!reference_result.first || *reference_result.first == *candidate_result.first,
                    "Differential insert value mismatch");
        }
        else if (operation == 2)
        {
            require(reference.set_secondary(primary, optional_secondary) ==
                        candidate.set_secondary(primary, optional_secondary),
                    "Differential secondary update mismatch");
        }
        else if (operation == 3)
        {
            require(reference.erase_primary(primary) == candidate.erase_primary(primary),
                    "Differential primary erase mismatch");
        }
        else if (operation == 4)
        {
            require(reference.erase_secondary(secondary) == candidate.erase_secondary(secondary),
                    "Differential secondary erase mismatch");
        }
        else if (operation == 5)
        {
            const auto reference_value = reference.find_primary(primary);
            const auto candidate_value = candidate.find_primary(primary);
            require((reference_value == nullptr) == (candidate_value == nullptr),
                    "Differential primary lookup mismatch");
            require(!reference_value || *reference_value == *candidate_value, "Differential primary value mismatch");
        }
        else if (random() % 100 == 0)
        {
            reference.clear();
            candidate.clear();
        }

        if (step % 25 == 0)
            compare_maps<Mode>(reference, candidate);
    }
    compare_maps<Mode>(reference, candidate);
}

void test_rehash_and_pointer_stability()
{
    DualIndexMap<int, int, std::string> values;
    auto [first, inserted] = values.insert(1, 1, "stable");
    require(inserted && first, "Failed to insert stable value");

    for (int index = 2; index <= 4096; ++index)
        require(values.insert(index, index % 17, std::to_string(index)).second, "Failed during hash-table growth");

    require(values.find_primary(1) == first && *first == "stable", "Rehash invalidated a value pointer");
    require(values.set_secondary(1, 31), "Failed to rebind after rehash");
    require(values.find_primary(1) == first && values.count_secondary(31) == 1,
            "Secondary rebind invalidated a value pointer");

    for (int secondary = 0; secondary < 17; ++secondary)
        values.erase_secondary(secondary);
    require(values.size() == 1 && values.find_primary(1) == first, "Bulk secondary erase damaged another chain");
}

template <class Map> void test_shared_value_lifetime()
{
    Map values;
    auto shared = std::make_shared<int>(42);
    std::weak_ptr<int> weak = shared;
    require(values.insert(1, 7, shared).second, "Failed to insert shared value");
    shared.reset();
    require(!weak.expired(), "Container did not retain its value");
    require(values.erase_primary(1), "Failed to erase shared value");
    require(weak.expired(), "Erased node retained its value");
}

void test_hash_collisions()
{
    using Reference = StdDualIndexMap<CollisionKey, CollisionKey, int>;
    using Candidate = DualIndexMap<CollisionKey, CollisionKey, int>;
    Reference reference;
    Candidate candidate;

    for (int key = 0; key < 512; ++key)
    {
        const CollisionKey primary{key};
        const CollisionKey secondary{key % 11};
        require(reference.insert(primary, secondary, key).second == candidate.insert(primary, secondary, key).second,
                "Collision insert mismatch");
    }

    for (int key = 0; key < 512; key += 3)
        require(reference.erase_primary(CollisionKey{key}) == candidate.erase_primary(CollisionKey{key}),
                "Collision primary erase mismatch");

    for (int key = 1; key < 512; key += 4)
        require(reference.set_secondary(CollisionKey{key}, CollisionKey{99}) ==
                    candidate.set_secondary(CollisionKey{key}, CollisionKey{99}),
                "Collision secondary update mismatch");

    require(reference.size() == candidate.size(), "Collision size mismatch");
    for (int secondary = 0; secondary < 12; ++secondary)
        require(reference.count_secondary(CollisionKey{secondary}) ==
                    candidate.count_secondary(CollisionKey{secondary}),
                "Collision secondary count mismatch");
    require(reference.count_secondary(CollisionKey{99}) == candidate.count_secondary(CollisionKey{99}),
            "Collision rebound count mismatch");
    require(reference.erase_secondary(CollisionKey{99}) == candidate.erase_secondary(CollisionKey{99}),
            "Collision secondary erase mismatch");

    for (int key = 0; key < 512; ++key)
    {
        const auto reference_value = reference.find_primary(CollisionKey{key});
        const auto candidate_value = candidate.find_primary(CollisionKey{key});
        require((reference_value == nullptr) == (candidate_value == nullptr), "Collision lookup mismatch");
        require(!reference_value || *reference_value == *candidate_value, "Collision value mismatch");
    }
}

void test_hash_failure_rollback()
{
    using Map = DualIndexMap<ThrowingHashKey, ThrowingHashKey, int, SecondaryKeyMode::Unique>;
    Map values;
    auto [value, inserted] = values.insert(ThrowingHashKey{1}, ThrowingHashKey{10}, 42);
    require(inserted && value, "Failed to prepare hash rollback test");

    ThrowingHashKey::rejected = 2;
    try
    {
        static_cast<void>(values.insert(ThrowingHashKey{2}, ThrowingHashKey{20}, 84));
        require(false, "Throwing primary hash was accepted");
    }
    catch (const std::runtime_error &)
    {
    }
    ThrowingHashKey::rejected = -1;
    require(values.size() == 1 && values.find_primary(ThrowingHashKey{1}) == value,
            "Failed insert changed the existing record");

    ThrowingHashKey::rejected = 20;
    try
    {
        static_cast<void>(values.set_secondary(ThrowingHashKey{1}, ThrowingHashKey{20}));
        require(false, "Throwing secondary hash was accepted");
    }
    catch (const std::runtime_error &)
    {
    }
    ThrowingHashKey::rejected = -1;
    const auto secondary = values.secondary_key(ThrowingHashKey{1});
    require(secondary && secondary->value == 10 && values.find_secondary(ThrowingHashKey{10}) == value,
            "Failed rebind changed the existing secondary index");
}
} // namespace

int main()
{
    try
    {
        using StdUnique = StdDualIndexMap<std::string, int, std::string, SecondaryKeyMode::Unique>;
        using HashUnique = DualIndexMap<std::string, int, std::string, SecondaryKeyMode::Unique>;
        using StdNonUnique = StdDualIndexMap<std::string, int, int>;
        using HashNonUnique = DualIndexMap<std::string, int, int>;
        test_unique_secondary<StdUnique>();
        test_unique_secondary<HashUnique>();
        test_non_unique_secondary<StdNonUnique>();
        test_non_unique_secondary<HashNonUnique>();
        test_random_differential<SecondaryKeyMode::Unique>();
        test_random_differential<SecondaryKeyMode::NonUnique>();
        test_rehash_and_pointer_stability();
        test_shared_value_lifetime<StdDualIndexMap<int, int, std::shared_ptr<int>>>();
        test_shared_value_lifetime<DualIndexMap<int, int, std::shared_ptr<int>>>();
        test_hash_collisions();
        test_hash_failure_rollback();
        std::cout << "[PASS] DualIndexMap matches the standard-container reference\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
