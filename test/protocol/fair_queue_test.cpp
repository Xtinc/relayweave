#include "fair_queue.h"
#include "fair_queue_candidates.h"
#include "flow_protocol.h"
#include <array>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <vector>

static void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}

struct ConstantHash
{
    std::size_t operator()(int) const noexcept { return 0; }
};

struct Item
{
    int tag;
    unsigned sequence;
    bool operator==(const Item &) const = default;
};

// Deliberately simple index-based specification: the next item's position must
// follow all existing items of its tag and the longest distinct-tag prefix there.
static void reference_push(std::vector<Item> &items, Item value)
{
    std::size_t after = 0;
    for (std::size_t index = 0; index < items.size(); ++index)
    {
        if (items[index].tag == value.tag)
        {
            after = index + 1;
        }
    }
    for (std::size_t end = after; end < items.size(); ++end)
    {
        for (std::size_t earlier = after; earlier < end; ++earlier)
        {
            if (items[earlier].tag == items[end].tag)
            {
                items.insert(items.begin() + end, value);
                return;
            }
        }
    }
    items.push_back(value);
}

template <template <class, class, class> class Queue, class Hash> static void examples()
{
    Queue<int, Item, Hash> queue;
    for (int tag = 0; tag < 3; ++tag)
    {
        for (unsigned sequence = 0; sequence < 4; ++sequence)
        {
            queue.push(tag, Item{tag, sequence});
        }
    }
    for (unsigned sequence = 0; sequence < 4; ++sequence)
    {
        for (int tag = 0; tag < 3; ++tag)
        {
            const auto &view = std::as_const(queue);
            require(view.front() == Item{tag, sequence}, "bursts did not interleave by tag");
            queue.pop();
        }
    }
    require(queue.empty() && queue.size() == 0, "drained queue is not empty");
    queue.push(0, Item{0, 0});
    queue.push(1, Item{1, 0});
    queue.push(0, Item{0, 1});
    require(queue.erase(0) == 2 && queue.erase(0) == 0 && queue.size() == 1,
            "erase returned an incorrect count");
    require(queue.front() == Item{1, 0}, "erase changed surviving data");
    queue.clear();
    queue.clear();
    queue.push(2, Item{2, 0});
    require(queue.front() == Item{2, 0}, "clear prevented reuse");
}

template <template <class, class, class> class Queue, class Hash> static void trace(const std::vector<int> &operations)
{
    Queue<int, Item, Hash> queue;
    std::vector<Item> reference;
    std::array<unsigned, 4> next{};
    std::array<unsigned, 4> last{};
    std::array<bool, 4> received{};
    for (const int operation : operations)
    {
        if (operation < 4)
        {
            const Item value{operation, next[operation]++};
            queue.push(operation, value);
            reference_push(reference, value);
        }
        else if (operation == 4 && !reference.empty())
        {
            const auto value = queue.front();
            require(value == reference.front(), "queue disagrees with independent reference");
            require(!received[value.tag] || last[value.tag] < value.sequence, "per-tag FIFO was violated");
            received[value.tag] = true;
            last[value.tag] = value.sequence;
            queue.pop();
            reference.erase(reference.begin());
        }
        else if (operation < 9 && operation >= 5)
        {
            const auto tag = operation - 5;
            const auto erased = std::erase_if(reference, [tag](Item value) { return value.tag == tag; });
            require(queue.erase(tag) == erased, "erase disagrees with reference");
        }
        else if (operation == 9)
        {
            queue.clear();
            reference.clear();
        }
        require(queue.size() == reference.size() && queue.empty() == reference.empty(), "queue size diverged");
        if (!reference.empty())
        {
            require(queue.front() == reference.front(), "front diverged after mutation");
        }
    }
    for (const auto value : reference)
    {
        require(queue.front() == value, "drain lost or duplicated data");
        queue.pop();
    }
    require(queue.empty(), "drain left extra data");
}

template <template <class, class, class> class Queue> static void verify()
{
    examples<Queue, std::hash<int>>();
    examples<Queue, ConstantHash>();
    // Grow deque blocks/maps, then exercise the cached insertion hint across
    // head removal, an erase that shifts elements, and a subsequent push.
    std::vector<int> growing(1024, 0);
    growing.insert(growing.end(), 1024, 1);
    for (int index = 0; index < 256; ++index)
    {
        growing.push_back(4);
        growing.push_back(1);
    }
    growing.insert(growing.end(), {5, 1, 4, 9, 1});
    trace<Queue, std::hash<int>>(growing);
    // Exhaust all length-eight traces of three labels and dequeue.
    std::vector<int> operations(8);
    for (unsigned encoded = 0; encoded < 65536; ++encoded)
    {
        auto remaining = encoded;
        for (auto &operation : operations)
        {
            const auto digit = remaining % 4;
            operation = digit == 3 ? 4 : static_cast<int>(digit);
            remaining /= 4;
        }
        trace<Queue, ConstantHash>(operations);
    }
    std::mt19937 random(0x7fac19);
    operations.resize(10000);
    for (int run = 0; run < 10; ++run)
    {
        for (auto &operation : operations)
        {
            const auto choice = random() % 100;
            operation = choice < 55 ? random() % 4 : choice < 85 ? 4 : choice < 99 ? 5 + random() % 4 : 9;
        }
        trace<Queue, ConstantHash>(operations);
    }
    Queue<int, std::unique_ptr<int>, std::hash<int>> movable;
    movable.push(1, std::make_unique<int>(17));
    auto value = std::move(movable.front());
    movable.pop();
    require(*value == 17 && movable.empty(), "move-only data was not transferred");

    Queue<int, lnk::Frame, std::hash<int>> frames;
    auto pool = std::make_shared<std::pmr::synchronized_pool_resource>();
    const std::weak_ptr<std::pmr::synchronized_pool_resource> lifetime = pool;
    frames.push(1, lnk::Frame{{LnkFrType::Data}, lnk::PooledBuffer(pool, 16)});
    pool.reset();
    require(!lifetime.expired(), "queued payload lost its allocator");
    auto frame = std::move(frames.front());
    frames.pop();
    require(!lifetime.expired(), "dequeue released an in-flight payload");
    frame.payload = {};
    require(lifetime.expired(), "completed payload retained its allocator");
    pool = std::make_shared<std::pmr::synchronized_pool_resource>();
    const std::weak_ptr<std::pmr::synchronized_pool_resource> erased = pool;
    frames.push(1, lnk::Frame{{LnkFrType::Data}, lnk::PooledBuffer(pool, 16)});
    pool.reset();
    frames.erase(1);
    require(erased.expired(), "erase retained payload ownership");
}

// Independent finite-state exploration: detect repeatable schedules that could
// keep an old A queued forever, while B/C arrive before every dequeue.
static void progress_model()
{
    for (int burst = 1; burst <= 3; ++burst)
    {
        using State = std::vector<Item>;
        const auto encode = [](const State &items) {
            std::string result;
            for (const auto item : items)
            {
                result += item.tag == 0 && item.sequence == 7 ? 'A' : char('a' + item.tag);
            }
            return result;
        };
        State start;
        for (unsigned index = 0; index < 8; ++index)
        {
            start.push_back({0, index});
        }
        std::map<std::string, std::vector<std::string>> edges;
        std::vector<State> pending{start};
        while (!pending.empty())
        {
            auto state = std::move(pending.back());
            pending.pop_back();
            const auto key = encode(state);
            if (edges.contains(key))
            {
                continue;
            }
            auto &out = edges[key];
            if (state.front() == Item{0, 7})
            {
                continue;
            }
            for (int pattern = 0; pattern < (1 << burst); ++pattern)
            {
                auto next = state;
                for (int bit = 0; bit < burst; ++bit)
                {
                    reference_push(next, Item{1 + ((pattern >> bit) & 1), 0});
                }
                next.erase(next.begin());
                out.push_back(encode(next));
                pending.push_back(std::move(next));
            }
        }
        std::map<std::string, std::size_t> incoming;
        for (const auto &[key, out] : edges)
        {
            incoming.try_emplace(key, 0);
            for (const auto &next : out)
            {
                ++incoming[next];
            }
        }
        std::vector<std::string> ready;
        for (const auto &[key, count] : incoming)
        {
            if (!count)
            {
                ready.push_back(key);
            }
        }
        std::size_t removed = 0;
        while (!ready.empty())
        {
            const auto key = std::move(ready.back());
            ready.pop_back();
            ++removed;
            for (const auto &next : edges.at(key))
            {
                if (--incoming.at(next) == 0)
                {
                    ready.push_back(next);
                }
            }
        }
        require(removed == edges.size(), "bounded model found a starvation cycle");
    }
}

static void flow_identity()
{
    asio::io_context io(1);
    auto first = std::make_shared<lnk::FlowState>(io.get_executor(), lnk::FlowSpec{1, 7, RelayProtocol::Tcp});
    auto second = std::make_shared<lnk::FlowState>(io.get_executor(), lnk::FlowSpec{1, 7, RelayProtocol::Tcp});
    const std::weak_ptr<lnk::FlowState> first_lifetime = first;
    const std::weak_ptr<lnk::FlowState> second_lifetime = second;
    FairQueue<lnk::FlowWriteTag, lnk::Frame, lnk::FlowWriteHash> queue;
    queue.push({first.get(), false}, lnk::Frame{{LnkFrType::Data, false, 0, 7, 1}, {}, first});
    queue.push({first.get(), true}, lnk::Frame{{LnkFrType::Fin, true, 0, 7, 1}, {}, first});
    queue.push({second.get(), false}, lnk::Frame{{LnkFrType::Data, false, 0, 7, 1}, {}, second});
    require(queue.erase({first.get(), false}) == 1 && queue.size() == 2,
            "identity erasure confused directions or same-ID Flow instances");
    auto in_flight = std::move(queue.front());
    queue.pop();
    require(in_flight.owner == first && in_flight.header.reverse, "direction FIFO or owner changed");
    first.reset();
    require(!first_lifetime.expired(), "dequeue destroyed an in-flight Flow");
    in_flight.owner.reset();
    require(first_lifetime.expired(), "in-flight completion retained a closed Flow");
    second.reset();
    require(!second_lifetime.expired(), "queue did not retain the other Flow");
    queue.clear();
    require(second_lifetime.expired(), "clear retained the other Flow");
}

int main()
{
    try
    {
        verify<DequeFairQueue>();
        verify<ListFairQueue>();
        verify<LinkedFairQueue>();
        verify<FairQueue>();
        progress_model();
        flow_identity();
        std::cout << "Fair queue models, traces and ownership checks passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
