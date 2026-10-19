#include "fair_queue_candidates.h"
#include "lnk_channel.h"
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#ifdef FAIR_QUEUE_COUNT_ALLOCATIONS
namespace allocation_count
{
struct Header
{
    void *base;
    std::size_t bytes;
    bool tracked;
};
bool enabled = false;
std::size_t allocations = 0;
std::size_t live = 0;
std::size_t peak = 0;

void *allocate(std::size_t bytes, std::size_t alignment)
{
    const auto base = std::malloc(bytes + alignment + sizeof(Header));
    if (!base)
    {
        throw std::bad_alloc();
    }
    const auto start = reinterpret_cast<std::uintptr_t>(base) + sizeof(Header);
    const auto address = (start + alignment - 1) & ~(alignment - 1);
    const auto header = reinterpret_cast<Header *>(address) - 1;
    *header = Header{base, bytes, enabled};
    if (enabled)
    {
        ++allocations;
        live += bytes;
        peak = std::max(peak, live);
    }
    return reinterpret_cast<void *>(address);
}
void release(void *memory) noexcept
{
    if (memory)
    {
        const auto header = static_cast<Header *>(memory) - 1;
        if (header->tracked)
        {
            live -= header->bytes;
        }
        std::free(header->base);
    }
}
} // namespace allocation_count

// Only this separately built executable replaces allocation. Timing uses the
// ordinary allocator in fair_queue_compare, without these headers or counters.
void *operator new(std::size_t bytes) { return allocation_count::allocate(bytes, alignof(std::max_align_t)); }
void *operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete(void *p) noexcept { allocation_count::release(p); }
void operator delete[](void *p) noexcept { ::operator delete(p); }
void operator delete(void *p, std::size_t) noexcept { ::operator delete(p); }
void operator delete[](void *p, std::size_t) noexcept { ::operator delete(p); }
void *operator new(std::size_t bytes, std::align_val_t alignment)
{
    return allocation_count::allocate(bytes, static_cast<std::size_t>(alignment));
}
void *operator new[](std::size_t bytes, std::align_val_t alignment) { return ::operator new(bytes, alignment); }
void operator delete(void *p, std::align_val_t) noexcept { ::operator delete(p); }
void operator delete[](void *p, std::align_val_t) noexcept { ::operator delete(p); }
void operator delete(void *p, std::size_t, std::align_val_t) noexcept { ::operator delete(p); }
void operator delete[](void *p, std::size_t, std::align_val_t) noexcept { ::operator delete(p); }
#endif

struct Tag
{
    std::uintptr_t flow;
    bool reverse;
    bool operator==(const Tag &) const = default;
};
struct Hash
{
    std::size_t operator()(Tag tag) const noexcept { return std::hash<std::uintptr_t>{}(tag.flow) ^ tag.reverse; }
};

using Clock = std::chrono::steady_clock;
using Frame = lnk::Frame;
std::uint64_t checksum = 0;

struct Operation
{
    char kind;
    Tag tag;
    std::uint64_t serial;
};

static std::vector<Operation> trace(unsigned flows, unsigned depth)
{
    std::vector<Operation> result;
    for (unsigned tag = 0; tag < flows; ++tag)
    {
        for (unsigned index = 0; index < depth; ++index)
        {
            result.push_back({'p', Tag{std::uintptr_t(tag) * 16, bool(tag % 2)}, result.size()});
        }
    }
    return result;
}

template <class Queue> static std::size_t replay(Queue &queue, const std::vector<Operation> &operations)
{
    for (const auto operation : operations)
    {
        if (operation.kind == 'p')
        {
            Frame value;
            value.header.sequence = operation.serial;
            queue.push(operation.tag, std::move(value));
        }
        else if (operation.kind == 'o')
        {
            if (!queue.empty())
            {
                checksum += queue.front().header.sequence;
                queue.pop();
            }
        }
        else
        {
            checksum += queue.erase(operation.tag);
        }
    }
    return operations.size();
}

static std::vector<Operation> workload(const std::string &kind, unsigned flows, unsigned depth)
{
    std::vector<Operation> result;
    if (kind == "push")
    {
        return trace(flows, depth);
    }
    if (kind == "pop")
    {
        result.resize(flows * depth, Operation{'o', {}, 0});
        return result;
    }
    if (kind == "erase")
    {
        for (unsigned index = 0; index < flows; ++index)
        {
            result.push_back({'e', Tag{std::uintptr_t(index) * 16, bool(index % 2)}, 0});
        }
        return result;
    }
    for (unsigned index = 0; index < std::max(32U, flows * 2); ++index)
    {
        const auto tag = index % flows;
        const Tag label{std::uintptr_t(tag) * 16, bool(tag % 2)};
        if (index % 16 == 0)
        {
            result.push_back({'e', label, 0});
            for (unsigned item = 0; item < depth; ++item)
            {
                result.push_back({'p', label, index * depth + item});
            }
        }
        result.push_back({'p', label, index});
        result.push_back({'o', {}, 0});
    }
    return result;
}

template <template <class, class, class> class Candidate>
static void sample(std::ostream &out, const char *name, const std::string &kind,
                   unsigned flows, unsigned depth, int repeat)
{
    using Queue = Candidate<Tag, Frame, Hash>;
    const auto initial = kind == "push" ? std::vector<Operation>{} : trace(flows, depth);
    const auto operations = workload(kind, flows, depth);
#ifdef FAIR_QUEUE_COUNT_ALLOCATIONS
    // One queue per allocation sample, so live bytes describe this workload's
    // actual depth, rather than the batching used to time short operations.
    allocation_count::allocations = 0;
    allocation_count::live = 0;
    allocation_count::peak = 0;
    allocation_count::enabled = true;
    {
        Queue queue;
        replay(queue, initial);
        replay(queue, operations);
    }
    allocation_count::enabled = false;
    if (allocation_count::live != 0)
    {
        throw std::runtime_error("allocation sample leaked queue storage");
    }
    if (repeat >= 0)
    {
        out << name << ',' << kind << ',' << flows << ',' << depth << ',' << repeat << ','
            << allocation_count::allocations << ',' << allocation_count::peak + sizeof(Queue) << '\n';
    }
#else
    // Setup is excluded for pop/erase/mixed. Batch small cases so individual
    // clock reads do not dominate the measured operations.
    const auto copies = std::max<std::size_t>(1, 4096 / (flows * depth));
    std::vector<std::unique_ptr<Queue>> queues;
    for (std::size_t index = 0; index < copies; ++index)
    {
        auto queue = std::make_unique<Queue>();
        replay(*queue, initial);
        queues.push_back(std::move(queue));
    }
    const auto started = Clock::now();
    for (auto &queue : queues)
    {
        replay(*queue, operations);
    }
    const auto elapsed = std::chrono::duration<double, std::nano>(Clock::now() - started).count();
    if (repeat >= 0)
    {
        out << name << ',' << kind << ',' << flows << ',' << depth << ',' << repeat << ','
            << copies * operations.size() << ',' << elapsed / (copies * operations.size()) << '\n';
    }
#endif
}

int main(int argc, char **argv)
{
    try
    {
        if (argc != 2)
        {
            throw std::runtime_error("usage: fair_queue_compare OUTPUT.csv");
        }
        std::ofstream out(argv[1]);
        out.exceptions(std::ios::failbit | std::ios::badbit);
#ifdef FAIR_QUEUE_COUNT_ALLOCATIONS
        out << "candidate,workload,flows,depth,repeat,allocations,peak_bytes\n";
#else
        out << "candidate,workload,flows,depth,repeat,operations,ns_per_op\n";
#endif
        for (const unsigned flows : {1, 10, 100, 400})
        {
            for (const unsigned depth : {1, 8, 64})
            {
                for (const std::string kind : {"push", "pop", "erase", "mixed"})
                {
                    for (int repeat = -1; repeat < 5; ++repeat)
                    {
                        // Rotate execution order after an unrecorded warmup.
                        for (int offset = 0; offset < 3; ++offset)
                        {
                            switch ((repeat + 1 + offset) % 3)
                            {
                            case 0: sample<DequeFairQueue>(out, "deque", kind, flows, depth, repeat); break;
                            case 1: sample<ListFairQueue>(out, "list", kind, flows, depth, repeat); break;
                            case 2: sample<LinkedFairQueue>(out, "linked", kind, flows, depth, repeat); break;
                            }
                        }
                    }
                }
            }
            std::cerr << "Completed " << flows << " tags\n";
        }
        std::cerr << "checksum=" << checksum << " sizeof(Frame)=" << sizeof(Frame) << '\n';
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
