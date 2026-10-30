#include "lnk_channel.h"
#include <asio.hpp>
#include <asio/experimental/channel.hpp>
#include <array>
#include <atomic>
#include <barrier>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

using lnk::PooledBuffer;

// Allocation instrumentation belongs only to this test executable. Track pool
// operations on each participating thread, excluding fixtures and thread setup.
namespace allocation_count
{
struct Header
{
    void *base;
    std::size_t bytes;
    bool tracked;
};
thread_local bool enabled = false;
std::atomic<std::size_t> allocations{0};
std::atomic<std::size_t> live{0};

struct Scope
{
    Scope() { enabled = true; }
    ~Scope() { enabled = false; }
};

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

void require_buffer(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
}

class CountingResource : public std::pmr::memory_resource
{
  public:
    std::size_t allocations = 0;
    bool valid_deallocations = true;
    std::map<void *, std::pair<std::size_t, std::size_t>> live;

  private:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        auto memory = std::pmr::new_delete_resource()->allocate(bytes, alignment);
        live.emplace(memory, std::pair(bytes, alignment));
        ++allocations;
        return memory;
    }

    void do_deallocate(void *memory, std::size_t bytes, std::size_t alignment) override
    {
        auto found = live.find(memory);
        if (found == live.end())
        {
            valid_deallocations = false;
            return;
        }
        valid_deallocations = valid_deallocations && found->second == std::pair(bytes, alignment);
        std::pmr::new_delete_resource()->deallocate(memory, found->second.first, found->second.second);
        live.erase(found);
    }

    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }
};

void ownership()
{
    auto resource = std::make_shared<CountingResource>();
    {
        PooledBuffer empty(resource, 0);
        require_buffer(resource->allocations == 0, "empty control frame allocated storage");
        PooledBuffer packet(resource, 128);
        auto storage = packet.data();
        packet.data()[0] = 17;
        packet.data()[2] = 255;
        packet.truncate(3);
        PooledBuffer moved(std::move(packet));
        PooledBuffer assigned(resource, 16);
        assigned = std::move(moved);
        require_buffer(!packet.data() && !moved.data(), "move retained a second buffer owner");
        require_buffer(packet.bytes().empty() && moved.bytes().empty() &&
                           packet.capacity() == 0 && moved.capacity() == 0,
                       "moved buffer retained a payload or allocation charge");
        require_buffer(assigned.data() == storage && assigned.size() == 3 && assigned.capacity() == 128 &&
                           assigned.bytes().front() == 17 && assigned.bytes().back() == 255,
                       "move copied or corrupted the payload");
        require_buffer(resource->allocations == 2 && resource->live.size() == 1,
                       "move assignment allocated or retained the previous block");
        auto &same = assigned;
        assigned = std::move(same);
        require_buffer(assigned.data() == storage && assigned.size() == 3 && assigned.capacity() == 128,
                       "self move lost the payload or storage ownership");
        const std::array<std::uint8_t, 3> external{0, 42, 255};
        PooledBuffer copied(resource, external);
        require_buffer(std::equal(copied.bytes().begin(), copied.bytes().end(), external.begin()),
                       "public payload import changed bytes");
        copied.truncate(0);
        require_buffer(copied.size() == 0 && copied.capacity() == external.size(),
                       "empty valid payload lost its owned allocation");
    }
    require_buffer(resource->valid_deallocations && resource->live.empty(),
                   "payload was not returned with its original address, size and alignment");
}

void channel_transfer()
{
    auto resource = std::make_shared<CountingResource>();
    {
        asio::io_context io(1);
        asio::experimental::channel<void(asio::error_code, PooledBuffer)> queue(io, 1);
        PooledBuffer first(resource, 4136);
        first.data()[0] = 42;
        const auto storage = first.data();
        require_buffer(queue.try_send(asio::error_code{}, std::move(first)), "pooled buffer was not admitted");
        PooledBuffer rejected(resource, 8);
        const auto unsent = rejected.data();
        require_buffer(!queue.try_send(asio::error_code{}, std::move(rejected)) && rejected.data() == unsent,
                       "full queue consumed an unsent buffer");
        auto pending = asio::co_spawn(io, queue.async_receive(asio::use_awaitable), asio::use_future);
        io.run();
        auto packet = pending.get();
        require_buffer(packet.data() == storage && packet.size() == 4136 && packet.bytes().front() == 42 &&
                           resource->allocations == 2,
                       "channel or coroutine transfer copied or reallocated payload storage");
    }
    require_buffer(resource->valid_deallocations && resource->live.empty(), "queued buffer storage leaked");
}

void pool_reuse()
{
    const auto initial_live = allocation_count::live.load();
    const auto initial_allocations = allocation_count::allocations.load();
    {
        auto pool = std::make_shared<std::pmr::synchronized_pool_resource>(
            std::pmr::pool_options{.largest_required_pool_block = 65536}, std::pmr::new_delete_resource());
        std::size_t warmed_allocations = 0;
        std::barrier warmed(2, [&]() noexcept { warmed_allocations = allocation_count::allocations.load(); });
        auto transfer = [&] {
            constexpr std::array<std::size_t, 5> sizes{64, 512, 4096, 4137, 65536};
            std::vector<PooledBuffer> warm;
            warm.reserve(sizes.size() * 16);
            const allocation_count::Scope counting;
            for (auto size : sizes)
                for (int i = 0; i < 16; ++i)
                    warm.emplace_back(pool, size);
            warm.clear();
            // Standard synchronized pools can initialise per-thread state.
            warmed.arrive_and_wait();
            for (int i = 0; i < 10000; ++i)
            {
                PooledBuffer packet(pool, sizes[i % sizes.size()]);
                packet.bytes().front() = 42;
                packet.bytes().back() = 255;
            }
        };
        std::thread first(transfer);
        std::thread second(transfer);
        first.join();
        second.join();
        require_buffer(warmed_allocations > initial_allocations, "allocation instrumentation missed the pool");
        require_buffer(allocation_count::allocations == warmed_allocations,
                       "steady traffic kept allocating upstream after per-thread warmup");
        std::cout << "[PASS] 20000 standard-pool packets reused storage after per-thread warmup\n";
    }
    require_buffer(allocation_count::live == initial_live, "pool retained storage after destruction");
}

void logical_charge()
{
    for (const auto [size, charge] : std::array<std::pair<std::size_t, std::size_t>, 8>{
             {{0, 128}, {1, 384}, {255, 384}, {256, 384}, {257, 640}, {4097, 8320}, {65467, 65664}, {65536, 65664}}})
    {
        require_buffer(lnk::Frame::charge_for_payload(size) == charge, "wire credit charge changed");
    }
    auto resource = std::make_shared<CountingResource>();
    lnk::Frame frame{{}, PooledBuffer(resource, 32768)};
    frame.payload.truncate(1);
    require_buffer(frame.charge() == 384 && frame.memory_charge() == 32896,
                   "short read hid retained storage or changed wire credit");
    frame.payload.truncate(0);
    require_buffer(frame.charge() == 128 && frame.memory_charge() == 32896,
                   "empty DATA hid retained storage or changed wire credit");
}

void shared_resource_lifetime()
{
    const auto initial_live = allocation_count::live.load();
    std::weak_ptr<std::pmr::synchronized_pool_resource> weak;
    PooledBuffer payload;
    {
        auto pool = std::make_shared<std::pmr::synchronized_pool_resource>();
        weak = pool;
        const allocation_count::Scope counting;
        payload = PooledBuffer(pool, 1024);
        payload.bytes().front() = 42;
    }
    require_buffer(!weak.expired(), "business delivery released its allocator too early");
    std::thread writer([payload = std::move(payload)]() mutable {
        require_buffer(payload.bytes().front() == 42, "cross-executor delivery lost its payload");
        payload = {};
    });
    writer.join();
    require_buffer(weak.expired() && allocation_count::live == initial_live,
                   "last business delivery did not release its shared allocator");
}

int main()
{
    try
    {
        ownership();
        channel_transfer();
        pool_reuse();
        logical_charge();
        shared_resource_lifetime();
        std::cout << "[PASS] exclusive ownership, exact payload storage and asynchronous channel transfers\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
