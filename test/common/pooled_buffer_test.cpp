#include "lnk_channel.h"
#include <asio.hpp>
#include <asio/experimental/channel.hpp>
#include <array>
#include <atomic>
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
        packet.data()[40] = 17;
        packet.data()[63] = 255;
        packet.slice(40, 24);
        PooledBuffer moved(std::move(packet));
        PooledBuffer assigned(resource, 16);
        assigned = std::move(moved);
        require_buffer(!packet.data() && !moved.data(), "move retained a second buffer owner");
        require_buffer(packet.bytes().empty() && packet.allocation_size() == 0 && moved.bytes().empty() &&
                           moved.allocation_size() == 0,
                       "moved buffer retained a payload view or allocation charge");
        require_buffer(assigned.data() == storage + 40 && assigned.size() == 24 && assigned.allocation_size() == 128 &&
                           assigned.bytes().front() == 17 && assigned.bytes().back() == 255,
                       "move copied or corrupted the payload view");
        require_buffer(resource->allocations == 2 && resource->live.size() == 1,
                       "move assignment allocated or retained the previous block");
        auto &same = assigned;
        assigned = std::move(same);
        require_buffer(assigned.data() == storage + 40 && assigned.size() == 24 && assigned.allocation_size() == 128,
                       "self move lost the payload view or storage ownership");
        const std::array<std::uint8_t, 3> external{0, 42, 255};
        PooledBuffer copied(resource, external);
        require_buffer(std::equal(copied.bytes().begin(), copied.bytes().end(), external.begin()),
                       "public payload import changed bytes");
    }
    require_buffer(resource->valid_deallocations && resource->live.empty(),
                   "offset payload was not returned with its original address, size and alignment");
}

void channel_transfer()
{
    auto resource = std::make_shared<CountingResource>();
    {
        asio::io_context io(1);
        asio::experimental::channel<void(asio::error_code, PooledBuffer)> queue(io, 1);
        PooledBuffer first(resource, 4136);
        first.data()[40] = 42;
        first.slice(40, 1);
        const auto storage = first.data();
        require_buffer(queue.try_send(asio::error_code{}, std::move(first)), "pooled buffer was not admitted");
        PooledBuffer rejected(resource, 8);
        const auto unsent = rejected.data();
        require_buffer(!queue.try_send(asio::error_code{}, std::move(rejected)) && rejected.data() == unsent,
                       "full queue consumed an unsent buffer");
        auto pending = asio::co_spawn(io, queue.async_receive(asio::use_awaitable), asio::use_future);
        io.run();
        auto packet = pending.get();
        require_buffer(packet.data() == storage && packet.size() == 1 && packet.bytes().front() == 42 &&
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
        constexpr std::array<std::size_t, 4> sizes{64, 512, 4096, 4137};
        auto pool = std::make_shared<lnk::PayloadPool>(4 * 1024 * 1024);
        {
            std::vector<PooledBuffer> warm;
            warm.reserve(400);
            const allocation_count::Scope counting;
            for (auto size : sizes)
            {
                for (int i = 0; i < 100; ++i)
                {
                    warm.emplace_back(pool, size);
                }
            }
        }
        const auto warmed_allocations = allocation_count::allocations.load();
        require_buffer(warmed_allocations > initial_allocations, "allocation instrumentation missed the pool");
        {
            const allocation_count::Scope counting;
            for (int i = 0; i < 10000; ++i)
            {
                PooledBuffer packet(pool, sizes[i % sizes.size()]);
                packet.bytes().front() = 42;
                packet.bytes().back() = 255;
            }
        }
        require_buffer(allocation_count::allocations == warmed_allocations,
                       "steady packet traffic kept allocating from the upstream heap after warmup");
        auto transfer = [pool, sizes] {
            const allocation_count::Scope counting;
            for (int i = 0; i < 5000; ++i)
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
        require_buffer(allocation_count::allocations == warmed_allocations,
                       "cross-executor pool reuse kept allocating from the upstream heap");
        std::cout << "[PASS] 20000 pooled packets required 0 upstream allocations after warmup\n";
    }
    require_buffer(allocation_count::live == initial_live, "pool retained storage after destruction");
}

// Verify the shared pool's hard capacity and idle-block reclamation independently of Flow credit.
void pool_capacity()
{
    const auto initial_live = allocation_count::live.load();
    auto retained_bytes = [&] {
        return allocation_count::live.load() - initial_live;
    };
    {
        auto pool = std::make_shared<lnk::PayloadPool>(4096);
        const allocation_count::Scope counting;
        for (int round = 0; round < 64; ++round)
        {
            {
                PooledBuffer first(pool, 2048);
                PooledBuffer second(pool, 2048);
                require_buffer(retained_bytes() == 4096, "pool did not charge rounded blocks");
            }
            {
                PooledBuffer small(pool, 255);
                bool rejected = false;
                try
                {
                    PooledBuffer large(pool, 3072);
                }
                catch (const std::bad_alloc &)
                {
                    rejected = true;
                }
                require_buffer(rejected, "pool exceeded hard live budget");
            }
            {
                PooledBuffer large(pool, 3072);
                require_buffer(retained_bytes() == 4096, "pool failed to reclaim idle blocks");
            }
            require_buffer(retained_bytes() <= 4096, "pool retained unbounded storage");
        }
    }
    require_buffer(allocation_count::live == initial_live, "capacity test leaked pool storage");
}

void shared_resource_lifetime()
{
    const auto initial_live = allocation_count::live.load();
    std::weak_ptr<lnk::PayloadPool> weak;
    PooledBuffer payload;
    {
        auto pool = std::make_shared<lnk::PayloadPool>(65536);
        weak = pool;
        const allocation_count::Scope counting;
        payload = PooledBuffer(pool, 1024);
        payload.bytes().front() = 42;
        payload.slice(0, 1);
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
        pool_capacity();
        shared_resource_lifetime();
        std::cout << "[PASS] exclusive ownership, offset views and asynchronous channel transfers\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
