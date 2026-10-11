#include "lnk_channel.h"
#include <asio.hpp>
#include <asio/experimental/channel.hpp>
#include <array>
#include <future>
#include <iostream>
#include <map>
#include <vector>

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
    CountingResource resource;
    {
        PooledBuffer empty(resource, 0);
        require_buffer(resource.allocations == 0, "empty control frame allocated storage");
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
        require_buffer(resource.allocations == 2 && resource.live.size() == 1,
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
    require_buffer(resource.valid_deallocations && resource.live.empty(),
                   "offset payload was not returned with its original address, size and alignment");
}

void channel_transfer()
{
    CountingResource resource;
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
                           resource.allocations == 2,
                       "channel or coroutine transfer copied or reallocated payload storage");
    }
    require_buffer(resource.valid_deallocations && resource.live.empty(), "queued buffer storage leaked");
}

void pool_reuse()
{
    CountingResource upstream;
    {
        constexpr std::array<std::size_t, 4> sizes{64, 512, 4096, 4137};
        std::pmr::unsynchronized_pool_resource pool({64, sizes.back()}, &upstream);
        {
            std::vector<PooledBuffer> warm;
            warm.reserve(400);
            for (auto size : sizes)
            {
                for (int i = 0; i < 100; ++i)
                {
                    warm.emplace_back(pool, size);
                }
            }
        }
        const auto warmed_allocations = upstream.allocations;
        for (int i = 0; i < 10000; ++i)
        {
            PooledBuffer packet(pool, sizes[i % sizes.size()]);
            packet.bytes().front() = 42;
            packet.bytes().back() = 255;
        }
        require_buffer(upstream.allocations == warmed_allocations,
                       "steady packet traffic kept allocating from the upstream heap after warmup");
        std::cout << "[PASS] 10000 pooled packets required 0 upstream allocations after warmup\n";
    }
    require_buffer(upstream.valid_deallocations && upstream.live.empty(), "pool retained storage after destruction");
}

int main()
{
    try
    {
        ownership();
        channel_transfer();
        pool_reuse();
        std::cout << "[PASS] exclusive ownership, offset views and asynchronous channel transfers\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
