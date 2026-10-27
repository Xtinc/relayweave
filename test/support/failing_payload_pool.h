#pragma once

#include <memory_resource>
#include <new>

// Test-only allocation failure, independent of production admission policy.
class FailingPayloadPool : public std::pmr::synchronized_pool_resource
{
  public:
    bool fail_allocations = false;

  private:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        if (fail_allocations)
        {
            throw std::bad_alloc();
        }
        return std::pmr::synchronized_pool_resource::do_allocate(bytes, alignment);
    }
};
