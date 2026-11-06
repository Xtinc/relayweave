#include "node_common.h"
#include "flow_protocol.h"
#include "datagram_mgr.h"
#include "member_access.h"
#include <cstdlib>
#include <future>
#include <iostream>
#include <new>
#include <stdexcept>

TEST_MEMBER(DatagramMgr, local_pairs_)
TEST_MEMBER(DatagramMgr, remote_pairs_)
TEST_MEMBER(DatagramMgr, bindings_)
TEST_MEMBER(RelayIdAllocator, active_ids_)

// Fail one C++ allocation during installation, then allow cleanup to run normally.
static thread_local std::ptrdiff_t allocations_before_failure = -1;

void *operator new(std::size_t bytes)
{
    if (allocations_before_failure >= 0 && allocations_before_failure-- == 0)
    {
        throw std::bad_alloc();
    }
    if (auto *storage = std::malloc(bytes ? bytes : 1))
    {
        return storage;
    }
    throw std::bad_alloc();
}
void *operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete(void *storage) noexcept { std::free(storage); }
void operator delete[](void *storage) noexcept { ::operator delete(storage); }
void operator delete(void *storage, std::size_t) noexcept { ::operator delete(storage); }
void operator delete[](void *storage, std::size_t) noexcept { ::operator delete(storage); }

namespace
{
void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

/** @brief Delay port delivery to exercise closure during an outstanding binding operation. */
struct PendingRemotePair : RemoteRelayPair
{
    explicit PendingRemotePair(asio::any_io_executor executor)
        : RemoteRelayPair(executor, RelayAttach::Consumer), release(executor)
    {
    }

    /** @brief Close any delivered port, including one delivered after an earlier close. */
    void close() override
    {
        port_open = false;
        RemoteRelayPair::close();
    }

    AsyncEvent release;
    bool binding_started = false;
    bool port_open = false;

  private:
    /** @brief Suspend delivery until the test has closed the pair. */
    asio::awaitable<void> bind_flow(FlowProtocol &, std::uint64_t, std::uint64_t,
                                    asio::any_io_executor) override
    {
        binding_started = true;
        if (!co_await release.wait())
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        port_open = true;
    }
};

void local_accessor_lifetime()
{
    asio::io_context io;
    const TrafficLimitConfig limits;
    auto traffic = std::make_shared<ServiceTraffic>();
    LocalRelayPair first(io.get_executor());
    LocalRelayPair second(io.get_executor());
    first.attached.notify_all();
    second.attached.notify_all();
    auto first_ready = asio::co_spawn(io, first.prepare(limits, traffic, "consumer"), asio::use_future);
    auto second_ready = asio::co_spawn(io, second.prepare(limits, traffic, "consumer"), asio::use_future);
    io.run();
    first_ready.get();
    second_ready.get();
    require(!first.active && !second.active && traffic->accessors().empty(),
            "Preparation activated a pair before control coordination");
    first.activate();
    second.activate();
    require(traffic->accessors().at("consumer") == 2, "Local relays did not share the accessor count");

    RelayPair &closing = first;
    closing.close();
    closing.close();
    require(first.closed && !first.active, "Local close retained active state");
    require(traffic->accessors().at("consumer") == 1, "Repeated close removed another relay's accessor");
    second.close();
    require(traffic->accessors().empty(), "The final local accessor was not released");
}

void close_during_binding()
{
    asio::io_context io;
    FlowProtocol protocol(io.get_executor(), io.get_executor(), "node", "127.0.0.1", 0, "127.0.0.1", 0);
    const TrafficLimitConfig limits;
    auto traffic = std::make_shared<ServiceTraffic>();
    PendingRemotePair pair(io.get_executor());
    pair.attached.notify_all();
    auto ready = asio::co_spawn(io, pair.prepare(protocol, 1, 1, io.get_executor(), limits, traffic, "consumer"),
                                asio::use_future);
    io.poll();
    require(pair.binding_started && !pair.port_open, "Binding did not wait for port delivery");
    RelayPair &closing = pair;
    closing.close();
    pair.release.notify_all();
    io.restart();
    io.run();

    std::string failure;
    try
    {
        ready.get();
    }
    catch (const std::runtime_error &error)
    {
        failure = error.what();
    }
    require(failure == "relay endpoint closed during binding", "Late binding did not preserve its failure");
    require(pair.closed && !pair.port_open, "A port delivered after closure remained open");
    require(!pair.traffic && traffic->accessors().empty(), "Closed preparation installed service accounting");
}

void datagram_install_rollback(bool remote)
{
    for (std::ptrdiff_t failure = 0; failure < 64; ++failure)
    {
        asio::io_context io;
        auto ids = std::make_shared<RelayIdAllocator>();
        DatagramMgr manager(io.get_executor(), ids, {asio::ip::address_v4::loopback(), 0}, 1, {});
        const auto install = [&] {
            return remote ? manager.install_remote_pair(RelayAttach::Consumer).uuid
                          : manager.install_local_pair().consumer.uuid;
        };
        const auto close = [&](std::uint64_t uuid) {
            if (remote)
                manager.close_remote_pair(uuid);
            else
                manager.close_local_pair(uuid);
        };
        std::uint64_t uuid = 0;
        bool failed = false;
        allocations_before_failure = failure;
        try
        {
            uuid = install();
        }
        catch (const std::bad_alloc &)
        {
            failed = true;
        }
        allocations_before_failure = -1;
        if (!failed)
        {
            close(uuid);
        }
        require(test_access::DatagramMgr_local_pairs_(manager).empty() &&
                    test_access::DatagramMgr_remote_pairs_(manager).empty() &&
                    test_access::DatagramMgr_bindings_(manager).empty() &&
                    test_access::RelayIdAllocator_active_ids_(*ids).empty(),
                "Datagram installation retained a pair, binding or UUID after allocation failure");
        if (!failed)
        {
            require(failure > 0, "Allocation failure injection did not exercise installation");
            return;
        }
        // Failed installation must leave the only capacity slot usable.
        close(install());
    }
    throw std::runtime_error("Datagram allocation failure coverage never reached successful installation");
}
} // namespace

int main()
{
    try
    {
        local_accessor_lifetime();
        close_during_binding();
        datagram_install_rollback(false);
        datagram_install_rollback(true);
        std::cout << "[PASS] Pair lifetime, Flow binding closure and datagram installation rollback\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
