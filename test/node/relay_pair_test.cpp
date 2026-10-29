#include "node_common.h"
#include "flow_protocol.h"
#include <future>
#include <iostream>
#include <stdexcept>

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
} // namespace

int main()
{
    try
    {
        local_accessor_lifetime();
        close_during_binding();
        std::cout << "[PASS] Pair accessor lifetime and closure during Flow binding\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
