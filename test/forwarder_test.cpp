#include "forwarder.h"
#include <chrono>
#include <future>
#include <iostream>

#include "member_access.h"
TEST_MEMBER(Forwarder, datagram_forwards_)
TEST_MEMBER(Forwarder, service_nodes_)
TEST_MEMBER(Forwarder, active_tasks_)
TEST_MEMBER(Forwarder, schedule_datagram_retry)
TEST_MEMBER(Forwarder, relays_)
TEST_MEMBER(Forwarder, state_)

namespace
{
struct ForwarderTestAccess
{
    // Initial setup runs before either executor is pumped.
    static void prepare(Forwarder &owner)
    {
        const ServiceKey service{"retry-test", RelayProtocol::Udp};
        auto &forward = test_access::Forwarder_datagram_forwards_(owner).at(0);
        test_access::Forwarder_service_nodes_(owner).emplace(service, "old-node");
        forward.retry_delay = std::chrono::seconds(10);
        const auto listeners = test_access::Forwarder_active_tasks_(owner);
        test_access::Forwarder_schedule_datagram_retry(owner, 0);
        if (test_access::Forwarder_active_tasks_(owner) != listeners + 1)
        {
            throw std::runtime_error("UDP retry was not registered for shutdown draining");
        }
    }

    static void cancel_and_recover(Forwarder &owner, bool recover)
    {
        const ServiceKey service{"retry-test", RelayProtocol::Udp};
        auto &forward = test_access::Forwarder_datagram_forwards_(owner).at(0);
        const auto tasks = test_access::Forwarder_active_tasks_(owner);
        owner.clear_service(service);
        if (!forward.retry_scheduled)
        {
            throw std::runtime_error("Service loss cleared the retry flag before its task finished");
        }
        if (recover)
        {
            owner.set_service(service, "new-node");
            if (forward.relay || test_access::Forwarder_active_tasks_(owner) != tasks)
            {
                throw std::runtime_error("Service recovery started a second task before the cancelled retry finished");
            }
        }
    }

    // Runtime calls stay on transfer_io; control_io is pumped separately.
    static asio::awaitable<void> retry_lifecycle(Forwarder &owner, bool wait_started, bool recover)
    {
        const ServiceKey service{"retry-test", RelayProtocol::Udp};
        auto &forward = test_access::Forwarder_datagram_forwards_(owner).at(0);
        if (wait_started)
        {
            cancel_and_recover(owner, recover);
        }
        if (recover)
        {
            co_await asio::post(asio::use_awaitable);
            co_await asio::post(asio::use_awaitable);
            if (!forward.relay || test_access::Forwarder_relays_(owner).size() != 1 || test_access::Forwarder_relays_(owner).begin()->second != forward.relay)
            {
                throw std::runtime_error("Cancelled retry did not establish one current UDP session on recovery");
            }
        }
        const std::weak_ptr<AgentSession> session = forward.relay;
        owner.clear_service(service);
        co_await await_transfers(owner.async_stop(), owner.async_stop());
        co_await owner.async_stop();
        if (test_access::Forwarder_state_(owner) != std::remove_cvref_t<decltype(test_access::Forwarder_state_(owner))>::Stopped || test_access::Forwarder_active_tasks_(owner) != 0 || forward.retry_scheduled ||
            forward.relay || !test_access::Forwarder_relays_(owner).empty() || !session.expired())
        {
            throw std::runtime_error("Forwarder shutdown retained a retry task or UDP session association");
        }
    }
};

} // namespace

using namespace std::chrono_literals;

int main()
{
    try
    {
        asio::ssl::context ssl(asio::ssl::context::tls_client);
        for (const bool wait_started : {false, true})
        {
            for (const bool recover : {false, true})
            {
                asio::io_context control(1);
                asio::io_context transfer(1);
                auto agent = std::make_shared<RelayAgent>(control, transfer, ssl, AgentConfig{});
                auto owner = std::make_shared<Forwarder>(
                    *agent, transfer.get_executor(), ssl, std::nullopt, 1s, 1s, 2s, std::vector<AgentServiceConfig>{},
                    std::vector<AgentForwardConfig>{{"retry-test", "127.0.0.1", 0, RelayProtocol::Udp}});
                std::weak_ptr<Forwarder> weak_owner = owner;
                owner->start();
                ForwarderTestAccess::prepare(*owner);
                if (!wait_started)
                {
                    ForwarderTestAccess::cancel_and_recover(*owner, recover);
                }
                auto result = asio::co_spawn(transfer,
                                             ForwarderTestAccess::retry_lifecycle(*owner, wait_started, recover),
                                             asio::use_future);
                const auto deadline = std::chrono::steady_clock::now() + 2s;
                while (result.wait_for(0ms) != std::future_status::ready)
                {
                    if (std::chrono::steady_clock::now() >= deadline)
                    {
                        throw std::runtime_error("Forwarder shutdown waited for the ten-second retry deadline");
                    }
                    control.restart();
                    control.poll();
                    transfer.restart();
                    transfer.run_for(1ms);
                }
                result.get();
                owner.reset();
                if (!weak_owner.expired())
                {
                    throw std::runtime_error("Stopped Forwarder was retained by a retry or listener task");
                }
            }
        }
        std::cout << "[PASS] UDP retries are owned, recover without overlap and drain before Forwarder destruction\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
