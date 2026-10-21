#include "node_control.h"
#include "registry_mgr.h"
#include "flow_protocol.h"
#include "member_access.h"
#include <iostream>

TEST_MEMBER(FlowProtocol, forwarded_bytes_)

using namespace std::chrono_literals;

static void require_metrics(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

int main()
{
    try
    {
        asio::io_context control(1);
        asio::io_context tcp(1);
        asio::io_context udp(1);
        asio::io_context data(1);
        RegistryMgr registry(8, 8, 8);
        asio::ssl::context ssl(asio::ssl::context::tls);
        auto session = std::make_shared<TLSChannel>(asio::ip::tcp::socket(control), ssl, TLSChannelRole::S);
        registry.add(1, session);
        require_metrics(registry.register_service(1, "metrics", RelayProtocol::Tcp) == RegistryMgr::Result::Registered,
                        "Metrics service was not registered");
        auto traffic = registry.find_service("metrics")->traffic;
        auto protocol = std::make_shared<FlowProtocol>(data.get_executor(), control.get_executor(),
                                                       "metrics", "127.0.0.1", 0, "127.0.0.1", 0);
        auto metrics = std::make_shared<NodeMetrics>(control.get_executor(), tcp.get_executor(), udp.get_executor(),
                                                     registry, 1);
        metrics->start(*protocol, std::chrono::steady_clock::now() - 5s);
        asio::post(control, [&] {
            const auto initial = metrics->queue_delays();
            const auto invalid = std::numeric_limits<std::uint32_t>::max();
            require_metrics(initial.control_queue_delay_us == invalid && initial.transfer_tcp_queue_delay_us == invalid &&
                                initial.transfer_udp_queue_delay_us == invalid,
                            "Unmeasured executor delay was reported as healthy");
            require_metrics(metrics->available() == 1.0 && metrics->uptime() >= 5s,
                            "Bootstrap did not record uptime before probe activation");
            // Inject accumulated data-plane traffic through the existing protocol counter.
            test_access::FlowProtocol_forwarded_bytes_(*protocol).store(100'000'000);
            traffic->rx.add(1000);
            traffic->tx.add(2000);
            metrics->activate();
        });
        control.poll();

        bool sampled = false;
        double available = 1.0;
        const auto deadline = std::chrono::steady_clock::now() + 3s;
        while (!sampled && std::chrono::steady_clock::now() < deadline)
        {
            tcp.restart();
            tcp.poll();
            udp.restart();
            udp.poll();
            control.restart();
            asio::post(control, [&] {
                const auto delays = metrics->queue_delays();
                const auto invalid = std::numeric_limits<std::uint32_t>::max();
                sampled = delays.control_queue_delay_us != invalid && delays.transfer_tcp_queue_delay_us != invalid &&
                          delays.transfer_udp_queue_delay_us != invalid;
                available = metrics->available();
            });
            control.run_for(10ms);
        }
        require_metrics(sampled, "One or more executor probes did not run");
        require_metrics(available == 0.0 && protocol->take_forwarded_bytes() == 0,
                        "Transit traffic was omitted from available bandwidth sampling");
        require_metrics(traffic->rx_bytes_per_second() > 0 && traffic->tx_bytes_per_second() > 0,
                        "Control probe did not sample service traffic");

        // Cancel control first. Keep the transfer cancellations queued until the owner is released.
        asio::post(control, [held = metrics] { held->stop_control(); });
        control.restart();
        control.poll();
        asio::post(tcp, [held = metrics] { held->stop_tcp(); });
        asio::post(udp, [held = metrics] { held->stop_udp(); });
        std::weak_ptr<NodeMetrics> weak = metrics;
        metrics.reset();
        for (unsigned attempt = 0; attempt < 8; ++attempt)
        {
            for (auto *io : {&control, &tcp, &udp})
            {
                io->restart();
                io->poll();
            }
        }
        require_metrics(weak.expired(), "Cancelled probe callbacks rearmed or retained metrics after shutdown");
        std::cout << "[PASS] Metrics bootstrap, executor probes, transit bandwidth and cancellation ownership\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
