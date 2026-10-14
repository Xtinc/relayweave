#include "benchmark.h"
#include "registry_mgr.h"

namespace
{
using bench::require;

void registry(bench::Report &report)
{
    asio::io_context io(1);
    asio::ssl::context context(asio::ssl::context::tls_server);
    for (const std::size_t services : {100, 10000})
    {
        constexpr std::size_t sessions = 100;
        RegistryMgr registry(sessions, services, services);
        std::vector<ControlSessionPtr> owners;
        std::vector<std::string> names;
        for (std::size_t i = 1; i <= sessions; ++i)
        {
            // Registry borrows live sessions; handshakes and network I/O are outside this workload.
            owners.push_back(std::make_shared<TLSChannel>(asio::ip::tcp::socket(io), context, TLSChannelRole::S));
            registry.add(i, owners.back());
        }
        for (std::size_t i = 0; i < services; ++i)
        {
            names.push_back("service-" + std::to_string(i));
            require(registry.register_service(i % sessions + 1, names.back(), RelayProtocol::Tcp) == RegistryMgr::Result::Registered,
                    "registry setup failed");
        }
        std::uint64_t checksum = 0;
        bench::Timer timer;
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            const auto service = registry.find_service(names[i % services]);
            require(service && service->protocol == RelayProtocol::Tcp && service->session == owners[i % services % sessions],
                    "registry returned wrong owner");
            checksum += bool(service->traffic);
        }
        report.add("registry/service_lookup/" + std::to_string(services), report.iterations, 0, timer, checksum,
                   {{"services", services}, {"sessions", sessions}});
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < report.iterations; ++i)
        {
            require(registry.find_session(i % sessions + 1) == owners[i % sessions], "registry session lookup failed");
            checksum += registry.contains(i % sessions + 1);
        }
        report.add("registry/session_lookup/" + std::to_string(services), report.iterations, 0, timer, checksum);
        const auto scans = std::max<std::size_t>(1, report.iterations / 1000);
        checksum = 0;
        timer = {};
        for (std::size_t i = 0; i < scans; ++i)
        {
            const auto listed = registry.service_names();
            require(listed.size() == services, "registry listing lost services");
            checksum += listed.size();
        }
        report.add("registry/service_list/" + std::to_string(services), scans, 0, timer, checksum);
        timer = {};
        for (std::size_t i = 0; i < scans; ++i)
        {
            registry.sample_traffic();
            const auto traffic = registry.traffic_report();
            require(traffic.size() == services, "traffic report lost services");
        }
        report.add("registry/traffic_report/" + std::to_string(services), scans, 0, timer, services * scans);
        // Remove one session's services and register them again, including fresh per-service counters.
        const auto churns = std::max<std::size_t>(1, report.iterations / 100);
        timer = {};
        for (std::size_t i = 0; i < churns; ++i)
        {
            const auto index = i % sessions;
            registry.remove(index + 1);
            registry.add(index + 1, owners[index]);
            for (std::size_t service = index; service < services; service += sessions)
                require(registry.register_service(index + 1, names[service], RelayProtocol::Tcp) == RegistryMgr::Result::Registered,
                        "registry churn failed");
        }
        report.add("registry/session_churn/" + std::to_string(services), churns, 0, timer, registry.session_count(),
                   {{"services_per_session", services / sessions}});
        require(registry.service_names().size() == services, "registry churn lost services");
        registry.stop();
    }
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 100000);
        for (unsigned i = 0; i < report.repeats; ++i)
            registry(report);
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
