#include "registry_mgr.h"
#include <algorithm>

std::string_view RegistryMgr::result2string(Result result)
{
    using enum Result;
    switch (result)
    {
    case Result::NameConflict:
        return "service name is already registered";
    case Result::SessionLimitReached:
        return "session service capacity reached";
    case Result::GlobalLimitReached:
        return "service capacity reached";
    case Result::RegistryStopped:
        return "server stopping";
    case Result::Registered:
        return "service registered";
    default:
        break;
    }
    return "unknown";
}

RegistryMgr::RegistryMgr(std::size_t max_sessions, std::size_t max_services, std::size_t max_services_per_session)
    : max_sessions_(max_sessions), max_services_(max_services), max_services_per_session_(max_services_per_session)
{
}

bool RegistryMgr::full() noexcept
{
    for (auto entry = sessions_.begin(); entry != sessions_.end();)
    {
        if (!entry->second.expired())
        {
            ++entry;
            continue;
        }
        services_.erase_secondary(entry->first);
        entry = sessions_.erase(entry);
    }
    return stopped_ || sessions_.size() >= max_sessions_;
}

std::size_t RegistryMgr::session_count() noexcept
{
    return sessions_.size();
}

bool RegistryMgr::contains(SessionId id) noexcept
{
    return sessions_.contains(id);
}

void RegistryMgr::add(SessionId id, const ControlSessionPtr &session)
{
    if (full())
    {
        throw std::logic_error("Control session registry is full or stopped");
    }

    if (id == 0 || !session)
    {
        throw std::invalid_argument("Control session ID and session must be valid");
    }

    if (contains(id))
    {
        throw std::logic_error("Control session ID is already registered");
    }
    sessions_.emplace(id, session);
}

void RegistryMgr::remove(SessionId id) noexcept
{
    for (auto entry = sessions_.begin(); entry != sessions_.end();)
    {
        if (entry->first != id && !entry->second.expired())
        {
            ++entry;
            continue;
        }
        services_.erase_secondary(entry->first);
        entry = sessions_.erase(entry);
    }
}

ControlSessionPtr RegistryMgr::find_session(SessionId id) const
{
    const auto entry = sessions_.find(id);
    return entry == sessions_.end() ? ControlSessionPtr{} : entry->second.lock();
}

RegistryMgr::Result RegistryMgr::register_service(SessionId id, const std::string &service, RelayProtocol protocol,
                                                  SRVTrafficPtr &traffic)
{
    traffic.reset();
    const auto session = sessions_.find(id);
    if (stopped_ || session == sessions_.end())
    {
        return Result::RegistryStopped;
    }

    if (const auto existing = services_.find_primary(service))
    {
        const auto owner = services_.secondary_key(service);
        if (owner && *owner == id && existing->protocol == protocol)
        {
            traffic = existing->traffic;
            return Result::Registered;
        }
        return Result::NameConflict;
    }

    if (services_.size() >= max_services_)
    {
        return Result::GlobalLimitReached;
    }
    if (services_.count_secondary(id) >= max_services_per_session_)
    {
        return Result::SessionLimitReached;
    }

    traffic = std::make_shared<ServiceTraffic>();
    if (!services_.insert(service, id, ServiceEntry{protocol, traffic}).second)
    {
        traffic.reset();
        return Result::NameConflict;
    }
    return Result::Registered;
}

std::optional<RegistryMgr::Service> RegistryMgr::find_service(const std::string &service)
{
    const auto registered = services_.find_primary(service);
    const auto owner = services_.secondary_key(service);
    if (!registered || !owner)
        return std::nullopt;
    const auto session_entry = sessions_.find(*owner);
    if (session_entry == sessions_.end())
        return std::nullopt;
    auto session = session_entry->second.lock();
    if (!session)
        return std::nullopt;
    return Service{std::move(session), registered->protocol, registered->traffic};
}

void RegistryMgr::sample_traffic()
{
    const auto now = ServiceTraffic::Clock::now();
    services_.for_each([now](const std::string &, ServiceEntry &service) { service.traffic->sample(now); });
}

njson RegistryMgr::traffic_report()
{
    njson report = njson::array();
    for (const auto &name : service_names())
    {
        const auto service = services_.find_primary(name);
        const auto owner = services_.secondary_key(name);
        const auto session = owner ? sessions_.find(*owner) : sessions_.end();
        if (!service || session == sessions_.end() || session->second.expired())
            continue;
        report.push_back(njson{{"service", name},
                               {"protocol", relay_protocol_name(service->protocol)},
                               {"rx_bytes", service->traffic->rx.total()},
                               {"tx_bytes", service->traffic->tx.total()},
                               {"rx_bytes_per_second", service->traffic->rx_bytes_per_second()},
                               {"tx_bytes_per_second", service->traffic->tx_bytes_per_second()},
                               {"accessors", service->traffic->accessors()}});
    }
    return report;
}

std::vector<std::string> RegistryMgr::service_names()
{
    std::vector<std::string> names;
    names.reserve(services_.size());
    services_.for_each([&](const std::string &name, ServiceEntry &) {
        const auto owner = services_.secondary_key(name);
        const auto session = owner ? sessions_.find(*owner) : sessions_.end();
        if (session != sessions_.end() && !session->second.expired())
            names.push_back(name);
    });
    std::ranges::sort(names);
    return names;
}

void RegistryMgr::stop()
{
    if (stopped_)
    {
        return;
    }
    stopped_ = true;

    for (const auto &[_, session_entry] : sessions_)
    {
        if (auto session = session_entry.lock())
        {
            asio::co_spawn(
                session->executor(), [session]() -> asio::awaitable<void> { co_await session->async_disconnect(); },
                asio::detached);
        }
    }
    services_.clear();
    sessions_.clear();
}
