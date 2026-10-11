#ifndef PROXY_REGISTRY_MGR_H
#define PROXY_REGISTRY_MGR_H

#include "app_common.h"
#include "dualindex_map.h"
#include "tls_channel.h"
#include <map>
#include <vector>

class RegistryMgr
{
  public:
    using SessionId = std::uint64_t;
    enum class Result
    {
        Registered,
        NameConflict,
        SessionLimitReached,
        GlobalLimitReached,
        RegistryStopped,
    };

    struct Service
    {
        ControlSessionPtr session;
        RelayProtocol protocol;
        SRVTrafficPtr traffic;
    };

    static std::string_view result2string(Result result);

    RegistryMgr(std::size_t max_sessions, std::size_t max_services, std::size_t max_services_per_session);

    bool full() noexcept;
    std::size_t session_count() noexcept;
    bool contains(SessionId id) noexcept;
    // The control task retains the session until remove(id); the registry only borrows it.
    void add(SessionId id, const ControlSessionPtr &session);
    void remove(SessionId id) noexcept;
    ControlSessionPtr find_session(SessionId id) const;

    Result register_service(SessionId id, const std::string &service, RelayProtocol protocol);
    std::optional<Service> find_service(const std::string &service);
    std::vector<std::string> service_names();
    void sample_traffic();
    njson traffic_report();

    void stop();

  private:
    struct ServiceEntry
    {
        RelayProtocol protocol;
        SRVTrafficPtr traffic;
    };

    std::size_t max_sessions_;
    std::size_t max_services_;
    std::size_t max_services_per_session_;
    std::map<SessionId, std::weak_ptr<ControlSession>> sessions_;
    DualIndexMap<std::string, SessionId, ServiceEntry> services_;
    bool stopped_ = false;
};

#endif // PROXY_REGISTRY_MGR_H
