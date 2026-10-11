#ifndef RELAYWEAVE_RELAY_AGENT_H
#define RELAYWEAVE_RELAY_AGENT_H

#include "agent_routing.h"
#include "async_event.h"
#include "dualindex_map.h"
#include "lru_cache.h"
#include "tls_channel.h"
#include <exception>
#include <functional>
#include <map>
#include <string_view>
#include <type_traits>

struct AgentServiceConfig
{
    std::string name;
    std::string target_host;
    std::uint16_t target_port = 0;
    RelayProtocol protocol = RelayProtocol::Tcp;
};

struct AgentForwardConfig
{
    std::string service;
    std::string listen_address;
    std::uint16_t listen_port = 0;
    RelayProtocol protocol = RelayProtocol::Tcp;
};

struct AgentConfig
{
    std::string host;
    std::uint16_t port = 0;
    std::filesystem::path ca_file;
    std::filesystem::path certificate_chain;
    std::filesystem::path private_key;
    std::optional<std::string> server_name;
    std::chrono::steady_clock::duration connect_timeout = std::chrono::seconds(5);
    std::chrono::steady_clock::duration reconnect_initial_delay = std::chrono::milliseconds(500);
    std::chrono::steady_clock::duration reconnect_max_delay = std::chrono::seconds(10);
    std::chrono::steady_clock::duration relay_open_timeout = std::chrono::seconds(10);
    std::vector<AgentServiceConfig> services;
    std::vector<AgentForwardConfig> forwards;
    TLSChannelConfig channel;
    std::size_t routing_max_nodes = 4;
};

AgentConfig load_agent_config(const std::filesystem::path &path);

class Forwarder;
class AgentSession;
class NodeConnection;

struct ServiceKey
{
    std::string service;
    RelayProtocol protocol;
    bool operator==(const ServiceKey &) const = default;
};

template <> struct std::hash<ServiceKey>
{
    std::size_t operator()(const ServiceKey &key) const
    {
        using ProtocolType = std::underlying_type_t<RelayProtocol>;
        auto result = std::hash<std::string>{}(key.service);
        const auto protocol = std::hash<ProtocolType>{}(static_cast<ProtocolType>(key.protocol));
        result ^= protocol + 0x9e3779b9U + (result << 6) + (result >> 2);
        return result;
    }
};

struct ServerRoute
{
    std::string id;
    std::string host;
    std::weak_ptr<TLSChannel> channel;
};

struct RelaySelection
{
    ServerRoute server;
    std::vector<std::string> path;
    std::uint64_t epoch = 0;
    std::shared_ptr<NodeConnection> connection;
};

class RelayAgent : public std::enable_shared_from_this<RelayAgent>
{
  public:
    RelayAgent(asio::io_context &control_io, asio::io_context &transfer_io, asio::ssl::context &ssl_context,
               AgentConfig config);

    void start();
    asio::awaitable<void> async_stop();

  private:
    friend class NodeConnection;
    friend class Forwarder;
    friend class AgentSession;
    static constexpr std::size_t MAX_LOGGED_CANDIDATES = 3;
    static constexpr std::size_t PATH_CACHE_CAPACITY = 16;
    static constexpr auto PATH_CACHE_TTL = std::chrono::seconds(15);
    static constexpr auto CONNECTION_IDLE_TIMEOUT = std::chrono::seconds(60);

    enum class State
    {
        Created,
        Running,
        StoppingControl,
        StoppingForwarder,
        Stopped,
    };

    asio::awaitable<void> discovery_loop();
    std::shared_ptr<NodeConnection> ensure_connection(std::string node_id, std::string host, std::uint16_t port);
    void connection_ready(NodeConnection &connection, const std::shared_ptr<TLSChannel> &channel);
    void connection_closed(const NodeConnection &connection);
    void connection_finished(NodeConnection &connection, std::exception_ptr failure);
    void collect_idle_connections(std::chrono::steady_clock::time_point now);
    void spawn_control_task(asio::awaitable<void> task, std::string_view context, NodeConnection *connection = nullptr);
    void query_services(bool refresh = false);
    void query_topology();
    void update_probe_targets();
    std::vector<std::string> calculate_service_paths(const ServiceKey &service, const std::string &destination);
    asio::awaitable<RelaySelection> select_relay(ServiceKey service, std::string destination,
                                                 std::chrono::steady_clock::time_point deadline);
    void invalidate_entries(std::string reason, const std::optional<ServiceKey> &service = std::nullopt);
    struct EntryWait
    {
        EntryWait(asio::any_io_executor executor, ServiceKey service, std::string node,
                  std::chrono::steady_clock::time_point deadline);
        asio::steady_timer changed;
        ServiceKey service;
        std::string node;
        std::chrono::steady_clock::time_point deadline;
        struct Location
        {
            std::string address;
            std::uint16_t port;
        };
        std::optional<Location> location;
        std::shared_ptr<NodeConnection> connection;
        std::string reason;
    };
    std::map<std::uint64_t, std::shared_ptr<EntryWait>> entry_waits_;
    void locate_service(const njson &params);
    void forget_service(const ServiceKey &service, std::string reason);
    void handle_control_message(const NodeConnection &connection, CtrlMessage message);
    std::uint64_t allocate_request_id();
    asio::awaitable<void> stop_on_control_executor();
    void try_stop_forwarder();
    void complete_stop() noexcept;

    asio::any_io_executor control_executor_;
    asio::any_io_executor transfer_executor_;
    asio::ssl::context &ssl_context_;
    AgentConfig config_;
    AgentRouting routing_;
    // Confined to control_executor_; TTL is independent of topology/probe validity.
    LRUCache<std::string, std::vector<RouteGraph::Path>> path_cache_{PATH_CACHE_CAPACITY, PATH_CACHE_TTL};
    asio::steady_timer discovery_timer_;
    std::shared_ptr<Forwarder> forwarder_;
    std::string primary_connection_id_;
    DualIndexMap<std::string, std::string, std::shared_ptr<NodeConnection>, SecondaryKeyMode::Unique> connections_;
    struct ServiceLocation
    {
        std::uint64_t request = 0;
        std::string address;
        std::uint16_t port = 0;
    };
    // The secondary key is the service Node ID, independent of control connections.
    DualIndexMap<ServiceKey, std::string, ServiceLocation> service_locations_;
    std::uint64_t next_request_id_ = 1;
    std::size_t active_tasks_ = 0;
    AsyncEvent stopped_event_;
    State state_ = State::Created;
};

#endif // RELAYWEAVE_RELAY_AGENT_H
