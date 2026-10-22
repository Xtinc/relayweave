#ifndef RELAYWEAVE_AGENT_COMMON_H
#define RELAYWEAVE_AGENT_COMMON_H

#include "tls_channel.h"
#include <functional>
#include <type_traits>

class NodeConnection;

/** @brief Identify a service by name and Agent-facing transport. */
struct ServiceKey
{
    std::string service;
    RelayProtocol protocol;
    bool operator==(const ServiceKey &) const = default;
};

/** @brief Hash both service identity fields for Agent lookup containers. */
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

/** @brief Copy an entrance endpoint and weak control channel across execution domains. */
struct ServerRoute
{
    std::string id;
    std::string host;
    std::weak_ptr<TLSChannel> channel;
};

/** @brief Carry one selected path, epoch, and shared entrance lifetime into a consumer session. */
struct RelaySelection
{
    ServerRoute server;
    std::vector<std::string> path;
    std::uint64_t epoch = 0;
    std::shared_ptr<NodeConnection> connection;
};

#endif
