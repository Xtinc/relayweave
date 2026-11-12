#ifndef RELAYWEAVE_NODE_ADDRESS_FIXTURE_H
#define RELAYWEAVE_NODE_ADDRESS_FIXTURE_H

#include "message.h"
#include <initializer_list>
#include <utility>

inline NodeAddress test_node_address(std::initializer_list<NodeAddress::Entry> entries)
{
    auto ports = njson::array();
    for (const auto &entry : entries)
    {
        ports.push_back({{"label", entry.label}, {"address", entry.address}});
    }
    return NodeAddress::from_params(njson{{"network_ports", std::move(ports)}}, "test");
}

inline NodeAddress test_node_address(std::string address)
{
    return test_node_address({{"default", std::move(address)}});
}

#endif
