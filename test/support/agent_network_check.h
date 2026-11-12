#ifndef RELAYWEAVE_TEST_AGENT_NETWORK_CHECK_H
#define RELAYWEAVE_TEST_AGENT_NETWORK_CHECK_H

#include "agent_session.h"
#include "forwarder.h"
#include "member_access.h"

TEST_MEMBER(RelayAgent, forwarder_)
TEST_MEMBER(Forwarder, relays_)
TEST_MEMBER(AgentSession, data)

namespace test_agent
{
/**
 * @brief Verify real data sockets use the expected Node interface on transfer_io.
 * @param agent Agent with live relays retained by the caller.
 * @param expected Expected remote IPv4 address for all currently connected data sockets.
 * @return Completes after observing at least one connected socket; throws on mismatched or missing sockets.
 */
inline asio::awaitable<void> check_network(RelayAgent &agent, std::string expected)
{
    std::size_t connected = 0;
    for (const auto &[id, session] : test_access::Forwarder_relays_(*test_access::RelayAgent_forwarder_(agent)))
    {
        std::visit([&](auto &sockets) {
            asio::error_code error;
            std::string address;
            if (sockets.transfer.is_open())
            {
                address = sockets.transfer.remote_endpoint(error).address().to_string();
            }
            else if constexpr (requires { sockets.tls; })
            {
                if (sockets.tls)
                {
                    address = sockets.tls->lowest_layer().remote_endpoint(error).address().to_string();
                }
            }
            if (!address.empty() && !error)
            {
                if (address != expected)
                {
                    throw std::runtime_error("Agent connected to " + address + " instead of " + expected);
                }
                ++connected;
            }
        }, test_access::AgentSession_data(*session));
    }
    if (connected == 0)
    {
        throw std::runtime_error("No connected Agent data socket was observed");
    }
    co_return;
}
} // namespace test_agent

#endif
