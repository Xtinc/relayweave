#ifndef RELAYWEAVE_TEST_FLOW_STATE_H
#define RELAYWEAVE_TEST_FLOW_STATE_H

#include "flow_protocol.h"
#include "member_access.h"

TEST_MEMBER(FlowProtocol, flows_)

// Fixed protocol expectations, independent of private implementation constants.
namespace test_flow
{
inline constexpr std::size_t window_bytes = 512 * 1024;
inline constexpr std::size_t buffer_budget_bytes = 256 * 1024 * 1024;
}

namespace test_access
{
// Read on the data executor, or while a manually pumped fixture is idle.
inline std::size_t flow_buffered_bytes(const FlowProtocol &channel)
{
    std::size_t total = 0;
    for (const auto &[id, flow] : FlowProtocol_flows_(channel))
    {
        if (flow->transport == RelayProtocol::Tcp)
        {
            total += flow->stream->directions[0].buffered_bytes + flow->stream->directions[1].buffered_bytes;
        }
    }
    return total;
}
} // namespace test_access

#endif
