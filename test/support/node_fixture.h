#ifndef RELAYWEAVE_TEST_NODE_FIXTURE_H
#define RELAYWEAVE_TEST_NODE_FIXTURE_H

#include "node_test_config.h"
#include "member_access.h"

TEST_MEMBER(RelayNode, nodelink_mgr_)
TEST_MEMBER(RelayNode, cluster_mgr_)
TEST_MEMBER(NodeLinkMgr, link_requests_)

namespace test_node
{
inline NodeLinkMgr &links(const std::shared_ptr<RelayNode> &node)
{
    return *test_access::RelayNode_nodelink_mgr_(*node);
}
inline ClusterMgr &cluster(const std::shared_ptr<RelayNode> &node)
{
    return *test_access::RelayNode_cluster_mgr_(*node);
}
// Scenarios run on control_io, like the production NodeSession callers.
inline asio::awaitable<LinkResult> ensure_link(std::shared_ptr<RelayNode> node, std::string left,
                                               std::string right, RelayProtocol transport)
{
    co_return co_await links(node).ensure_link(std::move(left), std::move(right), transport);
}
inline asio::awaitable<FlowResult> open_flow(std::shared_ptr<RelayNode> node, std::vector<std::string> path,
                                            RelayProtocol transport)
{
    co_return co_await links(node).open_flow(std::move(path), transport);
}
inline asio::awaitable<void> close_flow(std::shared_ptr<RelayNode> node, std::uint64_t epoch, std::uint64_t id)
{
    co_await links(node).close_flow(epoch, id);
}
inline asio::awaitable<FlowSendStatus> send_flow(std::shared_ptr<RelayNode> node, FlowFrame frame)
{
    co_return co_await links(node).channel().async_send_flow(std::move(frame));
}
inline asio::awaitable<FlowFrame> receive_flow(std::shared_ptr<RelayNode> node, std::uint64_t epoch, std::uint64_t id)
{
    co_return co_await links(node).channel().receive_flow(epoch, id);
}
inline asio::awaitable<void> close_link(std::shared_ptr<RelayNode> node, std::uint64_t id)
{
    // Inject the same authenticated endpoint notification as a closed data socket.
    auto &manager = links(node);
    for (const auto &[_, request] : test_access::NodeLinkMgr_link_requests_(manager))
    {
        if (request->params.at("id") == id)
        {
            auto params = request->params;
            params["source"] = params.at("left");
            manager.handle(CtrlMessage(CtrlCommand::LinkClosed, std::move(params)));
            break;
        }
    }
    co_return;
}
} // namespace test_node
#endif
