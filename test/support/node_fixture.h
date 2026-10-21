#ifndef RELAYWEAVE_TEST_NODE_FIXTURE_H
#define RELAYWEAVE_TEST_NODE_FIXTURE_H

#include "node_test_config.h"
#include "node_control.h"
#include "member_access.h"
#include "flow_test_io.h"

TEST_MEMBER(RelayNode, nodelink_mgr_)
TEST_MEMBER(RelayNode, cluster_mgr_)
TEST_MEMBER(RelayNode, metrics_)
TEST_MEMBER(NodeLinkMgr, link_requests_)

namespace test_node
{
inline NodeMetrics &metrics(const std::shared_ptr<RelayNode> &node)
{
    return *test_access::RelayNode_metrics_(*node);
}
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
inline asio::awaitable<bool> send_data(std::shared_ptr<RelayNode> node, std::uint64_t epoch,
                                      std::uint64_t id, bool reverse, BytesBuf payload)
{
    co_return co_await test_flow::send_data(links(node).protocol().shared_from_this(), epoch, id, reverse,
                                          std::move(payload));
}
inline asio::awaitable<test_flow::Port> port(std::shared_ptr<RelayNode> node, std::uint64_t epoch,
                                          std::uint64_t id, bool reverse)
{
    co_return co_await test_flow::port(links(node).protocol().shared_from_this(), epoch, id, reverse);
}
inline asio::awaitable<std::optional<BytesBuf>> receive_flow(std::shared_ptr<RelayNode> node, std::uint64_t epoch,
                                                           std::uint64_t id)
{
    co_return co_await test_flow::receive(links(node).protocol().shared_from_this(), epoch, id);
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
