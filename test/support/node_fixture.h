#ifndef RELAYWEAVE_TEST_NODE_FIXTURE_H
#define RELAYWEAVE_TEST_NODE_FIXTURE_H

#include "node_test_config.h"
#include "node_control.h"
#include "member_access.h"
#include "flow_test_io.h"

TEST_MEMBER(RelayNode, nodelink_mgr_)
TEST_MEMBER(RelayNode, cluster_mgr_)
TEST_MEMBER(RelayNode, metrics_)
TEST_MEMBER(NodeLinkMgr, shared_links_)
TEST_MEMBER(NodeLinkMgr, start_link)

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
/** @brief Snapshot one Link attempt for tests without adding a production waiting API. */
struct LinkResult
{
    std::uint64_t id = 0;
    std::string stage;
    std::string reason;
    /**
     * @brief Report whether the observed Link completed successfully.
     * @return True for a nonzero identity without a failure reason.
     */
    explicit operator bool() const noexcept
    {
        return id != 0 && reason.empty();
    }
};

/**
 * @brief Start or reuse a Link and observe its completion on control_io for tests.
 * @param node Node retained until observation ends.
 * @param left First adjacent Node identity.
 * @param right Second adjacent Node identity; order is normalized by the production starter.
 * @param transport TCP or UDP transport to observe.
 * @return Observed Link identity, stage and reason; invalid start requests return an ensure failure.
 * @details The bounded test-only poll leaves production free of Link waiter events and coroutine wrappers.
 */
inline asio::awaitable<LinkResult> ensure_link(std::shared_ptr<RelayNode> node, std::string left,
                                               std::string right, RelayProtocol transport)
{
    using Request = decltype(test_access::NodeLinkMgr_start_link(links(node), left, right, transport));
    Request request;
    try
    {
        request = test_access::NodeLinkMgr_start_link(links(node), std::move(left), std::move(right), transport);
    }
    catch (const std::exception &error)
    {
        co_return LinkResult{0, "ensure", error.what()};
    }
    asio::steady_timer changed(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (!request->completed)
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            throw std::runtime_error("Link completion exceeded the test deadline");
        }
        changed.expires_after(std::chrono::milliseconds(1));
        co_await changed.async_wait(asio::use_awaitable);
    }
    co_return LinkResult{request->params.id, request->stage, request->reason};
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
    for (const auto &[_, request] : test_access::NodeLinkMgr_shared_links_(manager))
    {
        if (request->params.id == id)
        {
            auto params = request->params.to_params();
            params["source"] = request->params.left;
            manager.handle(CtrlCommand::LinkClosed, CtrlMessage(CtrlCommand::LinkClosed, std::move(params)));
            break;
        }
    }
    co_return;
}
} // namespace test_node
#endif
