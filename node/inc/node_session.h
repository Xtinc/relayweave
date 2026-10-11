#ifndef RELAYWEAVE_NODE_SESSION_H
#define RELAYWEAVE_NODE_SESSION_H

#include "control_router_single.h"
#include "control_router_multi.h"
#include "pipeline_mgr.h"
#include "datagram_mgr.h"
#include <variant>

// One Node business instance; control state stays on control_io, data stays with its manager.
class NodeSession
{
  public:
    NodeSession(RelayNode &node, const ControlSessionPtr &session, njson params, bool ingress, SRVTrafficPtr traffic = {});
    NodeSession(RelayNode &node, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                njson params, SRVTrafficPtr traffic);
    asio::awaitable<void> run();
    void cancel(std::string stage, std::string reason);
    bool belongs_to(const ControlSessionPtr &session) const;
    bool handle(const ControlSessionPtr &session, const CtrlMessage &message);

  private:
    friend class RelayNode;
    friend class ControlRouterSingle;
    friend class ControlRouterMulti;
    // Called on control_io; data operations cross to the selected manager's executor.
    asio::awaitable<void> install(int role = RelayAttach::Consumer);
    asio::awaitable<bool> wait_attach(std::chrono::steady_clock::time_point deadline);
    asio::awaitable<void> bind(std::string accessor, std::uint64_t epoch = 0, std::uint64_t flow_id = 0);
    asio::awaitable<void> activate();
    asio::awaitable<void> bridge();
    asio::awaitable<void> close();
    std::uint64_t uuid() const;
    RelayNode &node_;
    std::variant<ControlRouterSingle, ControlRouterMulti> control_;
    std::variant<TcpPipeline *, TlsPipeline *, DatagramMgr *> data_;
    void select_data(RelayProtocol protocol);
    asio::any_io_executor data_executor() const;
    asio::cancellation_signal cancellation_;
    njson endpoint_;
    SRVTrafficPtr traffic_;
};

#endif
