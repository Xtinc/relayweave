#ifndef RELAYWEAVE_NODE_SESSION_H
#define RELAYWEAVE_NODE_SESSION_H

#include "control_router_single.h"
#include "control_router_multi.h"
#include "pipeline_mgr.h"
#include "datagram_mgr.h"
#include <memory>
#include <variant>
#include <vector>

class RelayNode;

/**
 * @brief Own one relay controller and dispatch its data operations to the selected manager.
 * @details All public entry points require control_io. The controller is stored by value;
 * data resources belong to the transfer manager. run() drains children before the Node
 * removes this instance, including after cancellation or a matching peer close.
 */
class NodeSession
{
  public:
    /**
     * @brief Construct a multi-node ingress or egress instance using validated relay parameters.
     * @param node Owning Node, which must outlive this business instance.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param params Relay request parameters containing service, protocol, request identity, and multi-node
     * path/Flow identity when applicable.
     * @param ingress True for the first Node on the selected path; false for the last Node.
     * @param traffic Local service accounting record; ingress instances may omit it when the service is
     * remote.
     */
    NodeSession(RelayNode &node, const ControlSessionPtr &session, njson params, bool ingress, SRVTrafficPtr traffic = {});
    /**
     * @brief Construct a single-node instance borrowing both Agent control sessions.
     * @param node Owning Node, which must outlive this business instance.
     * @param consumer Consumer Agent control session; borrowed until the relay task drains.
     * @param producer Producer Agent control session; borrowed until the relay task drains.
     * @param params Relay request parameters containing service, protocol, request identity, and multi-node
     * path/Flow identity when applicable.
     * @param traffic Local service accounting record; ingress instances may omit it when the service is
     * remote.
     */
    NodeSession(RelayNode &node, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                njson params, SRVTrafficPtr traffic);
    /** @brief Run the controller through setup, transfer, and cancellation-safe resource cleanup. */
    asio::awaitable<void> run();
    /**
     * @brief Idempotently request cancellation with the failing stage and diagnostic reason.
     * @param stage Setup or control stage associated with the failure.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     */
    void cancel(std::string stage, std::string reason);
    /**
     * @brief Cancel if the disconnected control session belongs to this instance.
     * @param session Agent control session whose identity is checked before applying the message.
     */
    void control_disconnected(const ControlSessionPtr &session);
    /**
     * @brief Invalidate a multi-node instance when its epoch or path members become stale.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     */
    void invalidate(std::uint64_t epoch, std::string reason);
    /**
     * @brief Consume a matching Agent message using the command already parsed by RelayNode.
     * @param command Control command already parsed by the caller.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True if the message belongs to this instance and is consumed, including an ignored matching
     * message.
     */
    bool handle(CtrlCommand command, const ControlSessionPtr &session, const CtrlMessage &message);
    /**
     * @brief Consume matching peer messages until this instance's task has fully drained.
     * @param command Control command already parsed by the caller.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True if the authenticated source, epoch, and Flow identity match this multi-node instance;
     * closing instances still consume matches.
     */
    bool handle_peer(CtrlCommand command, const CtrlMessage &message);
    /**
     * @brief Return the Agent-facing transport selected at construction.
     * @return Agent-facing transport selected at construction: TCP, TLS, or UDP.
     */
    RelayProtocol protocol() const noexcept;
    /**
     * @brief Return whether this instance uses a multi-node controller.
     * @return True when this instance owns ControlRouterMulti rather than ControlRouterSingle.
     */
    bool is_multi() const noexcept;
    /**
     * @brief Return the cancellation slot used by the Node-owned business task.
     * @return Slot connected to the business task owned and drained by RelayNode.
     */
    asio::cancellation_slot cancellation_slot() noexcept;
    /**
     * @brief Return ready business metadata without reading state owned by a data executor.
     * @return Metadata for a ready, live relay; std::nullopt while opening, closing, or lacking its control
     * session.
     */
    std::optional<njson> status_report() const;

  private:
    friend class ControlRouterSingle;
    friend class ControlRouterMulti;
    /** @brief Install a local pair or remote endpoint on its transfer executor from control_io. */
    asio::awaitable<void> install(int role = RelayAttach::Consumer);
    /** @brief Await attachment on the transfer executor with the controller's absolute deadline. */
    asio::awaitable<bool> wait_attach(std::chrono::steady_clock::time_point deadline);
    /** @brief Copy binding metadata across executors and bind a service or committed Flow endpoint. */
    asio::awaitable<void> bind(std::string accessor, std::uint64_t epoch = 0, std::uint64_t flow_id = 0);
    /** @brief Activate the installed resource on its owning transfer executor. */
    asio::awaitable<void> activate();
    /** @brief Await the manager-owned transfer operation on its transfer executor. */
    asio::awaitable<void> bridge();
    /** @brief Release the installed resource on its transfer executor after data tasks drain. */
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

using NodeSessionList = std::vector<std::shared_ptr<NodeSession>>;

#endif
