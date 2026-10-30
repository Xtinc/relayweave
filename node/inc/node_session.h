#ifndef RELAYWEAVE_NODE_SESSION_H
#define RELAYWEAVE_NODE_SESSION_H

#include "control_router_multi.h"
#include "control_router_single.h"
#include "datagram_mgr.h"
#include "pipeline_mgr.h"
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
    NodeSession(RelayNode &node, const AgentPeer &session, RelayRequest params, bool ingress,
                SRVTrafficPtr traffic = {});
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
    NodeSession(RelayNode &node, const AgentPeer &consumer, const AgentPeer &producer, RelayRequest params,
                SRVTrafficPtr traffic);
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
    void control_disconnected(const AgentPeer &session);
    /**
     * @brief Invalidate Multi paths or remote Agent peers after cluster membership or control loss.
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
    bool handle(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message);
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
    using Attachment = std::variant<std::monostate, AttachInfo, AttachPair>;
    /** @brief Install a local pair or remote endpoint on its transfer executor from control_io. */
    asio::awaitable<void> install(int role = RelayAttach::Consumer);
    /** @brief Await Single attachment, bind, and activate in one transfer-executor task within the deadline. */
    asio::awaitable<void> prepare_local(std::chrono::steady_clock::time_point deadline, std::string accessor);
    /** @brief Await Multi attachment and bind in one transfer-executor task; failures propagate directly. */
    asio::awaitable<void> prepare_remote(std::chrono::steady_clock::time_point deadline, std::string accessor,
                                         std::uint64_t epoch, std::uint64_t flow_id);
    /** @brief Activate the Multi endpoint on its transfer executor after peer coordination. */
    asio::awaitable<void> activate();
    /** @brief Await the manager-owned transfer operation on its transfer executor. */
    asio::awaitable<void> bridge();
    /** @brief Release the installed resource on its transfer executor after data tasks drain. */
    asio::awaitable<void> close();
    /** @brief Notify an Agent through its primary Node; throw if local submission fails. */
    void send_agent(const AgentPeer &route, CtrlMessage message) const;
    std::uint64_t uuid() const;
    RelayNode &node_;
    using DataManager = std::variant<TcpPipeline *, TlsPipeline *, DatagramMgr *>;
    DataManager data_manager_;
    std::variant<ControlRouterSingle, ControlRouterMulti> control_;
    DataManager select_data(RelayProtocol protocol) const;
    asio::any_io_executor data_executor() const;
    asio::cancellation_signal cancellation_;
    Attachment endpoint_;
    SRVTrafficPtr traffic_;
};

using NodeSessionList = std::vector<std::shared_ptr<NodeSession>>;

#endif
