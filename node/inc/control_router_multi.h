#ifndef RELAYWEAVE_CONTROL_ROUTER_MULTI_H
#define RELAYWEAVE_CONTROL_ROUTER_MULTI_H

#include "node_common.h"

class NodeSession;

/**
 * @brief Coordinate one ingress or egress relay instance and its peer on control_io.
 * @details Owned by NodeSession; borrows the local Agent control session and uses cluster
 * unicast for peer coordination. run() drains transfer and Flow-watch children before cleanup.
 * A closing instance keeps consuming its peer identity until that task has drained.
 */
class ControlRouterMulti
{
  public:
    /**
     * @brief Construct an ingress request or an egress instance for an already committed Flow.
     * @param relay Owning NodeSession that provides data operations and cancellation.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param params Ingress request with epoch and path, or egress peer-open parameters with source and Flow
     * ID.
     * @param ingress True for the first Node on the selected path; false for the last Node.
     */
    ControlRouterMulti(NodeSession &relay, const AgentPeer &session, njson params, bool ingress);
    /** @brief Establish and transfer alongside a Flow watcher, then drain and release resources. */
    asio::awaitable<void> run();
    /**
     * @brief Consume an Agent message matching the local control session and relay identity.
     * @param command Control command already parsed by the caller.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True if the message belongs to this instance and is consumed, including an ignored matching
     * message.
     */
    bool handle(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message);
    /**
     * @brief Consume matching authenticated peer messages, including duplicates during cleanup.
     * @param command Control command already parsed by the caller.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True if the authenticated source, epoch, and Flow identity match this multi-node instance;
     * closing instances still consume matches.
     */
    bool handle_peer(CtrlCommand command, const CtrlMessage &message);
    /**
     * @brief Cancel when the Agent control session disconnects.
     * @param session Agent control session whose identity is checked before applying the message.
     */
    void control_disconnected(const AgentPeer &session);
    /**
     * @brief Cancel on Flow authorization loss or Agent primary departure.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     */
    void invalidate(std::uint64_t epoch, std::string reason);
    /**
     * @brief Request cancellation and retain whether the peer initiated it for close notification.
     * @param stage Setup or control stage associated with the failure.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     * @param from_peer True if peer notification initiated cancellation, suppressing a redundant close reply.
     */
    void cancel(std::string stage, std::string reason, bool from_peer = false);
    /**
     * @brief Return ready ingress or egress metadata, or no report while opening or closing.
     * @return Metadata for a ready, live relay; std::nullopt while opening, closing, or lacking its control
     * session.
     */
    std::optional<njson> status_report() const;

  private:
    enum class Progress
    {
        OpeningFlow,
        PeerOpened,
        AgentNotified,
        Ready
    };

    /** @brief Establish on control_io and bridge data; ingress waits for both FIN directions before Flow close. */
    asio::awaitable<void> establish_and_transfer();
    bool matches(const AgentPeer &session, const njson &params) const;
    /** @brief Await Flow closure on control_io so the parallel group cancels and drains transfer. */
    asio::awaitable<void> watch_flow();
    void notify_agent() const;
    void close_agent() const;
    void ready_agent();
    void send_peer(CtrlCommand command, njson params) const;
    NodeSession &relay_;
    AgentPeer session_;
    njson params_;
    std::uint64_t epoch_ = 0;
    std::uint64_t flow_id_ = 0;
    std::string peer_;
    std::chrono::steady_clock::time_point deadline_;
    asio::steady_timer changed_;
    std::string stage_ = "open_flow";
    std::string reason_;
    Progress progress_ = Progress::OpeningFlow;
    bool ingress_;
    bool peer_prepared_ = false;
    bool peer_finished_ = false;
    bool stop_requested_ = false;
    bool from_peer_ = false;
};

#endif
