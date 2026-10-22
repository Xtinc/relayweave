#ifndef RELAYWEAVE_CONTROL_ROUTER_SINGLE_H
#define RELAYWEAVE_CONTROL_ROUTER_SINGLE_H

#include "tls_channel.h"

class NodeSession;

/**
 * @brief Coordinate both Agent roles for one single-node relay on control_io.
 * @details The owning NodeSession supplies data operations. Control sessions are borrowed;
 * cancellation stops progression, while run() still drains data tasks and releases resources.
 */
class ControlRouterSingle
{
  public:
    /**
     * @brief Borrow both control sessions and derive the shared setup deadline from Node configuration.
     * @param relay Owning NodeSession that provides data operations and cancellation.
     * @param consumer Consumer Agent control session; borrowed until the relay task drains.
     * @param producer Producer Agent control session; borrowed until the relay task drains.
     * @param params Single-node relay request containing service, protocol, and the Consumer request ID.
     */
    ControlRouterSingle(NodeSession &relay, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                        njson params);
    /** @brief Attach both roles, bind and activate data, notify ready, then drain and close. */
    asio::awaitable<void> run();
    /**
     * @brief Request idempotent cancellation; stage is accepted for the shared controller interface.
     * @param stage Accepted for the common controller interface; unused by single-node cancellation.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     */
    void cancel(std::string stage, std::string reason);
    /**
     * @brief Cancel when either borrowed Agent control session disconnects.
     * @param session Agent control session whose identity is checked before applying the message.
     */
    void control_disconnected(const ControlSessionPtr &session);
    /**
     * @brief Return whether an Agent message matches this relay and is consumed.
     * @param command Control command already parsed by the caller.
     * @param session Agent control session whose identity is checked before applying the message.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True if the message belongs to this instance and is consumed, including an ignored matching
     * message.
     */
    bool handle(CtrlCommand command, const ControlSessionPtr &session, const CtrlMessage &message);
    /**
     * @brief Return ready relay metadata, or no report while opening or closing.
     * @return Metadata for a ready, live relay; std::nullopt while opening, closing, or lacking its control
     * session.
     */
    std::optional<njson> status_report() const;

  private:
    bool belongs_to(const ControlSessionPtr &session) const;
    bool matches(const ControlSessionPtr &session, const njson &params) const;
    void notify_agents() const;
    /** @brief Send one ready notification when both roles share the same control connection. */
    void ready_agents() const;
    /** @brief Notify setup failures and UDP closure; ready streams carry normal EOF through data sockets. */
    void close_agents() const;
    NodeSession &relay_;
    std::weak_ptr<ControlSession> consumer_;
    std::weak_ptr<ControlSession> producer_;
    njson params_;
    std::chrono::steady_clock::time_point deadline_;
    std::string reason_;
    bool ready_ = false;
    bool stop_requested_ = false;
};

#endif
