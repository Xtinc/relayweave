#ifndef RELAYWEAVE_NODE_SESSION_H
#define RELAYWEAVE_NODE_SESSION_H

#include "node_common.h"
#include <array>
#include <asio/steady_timer.hpp>
#include <exception>
#include <memory>
#include <optional>
#include <vector>

class RelayNode;

/**
 * @brief Own one data task on the borrowed transport's fixed executor.
 * @details Public operations require control_io. Stage waits are sequential: installed(), prepared(),
 * activate(), transfer(). Creating their awaitables does not advance the task. Setup waits share the
 * original deadline; data failures and cancellation propagate through the stage waits.
 * NodeSession shares ownership with the native completion handler and any posted stop request.
 * The borrowed transport and optional FlowProtocol must outlive the data task; late completion and
 * stop handlers retain only this object and do not access those dependencies.
 */
class SessionTransfer : public std::enable_shared_from_this<SessionTransfer>
{
  public:
    /**
     * @brief Carry one-time initialization and borrowed Flow identity into the data task.
     * @details deadline is an absolute steady-clock setup deadline; traffic and accessor move into
     * the pair during preparation. A null flow_protocol selects a local pair. Otherwise the borrowed
     * protocol must outlive the task, epoch/flow_id identify an installed Flow, and role is Consumer or Producer.
     */
    struct Setup
    {
        std::chrono::steady_clock::time_point deadline;
        SRVTrafficPtr traffic;
        std::string accessor;
        FlowProtocol *flow_protocol = nullptr;
        std::uint64_t epoch = 0;
        std::uint64_t flow_id = 0;
        int role = RelayAttach::Consumer;
    };

    /**
     * @brief Construct a transfer task without starting resource installation.
     * @param control Executor for public operations and data-task completion notifications.
     * @param transport Node-owned transport borrowed until the data task completes.
     * @param setup Initialization moved into the task; contains an optional borrowed protocol.
     */
    SessionTransfer(asio::any_io_executor control, RelayTransport &transport, Setup setup);
    /**
     * @brief Launch the only data task from control_io.
     * @details Native completion confirms exit even when cancellation prevents entry into run().
     * @pre Shared ownership is established; call exactly once before stage waits or attach().
     */
    void start();
    /**
     * @brief Await installed attachment credentials on control_io.
     * @return Local AttachPair or remote AttachInfo; throws on installation failure, timeout or cancellation.
     */
    asio::awaitable<AttachResult> installed();
    /**
     * @brief Await completed attachment, accounting preparation and any remote Flow binding on control_io.
     * @return Completes when preparation succeeds; throws on data failure, setup timeout or cancellation.
     */
    asio::awaitable<void> prepared();
    /**
     * @brief Permit activation and await its confirmation on control_io before announcing relay.ready.
     * @return Completes when the endpoint is active; throws on data failure, setup timeout or cancellation.
     */
    asio::awaitable<void> activate();
    /**
     * @brief Permit forwarding after relay.ready and await its completion on control_io.
     * @return Completes when forwarding finishes normally; throws on data failure or cancellation.
     * @details The endpoint remains installed until stop() so peer completion can finish first.
     */
    asio::awaitable<void> transfer();
    /**
     * @brief Submit one validated stream authorization per role on control_io.
     * @param request Copied authorization with a valid ticket, Consumer/Producer role and present connection_id.
     * @details Identical repeats are ignored; authorizations are ignored once stopping or stopped.
     * @throws std::invalid_argument A different connection was already authorized for the same role.
     */
    void attach(const RelayAttach &request);
    /**
     * @brief Interrupt data work and await native task completion on control_io.
     * @return Completes after resource cleanup, or immediately if never started or already stopped.
     * @details Drains earlier notifications without rethrowing data failures; stage waits report those failures.
     * The stop request emits cancellation only on the transfer executor. Repeated calls are idempotent.
     * @pre Stage waits have finished or been cancelled and drained; the caller has disabled cancellation.
     */
    asio::awaitable<void> stop();

  private:
    /** @brief Identify data-to-control notifications independently of stored task lifetime. */
    enum class Event
    {
        Installed,
        Prepared,
        Activated,
        Finished,
        Stopped
    };
    /** @brief Track task lifetime exclusively on control_io. */
    enum class State
    {
        Created,
        Running,
        Stopping,
        Stopped
    };
    asio::awaitable<void> run();
    /** @brief Send any continuation only when awaited, then receive the expected event or failure on control_io. */
    asio::awaitable<void> wait(Event expected);
    /** @brief Publish each event at most once from the transfer executor to control_io. */
    void publish(Event event, AttachResult attachment = {});
    bool remote() const noexcept
    {
        return setup_.flow_protocol != nullptr;
    }

    asio::any_io_executor executor_;
    RelayTransport &transport_;
    Setup setup_;
    asio::experimental::concurrent_channel<void(std::exception_ptr, Event, AttachResult)> events_;
    RelayTransport::Authorizations authorizations_;
    asio::experimental::concurrent_channel<void(asio::error_code)> proceed_;

    std::array<std::optional<RelayAttach::ConnectionId>, 2> authorized_;
    State state_ = State::Created;
    asio::cancellation_signal cancellation_;
};

/**
 * @brief Hold common relay state, root cancellation and the independent data task on control_io.
 * @details RelayNode uniquely owns the concrete session until run() drains its work. The Node and its
 * transport/protocol services outlive that run. Agent routes are weak or remote identities, not owners.
 * Historical progress remains available during cleanup. A present close_reason_ marks closing and
 * retains the first cause, including an empty string; cleanup supplies a cause for normal completion.
 */
class NodeSession
{
  public:
    /** @brief Destroy an unstarted session or a session whose run() has completed. */
    virtual ~NodeSession() = default;
    /**
     * @brief Run establishment, forwarding, failure handling and cleanup on control_io.
     * @return Completes after the data task and native child operations have drained and Flow cleanup finishes.
     * @details Records the first close cause and disables cancellation during cleanup. The owning root
     * co_spawn binds cancellation_slot() to support cancellation of this run and its native child operations.
     * Business exceptions become close reasons here; escaping exceptions indicate cleanup failure.
     * @pre Call once and retain the session until completion.
     */
    virtual asio::awaitable<void> run() = 0;
    /**
     * @brief Record the first close cause and request root cancellation on control_io.
     * @param stage Diagnostic stage retained with the first close cause.
     * @param reason Close explanation; an empty string is valid and is preserved.
     * @details Later calls do not overwrite the cause or emit cancellation again. This does not join run().
     */
    void cancel(std::string stage, std::string reason);
    /**
     * @brief Cancel the session if its Agent control connection was lost, on control_io.
     * @param session Authenticated identity of the disconnected local or remote Agent.
     */
    virtual void control_disconnected(const AgentPeer &session) = 0;
    /**
     * @brief Revalidate the session after a topology or cluster-control change on control_io.
     * @param epoch Current topology epoch, used to validate multi-node sessions.
     * @param reason Optional cluster failure text; empty requests epoch and membership validation.
     */
    virtual void invalidate(std::uint64_t epoch, std::string reason) = 0;
    /**
     * @brief Match a parsed Agent open retry on control_io; Single sessions do not consume retries.
     * @param session Authenticated identity of the original Agent control session.
     * @param request Parsed open fields, including the original request ID and optional nonzero endpoint UUID.
     * @return True if consumed; false for an unrelated identity, closing session or unsupported mode.
     * @throws std::invalid_argument If a matching retry conflicts with the established request.
     */
    virtual bool handle_open(const AgentPeer &session, const RelayRequest &request);
    /**
     * @brief Match and consume a parsed Agent cancellation or rejection on control_io.
     * @param command RelayCancel or RelayReject; the concrete session applies role and phase rules.
     * @param session Authenticated identity of the original Agent control session.
     * @param request Parsed optional identities and reason, with a nonzero UUID when present.
     * @return True if consumed, including ignored rejections; false if unrelated or closing.
     */
    virtual bool handle_cancel(CtrlCommand command, const AgentPeer &session, const CancelRequest &request) = 0;
    /**
     * @brief Match an Agent attachment and validate its authorization on control_io.
     * @param session Authenticated identity of the original Agent control session.
     * @param request Attachment parsed by RelayAttach::from_msg(), with a valid role and nonzero UUID.
     * @return True if the identity and UUID belong to this open session, even if authorization is ignored;
     * false if unmatched or closing. A consumed request does not confirm successful binding.
     */
    virtual bool handle_attach(const AgentPeer &session, const RelayAttach &request) = 0;
    /**
     * @brief Match and consume a peer-node relay notification on control_io.
     * @param command RelayPeer notification to process.
     * @param event Parsed epoch, flow ID, authenticated source and any close diagnostics.
     * @return True for a consumed peer identity, including repeats while closing; false for a mismatch
     * or a single-node session.
     */
    virtual bool handle_peer(CtrlCommand command, const PeerEvent &event);
    /**
     * @brief Build a status entry for an active ready session on control_io.
     * @return Session JSON, or no value before Ready, while closing or after a local Agent route expires.
     */
    virtual std::optional<njson> status_report() const = 0;
    /**
     * @brief Identify the session's fixed relay mode on control_io.
     * @return True for a multi-node session; false for a single-node session.
     */
    virtual bool is_multi() const noexcept = 0;
    /**
     * @brief Read the configured business protocol on control_io.
     * @return Tcp, Tls or Udp; TCP and TLS share the stream transport.
     */
    RelayProtocol protocol() const noexcept;
    /**
     * @brief Obtain the root task's cancellation slot on control_io.
     * @return Slot borrowed from this session and valid until its destruction; bind it to the root co_spawn.
     */
    asio::cancellation_slot cancellation_slot() noexcept;

  protected:
    /**
     * @brief Initialize shared session state on control_io without starting data work.
     * @param node Owning Node borrowed until run() completes.
     * @param params Construction parameters whose service name is moved into this session.
     * @param traffic Accounting ownership to move into the data pair when preparation starts.
     */
    NodeSession(RelayNode &node, RelayRequest &params, SRVTrafficPtr traffic);
    /** @brief Retain protocol milestones on control_io even after a close cause is recorded. */
    enum class Progress
    {
        Opening,
        PeerOpened,
        AgentNotified,
        Ready
    };
    /**
     * @brief Create and start the only data task on control_io.
     * @param setup One-time initialization moved into the task; borrowed services remain Node-owned.
     */
    void start_transfer(SessionTransfer::Setup setup);
    /**
     * @brief Check stream authorization on control_io; conflicting role authorization cancels the session.
     * @param request Parsed attachment whose role, Agent identity and UUID were matched by the concrete session.
     * @param expected Installed credentials used to validate the ticket within the original setup deadline.
     */
    void authorize_attach(const RelayAttach &request, const AttachInfo &expected);
    /**
     * @brief Submit an Agent notification on control_io; throw if its control route is unavailable.
     * @param route Non-owning local or remote Agent reply identity.
     * @param message Notification moved into the control transport.
     */
    void send_agent(const AgentPeer &route, CtrlMessage message) const;

    RelayNode &node_;
    RelayProtocol protocol_;
    std::string service_;
    std::uint64_t request_id_;
    std::chrono::steady_clock::time_point deadline_;
    SRVTrafficPtr traffic_;
    std::shared_ptr<SessionTransfer> transfer_;
    Progress progress_ = Progress::Opening;
    std::string stage_ = "open_flow";
    std::optional<std::string> close_reason_;

  private:
    asio::cancellation_signal cancellation_;
};

/**
 * @brief Coordinate both Agent endpoints of a single-node relay on control_io.
 * @details RelayNode owns this session; Agent routes remain non-owning. run() stops and joins the
 * inherited data task before sending the final Agent notifications.
 */
class SingleSession final : public NodeSession
{
  public:
    /**
     * @brief Construct a local relay without starting establishment on control_io.
     * @param node Owning Node and borrowed services; must outlive run().
     * @param consumer Non-owning reply identity of the Consumer Agent.
     * @param producer Non-owning reply identity of the Producer Agent.
     * @param params Validated construction values moved into the session as needed.
     * @param traffic Service accounting transferred to the data pair during preparation.
     */
    SingleSession(RelayNode &node, const AgentPeer &consumer, const AgentPeer &producer, RelayRequest params,
                  SRVTrafficPtr traffic);
    /** @copydoc NodeSession::run */
    asio::awaitable<void> run() override;
    /** @copydoc NodeSession::control_disconnected */
    void control_disconnected(const AgentPeer &session) override;
    /** @copydoc NodeSession::invalidate */
    void invalidate(std::uint64_t epoch, std::string reason) override;
    /** @copydoc NodeSession::handle_cancel */
    bool handle_cancel(CtrlCommand command, const AgentPeer &session, const CancelRequest &request) override;
    /** @copydoc NodeSession::handle_attach */
    bool handle_attach(const AgentPeer &session, const RelayAttach &request) override;
    /** @copydoc NodeSession::status_report */
    std::optional<njson> status_report() const override;
    /** @copydoc NodeSession::is_multi */
    bool is_multi() const noexcept override
    {
        return false;
    }

  private:
    std::uint64_t uuid() const noexcept
    {
        return endpoint_.consumer.uuid;
    }
    void notify_agents() const;
    void ready_agents() const;
    void close_agents() const;

    AgentPeer consumer_;
    AgentPeer producer_;
    AttachPair endpoint_;
};

/**
 * @brief Coordinate one path endpoint and its peer Node on control_io.
 * @details RelayNode owns this session and outlives its data task. Agent and peer identities do not own
 * their connections. Ingress owns the Flow transaction; egress uses the peer-supplied identity.
 * run() joins establishment and Flow monitoring, then stops data work before ingress releases its Flow.
 */
class MultiSession final : public NodeSession
{
  public:
    /**
     * @brief Construct a path endpoint without starting establishment on control_io.
     * @param node Owning Node and borrowed services; must outlive run().
     * @param agent Non-owning reply identity of the Agent attached to this endpoint.
     * @param params Validated service and epoch; ingress requires a path, egress requires flow_id and peer.
     * @param ingress True for the Consumer endpoint that opens the Flow; false for the Producer endpoint.
     * @param traffic Optional service accounting transferred to the data pair during preparation.
     */
    MultiSession(RelayNode &node, const AgentPeer &agent, RelayRequest params, bool ingress,
                 SRVTrafficPtr traffic = {});
    /** @copydoc NodeSession::run */
    asio::awaitable<void> run() override;
    /** @copydoc NodeSession::control_disconnected */
    void control_disconnected(const AgentPeer &session) override;
    /** @copydoc NodeSession::invalidate */
    void invalidate(std::uint64_t epoch, std::string reason) override;
    /** @copydoc NodeSession::handle_open */
    bool handle_open(const AgentPeer &session, const RelayRequest &request) override;
    /** @copydoc NodeSession::handle_cancel */
    bool handle_cancel(CtrlCommand command, const AgentPeer &session, const CancelRequest &request) override;
    /** @copydoc NodeSession::handle_attach */
    bool handle_attach(const AgentPeer &session, const RelayAttach &request) override;
    /** @copydoc NodeSession::handle_peer */
    bool handle_peer(CtrlCommand command, const PeerEvent &event) override;
    /** @copydoc NodeSession::status_report */
    std::optional<njson> status_report() const override;
    /** @copydoc NodeSession::is_multi */
    bool is_multi() const noexcept override
    {
        return true;
    }

  private:
    asio::awaitable<void> establish_and_transfer();
    asio::awaitable<void> watch_flow();
    std::uint64_t uuid() const noexcept
    {
        return endpoint_.uuid;
    }
    void notify_agents() const;
    void ready_agents() const;
    void close_agents() const;
    void send_peer(CtrlCommand command, njson params) const;

    AgentPeer agent_;
    std::vector<std::string> path_;
    std::string accessor_;
    std::uint64_t epoch_;
    std::uint64_t flow_id_;
    std::string peer_;
    asio::steady_timer changed_;
    bool ingress_;
    bool peer_prepared_ = false;
    bool peer_finished_ = false;
    bool from_peer_ = false;
    AttachInfo endpoint_;
};

using NodeSessionList = std::vector<std::unique_ptr<NodeSession>>;

#endif
