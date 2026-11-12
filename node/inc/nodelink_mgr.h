#ifndef RELAYWEAVE_NODELINK_MGR_H
#define RELAYWEAVE_NODELINK_MGR_H

#include "async_event.h"
#include "cluster_mgr.h"
#include "flow_protocol.h"
#include "topology.h"
#include <map>
#include <tuple>

/** @brief Describe a Flow transaction by epoch, ID, failing stage, and diagnostic reason. */
struct FlowResult
{
    std::uint64_t epoch = 0;
    std::uint64_t id = 0;
    std::string stage;
    std::string reason;
    /**
     * @brief Return whether a nonzero epoch and Flow ID were committed without error.
     * @return True when the transaction has a usable nonzero identity and no failure reason.
     */
    explicit operator bool() const noexcept
    {
        return epoch && id && reason.empty();
    }
};

/**
 * @brief Coordinate shared NodeLinks and Flow path transactions on control_io.
 * @details Owns FlowProtocol, whose transport, flow, and credit state belongs to
 * cluster_data_io. The master retains path transactions, never Agent relay state.
 * Coordination operations require control_io; start() and activate() also support serialized
 * bootstrap before workers run. RelayNode starts listeners before publishing their data tasks.
 */
class NodeLinkMgr
{
  public:
    /** @brief Limit paths to eight nodes because acknowledgements use one bit per path node. */
    static constexpr std::size_t max_flow_nodes = 8;
    /**
     * @brief Borrow cluster and topology state and construct the protocol for its data executor.
     * @param control Single-threaded control executor owning Link/Flow transaction state.
     * @param data Executor owning the protocol's NodeLink sockets, Flow state, and credit accounting.
     * @param config Local cluster role, Node identity, and inter-Node listener ports.
     * @param tcp_address Local bind address for the inter-Node TCP listener.
     * @param udp_address Local bind address for the inter-Node UDP listener.
     * @param cluster Node-owned cluster control manager borrowed for message routing.
     * @param topology Node-owned topology and current membership state borrowed on control_io.
     */
    NodeLinkMgr(asio::any_io_executor control, asio::any_io_executor data, ClusterConfig config,
                std::string tcp_address, std::string udp_address, ClusterMgr &cluster, Topology &topology);
    /** @brief Open NodeLink listeners during serialized Node startup before publishing data tasks. */
    void start();
    /** @brief Activate protocol tasks and schedule the control-side notification receiver at startup. */
    void activate();
    /**
     * @brief Dispatch an authenticated cluster message using the caller's already parsed command.
     * @param command Control command already parsed by the caller.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     */
    void handle(CtrlCommand command, CtrlMessage message);
    /** @brief Revoke stale transactions and endpoints after membership changes. */
    void members_changed();
    /**
     * @brief Revoke authorization before scheduling data cleanup after cluster control failure.
     * @param reason Diagnostic reason retained for closure or sent to the affected peer.
     */
    void control_failed(std::string reason);
    /**
     * @brief Commit a TCP/UDP path locally on the master or through a bounded remote request.
     * @param path Ordered, loop-free Node IDs from ingress to egress, containing two to eight members.
     * @param transport NodeLink transport: TCP or UDP; TLS uses TCP on inter-Node links.
     * @return Committed Flow epoch and ID on success, or a failing stage and reason.
     * @pre Called on control_io; a slave request must originate at path.front().
     * @details Work begins when awaited. Remote replies have a 30-second bound; cancellation revokes the
     * pending reply registration and requests remote cleanup. On the master, cancellation closes only this path;
     * shared Links remain manager-owned. Link and Flow messages advance the transaction and publish its result.
     */
    asio::awaitable<FlowResult> open_flow(std::vector<std::string> path, RelayProtocol transport);
    /**
     * @brief Request closure; await path acknowledgements on the master, local invalidation elsewhere.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param id Flow ID to close within the supplied epoch.
     * @pre Called on control_io with the Flow's authorized epoch and ID.
     * @throws std::exception If request submission or release confirmation fails, times out, or is cancelled.
     * @details Non-master success confirms local invalidation, not full path release. Neither branch joins business I/O.
     */
    asio::awaitable<void> close_flow(std::uint64_t epoch, std::uint64_t id);
    /**
     * @brief Check that a committed local egress matches epoch, ingress, and transport.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param id Flow ID whose committed local egress authorization is checked.
     * @param ingress Expected first Node on the authorized path.
     * @param transport NodeLink transport: TCP or UDP; TLS uses TCP on inter-Node links.
     * @return True only for a committed local egress matching all supplied authorization fields.
     */
    bool egress_ready(std::uint64_t epoch, std::uint64_t id, const std::string &ingress, RelayProtocol transport) const;
    /**
     * @brief Observe local Flow invalidation on every Node, including the master; never initiates closure.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param id Flow ID to watch within the supplied epoch.
     * @return Local closure reason, or "flow closed" if no local Flow matches the epoch and ID.
     * @pre Called on control_io; cancelling this wait does not close a healthy Flow.
     * @details Does not wait for path release or join business I/O. NodeSession drains its transfer task
     * separately; close_flow() waits for path release on the master.
     */
    asio::awaitable<std::string> wait_flow_closed(std::uint64_t epoch, std::uint64_t id);
    /**
     * @brief Stop transactions and protocol work; every normal return guarantees all tasks drained.
     * @return True after successful shutdown; false for handled errors. Every normal return guarantees
     * protocol and manager tasks have drained.
     */
    asio::awaitable<bool> stop();
    /**
     * @brief Borrow the protocol; its mutable data operations still require its own executor.
     * @return Borrowed protocol service; its mutable data operations require protocol().executor(), not
     * control_io.
     */
    FlowProtocol &protocol() const
    {
        return *protocol_;
    }

  private:
    using Clock = std::chrono::steady_clock;
    /** @brief Retain shared Link authorization on control_io; peer fields are derived per endpoint. */
    struct LinkParams
    {
        /** @brief Encode Link parameters, deriving peer fields when addressing an endpoint. */
        njson to_params(std::string_view node = {}) const;
        std::uint64_t id = 0;
        std::uint64_t epoch = 0;
        std::string master;
        std::string left;
        std::string right;
        RelayProtocol transport = RelayProtocol::Tcp;
        std::string token;
        std::uint16_t tcp_port = 0;
        std::uint16_t udp_port = 0;
        // Freeze membership for selection and invalidation; selected addresses are derived from it.
        NodeAddress left_member;
        NodeAddress right_member;
    };
    /** @brief Retain validated path parameters without a JSON runtime mirror. */
    struct FlowParams
    {
        /** @brief Encode complete Prepare parameters; its deadline is supplied by the transaction. */
        njson to_params() const;
        /** @brief Encode the identity shared by later phases and acknowledgements. */
        njson identity_params() const;
        bool operator==(const FlowParams &) const = default;
        std::uint64_t epoch = 0;
        std::string master;
        std::uint64_t id = 0;
        std::uint64_t request_id = 0;
        std::vector<std::string> path;
        std::vector<std::string> addresses;
        std::vector<std::uint64_t> links;
        RelayProtocol transport = RelayProtocol::Tcp;
    };
    /** @brief Share a local invalidation notification with suspended observers after authorization removal. */
    struct CloseNotice
    {
        explicit CloseNotice(asio::any_io_executor executor) : closed(executor)
        {
        }
        std::string reason;
        AsyncEvent closed;
    };
    /** @brief Own this node's Flow authorization, including transit nodes; observers retain only closure. */
    struct LocalFlow
    {
        FlowParams params;
        bool committed = false;
        std::shared_ptr<CloseNotice> closure;
    };
    /** @brief Keep a reusable master Link record; bit 0 is left and bit 1 is right. */
    struct SharedLink
    {
        /** @brief Retain validated Link parameters and construct deadline and completion state on control_io. */
        SharedLink(asio::any_io_executor executor, LinkParams params);
        LinkParams params;
        std::string stage;
        std::string reason;
        asio::steady_timer timeout;
        std::uint8_t prepared = 0;
        std::uint8_t ready = 0;
        bool completed = false;
    };
    /**
     * @brief Get or start a reusable Link synchronously on control_io; stopping or non-master calls throw.
     * @pre The caller supplies distinct current members and TCP/UDP from a validated path.
     */
    std::shared_ptr<SharedLink> start_link(std::string left, std::string right, RelayProtocol transport);
    bool send_both(const SharedLink &request, CtrlCommand command);
    void finish_link(std::shared_ptr<SharedLink> request, std::string stage, std::string reason);
    using LinkKey = std::tuple<std::string, std::string, RelayProtocol>;
    enum class PathPhase
    {
        Linking,
        Preparing,
        Committing,
        Active,
        Closing,
        Released
    };
    /**
     * @brief Own one master path through message-driven establishment and close coordination on control_io.
     * @details acknowledged tracks ready edges in Linking, then node acknowledgements in later phases.
     * reply_to is empty for local opens and is consumed once when publishing a remote open result.
     */
    struct MasterPath
    {
        /** @brief Retain path parameters and construct setup and release waiters on control_io. */
        MasterPath(asio::any_io_executor executor, FlowParams params);
        FlowParams params;
        FlowResult result;
        asio::steady_timer timeout;
        AsyncEvent opened;
        AsyncEvent released;
        PathPhase phase = PathPhase::Linking;
        std::uint8_t acknowledged = 0;
        std::string close_error;
        std::string reply_to;
    };
    /**
     * @brief Own one remote request in pending_opens_ until its open_flow() wait exits on control_io.
     * @details Reply and failure handlers update and notify the entry without removing it.
     * open_flow() borrows the entry; scope cleanup erases it and requests closure of any unaccepted submission.
     */
    struct PendingOpen
    {
        /** @brief Initialize the remote result event on control_io. */
        explicit PendingOpen(asio::any_io_executor executor) : completed(executor)
        {
        }
        FlowResult result;
        AsyncEvent completed;
    };
    std::shared_ptr<MasterPath> create_path(std::vector<std::string> path, RelayProtocol transport,
                                            std::uint64_t requested_id = 0);
    /** @brief Start or reuse all adjacent Links on control_io without waiting or launching edge tasks. */
    void start_path(const std::shared_ptr<MasterPath> &request);
    /**
     * @brief Enter Preparing once every edge is ready; reuse the acknowledgement bitmap on control_io.
     * @pre The caller owns a Linking transaction; invalidation closes it before publishing Link results.
     */
    void prepare_path(const std::shared_ptr<MasterPath> &request);
    /** @brief Publish the open result to local waiters and consume any remote reply target on control_io. */
    void finish_open(const std::shared_ptr<MasterPath> &request);
    /** @brief Reserve remote requests synchronously before a subsequent cancellation can arrive. */
    void handle_flow_request(CtrlCommand command, CtrlMessage message);
    void notify_flow_closed(std::uint64_t id, std::string reason);
    void fail_waiters(const std::string &reason);
    void arm_path_timeout(const std::shared_ptr<MasterPath> &request);
    bool send_all(const MasterPath &request, CtrlCommand command);
    void close_path(std::shared_ptr<MasterPath> request, std::string stage, std::string reason);
    void release_path(std::shared_ptr<MasterPath> request, std::string error = {});
    bool valid_flow(const FlowParams &params) const;
    void handle_link(CtrlCommand command, CtrlMessage message);
    /** @brief Apply authenticated master commands to local Link authorizations on control_io. */
    void handle_link_command(CtrlCommand command, const njson &params, std::uint64_t id, std::uint64_t epoch,
                             const std::string &source);
    /** @brief Collect authenticated endpoint replies for a master-owned shared Link on control_io. */
    void handle_link_reply(CtrlCommand command, const njson &params, std::uint64_t id, const std::string &source);
    void handle_flow(CtrlCommand command, CtrlMessage message);
    /** @brief Apply authenticated master commands to local Flow authorizations on control_io. */
    void handle_flow_command(CtrlCommand command, const njson &params, std::uint64_t id, std::uint64_t epoch,
                             const std::string &source);
    /** @brief Advance a master-owned path from authenticated endpoint replies on control_io. */
    void handle_flow_reply(CtrlCommand command, const njson &params, std::uint64_t id, std::uint64_t epoch,
                           const std::string &source);
    /** @brief Receive data-domain events on control_io; notification loss terminates the data service. */
    asio::awaitable<void> receive_events();
    /** @brief Stop requests and await data tasks on control_io before cluster control disconnects. */
    asio::awaitable<void> shutdown();
    asio::any_io_executor control_;
    ClusterConfig config_;
    ClusterMgr &cluster_;
    Topology &topology_;
    std::shared_ptr<FlowProtocol> protocol_;
    // Master-wide coordination. Closing paths remain in paths_ until acknowledgement or timeout.
    std::map<LinkKey, std::shared_ptr<SharedLink>> shared_links_;
    std::map<std::uint64_t, std::shared_ptr<MasterPath>> paths_;
    // This node's authorizations, including transit Flows. Observers retain only CloseNotice.
    std::map<std::uint64_t, LinkParams> local_links_;
    std::map<std::uint64_t, LocalFlow> local_flows_;
    std::map<std::uint64_t, PendingOpen> pending_opens_;
    asio::steady_timer events_done_;
    bool stop_ok_ = true;
    bool running_ = false;
    bool events_running_ = false;
};

#endif
