#ifndef RELAYWEAVE_NODELINK_MGR_H
#define RELAYWEAVE_NODELINK_MGR_H

#include "async_event.h"
#include "cluster_mgr.h"
#include "flow_protocol.h"
#include "topology.h"
#include <map>
#include <tuple>

/** @brief Describe a Link transaction; success requires a nonzero ID and an empty reason. */
struct LinkResult
{
    std::uint64_t id = 0;
    std::string stage;
    std::string reason;
    /**
     * @brief Return whether Link establishment succeeded.
     * @return True when the transaction has a usable nonzero identity and no failure reason.
     */
    explicit operator bool() const noexcept
    {
        return reason.empty() && id != 0;
    }
};

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
     * @brief Ensure or reuse an adjacent TCP/UDP Link on the master and await both endpoints.
     * @param left ID of one adjacent Node; endpoint order is normalized for Link reuse.
     * @param right ID of the other adjacent Node.
     * @param transport NodeLink transport: TCP or UDP; TLS uses TCP on inter-Node links.
     * @return Established Link ID on success, or a failing stage and reason without a usable ID.
     * @pre Called on control_io; only the cluster master can establish or reuse adjacent Links.
     * @details A fresh outer attempt uses new IDs and credentials, including after earlier failures.
     */
    asio::awaitable<LinkResult> ensure_link(std::string left, std::string right, RelayProtocol transport);
    /**
     * @brief Commit a TCP/UDP path locally on the master or through a bounded remote request.
     * @param path Ordered, loop-free Node IDs from ingress to egress, containing two to eight members.
     * @param transport NodeLink transport: TCP or UDP; TLS uses TCP on inter-Node links.
     * @return Committed Flow epoch and ID on success, or a failing stage and reason.
     * @pre Called on control_io; a slave request must originate at path.front().
     * @details Cancellation of establishment propagates through its owned transaction cleanup.
     */
    asio::awaitable<FlowResult> open_flow(std::vector<std::string> path, RelayProtocol transport);
    /**
     * @brief Request Flow closure and await bounded release confirmation without closing shared Links.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param id Flow ID to close within the supplied epoch.
     * @pre Called on control_io with the Flow's authorized epoch and ID.
     * @throws std::exception If release confirmation fails, times out, or is cancelled.
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
    bool egress_ready(std::uint64_t epoch, std::uint64_t id, const std::string &ingress,
                      RelayProtocol transport) const;
    /**
     * @brief Await Flow invalidation and return its close reason.
     * @param epoch Cluster membership epoch that authorizes this Flow.
     * @param id Flow ID to watch within the supplied epoch.
     * @return Reason for Flow closure, or an immediate diagnostic if the epoch or Flow is already invalid.
     * @pre Called on control_io; cancelling this wait does not close a healthy Flow.
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
    /** @brief Retain Link parameters and acknowledgements; bit 0 is left and bit 1 is right. */
    struct LinkRequest
    {
        /** @brief Retain validated Link parameters and construct deadline and completion state on control_io. */
        LinkRequest(asio::any_io_executor executor, njson params);
        njson params;
        LinkResult result;
        asio::steady_timer timeout;
        AsyncEvent completed_event;
        std::uint8_t prepared = 0;
        std::uint8_t ready = 0;
        bool completed = false;
    };
    bool send_both(const LinkRequest &request, CtrlCommand command);
    void finish_link(std::shared_ptr<LinkRequest> request, std::string stage, std::string reason);
    using LinkKey = std::tuple<std::string, std::string, RelayProtocol>;
    static LinkKey link_key(const njson &params);
    /** @brief Retain a master path transaction; acknowledgement bit i denotes path node i. */
    struct FlowRequest
    {
        /** @brief Retain path parameters and construct setup and release waiters on control_io. */
        FlowRequest(asio::any_io_executor executor, njson params);
        njson params;
        FlowResult result;
        asio::steady_timer timeout;
        AsyncEvent completed_event;
        AsyncEvent released_event;
        std::uint8_t prepared = 0;
        std::uint8_t committed = 0;
        std::uint8_t closed = 0;
        bool completed = false;
        bool released = false;
        std::string close_error;
    };
    /** @brief Hold a remote ingress request until the master's result or a bounded timeout. */
    struct RemoteFlow
    {
        /** @brief Initialize the remote result event on control_io. */
        explicit RemoteFlow(asio::any_io_executor executor) : completed(executor) {}
        FlowResult result;
        AsyncEvent completed;
    };
    /** @brief Retain a one-shot Flow close notification and its diagnostic reason. */
    struct FlowWatch
    {
        /** @brief Initialize the one-shot closure event on control_io. */
        explicit FlowWatch(asio::any_io_executor executor) : closed(executor) {}
        std::string reason;
        AsyncEvent closed;
    };
    std::shared_ptr<FlowRequest> create_flow(std::vector<std::string> path, RelayProtocol transport,
                                             std::uint64_t requested_id = 0);
    asio::awaitable<FlowResult> establish_flow(std::shared_ptr<FlowRequest> request);
    /** @brief Await the master's shared FlowRequest result on control_io with a 30-second bound. */
    asio::awaitable<FlowResult> request_flow(std::vector<std::string> path, RelayProtocol transport);
    asio::awaitable<void> reply_flow(std::shared_ptr<FlowRequest> request, std::string target);
    /** @brief Reserve remote requests synchronously before a subsequent cancellation can arrive. */
    void handle_flow_request(CtrlCommand command, CtrlMessage message);
    void flow_closed(std::uint64_t id, std::string reason);
    void fail_remote_flows(const std::string &reason);
    void arm_flow_timeout(const std::shared_ptr<FlowRequest> &request);
    bool send_all(const FlowRequest &request, CtrlCommand command);
    void finish_flow(std::shared_ptr<FlowRequest> request, std::string stage, std::string reason);
    void released(std::shared_ptr<FlowRequest> request, std::string error = {});
    bool valid_flow(const njson &params) const;
    void handle_link(CtrlCommand command, CtrlMessage message);
    void handle_flow(CtrlCommand command, CtrlMessage message);
    /** @brief Receive data-domain events on control_io; notification loss terminates the data service. */
    asio::awaitable<void> receive_events();
    /** @brief Stop requests and await data tasks on control_io before cluster control disconnects. */
    asio::awaitable<void> shutdown();
    asio::any_io_executor control_;
    ClusterConfig config_;
    ClusterMgr &cluster_;
    Topology &topology_;
    std::shared_ptr<FlowProtocol> protocol_;
    std::map<LinkKey, std::shared_ptr<LinkRequest>> link_requests_;
    std::map<std::uint64_t, njson> link_endpoints_;
    std::map<std::uint64_t, std::shared_ptr<FlowRequest>> flow_requests_;
    std::map<std::uint64_t, njson> flow_endpoints_;
    std::map<std::uint64_t, std::shared_ptr<FlowRequest>> closing_flows_;
    std::map<std::uint64_t, std::shared_ptr<RemoteFlow>> remote_flows_;
    std::map<std::uint64_t, std::shared_ptr<FlowWatch>> flow_watches_;
    std::size_t flow_tasks_ = 0;
    asio::steady_timer done_;
    bool stop_ok_ = true;
    bool running_ = false;
    bool events_running_ = false;
};

#endif
