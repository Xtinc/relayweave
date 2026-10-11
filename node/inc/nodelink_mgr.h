#ifndef RELAYWEAVE_NODELINK_MGR_H
#define RELAYWEAVE_NODELINK_MGR_H

#include "async_event.h"
#include "cluster_mgr.h"
#include "lnk_channel.h"
#include "topology.h"
#include <map>
#include <tuple>

struct LinkResult
{
    std::uint64_t id = 0;
    std::string stage;
    std::string reason;
    explicit operator bool() const noexcept
    {
        return reason.empty() && id != 0;
    }
};

struct FlowResult
{
    std::uint64_t epoch = 0;
    std::uint64_t id = 0;
    std::string stage;
    std::string reason;
    explicit operator bool() const noexcept
    {
        return epoch && id && reason.empty();
    }
};

// Link and Flow coordination belongs to control_io; LnkChannel owns cluster_data_io state.
class NodeLinkMgr
{
  public:
    // Flow acknowledgements use one bit per path node: at most 8 nodes.
    static constexpr std::size_t max_flow_nodes = 8;
    NodeLinkMgr(asio::any_io_executor control, asio::any_io_executor data, ClusterConfig config,
                std::string tcp_address, std::string udp_address, ClusterMgr &cluster, Topology &topology);
    void start();
    void activate();
    void rollback() noexcept;
    void handle(CtrlMessage message);
    void members_changed();
    void control_failed(std::string reason);
    // Coroutine entry points require control_io.
    asio::awaitable<LinkResult> ensure_link(std::string left, std::string right, RelayProtocol transport);
    asio::awaitable<FlowResult> open_flow(std::vector<std::string> path, RelayProtocol transport);
    asio::awaitable<void> close_flow(std::uint64_t epoch, std::uint64_t id);
    bool egress_ready(std::uint64_t epoch, std::uint64_t id, const std::string &ingress,
                      RelayProtocol transport) const;
    asio::awaitable<std::string> wait_flow_closed(std::uint64_t epoch, std::uint64_t id);
    asio::awaitable<void> stop();
    LnkChannel &channel() const
    {
        return *channel_;
    }

  private:
    using Clock = std::chrono::steady_clock;
    struct LinkRequest
    {
        LinkRequest(asio::any_io_executor executor, njson params);
        njson params;
        LinkResult result;
        asio::steady_timer timeout;
        AsyncEvent completed_event;
        // Bit 0: left, bit 1: right.
        std::uint8_t prepared = 0;
        std::uint8_t ready = 0;
        bool completed = false;
    };
    void send_both(const LinkRequest &request, CtrlCommand command);
    void finish_link(std::shared_ptr<LinkRequest> request, std::string stage, std::string reason);
    using LinkKey = std::tuple<std::string, std::string, RelayProtocol>;
    static LinkKey link_key(const njson &params);
    struct FlowRequest
    {
        FlowRequest(asio::any_io_executor executor, njson params);
        njson params;
        FlowResult result;
        asio::steady_timer timeout;
        AsyncEvent completed_event;
        AsyncEvent released_event;
        // Bit i corresponds to params["path"][i].
        std::uint8_t prepared = 0;
        std::uint8_t committed = 0;
        std::uint8_t closed = 0;
        bool completed = false;
        bool released = false;
        std::string close_error;
    };
    struct RemoteFlow
    {
        explicit RemoteFlow(asio::any_io_executor executor) : completed(executor) {}
        FlowResult result;
        AsyncEvent completed;
    };
    struct FlowWatch
    {
        explicit FlowWatch(asio::any_io_executor executor) : closed(executor) {}
        std::string reason;
        AsyncEvent closed;
    };
    std::shared_ptr<FlowRequest> create_flow(std::vector<std::string> path, RelayProtocol transport,
                                             std::uint64_t requested_id = 0);
    asio::awaitable<FlowResult> establish_flow(std::shared_ptr<FlowRequest> request);
    asio::awaitable<FlowResult> request_flow(std::vector<std::string> path, RelayProtocol transport);
    asio::awaitable<void> reply_flow(std::shared_ptr<FlowRequest> request, std::string target);
    void handle_flow_request(CtrlMessage message);
    void flow_closed(std::uint64_t id, std::string reason);
    void fail_remote_flows(const std::string &reason);
    void arm_flow_timeout(const std::shared_ptr<FlowRequest> &request);
    void send_all(const FlowRequest &request, CtrlCommand command);
    void finish_flow(std::shared_ptr<FlowRequest> request, std::string stage, std::string reason);
    void released(std::shared_ptr<FlowRequest> request, std::string error = {});
    bool valid_flow(const njson &params) const;
    void handle_link(CtrlMessage message);
    void handle_flow(CtrlMessage message);
    asio::awaitable<void> receive_events();
    asio::awaitable<void> shutdown();
    asio::any_io_executor control_;
    ClusterConfig config_;
    ClusterMgr &cluster_;
    Topology &topology_;
    std::shared_ptr<LnkChannel> channel_;
    std::map<LinkKey, std::shared_ptr<LinkRequest>> link_requests_;
    std::map<std::uint64_t, njson> link_endpoints_;
    std::map<std::uint64_t, std::shared_ptr<FlowRequest>> flow_requests_;
    std::map<std::uint64_t, njson> flow_endpoints_;
    std::map<std::uint64_t, std::shared_ptr<FlowRequest>> closing_flows_;
    std::map<std::uint64_t, std::shared_ptr<RemoteFlow>> remote_flows_;
    std::map<std::uint64_t, std::shared_ptr<FlowWatch>> flow_watches_;
    std::size_t flow_tasks_ = 0;
    asio::steady_timer done_;
    bool running_ = false;
    bool events_running_ = false;
};

#endif
