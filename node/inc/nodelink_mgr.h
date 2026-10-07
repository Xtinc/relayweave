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

struct LinkStatus
{
    LinkResult result;
    std::string llink;
    std::string rlink;
    bool lready = false;
    bool rready = false;
    bool complete() const noexcept
    {
        return !llink.empty() && !rlink.empty();
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
    // Coroutine entry points require control_io; RelayNode binds external callers.
    asio::awaitable<LinkResult> ensure_link(std::string left, std::string right, RelayProtocol transport);
    asio::awaitable<LinkStatus> link_status(std::uint64_t id);
    asio::awaitable<void> close_link(std::uint64_t id);
    asio::awaitable<FlowResult> open_flow(std::vector<std::string> path, RelayProtocol transport);
    asio::awaitable<void> close_flow(std::uint64_t epoch, std::uint64_t id);
    asio::awaitable<void> stop();
    LnkChannel &channel() const
    {
        return *channel_;
    }

  private:
    using Clock = std::chrono::steady_clock;
    struct StatusQuery
    {
        StatusQuery(asio::any_io_executor executor, std::uint64_t link_id);
        std::uint64_t id;
        LinkStatus result;
        asio::steady_timer timeout;
        AsyncEvent completed_event;
    };
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
        std::shared_ptr<StatusQuery> status;
        bool completed = false;
    };
    void send_both(const LinkRequest &request, CtrlCommand command);
    void finish_status(LinkRequest &request, std::string reason = {});
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
    asio::steady_timer done_;
    bool running_ = false;
    bool events_running_ = false;
};

#endif
