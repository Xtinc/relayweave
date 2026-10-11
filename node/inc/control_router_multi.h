#ifndef RELAYWEAVE_CONTROL_ROUTER_MULTI_H
#define RELAYWEAVE_CONTROL_ROUTER_MULTI_H

#include "nodelink_mgr.h"
#include "tls_channel.h"

class RelayNode;
class NodeSession;

// One multi-node Relay controller, owned by its NodeSession. All access requires control_io.
class ControlRouterMulti
{
  public:
    ControlRouterMulti(NodeSession &relay, const ControlSessionPtr &session, njson params, bool ingress);
    asio::awaitable<void> run();
    bool handle(const ControlSessionPtr &session, const CtrlMessage &message);
    void handle_peer(const CtrlMessage &message);
    bool belongs_to(const ControlSessionPtr &session) const;
    void cancel(std::string stage, std::string reason, bool from_peer = false);
    bool matches(const ControlSessionPtr &session, const njson &params) const;
    std::optional<njson> status_report() const;

  private:
    friend class RelayNode;
    enum class Progress
    {
        OpeningFlow,
        PeerOpened,
        AgentNotified,
        Ready
    };

    asio::awaitable<void> establish_and_transfer();
    asio::awaitable<void> watch_flow();
    void notify_agent() const;
    void close_agent() const;
    void ready_agent();
    void send_peer(std::string command, njson params) const;
    NodeSession &relay_;
    std::weak_ptr<ControlSession> session_;
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
    bool closed_ = false;
    bool from_peer_ = false;
};

#endif
