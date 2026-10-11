#ifndef RELAYWEAVE_CONTROL_ROUTER_SINGLE_H
#define RELAYWEAVE_CONTROL_ROUTER_SINGLE_H

#include "tls_channel.h"

class NodeSession;

// One single-node relay controller. All access requires control_io.
class ControlRouterSingle
{
  public:
    ControlRouterSingle(NodeSession &relay, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                        njson params);
    asio::awaitable<void> run();
    void cancel(std::string stage, std::string reason);
    bool handle(const ControlSessionPtr &session, const CtrlMessage &message);
    bool belongs_to(const ControlSessionPtr &session) const;
    std::optional<njson> status_report() const;

  private:
    bool matches(const ControlSessionPtr &session, const njson &params) const;
    void notify_agents() const;
    void ready_agents() const;
    void close_agents() const;
    NodeSession &relay_;
    std::weak_ptr<ControlSession> consumer_;
    std::weak_ptr<ControlSession> producer_;
    njson params_;
    std::chrono::steady_clock::time_point deadline_;
    std::string reason_;
    bool ready_ = false;
    bool closed_ = false;
};

#endif
