#include "control_router_multi.h"
#include "relay_session.h"
#include "relay_node.h"
#include <algorithm>
#include <asio/experimental/parallel_group.hpp>

using Clock = std::chrono::steady_clock;

ControlRouterMulti::ControlRouterMulti(RelaySession &relay, const ControlSessionPtr &session, njson params, bool ingress)
    : relay_(relay), session_(session), params_(std::move(params)),
      deadline_(Clock::now() + std::chrono::milliseconds(config::require_unsigned(params_, "budget_ms", true))),
      changed_(relay.node_.control_executor_), ingress_(ingress)
{
    epoch_ = config::require_unsigned(params_, "epoch", true);
    if (ingress_)
    {
        peer_ = params_.at("path").back().get<std::string>();
    }
    else
    {
        flow_id_ = config::require_unsigned(params_, "flow_id", true);
        peer_ = params_.at("source").get<std::string>();
        peer_opened_ = true;
    }
}

void ControlRouterMulti::send_peer(std::string command, njson params) const
{
    params["epoch"] = epoch_;
    params["flow_id"] = flow_id_;
    relay_.node_.cluster_mgr_->send(peer_, CtrlMessage(std::move(command), std::move(params)));
}

void ControlRouterMulti::notify_agent() const
{
    if (auto session = session_.lock())
    {
        njson values{{"service", params_.at("service")}, {"protocol", params_.at("protocol")},
                     {"epoch", epoch_}, {"flow_id", flow_id_},
                     {"budget_ms", std::max<std::int64_t>(1, std::chrono::ceil<std::chrono::milliseconds>(deadline_ - Clock::now()).count())}};
        values.update(relay_.endpoint_);
        if (ingress_)
        {
            values["request_id"] = params_.at("request_id");
        }
        session->send(CtrlMessage(ingress_ ? CtrlCommand::RelayOpened : CtrlCommand::RelayOffer, std::move(values)));
    }
}

void ControlRouterMulti::ready_agent()
{
    ready_ = true;
    if (auto session = session_.lock())
    {
        njson values{{"uuid", relay_.endpoint_.at("uuid")}, {"protocol", params_.at("protocol")},
                     {"epoch", epoch_}, {"flow_id", flow_id_}};
        session->send(CtrlMessage(CtrlCommand::RelayReady, std::move(values)));
    }
    PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", params_.at("protocol").get<std::string>().c_str(),
        params_.at("service").get<std::string>().c_str(),
        static_cast<unsigned long long>(relay_.endpoint_.at("uuid").get<std::uint64_t>()));
}

void ControlRouterMulti::close_agent() const
{
    if (auto session = session_.lock(); session && (ingress_ || notified_))
    {
        njson values{{"service", params_.at("service")}, {"protocol", params_.at("protocol")},
                     {"epoch", epoch_}, {"stage", stage_}, {"reason", reason_}};
        if (flow_id_)
        {
            values["flow_id"] = flow_id_;
        }
        values.update(relay_.endpoint_);
        if (ingress_)
        {
            values["request_id"] = params_.at("request_id");
            values["path"] = params_.at("path");
        }
        session->send(CtrlMessage(ingress_ && !ready_ ? CtrlCommand::RelayError : CtrlCommand::RelayClosed, std::move(values)));
    }
}

void ControlRouterMulti::cancel(std::string stage, std::string reason, bool from_peer)
{
    if (closed_)
    {
        return;
    }
    closed_ = true;
    stage_ = std::move(stage);
    reason_ = std::move(reason);
    from_peer_ = from_peer;
    relay_.cancellation_.emit(asio::cancellation_type::all);
    changed_.cancel();
}

bool ControlRouterMulti::matches(const ControlSessionPtr &session, const njson &params) const
{
    if (closed_ || session_.lock() != session)
    {
        return false;
    }
    const auto request = config::optional_unsigned(params, "request_id");
    const auto uuid = config::optional_unsigned(params, "uuid");
    return (ingress_ && request && params_.at("request_id") == *request) ||
           (uuid && !relay_.endpoint_.empty() && relay_.endpoint_.at("uuid") == *uuid);
}

bool ControlRouterMulti::belongs_to(const ControlSessionPtr &session) const
{
    return session_.lock() == session;
}

bool ControlRouterMulti::handle(const ControlSessionPtr &session, const CtrlMessage &message)
{
    if (!matches(session, config::message_params(message)))
        return false;
    if (message.type() == CtrlCommand::RelayReject && (ingress_ || ready_))
        return true;
    cancel(ready_ ? "transfer" : "attach", config::optional_string(config::message_params(message), "reason",
        message.type() == CtrlCommand::RelayReject ? "producer rejected relay" : "relay cancelled"));
    return true;
}

void ControlRouterMulti::handle_peer(const CtrlMessage &message)
{
    const auto &params = config::message_params(message);
    if (closed_)
        return;
    if (message.command == "relay.peer.attached" && ingress_)
        peer_attached_ = true;
    else if (message.command == "relay.peer.ready" && !ingress_)
        peer_ready_ = true;
    else if (message.command == "relay.peer.finished")
        peer_finished_ = true;
    else if (message.command == "relay.peer.close")
        cancel(params.value("stage", "control"), params.value("reason", "peer closed"), true);
    changed_.cancel();
}

asio::awaitable<void> ControlRouterMulti::watch_flow()
{
    const auto reason = co_await relay_.node_.nodelink_mgr_->wait_flow_closed(epoch_, flow_id_);
    if (!closed_)
    {
        stage_ = "flow";
        reason_ = reason;
        closed_ = true;
    }
    throw std::runtime_error(reason);
}

asio::awaitable<void> ControlRouterMulti::run()
{
    auto &node = relay_.node_;
    try
    {
        if (closed_)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        if (ingress_)
        {
            const auto submitted_epoch = epoch_;
            const auto flow = co_await asio::co_spawn(node.control_executor_, node.nodelink_mgr_->open_flow(
                params_.at("path").get<std::vector<std::string>>(),
                config::message_protocol(params_) == RelayProtocol::Udp ? RelayProtocol::Udp : RelayProtocol::Tcp),
                asio::cancel_after(deadline_ - Clock::now(), asio::use_awaitable));
            if (!flow)
            {
                stage_ = flow.stage;
                throw std::runtime_error(flow.reason);
            }
            epoch_ = flow.epoch;
            flow_id_ = flow.id;
            if (closed_ || epoch_ != submitted_epoch || epoch_ != node.topology_->epoch())
            {
                throw std::runtime_error("route epoch changed or request cancelled");
            }
            peer_opened_ = true;
            send_peer("relay.peer.open", njson{{"service", params_.at("service")}, {"protocol", params_.at("protocol")},
                {"budget_ms", std::max<std::int64_t>(1, std::chrono::ceil<std::chrono::milliseconds>(deadline_ - Clock::now()).count())},
                {"accessor", std::string(session_.lock()->peer())}});
        }
        // The watcher is a child of this instance, cancelled and drained when either task finishes.
        const auto [order, transfer_error, flow_error] = co_await asio::experimental::make_parallel_group(
            asio::co_spawn(node.control_executor_, establish_and_transfer(), asio::deferred),
            asio::co_spawn(node.control_executor_, watch_flow(), asio::deferred)
        ).async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
        if (const auto error = order.front() == 0 ? transfer_error : flow_error)
            std::rethrow_exception(error);
    }
    catch (const std::exception &)
    {
        if (reason_.empty())
        {
            reason_ = !ready_ && Clock::now() >= deadline_ ? "relay establishment timed out" :
                      exception_description(std::current_exception());
            if (!closed_)
            {
                PROXY_ERROR_PRINT("Relay failed %s service=%s stage=%s reason=%s",
                    params_.at("protocol").get<std::string>().c_str(),
                    params_.at("service").get<std::string>().c_str(), stage_.c_str(), reason_.c_str());
            }
        }
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    closed_ = true;
    if (ready_)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s", params_.at("protocol").get<std::string>().c_str(),
            params_.at("service").get<std::string>().c_str(),
            static_cast<unsigned long long>(relay_.endpoint_.at("uuid").get<std::uint64_t>()), reason_.c_str());
    }
    close_agent();
    if (peer_opened_ && !from_peer_)
    {
        send_peer("relay.peer.close", njson{{"stage", stage_}, {"reason", reason_}});
    }
    co_await relay_.close();
    if (ingress_ && flow_id_)
    {
        try
        {
            co_await node.nodelink_mgr_->close_flow(epoch_, flow_id_);
        }
        catch (const std::exception &error)
        {
            PROXY_DEBUG_PRINT("Relay Flow cleanup reason=%s", error.what());
        }
    }
}


asio::awaitable<void> ControlRouterMulti::establish_and_transfer()
{
    auto &node = relay_.node_;
    stage_ = "bind";
    // A cross-domain allocation must complete before cancellation cleanup can release its handle.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await relay_.install(ingress_ ? RelayAttach::Consumer : RelayAttach::Producer);
    if (closed_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    co_await asio::this_coro::reset_cancellation_state(asio::enable_total_cancellation());
    if (Clock::now() >= deadline_)
    {
        throw std::runtime_error("relay establishment timed out");
    }
    if (session_.expired())
    {
        throw std::runtime_error("control session disconnected");
    }
    notified_ = true;
    notify_agent();
    stage_ = "attach";
    if (!co_await asio::co_spawn(node.control_executor_, relay_.wait_attach(),
                                asio::cancel_after(deadline_ - Clock::now(), asio::use_awaitable)))
    {
        throw std::runtime_error("relay endpoint closed before attach");
    }
    stage_ = "bridge";
    // Binding must finish before cancellation cleanup can release the endpoint.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await relay_.bind(params_.value("accessor", std::string{}), epoch_, flow_id_);
    if (closed_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    co_await asio::this_coro::reset_cancellation_state(asio::enable_total_cancellation());
    if (!ingress_)
    {
        send_peer("relay.peer.attached", njson::object());
    }
    // Ingress waits for the tail's binding; egress waits for the head's activation.
    while (!(ingress_ ? peer_attached_ : peer_ready_) && !closed_)
    {
        changed_.expires_at(deadline_);
        co_await changed_.async_wait(use_nothrow_awaitable);
        if (Clock::now() >= deadline_)
        {
            throw std::runtime_error("relay establishment timed out");
        }
    }
    if (closed_ || Clock::now() >= deadline_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    co_await relay_.activate();
    if (closed_ || Clock::now() >= deadline_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    if (ingress_)
    {
        send_peer("relay.peer.ready", njson::object());
    }
    ready_agent();
    stage_ = "transfer";
    co_await relay_.bridge();
    // Both FIN directions must reach both endpoints before the ingress closes the Flow.
    send_peer("relay.peer.finished", njson::object());
    while (!peer_finished_ && !closed_)
    {
        changed_.expires_at(Clock::time_point::max());
        co_await changed_.async_wait(use_nothrow_awaitable);
    }
    if (reason_.empty())
    {
        reason_ = "stream complete";
    }
}
