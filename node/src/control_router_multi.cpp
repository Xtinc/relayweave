#include "control_router_multi.h"
#include "node_session.h"
#include "relay_node.h"
#include <asio/experimental/parallel_group.hpp>

using Clock = std::chrono::steady_clock;

ControlRouterMulti::ControlRouterMulti(NodeSession &relay, const AgentPeer &session, njson params, bool ingress)
    : relay_(relay), session_(session), params_(std::move(params)), changed_(relay.node_.control_executor_),
      ingress_(ingress)
{
    const auto protocol = config::message_protocol(params_);
    const auto &config = relay.node_.config_;
    const auto timeout = protocol == RelayProtocol::Udp   ? config.datagram.setup_timeout
                         : protocol == RelayProtocol::Tls ? config.tls.setup_timeout
                                                          : config.tcp.setup_timeout;
    deadline_ = Clock::now() + timeout;
    epoch_ = params_.at("epoch").get<std::uint64_t>();
    if (ingress_)
    {
        peer_ = params_.at("path").back().get<std::string>();
    }
    else
    {
        flow_id_ = params_.at("flow_id").get<std::uint64_t>();
        peer_ = params_.at("source").get<std::string>();
        progress_ = Progress::PeerOpened;
    }
}

void ControlRouterMulti::send_peer(CtrlCommand command, njson params) const
{
    params["epoch"] = epoch_;
    params["flow_id"] = flow_id_;
    if (relay_.node_.cluster_mgr_->send(peer_, CtrlMessage(command, std::move(params))) !=
            ClusterMgr::SendResult::Submitted &&
        command != CtrlCommand::RelayPeerClose)
    {
        throw std::runtime_error("peer control submission failed");
    }
}

std::optional<njson> ControlRouterMulti::status_report() const
{
    const auto &session = session_;
    if (progress_ != Progress::Ready || stop_requested_ || !session)
    {
        return std::nullopt;
    }
    njson report{{"mode", "multi"},
                 {"role", ingress_ ? "ingress" : "egress"},
                 {"service", params_.at("service")},
                 {"protocol", params_.at("protocol")},
                 {"epoch", epoch_},
                 {"flow_id", flow_id_},
                 {"agent_peer", std::string(session.peer())}};
    if (ingress_)
    {
        report["path"] = params_.at("path");
    }
    return report;
}

void ControlRouterMulti::notify_agent() const
{
    if (const auto &session = session_; session)
    {
        auto values = std::get<AttachInfo>(relay_.endpoint_).to_params(relay_.protocol());
        values["service"] = params_.at("service");
        values["protocol"] = params_.at("protocol");
        values["epoch"] = epoch_;
        values["flow_id"] = flow_id_;
        if (ingress_)
        {
            values["request_id"] = params_.at("request_id");
        }
        relay_.send_agent(
            session, CtrlMessage(ingress_ ? CtrlCommand::RelayOpened : CtrlCommand::RelayOffer, std::move(values)));
    }
}

void ControlRouterMulti::ready_agent()
{
    progress_ = Progress::Ready;
    if (const auto &session = session_; session)
    {
        njson values{
            {"uuid", relay_.uuid()}, {"protocol", params_.at("protocol")}, {"epoch", epoch_}, {"flow_id", flow_id_}};
        relay_.send_agent(session, CtrlMessage(CtrlCommand::RelayReady, std::move(values)));
    }
    PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", params_.at("protocol").get<std::string>().c_str(),
                     params_.at("service").get<std::string>().c_str(), static_cast<unsigned long long>(relay_.uuid()));
}

void ControlRouterMulti::close_agent() const
{
    if (const auto &session = session_; session && (ingress_ || progress_ >= Progress::AgentNotified))
    {
        const auto *endpoint = std::get_if<AttachInfo>(&relay_.endpoint_);
        auto values = endpoint ? endpoint->to_params(relay_.protocol()) : njson::object();
        values["service"] = params_.at("service");
        values["protocol"] = params_.at("protocol");
        values["epoch"] = epoch_;
        values["stage"] = stage_;
        values["reason"] = reason_;
        if (flow_id_)
        {
            values["flow_id"] = flow_id_;
        }
        if (ingress_)
        {
            values["request_id"] = params_.at("request_id");
            values["path"] = params_.at("path");
        }
        relay_.node_.send_relay(
            session,
            CtrlMessage(ingress_ && progress_ != Progress::Ready ? CtrlCommand::RelayError : CtrlCommand::RelayClosed,
                        std::move(values)));
    }
}

void ControlRouterMulti::cancel(std::string stage, std::string reason, bool from_peer)
{
    if (stop_requested_)
    {
        return;
    }

    stop_requested_ = true;
    stage_ = std::move(stage);
    reason_ = std::move(reason);
    from_peer_ = from_peer;
    relay_.cancellation_.emit(asio::cancellation_type::all);
    changed_.cancel();
}

bool ControlRouterMulti::matches(const AgentPeer &session, const njson &params) const
{
    if (stop_requested_ || session_ != session)
    {
        return false;
    }

    const auto request = config::optional_unsigned(params, "request_id");
    const auto uuid = config::optional_unsigned(params, "uuid");
    const auto id = relay_.uuid();
    return (ingress_ && request && params_.at("request_id") == *request) || (uuid && id && id == *uuid);
}

bool ControlRouterMulti::handle(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message)
{
    const auto &params = *message.params;
    const bool opening = command == CtrlCommand::RelayOpen;
    if (opening && !ingress_)
    {
        return false;
    }
    if (!matches(session, params))
    {
        return false;
    }

    if (opening)
    {
        for (const auto *field : {"path", "epoch", "service", "protocol"})
        {
            if (params_.at(field) != params.at(field))
            {
                throw std::invalid_argument("relay request conflicts with existing request");
            }
        }
        if (progress_ >= Progress::AgentNotified)
        {
            notify_agent();
        }
        return true;
    }

    if (command == CtrlCommand::RelayReject && (ingress_ || progress_ == Progress::Ready))
    {
        return true;
    }

    const auto fallback = command == CtrlCommand::RelayReject ? "producer rejected relay" : "relay cancelled";
    cancel(progress_ == Progress::Ready ? "transfer" : "prepare", config::optional_string(params, "reason", fallback));
    return true;
}

void ControlRouterMulti::control_disconnected(const AgentPeer &session)
{
    if (session_ == session)
    {
        cancel("control", ingress_ ? "consumer control disconnected" : "producer control disconnected");
    }
}

void ControlRouterMulti::invalidate(std::uint64_t epoch, std::string reason)
{
    if (!reason.empty() || epoch_ != epoch ||
        (!session_.node.empty() && !relay_.node_.topology_->members().contains(session_.node)))
    {
        cancel("control", reason.empty() ? "flow epoch changed" : std::move(reason));
    }
}

bool ControlRouterMulti::handle_peer(CtrlCommand command, const CtrlMessage &message)
{
    const auto &params = *message.params;
    if (params.at("epoch") != epoch_ || params.at("flow_id") != flow_id_ || params.at("source") != peer_)
    {
        return false;
    }
    if (stop_requested_)
    {
        return true;
    }

    if ((command == CtrlCommand::RelayPeerAttached && ingress_) ||
        (command == CtrlCommand::RelayPeerReady && !ingress_))
    {
        peer_prepared_ = true;
    }
    else if (command == CtrlCommand::RelayPeerFinished)
    {
        peer_finished_ = true;
    }
    else if (command == CtrlCommand::RelayPeerClose)
    {
        cancel(params.value("stage", "control"), params.value("reason", "peer closed"), true);
        return true;
    }
    else
    {
        return true;
    }
    changed_.cancel();
    return true;
}

asio::awaitable<void> ControlRouterMulti::watch_flow()
{
    const auto reason = co_await relay_.node_.nodelink_mgr_->wait_flow_closed(epoch_, flow_id_);
    if (!stop_requested_)
    {
        stage_ = "flow";
        reason_ = reason;
        stop_requested_ = true;
    }
    co_return;
}

asio::awaitable<void> ControlRouterMulti::run()
{
    auto &node = relay_.node_;
    try
    {
        if (stop_requested_)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }

        if (ingress_)
        {
            const auto submitted_epoch = epoch_;
            const auto flow = co_await asio::co_spawn(
                node.control_executor_,
                node.nodelink_mgr_->open_flow(params_.at("path").get<std::vector<std::string>>(),
                                              relay_.protocol() == RelayProtocol::Udp ? RelayProtocol::Udp
                                                                                      : RelayProtocol::Tcp),
                asio::cancel_at(deadline_, asio::use_awaitable));
            if (!flow)
            {
                stage_ = flow.stage;
                throw std::runtime_error(flow.reason);
            }

            epoch_ = flow.epoch;
            flow_id_ = flow.id;
            if (stop_requested_ || epoch_ != submitted_epoch || epoch_ != node.topology_->epoch())
            {
                throw std::runtime_error("route epoch changed or request cancelled");
            }
            const auto &session = session_;
            progress_ = Progress::PeerOpened;
            send_peer(CtrlCommand::RelayPeerOpen, njson{{"service", params_.at("service")},
                                                        {"protocol", params_.at("protocol")},
                                                        {"accessor", std::string(session.peer())}});
        }
        const auto [order, transfer_error, flow_error] =
            co_await asio::experimental::make_parallel_group(
                asio::co_spawn(node.control_executor_, establish_and_transfer(), asio::deferred),
                asio::co_spawn(node.control_executor_, watch_flow(), asio::deferred))
                .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
        if (const auto error = order.front() == 0 ? transfer_error : flow_error)
        {
            std::rethrow_exception(error);
        }
    }
    catch (const std::exception &error)
    {
        if (reason_.empty())
        {
            reason_ = progress_ != Progress::Ready && Clock::now() >= deadline_ ? "relay establishment timed out"
                                                                                : error.what();
            if (!stop_requested_)
            {
                PROXY_ERROR_PRINT("Relay failed %s service=%s stage=%s reason=%s",
                                  params_.at("protocol").get<std::string>().c_str(),
                                  params_.at("service").get<std::string>().c_str(), stage_.c_str(), reason_.c_str());
            }
        }
    }

    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    stop_requested_ = true;
    if (progress_ == Progress::Ready)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s",
                         params_.at("protocol").get<std::string>().c_str(),
                         params_.at("service").get<std::string>().c_str(),
                         static_cast<unsigned long long>(relay_.uuid()), reason_.c_str());
    }
    close_agent();
    if (progress_ >= Progress::PeerOpened && !from_peer_)
    {
        send_peer(CtrlCommand::RelayPeerClose, njson{{"stage", stage_}, {"reason", reason_}});
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
    stage_ = "bind";
    co_await relay_.install(ingress_ ? RelayAttach::Consumer : RelayAttach::Producer);
    if (stop_requested_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    progress_ = Progress::AgentNotified;
    notify_agent();
    stage_ = "prepare";
    co_await relay_.prepare_remote(deadline_, params_.value("accessor", std::string{}), epoch_, flow_id_);
    if (stop_requested_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    if (!ingress_)
    {
        send_peer(CtrlCommand::RelayPeerAttached, njson::object());
    }
    while (!peer_prepared_ && !stop_requested_)
    {
        changed_.expires_at(deadline_);
        co_await changed_.async_wait(use_nothrow_awaitable);
        if (Clock::now() >= deadline_)
        {
            throw std::runtime_error("relay establishment timed out");
        }
    }
    if (stop_requested_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    co_await relay_.activate();
    if (stop_requested_ || Clock::now() >= deadline_)
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
    if (ingress_)
    {
        send_peer(CtrlCommand::RelayPeerReady, njson::object());
    }
    ready_agent();
    stage_ = "transfer";
    co_await relay_.bridge();
    send_peer(CtrlCommand::RelayPeerFinished, njson::object());
    while (!peer_finished_ && !stop_requested_)
    {
        changed_.expires_at(Clock::time_point::max());
        co_await changed_.async_wait(use_nothrow_awaitable);
    }

    if (reason_.empty())
    {
        reason_ = "stream complete";
    }
}
