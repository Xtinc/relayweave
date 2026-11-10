#include "node_session.h"
#include "relay_node.h"
#include <asio/experimental/parallel_group.hpp>
#include <cassert>
#include <tuple>

using Clock = std::chrono::steady_clock;

NodeSession::NodeSession(RelayNode &node, RelayRequest &params, SRVTrafficPtr traffic)
    : node_(node), protocol_(params.protocol), service_(std::move(params.service)), request_id_(params.request_id),
      deadline_(Clock::now() + (protocol_ == RelayProtocol::Udp ? node.config_.datagram.setup_timeout
                                                                : node.config_.tcp.setup_timeout)),
      traffic_(std::move(traffic))
{
}

SingleSession::SingleSession(RelayNode &node, const AgentPeer &consumer, const AgentPeer &producer, RelayRequest params,
                             SRVTrafficPtr traffic)
    : NodeSession(node, params, std::move(traffic)), consumer_(consumer), producer_(producer)
{
}

MultiSession::MultiSession(RelayNode &node, const AgentPeer &agent, RelayRequest params, bool ingress,
                           SRVTrafficPtr traffic)
    : NodeSession(node, params, std::move(traffic)), agent_(agent), path_(std::move(params.path)),
      accessor_(std::move(params.accessor)), epoch_(params.epoch), flow_id_(ingress ? 0 : params.flow_id),
      peer_(ingress ? path_.back() : std::move(params.peer)), changed_(node.control_executor_), ingress_(ingress)
{
    if (!ingress_)
    {
        progress_ = Progress::PeerOpened;
    }
}

void NodeSession::cancel(std::string stage, std::string reason)
{
    if (close_reason_)
    {
        return;
    }
    stage_ = std::move(stage);
    close_reason_ = std::move(reason);
    cancellation_.emit(asio::cancellation_type::all);
}

RelayProtocol NodeSession::protocol() const noexcept
{
    return protocol_;
}

asio::cancellation_slot NodeSession::cancellation_slot() noexcept
{
    return cancellation_.slot();
}

bool NodeSession::handle_open(const AgentPeer &, const RelayRequest &)
{
    return false;
}

bool NodeSession::handle_peer(CtrlCommand, const PeerEvent &)
{
    return false;
}

void NodeSession::start_transfer(SessionTransfer::Setup setup)
{
    stage_ = "bind";
    RelayTransport &transport =
        protocol_ == RelayProtocol::Udp ? static_cast<RelayTransport &>(*node_.datagram_mgr_) : *node_.tcp_pipeline_;
    transfer_ = std::make_shared<SessionTransfer>(node_.control_executor_, transport, std::move(setup));
    transfer_->start();
}

void NodeSession::authorize_attach(const RelayAttach &request, const AttachInfo &expected)
{
    if (Clock::now() >= deadline_ || protocol_ == RelayProtocol::Udp || !request.connection_id ||
        request.ticket != expected.ticket)
    {
        return;
    }
    try
    {
        transfer_->attach(request);
    }
    catch (const std::exception &error)
    {
        cancel("attach", error.what());
    }
}

void NodeSession::send_agent(const AgentPeer &route, CtrlMessage message) const
{
    if (!node_.send_relay(route, std::move(message)))
    {
        throw std::runtime_error("Agent control unavailable");
    }
}

asio::awaitable<void> SingleSession::run()
{
    try
    {
        if (!close_reason_)
        {
            start_transfer({deadline_, std::move(traffic_), std::string(consumer_.peer())});
            endpoint_ = std::get<AttachPair>(co_await transfer_->installed());
        }
        if (!close_reason_)
        {
            progress_ = Progress::AgentNotified;
            notify_agents();
            stage_ = "prepare";
            co_await transfer_->prepared();
            co_await transfer_->activate();
        }
        if (!close_reason_)
        {
            if (Clock::now() >= deadline_)
            {
                throw asio::system_error(asio::error::timed_out);
            }
            ready_agents();
            progress_ = Progress::Ready;
            PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", relay_protocol_name(protocol_).data(),
                             service_.c_str(), static_cast<unsigned long long>(uuid()));
            stage_ = "transfer";
            co_await transfer_->transfer();
        }
    }
    catch (const std::exception &error)
    {
        if (!close_reason_)
        {
            close_reason_ =
                progress_ != Progress::Ready && Clock::now() >= deadline_ ? "relay setup timed out" : error.what();
        }
    }
    catch (...)
    {
        if (!close_reason_)
        {
            close_reason_ = progress_ != Progress::Ready && Clock::now() >= deadline_ ? "relay setup timed out"
                                                                                      : "unknown exception";
        }
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    if (!close_reason_)
    {
        close_reason_ = "stream complete";
    }
    if (progress_ == Progress::Ready)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s", relay_protocol_name(protocol_).data(),
                         service_.c_str(), static_cast<unsigned long long>(uuid()), close_reason_->c_str());
    }
    if (transfer_)
    {
        co_await transfer_->stop();
    }
    close_agents();
}

void SingleSession::control_disconnected(const AgentPeer &session)
{
    if (consumer_ == session || producer_ == session)
    {
        cancel("control", "control session disconnected");
    }
}

void SingleSession::invalidate(std::uint64_t, std::string reason)
{
    for (const auto *route : {&consumer_, &producer_})
    {
        if (!route->node.empty() && (!reason.empty() || !node_.topology_->members().contains(route->node)))
        {
            cancel("control", reason.empty() ? "Agent primary disconnected" : std::move(reason));
            return;
        }
    }
}

bool SingleSession::handle_cancel(CtrlCommand command, const AgentPeer &session, const CancelRequest &request)
{
    if (close_reason_ || (consumer_ != session && producer_ != session))
    {
        return false;
    }
    const bool request_matches = request.request_id && request_id_ == *request.request_id && consumer_ == session;
    if (!request_matches && !(request.uuid && uuid() == *request.uuid))
    {
        return false;
    }
    if (command == CtrlCommand::RelayReject && (progress_ == Progress::Ready || producer_ != session))
    {
        return true;
    }
    cancel("control", request.reason);
    return true;
}

bool SingleSession::handle_attach(const AgentPeer &session, const RelayAttach &request)
{
    if (close_reason_ || request.uuid != uuid() || (consumer_ != session && producer_ != session))
    {
        return false;
    }
    if ((request.role == RelayAttach::Producer ? producer_ : consumer_) == session)
    {
        authorize_attach(request, request.role == RelayAttach::Producer ? endpoint_.producer : endpoint_.consumer);
    }
    return true;
}

std::optional<njson> SingleSession::status_report() const
{
    if (progress_ != Progress::Ready || close_reason_ || !consumer_ || !producer_)
    {
        return std::nullopt;
    }
    return njson{{"mode", "single"},
                 {"service", service_},
                 {"protocol", relay_protocol_name(protocol_)},
                 {"uuid", uuid()},
                 {"consumer_peer", std::string(consumer_.peer())},
                 {"producer_peer", std::string(producer_.peer())}};
}

void SingleSession::notify_agents() const
{
    for (const auto producer : {false, true})
    {
        auto values = (producer ? endpoint_.producer : endpoint_.consumer).to_params(protocol_);
        values["service"] = service_;
        values["protocol"] = relay_protocol_name(protocol_);
        if (!producer)
        {
            values["request_id"] = request_id_;
        }
        send_agent(producer ? producer_ : consumer_,
                   CtrlMessage(producer ? CtrlCommand::RelayOffer : CtrlCommand::RelayOpened, std::move(values)));
    }
}

void SingleSession::ready_agents() const
{
    njson values{{"uuid", uuid()}, {"protocol", relay_protocol_name(protocol_)}};
    send_agent(consumer_, CtrlMessage(CtrlCommand::RelayReady, values));
    if (producer_ != consumer_)
    {
        send_agent(producer_, CtrlMessage(CtrlCommand::RelayReady, std::move(values)));
    }
}

void SingleSession::close_agents() const
{
    if (progress_ == Progress::Ready && protocol_ != RelayProtocol::Udp)
    {
        return;
    }
    njson values{{"request_id", request_id_},
                 {"service", service_},
                 {"protocol", relay_protocol_name(protocol_)},
                 {"reason", *close_reason_}};
    if (uuid())
    {
        values["uuid"] = uuid();
    }
    if (producer_ && producer_ != consumer_ && uuid())
    {
        node_.send_relay(producer_, CtrlMessage(CtrlCommand::RelayClosed, values));
    }
    if (consumer_)
    {
        node_.send_relay(consumer_,
                         CtrlMessage(progress_ == Progress::Ready ? CtrlCommand::RelayClosed : CtrlCommand::RelayError,
                                     std::move(values)));
    }
}

asio::awaitable<void> MultiSession::run()
{
    std::exception_ptr failure;
    try
    {
        if (!close_reason_)
        {
            if (ingress_)
            {
                const auto submitted_epoch = epoch_;
                const auto flow = co_await asio::co_spawn(
                    node_.control_executor_,
                    node_.nodelink_mgr_->open_flow(path_, protocol_ == RelayProtocol::Udp ? RelayProtocol::Udp
                                                                                          : RelayProtocol::Tcp),
                    asio::cancel_at(deadline_, asio::use_awaitable));
                if (!flow)
                {
                    if (!close_reason_)
                    {
                        stage_ = flow.stage;
                    }
                    throw std::runtime_error(flow.reason);
                }
                epoch_ = flow.epoch;
                flow_id_ = flow.id;
                if (close_reason_ || epoch_ != submitted_epoch || epoch_ != node_.topology_->epoch())
                {
                    throw std::runtime_error("route epoch changed or request cancelled");
                }
                progress_ = Progress::PeerOpened;
                send_peer(CtrlCommand::RelayPeerOpen, njson{{"service", service_},
                                                            {"protocol", relay_protocol_name(protocol_)},
                                                            {"accessor", std::string(agent_.peer())}});
            }
            const auto [order, transfer_error, flow_error] =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(node_.control_executor_, establish_and_transfer(), asio::deferred),
                    asio::co_spawn(node_.control_executor_, watch_flow(), asio::deferred))
                    .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);
            failure = order.front() == 0 ? transfer_error : flow_error;
        }
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    if (failure && !close_reason_)
    {
        close_reason_ = progress_ != Progress::Ready && Clock::now() >= deadline_ ? "relay establishment timed out"
                                                                                  : exception_description(failure);
        PROXY_ERROR_PRINT("Relay failed %s service=%s stage=%s reason=%s", relay_protocol_name(protocol_).data(),
                          service_.c_str(), stage_.c_str(), close_reason_->c_str());
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    if (!close_reason_)
    {
        close_reason_ = "stream complete";
    }
    if (progress_ == Progress::Ready)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s", relay_protocol_name(protocol_).data(),
                         service_.c_str(), static_cast<unsigned long long>(uuid()), close_reason_->c_str());
    }
    close_agents();
    if (progress_ >= Progress::PeerOpened && !from_peer_)
    {
        send_peer(CtrlCommand::RelayPeerClose, njson{{"stage", stage_}, {"reason", *close_reason_}});
    }
    if (transfer_)
    {
        co_await transfer_->stop();
    }
    if (ingress_ && flow_id_)
    {
        try
        {
            co_await node_.nodelink_mgr_->close_flow(epoch_, flow_id_);
        }
        catch (const std::exception &error)
        {
            PROXY_DEBUG_PRINT("Relay Flow cleanup reason=%s", error.what());
        }
    }
}

asio::awaitable<void> MultiSession::establish_and_transfer()
{
    {
        SessionTransfer::Setup setup{deadline_, std::move(traffic_), std::move(accessor_)};
        setup.flow_protocol = &node_.nodelink_mgr_->protocol();
        setup.epoch = epoch_;
        setup.flow_id = flow_id_;
        setup.role = ingress_ ? RelayAttach::Consumer : RelayAttach::Producer;
        start_transfer(std::move(setup));
    }
    endpoint_ = std::get<AttachInfo>(co_await transfer_->installed());
    if (close_reason_)
    {
        co_return;
    }
    progress_ = Progress::AgentNotified;
    notify_agents();
    stage_ = "prepare";
    co_await transfer_->prepared();
    if (close_reason_)
    {
        co_return;
    }
    if (!ingress_)
    {
        send_peer(CtrlCommand::RelayPeerAttached, njson::object());
    }
    changed_.expires_at(deadline_);
    while (!peer_prepared_)
    {
        co_await changed_.async_wait(use_nothrow_awaitable);
        if (Clock::now() >= deadline_)
        {
            throw std::runtime_error("relay establishment timed out");
        }
        if (close_reason_)
        {
            co_return;
        }
    }
    co_await transfer_->activate();
    if (close_reason_)
    {
        co_return;
    }
    if (Clock::now() >= deadline_)
    {
        throw asio::system_error(asio::error::timed_out);
    }
    if (ingress_)
    {
        send_peer(CtrlCommand::RelayPeerReady, njson::object());
    }
    ready_agents();
    progress_ = Progress::Ready;
    PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", relay_protocol_name(protocol_).data(), service_.c_str(),
                     static_cast<unsigned long long>(uuid()));
    stage_ = "transfer";
    co_await transfer_->transfer();
    send_peer(CtrlCommand::RelayPeerFinished, njson::object());
    changed_.expires_after(node_.config_.channel.disconnect_timeout);
    while (!peer_finished_ && !close_reason_)
    {
        if (Clock::now() >= changed_.expiry())
        {
            throw asio::system_error(asio::error::timed_out, "peer Finished timed out");
        }
        co_await changed_.async_wait(use_nothrow_awaitable);
    }
}

asio::awaitable<void> MultiSession::watch_flow()
{
    const auto reason = co_await node_.nodelink_mgr_->wait_flow_closed(epoch_, flow_id_);
    if (!close_reason_)
    {
        stage_ = "flow";
        close_reason_ = reason;
    }
}

void MultiSession::control_disconnected(const AgentPeer &session)
{
    if (agent_ == session)
    {
        cancel("control", ingress_ ? "consumer control disconnected" : "producer control disconnected");
    }
}

void MultiSession::invalidate(std::uint64_t epoch, std::string reason)
{
    if (!reason.empty() || epoch_ != epoch ||
        (!agent_.node.empty() && !node_.topology_->members().contains(agent_.node)))
    {
        cancel("control", reason.empty() ? "flow epoch changed" : std::move(reason));
    }
}

bool MultiSession::handle_open(const AgentPeer &session, const RelayRequest &request)
{
    if (close_reason_ || agent_ != session || !ingress_)
    {
        return false;
    }
    if (request_id_ != request.request_id && !(request.uuid && uuid() == *request.uuid))
    {
        return false;
    }
    if (request.path != path_ || request.epoch != epoch_ || request.service != service_ || request.protocol != protocol_)
    {
        throw std::invalid_argument("relay request conflicts with existing request");
    }
    if (progress_ >= Progress::AgentNotified)
    {
        notify_agents();
    }
    return true;
}

bool MultiSession::handle_cancel(CtrlCommand command, const AgentPeer &session, const CancelRequest &request)
{
    if (close_reason_ || agent_ != session)
    {
        return false;
    }
    const bool request_matches = request.request_id && request_id_ == *request.request_id && ingress_;
    if (!request_matches && !(request.uuid && uuid() == *request.uuid))
    {
        return false;
    }
    if (command == CtrlCommand::RelayReject && (progress_ == Progress::Ready || ingress_))
    {
        return true;
    }
    cancel(progress_ == Progress::Ready ? "transfer" : "prepare", request.reason);
    return true;
}

bool MultiSession::handle_attach(const AgentPeer &session, const RelayAttach &request)
{
    if (close_reason_ || request.uuid != uuid() || agent_ != session)
    {
        return false;
    }
    if (request.role == (ingress_ ? RelayAttach::Consumer : RelayAttach::Producer))
    {
        authorize_attach(request, endpoint_);
    }
    return true;
}

bool MultiSession::handle_peer(CtrlCommand command, const PeerEvent &event)
{
    if (event.epoch != epoch_ || event.flow_id != flow_id_ || event.source != peer_)
    {
        return false;
    }
    if (close_reason_)
    {
        return true;
    }
    if (((command == CtrlCommand::RelayPeerAttached && ingress_) ||
         (command == CtrlCommand::RelayPeerReady && !ingress_)) &&
        !peer_prepared_)
    {
        peer_prepared_ = true;
    }
    else if (command == CtrlCommand::RelayPeerFinished && !peer_finished_)
    {
        peer_finished_ = true;
    }
    else if (command == CtrlCommand::RelayPeerClose)
    {
        from_peer_ = true;
        cancel(event.stage, event.reason);
        return true;
    }
    else
    {
        return true;
    }
    changed_.cancel();
    return true;
}

std::optional<njson> MultiSession::status_report() const
{
    if (progress_ != Progress::Ready || close_reason_ || !agent_)
    {
        return std::nullopt;
    }
    njson report{{"mode", "multi"},
                 {"role", ingress_ ? "ingress" : "egress"},
                 {"service", service_},
                 {"protocol", relay_protocol_name(protocol_)},
                 {"epoch", epoch_},
                 {"flow_id", flow_id_},
                 {"agent_peer", std::string(agent_.peer())}};
    if (ingress_)
    {
        report["path"] = path_;
    }
    return report;
}

void MultiSession::notify_agents() const
{
    auto values = endpoint_.to_params(protocol_);
    values["service"] = service_;
    values["protocol"] = relay_protocol_name(protocol_);
    values["epoch"] = epoch_;
    values["flow_id"] = flow_id_;
    if (ingress_)
    {
        values["request_id"] = request_id_;
    }
    send_agent(agent_, CtrlMessage(ingress_ ? CtrlCommand::RelayOpened : CtrlCommand::RelayOffer, std::move(values)));
}

void MultiSession::ready_agents() const
{
    send_agent(agent_, CtrlMessage(CtrlCommand::RelayReady, njson{{"uuid", uuid()},
                                                                  {"protocol", relay_protocol_name(protocol_)},
                                                                  {"epoch", epoch_},
                                                                  {"flow_id", flow_id_}}));
}

void MultiSession::close_agents() const
{
    if (!agent_ || (!ingress_ && progress_ < Progress::AgentNotified))
    {
        return;
    }
    auto values = endpoint_.uuid ? endpoint_.to_params(protocol_) : njson::object();
    values["service"] = service_;
    values["protocol"] = relay_protocol_name(protocol_);
    values["epoch"] = epoch_;
    values["stage"] = stage_;
    values["reason"] = *close_reason_;
    if (flow_id_)
    {
        values["flow_id"] = flow_id_;
    }
    if (ingress_)
    {
        values["request_id"] = request_id_;
        values["path"] = path_;
    }
    node_.send_relay(agent_, CtrlMessage(ingress_ && progress_ != Progress::Ready ? CtrlCommand::RelayError
                                                                                  : CtrlCommand::RelayClosed,
                                         std::move(values)));
}

void MultiSession::send_peer(CtrlCommand command, njson params) const
{
    params["epoch"] = epoch_;
    params["flow_id"] = flow_id_;
    if (node_.cluster_mgr_->send(peer_, CtrlMessage(command, std::move(params))) != ClusterMgr::SendResult::Submitted &&
        command != CtrlCommand::RelayPeerClose)
    {
        throw std::runtime_error("peer control submission failed");
    }
}

SessionTransfer::SessionTransfer(asio::any_io_executor control, RelayTransport &transport, Setup setup)
    : executor_(transport.executor()), transport_(transport), setup_(std::move(setup)), events_(std::move(control), 5),
      authorizations_(executor_, 2), proceed_(executor_, 1)
{
}

void SessionTransfer::start()
{
    assert(state_ == State::Created);
    asio::co_spawn(executor_, run(),
                   asio::bind_executor(events_.get_executor(),
                                       asio::bind_cancellation_slot(
                                           cancellation_.slot(), [self = shared_from_this()](std::exception_ptr error) {
                                               self->state_ = State::Stopped;
                                               self->events_.try_send(error, Event::Stopped, AttachResult{});
                                           })));
    state_ = State::Running;
}

asio::awaitable<AttachResult> SessionTransfer::installed()
{
    auto [event, attachment] = co_await events_.async_receive(asio::cancel_at(setup_.deadline, asio::use_awaitable));
    assert(event == Event::Installed);
    co_return std::move(attachment);
}

asio::awaitable<void> SessionTransfer::prepared()
{
    return wait(Event::Prepared);
}

asio::awaitable<void> SessionTransfer::activate()
{
    return wait(Event::Activated);
}

asio::awaitable<void> SessionTransfer::transfer()
{
    return wait(Event::Finished);
}

void SessionTransfer::attach(const RelayAttach &request)
{
    if (state_ != State::Running)
    {
        return;
    }
    auto &submitted = authorized_[request.role == RelayAttach::Producer ? 0 : 1];
    if (submitted)
    {
        if (*submitted != *request.connection_id)
        {
            throw std::invalid_argument("relay role already authorized");
        }
        return;
    }
    if (authorizations_.try_send(asio::error_code{}, request))
    {
        submitted = request.connection_id;
    }
}

asio::awaitable<void> SessionTransfer::wait(Event expected)
{
    if (expected == Event::Activated || expected == Event::Finished)
    {
        proceed_.try_send(asio::error_code{});
    }
    std::tuple<Event, AttachResult> event;
    if (expected != Event::Finished)
    {
        event = co_await events_.async_receive(asio::cancel_at(setup_.deadline, asio::use_awaitable));
    }
    else
    {
        event = co_await events_.async_receive(asio::use_awaitable);
    }
    assert(std::get<0>(event) == expected);
}

asio::awaitable<void> SessionTransfer::stop()
{
    if (state_ == State::Created)
    {
        co_return;
    }
    if (state_ == State::Running)
    {
        state_ = State::Stopping;
        asio::post(executor_, [self = shared_from_this()] { self->cancellation_.emit(asio::cancellation_type::all); });
    }
    while (state_ != State::Stopped)
    {
        co_await events_.async_receive(use_nothrow_awaitable);
    }
}

void SessionTransfer::publish(Event event, AttachResult attachment)
{
    const bool sent = events_.try_send(std::exception_ptr{}, event, std::move(attachment));
    assert(sent);
    (void)sent;
}

asio::awaitable<void> SessionTransfer::run()
{
    std::uint64_t uuid = 0;
    std::exception_ptr failure;
    try
    {
        if (Clock::now() >= setup_.deadline)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        AttachResult attachment;
        if (remote())
        {
            attachment = transport_.install_remote_pair(setup_.role);
        }
        else
        {
            attachment = transport_.install_local_pair();
        }
        uuid = remote() ? std::get<AttachInfo>(attachment).uuid : std::get<AttachPair>(attachment).consumer.uuid;
        publish(Event::Installed, std::move(attachment));
        if (remote())
        {
            co_await transport_.prepare_remote_pair(uuid, authorizations_, *setup_.flow_protocol, setup_.epoch,
                                                    setup_.flow_id, std::move(setup_.traffic),
                                                    std::move(setup_.accessor));
        }
        else
        {
            co_await transport_.prepare_local_pair(uuid, authorizations_, std::move(setup_.traffic),
                                                   std::move(setup_.accessor));
        }
        authorizations_.close();
        publish(Event::Prepared);
        co_await proceed_.async_receive(asio::use_awaitable);
        if (Clock::now() >= setup_.deadline)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        if (remote())
        {
            transport_.activate_remote_pair(uuid);
        }
        else
        {
            transport_.activate_local_pair(uuid);
        }
        publish(Event::Activated);
        co_await proceed_.async_receive(asio::use_awaitable);
        if (remote())
        {
            co_await transport_.run_remote_pair(uuid);
        }
        else
        {
            co_await transport_.run_local_pair(uuid);
        }
        publish(Event::Finished);
        co_await proceed_.async_receive(asio::use_awaitable);
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    authorizations_.close();
    proceed_.close();
    if (uuid)
    {
        if (remote())
        {
            transport_.close_remote_pair(uuid);
        }
        else
        {
            transport_.close_local_pair(uuid);
        }
    }
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}
