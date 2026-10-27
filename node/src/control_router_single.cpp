#include "control_router_single.h"
#include "node_session.h"
#include "relay_node.h"

using Clock = std::chrono::steady_clock;

ControlRouterSingle::ControlRouterSingle(NodeSession &relay, const AgentPeer &consumer,
                                         const AgentPeer &producer, njson params)
    : relay_(relay), consumer_(consumer), producer_(producer), params_(std::move(params))
{
    const auto protocol = config::message_protocol(params_);
    const auto &config = relay.node_.config_;
    const auto timeout = protocol == RelayProtocol::Udp   ? config.datagram.setup_timeout
                         : protocol == RelayProtocol::Tls ? config.tls.setup_timeout
                                                          : config.tcp.setup_timeout;
    deadline_ = Clock::now() + timeout;
}

bool ControlRouterSingle::belongs_to(const AgentPeer &session) const
{
    return consumer_ == session || producer_ == session;
}

std::optional<njson> ControlRouterSingle::status_report() const
{
    const auto &consumer = consumer_;
    const auto &producer = producer_;
    if (!ready_ || stop_requested_ || !consumer || !producer)
    {
        return std::nullopt;
    }
    return njson{{"mode", "single"},
                 {"service", params_.at("service")},
                 {"protocol", params_.at("protocol")},
                 {"uuid", relay_.uuid()},
                 {"consumer_peer", std::string(consumer.peer())},
                 {"producer_peer", std::string(producer.peer())}};
}

bool ControlRouterSingle::matches(const AgentPeer &session, const njson &params) const
{
    if (stop_requested_ || !belongs_to(session))
        return false;
    const auto uuid = config::optional_unsigned(params, "uuid", true);
    const auto request = config::optional_unsigned(params, "request_id");
    return (uuid && relay_.uuid() == *uuid) ||
           (request && consumer_ == session && params_.at("request_id") == *request);
}

bool ControlRouterSingle::handle(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message)
{
    if (command == CtrlCommand::RelayOpen)
    {
        return false;
    }
    const auto &params = *message.params;
    if (!matches(session, params))
    {
        return false;
    }
    if (command == CtrlCommand::RelayReject && (ready_ || producer_ != session))
    {
        return true;
    }
    const auto fallback = command == CtrlCommand::RelayReject ? "producer rejected relay" : "relay cancelled";
    cancel("control", config::optional_string(params, "reason", fallback));
    return true;
}

void ControlRouterSingle::control_disconnected(const AgentPeer &session)
{
    if (belongs_to(session))
    {
        cancel("control", "control session disconnected");
    }
}

void ControlRouterSingle::invalidate(std::uint64_t, std::string reason)
{
    for (const auto *route : {&consumer_, &producer_})
    {
        if (!route->node.empty() && (!reason.empty() || !relay_.node_.topology_->members().contains(route->node)))
        {
            cancel("control", reason.empty() ? "Agent primary disconnected" : std::move(reason));
            return;
        }
    }
}

void ControlRouterSingle::cancel(std::string, std::string reason)
{
    if (stop_requested_)
        return;
    stop_requested_ = true;
    reason_ = std::move(reason);
    relay_.cancellation_.emit(asio::cancellation_type::all);
}

void ControlRouterSingle::notify_agents() const
{
    const auto &pair = std::get<AttachPair>(relay_.endpoint_);
    for (const auto producer : {false, true})
    {
        if (const auto &session = producer ? producer_ : consumer_; session)
        {
            auto values = (producer ? pair.producer : pair.consumer).to_params(relay_.protocol());
            values["service"] = params_.at("service");
            values["protocol"] = params_.at("protocol");
            if (!producer)
                values["request_id"] = params_.at("request_id");
            relay_.send_agent(session,
                CtrlMessage(producer ? CtrlCommand::RelayOffer : CtrlCommand::RelayOpened, std::move(values)));
        }
    }
}

void ControlRouterSingle::ready_agents() const
{
    const njson values{{"uuid", relay_.uuid()}, {"protocol", params_.at("protocol")}};
    const auto &consumer = consumer_;
    if (consumer)
    {
        relay_.send_agent(consumer, CtrlMessage(CtrlCommand::RelayReady, values));
    }
    if (const auto &session = producer_; session && session != consumer)
    {
        relay_.send_agent(session, CtrlMessage(CtrlCommand::RelayReady, values));
    }
}

void ControlRouterSingle::close_agents() const
{
    const auto protocol = relay_.protocol();
    if (ready_ && protocol != RelayProtocol::Udp)
    {
        return;
    }
    njson values{{"request_id", params_.at("request_id")},
                 {"service", params_.at("service")},
                 {"protocol", params_.at("protocol")},
                 {"reason", reason_}};
    const auto id = relay_.uuid();
    if (id)
        values["uuid"] = id;
    const auto &consumer = consumer_;
    if (const auto &producer = producer_; producer && producer != consumer && id)
    {
        relay_.node_.send_relay(producer, CtrlMessage(CtrlCommand::RelayClosed, values));
    }
    if (consumer)
    {
        relay_.node_.send_relay(consumer,
            CtrlMessage(ready_ ? CtrlCommand::RelayClosed : CtrlCommand::RelayError, std::move(values)));
    }
}

asio::awaitable<void> ControlRouterSingle::run()
{
    try
    {
        if (stop_requested_)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        co_await relay_.install();
        if (stop_requested_)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        notify_agents();
        if (!co_await relay_.wait_attach(deadline_))
        {
            throw std::runtime_error("relay setup timed out");
        }
        {
            const auto &consumer = consumer_;
            if (!consumer)
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            co_await relay_.bind_and_activate(std::string(consumer.peer()));
        }
        if (stop_requested_ || Clock::now() >= deadline_)
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
        ready_agents();
        ready_ = true;
        PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", params_.at("protocol").get<std::string>().c_str(),
                         params_.at("service").get<std::string>().c_str(),
                         static_cast<unsigned long long>(relay_.uuid()));
        co_await relay_.bridge();
        if (reason_.empty())
        {
            reason_ = "stream complete";
        }
    }
    catch (const std::exception &error)
    {
        if (reason_.empty())
        {
            reason_ = !ready_ && Clock::now() >= deadline_ ? "relay setup timed out" : error.what();
        }
    }
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    stop_requested_ = true;
    if (ready_)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s",
                         params_.at("protocol").get<std::string>().c_str(),
                         params_.at("service").get<std::string>().c_str(),
                         static_cast<unsigned long long>(relay_.uuid()), reason_.c_str());
    }
    co_await relay_.close();
    close_agents();
}
