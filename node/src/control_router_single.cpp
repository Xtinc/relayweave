#include "control_router_single.h"
#include "node_session.h"
#include "relay_node.h"

using Clock = std::chrono::steady_clock;

ControlRouterSingle::ControlRouterSingle(NodeSession &relay, const ControlSessionPtr &consumer,
                                         const ControlSessionPtr &producer, njson params)
    : relay_(relay), consumer_(consumer), producer_(producer), params_(std::move(params))
{
    const auto protocol = config::message_protocol(params_);
    const auto &config = relay.node_.config_;
    const auto timeout = protocol == RelayProtocol::Udp   ? config.datagram.setup_timeout
                         : protocol == RelayProtocol::Tls ? config.tls.setup_timeout
                                                          : config.tcp.setup_timeout;
    deadline_ = Clock::now() + timeout;
}

bool ControlRouterSingle::belongs_to(const ControlSessionPtr &session) const
{
    return consumer_.lock() == session || producer_.lock() == session;
}

std::optional<njson> ControlRouterSingle::status_report() const
{
    const auto consumer = consumer_.lock();
    const auto producer = producer_.lock();
    if (!ready_ || closed_ || !consumer || !producer)
    {
        return std::nullopt;
    }
    return njson{{"mode", "single"}, {"service", params_.at("service")},
                 {"protocol", params_.at("protocol")}, {"uuid", relay_.uuid()},
                 {"consumer_peer", std::string(consumer->peer())},
                 {"producer_peer", std::string(producer->peer())}};
}

bool ControlRouterSingle::matches(const ControlSessionPtr &session, const njson &params) const
{
    if (closed_ || !belongs_to(session))
        return false;
    const auto uuid = config::optional_unsigned(params, "uuid", true);
    const auto request = config::optional_unsigned(params, "request_id");
    return (uuid && relay_.uuid() == *uuid) ||
           (request && consumer_.lock() == session && params_.at("request_id") == *request);
}

bool ControlRouterSingle::handle(const ControlSessionPtr &session, const CtrlMessage &message)
{
    if (!matches(session, config::message_params(message)))
        return false;
    if (message.type() == CtrlCommand::RelayReject && (ready_ || producer_.lock() != session))
        return true;
    cancel("control", config::optional_string(config::message_params(message), "reason",
                                              message.type() == CtrlCommand::RelayReject ? "producer rejected relay"
                                                                                         : "relay cancelled"));
    return true;
}

void ControlRouterSingle::cancel(std::string, std::string reason)
{
    if (closed_)
        return;
    closed_ = true;
    reason_ = std::move(reason);
    relay_.cancellation_.emit(asio::cancellation_type::all);
}

void ControlRouterSingle::notify_agents() const
{
    for (const auto producer : {false, true})
    {
        if (auto session = producer ? producer_.lock() : consumer_.lock())
        {
            auto values = relay_.endpoint_.at(producer ? "producer" : "consumer");
            values["service"] = params_.at("service");
            values["protocol"] = params_.at("protocol");
            if (!producer)
                values["request_id"] = params_.at("request_id");
            session->send(
                CtrlMessage(producer ? CtrlCommand::RelayOffer : CtrlCommand::RelayOpened, std::move(values)));
        }
    }
}

void ControlRouterSingle::ready_agents() const
{
    const njson values{{"uuid", relay_.uuid()}, {"protocol", params_.at("protocol")}};
    const auto consumer = consumer_.lock();
    if (consumer)
    {
        consumer->send(CtrlMessage(CtrlCommand::RelayReady, values));
    }
    // One frame reaches both roles when they share this Agent control connection.
    if (auto session = producer_.lock(); session && session != consumer)
    {
        session->send(CtrlMessage(CtrlCommand::RelayReady, values));
    }
}

void ControlRouterSingle::close_agents() const
{
    const auto protocol = config::message_protocol(params_);
    // Stream EOF is carried by data sockets; UDP requires an explicit lifetime notification.
    if (ready_ && protocol != RelayProtocol::Udp)
        return;
    njson values{{"request_id", params_.at("request_id")},
                 {"service", params_.at("service")},
                 {"protocol", params_.at("protocol")},
                 {"reason", reason_}};
    const auto id = relay_.uuid();
    if (id)
        values["uuid"] = id;
    const auto consumer = consumer_.lock();
    if (auto producer = producer_.lock(); producer && producer != consumer && id)
        producer->send(CtrlMessage(CtrlCommand::RelayClosed, values));
    if (consumer)
        consumer->send(CtrlMessage(ready_ ? CtrlCommand::RelayClosed : CtrlCommand::RelayError, std::move(values)));
}

asio::awaitable<void> ControlRouterSingle::run()
{
    try
    {
        if (closed_)
            throw asio::system_error(asio::error::operation_aborted);
        co_await relay_.install();
        if (closed_ || consumer_.expired() || producer_.expired())
            throw asio::system_error(asio::error::operation_aborted);
        notify_agents();
        if (Clock::now() >= deadline_ ||
            !co_await relay_.wait_attach(deadline_))
            throw std::runtime_error("relay setup timed out");
        {
            const auto consumer = consumer_.lock();
            if (closed_ || !consumer || producer_.expired())
            {
                throw asio::system_error(asio::error::operation_aborted);
            }
            co_await relay_.bind(std::string(consumer->peer()));
        }
        co_await relay_.activate();
        if (closed_ || Clock::now() >= deadline_)
            throw asio::system_error(asio::error::operation_aborted);
        ready_ = true;
        ready_agents();
        PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", params_.at("protocol").get<std::string>().c_str(),
                         params_.at("service").get<std::string>().c_str(),
                         static_cast<unsigned long long>(relay_.uuid()));
        co_await relay_.bridge();
        if (reason_.empty())
        {
            reason_ = "stream complete";
        }
    }
    catch (const std::exception &)
    {
        if (reason_.empty())
            reason_ = !ready_ && Clock::now() >= deadline_ ? "relay setup timed out"
                                                           : exception_description(std::current_exception());
    }
    // Cancellation can end the business task, but resource cleanup must still finish.
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    closed_ = true;
    if (ready_)
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=%s",
                         params_.at("protocol").get<std::string>().c_str(),
                         params_.at("service").get<std::string>().c_str(),
                         static_cast<unsigned long long>(relay_.uuid()), reason_.c_str());
    // Release capacity and attached sockets before telling the requester it can retry.
    co_await relay_.close();
    close_agents();
}
