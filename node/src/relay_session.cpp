#include "relay_session.h"
#include "relay_node.h"

RelaySession::RelaySession(RelayNode &node, const ControlSessionPtr &session, njson params, bool ingress, SRVTrafficPtr traffic)
    : node_(node), control_(std::in_place_type<ControlRouterMulti>, *this, session, params, ingress), traffic_(std::move(traffic))
{
    select_data(config::message_protocol(params));
}

RelaySession::RelaySession(RelayNode &node, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                           njson params, SRVTrafficPtr traffic)
    : node_(node), control_(std::in_place_type<ControlRouterSingle>, *this, consumer, producer, params), traffic_(std::move(traffic))
{
    select_data(config::message_protocol(params));
}

void RelaySession::select_data(RelayProtocol protocol)
{
    if (protocol == RelayProtocol::Udp)
    {
        data_ = node_.datagram_mgr_.get();
    }
    else if (protocol == RelayProtocol::Tls)
    {
        data_ = node_.tls_pipeline_.get();
    }
    else
    {
        data_ = node_.tcp_pipeline_.get();
    }
}

asio::any_io_executor RelaySession::data_executor() const
{
    return std::holds_alternative<DatagramMgr *>(data_) ? node_.transfer_udp_executor_ : node_.transfer_tcp_executor_;
}

asio::awaitable<void> RelaySession::run()
{
    co_await std::visit([](auto &controller) { return controller.run(); }, control_);
}

void RelaySession::cancel(std::string stage, std::string reason)
{
    std::visit([&](auto &controller) { controller.cancel(std::move(stage), std::move(reason)); }, control_);
}

bool RelaySession::belongs_to(const ControlSessionPtr &session) const
{
    return std::visit([&](const auto &controller) { return controller.belongs_to(session); }, control_);
}

bool RelaySession::handle(const ControlSessionPtr &session, const CtrlMessage &message)
{
    return std::visit([&](auto &controller) { return controller.handle(session, message); }, control_);
}

std::uint64_t RelaySession::uuid() const
{
    if (endpoint_.empty())
    {
        return 0;
    }
    const auto &endpoint = std::holds_alternative<ControlRouterSingle>(control_) ? endpoint_.at("consumer") : endpoint_;
    return config::require_unsigned(endpoint, "uuid", true);
}

asio::awaitable<void> RelaySession::install(int role)
{
    endpoint_ = co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, role, single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<njson> {
            co_return single ? manager->install_pair() : manager->install_endpoint(role);
        }, asio::use_awaitable);
    }, data_);
}

asio::awaitable<bool> RelaySession::wait_attach()
{
    co_return co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, id = uuid(), single = std::holds_alternative<ControlRouterSingle>(control_)]()
            -> asio::awaitable<bool> {
                co_return single ? co_await manager->wait_pair(id) : co_await manager->wait_endpoint(id);
            }, asio::use_awaitable);
    }, data_);
}

asio::awaitable<void> RelaySession::bind(std::string accessor, std::uint64_t epoch, std::uint64_t flow_id)
{
    auto *channel = std::holds_alternative<ControlRouterMulti>(control_) ? &node_.nodelink_mgr_->channel() : nullptr;
    // Copy immutable binding values before crossing to the owning data executor.
    co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, id = uuid(), epoch, flow_id, channel, traffic = traffic_, accessor]() -> asio::awaitable<void> {
            if (channel)
            {
                manager->bind_endpoint(id, *channel, epoch, flow_id, traffic, accessor);
            }
            else
            {
                manager->bind_pair(id, traffic, accessor);
            }
            co_return;
        }, asio::use_awaitable);
    }, data_);
}

asio::awaitable<void> RelaySession::activate()
{
    co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, id = uuid(), single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<void> {
            if (single)
            {
                manager->activate_pair(id);
            }
            else
            {
                manager->activate_endpoint(id);
            }
            co_return;
        }, asio::use_awaitable);
    }, data_);
}

asio::awaitable<void> RelaySession::bridge()
{
    co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, id = uuid(), single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<void> {
            if (single)
            {
                co_await manager->run_pair(id);
            }
            else
            {
                co_await manager->run_endpoint(id);
            }
        }, asio::use_awaitable);
    }, data_);
}

asio::awaitable<void> RelaySession::close()
{
    if (endpoint_.empty())
    {
        co_return;
    }
    co_await std::visit([&](auto *manager) {
        return asio::co_spawn(data_executor(), [manager, id = uuid(), single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<void> {
            if (single)
            {
                manager->close_pair(id);
            }
            else
            {
                manager->close_endpoint(id);
            }
            co_return;
        }, asio::use_awaitable);
    }, data_);
}
