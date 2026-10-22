#include "node_session.h"
#include "relay_node.h"

NodeSession::NodeSession(RelayNode &node, const ControlSessionPtr &session, njson params, bool ingress,
                         SRVTrafficPtr traffic)
    : node_(node), control_(std::in_place_type<ControlRouterMulti>, *this, session, params, ingress),
      traffic_(std::move(traffic))
{
    select_data(config::message_protocol(params));
}

NodeSession::NodeSession(RelayNode &node, const ControlSessionPtr &consumer, const ControlSessionPtr &producer,
                         njson params, SRVTrafficPtr traffic)
    : node_(node), control_(std::in_place_type<ControlRouterSingle>, *this, consumer, producer, params),
      traffic_(std::move(traffic))
{
    select_data(config::message_protocol(params));
}

void NodeSession::select_data(RelayProtocol protocol)
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

asio::any_io_executor NodeSession::data_executor() const
{
    return std::holds_alternative<DatagramMgr *>(data_) ? node_.transfer_udp_executor_ : node_.transfer_tcp_executor_;
}

asio::awaitable<void> NodeSession::run()
{
    return std::visit([](auto &controller) { return controller.run(); }, control_);
}

std::optional<njson> NodeSession::status_report() const
{
    return std::visit([](const auto &controller) { return controller.status_report(); }, control_);
}

void NodeSession::cancel(std::string stage, std::string reason)
{
    std::visit([&](auto &controller) { controller.cancel(std::move(stage), std::move(reason)); }, control_);
}

void NodeSession::control_disconnected(const ControlSessionPtr &session)
{
    std::visit([&](auto &controller) { controller.control_disconnected(session); }, control_);
}

void NodeSession::invalidate(std::uint64_t epoch, std::string reason)
{
    if (auto *controller = std::get_if<ControlRouterMulti>(&control_))
    {
        controller->invalidate(epoch, std::move(reason));
    }
}

bool NodeSession::handle(CtrlCommand command, const ControlSessionPtr &session, const CtrlMessage &message)
{
    return std::visit([&](auto &controller) { return controller.handle(command, session, message); }, control_);
}

bool NodeSession::handle_peer(CtrlCommand command, const CtrlMessage &message)
{
    if (auto *controller = std::get_if<ControlRouterMulti>(&control_))
    {
        return controller->handle_peer(command, message);
    }
    return false;
}

RelayProtocol NodeSession::protocol() const noexcept
{
    if (std::holds_alternative<DatagramMgr *>(data_))
    {
        return RelayProtocol::Udp;
    }
    return std::holds_alternative<TlsPipeline *>(data_) ? RelayProtocol::Tls : RelayProtocol::Tcp;
}

bool NodeSession::is_multi() const noexcept
{
    return std::holds_alternative<ControlRouterMulti>(control_);
}

asio::cancellation_slot NodeSession::cancellation_slot() noexcept
{
    return cancellation_.slot();
}

std::uint64_t NodeSession::uuid() const
{
    if (endpoint_.empty())
    {
        return 0;
    }
    const auto &endpoint = std::holds_alternative<ControlRouterSingle>(control_) ? endpoint_.at("consumer") : endpoint_;
    return config::require_unsigned(endpoint, "uuid", true);
}

asio::awaitable<void> NodeSession::install(int role)
{
    endpoint_ = co_await asio::co_spawn(
        data_executor(),
        [data = data_, role,
         single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<njson> {
            co_return std::visit(
                [&](auto *manager) {
                    return single ? manager->install_local_pair() : manager->install_remote_pair(role);
                },
                data);
        },
        asio::use_awaitable);
}

asio::awaitable<bool> NodeSession::wait_attach(std::chrono::steady_clock::time_point deadline)
{
    return std::visit(
        [&](auto *manager) {
            auto task = std::holds_alternative<ControlRouterSingle>(control_) ? manager->wait_local_pair(uuid())
                                                                              : manager->wait_remote_pair(uuid());
            return asio::co_spawn(data_executor(), std::move(task), asio::cancel_at(deadline, asio::use_awaitable));
        },
        data_);
}

asio::awaitable<void> NodeSession::bind(std::string accessor, std::uint64_t epoch, std::uint64_t flow_id)
{
    auto *channel = std::holds_alternative<ControlRouterMulti>(control_) ? &node_.nodelink_mgr_->protocol() : nullptr;
    return std::visit(
        [&](auto *manager) -> asio::awaitable<void> {
            if (channel)
            {
                return asio::co_spawn(
                    data_executor(),
                    manager->bind_remote_pair(uuid(), *channel, epoch, flow_id, traffic_, std::move(accessor)),
                    asio::use_awaitable);
            }
            return asio::co_spawn(
                data_executor(),
                [manager, id = uuid(), traffic = traffic_, accessor = std::move(accessor)]() -> asio::awaitable<void> {
                    manager->bind_local_pair(id, traffic, accessor);
                    co_return;
                },
                asio::use_awaitable);
        },
        data_);
}

asio::awaitable<void> NodeSession::activate()
{
    return asio::co_spawn(
        data_executor(),
        [data = data_, id = uuid(),
         single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<void> {
            std::visit(
                [&](auto *manager) {
                    if (single)
                        manager->activate_local_pair(id);
                    else
                        manager->activate_remote_pair(id);
                },
                data);
            co_return;
        },
        asio::use_awaitable);
}

asio::awaitable<void> NodeSession::bridge()
{
    return std::visit(
        [&](auto *manager) {
            auto task = std::holds_alternative<ControlRouterSingle>(control_) ? manager->run_local_pair(uuid())
                                                                              : manager->run_remote_pair(uuid());
            return asio::co_spawn(data_executor(), std::move(task), asio::use_awaitable);
        },
        data_);
}

asio::awaitable<void> NodeSession::close()
{
    if (endpoint_.empty())
    {
        co_return;
    }
    co_await asio::co_spawn(
        data_executor(),
        [data = data_, id = uuid(),
         single = std::holds_alternative<ControlRouterSingle>(control_)]() -> asio::awaitable<void> {
            std::visit(
                [&](auto *manager) {
                    if (single)
                        manager->close_local_pair(id);
                    else
                        manager->close_remote_pair(id);
                },
                data);
            co_return;
        },
        asio::use_awaitable);
}
