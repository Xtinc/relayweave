#include "node_session.h"
#include "relay_node.h"

NodeSession::NodeSession(RelayNode &node, const AgentPeer &session, RelayRequest params, bool ingress,
                         SRVTrafficPtr traffic)
    : node_(node), data_manager_(select_data(params.protocol)),
      control_(std::in_place_type<ControlRouterMulti>, *this, session, std::move(params), ingress),
      traffic_(std::move(traffic))
{
}

NodeSession::NodeSession(RelayNode &node, const AgentPeer &consumer, const AgentPeer &producer, RelayRequest params,
                         SRVTrafficPtr traffic)
    : node_(node), data_manager_(select_data(params.protocol)),
      control_(std::in_place_type<ControlRouterSingle>, *this, consumer, producer, std::move(params)),
      traffic_(std::move(traffic))
{
}

NodeSession::DataManager NodeSession::select_data(RelayProtocol protocol) const
{
    if (protocol == RelayProtocol::Udp)
    {
        return node_.datagram_mgr_.get();
    }
    else if (protocol == RelayProtocol::Tls)
    {
        return node_.tls_pipeline_.get();
    }
    else
    {
        return node_.tcp_pipeline_.get();
    }
}

asio::any_io_executor NodeSession::data_executor() const
{
    return std::holds_alternative<DatagramMgr *>(data_manager_) ? node_.transfer_udp_executor_ : node_.transfer_tcp_executor_;
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

void NodeSession::control_disconnected(const AgentPeer &session)
{
    std::visit([&](auto &controller) { controller.control_disconnected(session); }, control_);
}

void NodeSession::invalidate(std::uint64_t epoch, std::string reason)
{
    std::visit([&](auto &controller) { controller.invalidate(epoch, std::move(reason)); }, control_);
}

bool NodeSession::handle(CtrlCommand command, const AgentPeer &session, const CtrlMessage &message)
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
    if (std::holds_alternative<DatagramMgr *>(data_manager_))
    {
        return RelayProtocol::Udp;
    }
    return std::holds_alternative<TlsPipeline *>(data_manager_) ? RelayProtocol::Tls : RelayProtocol::Tcp;
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
    if (const auto *endpoint = std::get_if<AttachInfo>(&endpoint_))
    {
        return endpoint->uuid;
    }
    if (const auto *pair = std::get_if<AttachPair>(&endpoint_))
    {
        return pair->consumer.uuid;
    }
    return 0;
}

asio::awaitable<void> NodeSession::install(int role)
{
    endpoint_ = co_await asio::co_spawn(
        data_executor(),
        [data = data_manager_, role, single = !is_multi()]() -> asio::awaitable<Attachment> {
            co_return std::visit(
                [&](auto *manager) -> Attachment {
                    if (single)
                    {
                        return manager->install_local_pair();
                    }
                    return manager->install_remote_pair(role);
                },
                data);
        },
        asio::use_awaitable);
}

asio::awaitable<void> NodeSession::prepare_local(std::chrono::steady_clock::time_point deadline, std::string accessor)
{
    return std::visit(
        [&](auto *manager) {
            return asio::co_spawn(data_executor(), manager->prepare_local_pair(uuid(), traffic_, std::move(accessor)),
                                  asio::cancel_at(deadline, asio::use_awaitable));
        },
        data_manager_);
}

asio::awaitable<void> NodeSession::prepare_remote(std::chrono::steady_clock::time_point deadline, std::string accessor,
                                                  std::uint64_t epoch, std::uint64_t flow_id)
{
    return std::visit(
        [&](auto *manager) {
            return asio::co_spawn(data_executor(),
                                  manager->prepare_remote_pair(uuid(), node_.nodelink_mgr_->protocol(), epoch, flow_id,
                                                               traffic_, std::move(accessor)),
                                  asio::cancel_at(deadline, asio::use_awaitable));
        },
        data_manager_);
}

asio::awaitable<void> NodeSession::activate()
{
    return asio::co_spawn(
        data_executor(),
        [data = data_manager_, id = uuid()]() -> asio::awaitable<void> {
            std::visit([&](auto *manager) { manager->activate_remote_pair(id); }, data);
            co_return;
        },
        asio::use_awaitable);
}

asio::awaitable<void> NodeSession::bridge()
{
    return std::visit(
        [&](auto *manager) {
            auto task = is_multi() ? manager->run_remote_pair(uuid()) : manager->run_local_pair(uuid());
            return asio::co_spawn(data_executor(), std::move(task), asio::use_awaitable);
        },
        data_manager_);
}

asio::awaitable<void> NodeSession::close()
{
    const auto id = uuid();
    if (!id)
    {
        co_return;
    }
    co_await asio::co_spawn(
        data_executor(),
        [data = data_manager_, id, single = !is_multi()]() -> asio::awaitable<void> {
            std::visit(
                [&](auto *manager) {
                    if (single)
                    {
                        manager->close_local_pair(id);
                    }
                    else
                    {
                        manager->close_remote_pair(id);
                    }
                },
                data);
            co_return;
        },
        asio::use_awaitable);
}

void NodeSession::send_agent(const AgentPeer &route, CtrlMessage message) const
{
    if (!node_.send_relay(route, std::move(message)))
    {
        throw std::runtime_error("Agent control unavailable");
    }
}
