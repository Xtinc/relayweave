#include "node_session.h"
#include "relay_node.h"
#include <type_traits>

namespace
{
// Run one synchronous data operation, then return its result or exception to control_io.
template <typename Function>
auto invoke_on(asio::any_io_executor executor, Function function)
{
    using Result = std::invoke_result_t<Function>;
    auto initiate = [executor, function = std::move(function)](auto handler) mutable {
        const auto reply_executor = asio::get_associated_executor(handler);
        asio::dispatch(executor, [function = std::move(function), handler = std::move(handler),
                                  reply_executor, work = asio::make_work_guard(reply_executor)]() mutable {
            std::exception_ptr error;
            if constexpr (std::is_void_v<Result>)
            {
                try
                {
                    function();
                }
                catch (...)
                {
                    error = std::current_exception();
                }
                asio::post(reply_executor, [handler = std::move(handler), error, work = std::move(work)]() mutable {
                    std::move(handler)(error);
                });
            }
            else
            {
                Result result{};
                try
                {
                    result = function();
                }
                catch (...)
                {
                    error = std::current_exception();
                }
                asio::post(reply_executor, [handler = std::move(handler), error, result = std::move(result),
                                            work = std::move(work)]() mutable {
                    std::move(handler)(error, std::move(result));
                });
            }
        });
    };
    if constexpr (std::is_void_v<Result>)
    {
        return asio::async_initiate<decltype(asio::use_awaitable), void(std::exception_ptr)>(
            std::move(initiate), asio::use_awaitable);
    }
    else
    {
        return asio::async_initiate<decltype(asio::use_awaitable), void(std::exception_ptr, Result)>(
            std::move(initiate), asio::use_awaitable);
    }
}
} // namespace

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

bool NodeSession::belongs_to(const ControlSessionPtr &session) const
{
    return std::visit([&](const auto &controller) { return controller.belongs_to(session); }, control_);
}

bool NodeSession::handle(const ControlSessionPtr &session, const CtrlMessage &message)
{
    return std::visit([&](auto &controller) { return controller.handle(session, message); }, control_);
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
    endpoint_ = co_await invoke_on(data_executor(),
        [data = data_, role, single = std::holds_alternative<ControlRouterSingle>(control_)] {
            return std::visit([&](auto *manager) {
                return single ? manager->install_local_pair() : manager->install_remote_pair(role);
            }, data);
        });
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
    auto *channel = std::holds_alternative<ControlRouterMulti>(control_) ? &node_.nodelink_mgr_->channel() : nullptr;
    // Copy immutable binding values before crossing to the owning data executor.
    return invoke_on(data_executor(), [data = data_, id = uuid(), epoch, flow_id, channel,
                                       traffic = traffic_, accessor = std::move(accessor)] {
        std::visit([&](auto *manager) {
            if (channel)
                manager->bind_remote_pair(id, *channel, epoch, flow_id, traffic, accessor);
            else
                manager->bind_local_pair(id, traffic, accessor);
        }, data);
    });
}

asio::awaitable<void> NodeSession::activate()
{
    return invoke_on(data_executor(), [data = data_, id = uuid(),
                                       single = std::holds_alternative<ControlRouterSingle>(control_)] {
        std::visit([&](auto *manager) {
            if (single)
                manager->activate_local_pair(id);
            else
                manager->activate_remote_pair(id);
        }, data);
    });
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
    co_await invoke_on(data_executor(), [data = data_, id = uuid(),
                                        single = std::holds_alternative<ControlRouterSingle>(control_)] {
        std::visit([&](auto *manager) {
            if (single)
                manager->close_local_pair(id);
            else
                manager->close_remote_pair(id);
        }, data);
    });
}
