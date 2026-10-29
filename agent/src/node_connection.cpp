#include "node_connection.h"
#include "relay_agent.h"
#include <algorithm>

NodeConnection::Operations::Operations(asio::any_io_executor executor)
    : resolver(executor), socket(executor), retry(executor)
{
}

void NodeConnection::Operations::cancel()
{
    resolver.cancel();
    asio::error_code ignored;
    socket.close(ignored);
    retry.cancel();
}

NodeConnection::NodeConnection(RelayAgent &agent, asio::any_io_executor executor, asio::ssl::context &ssl_context,
                               const AgentConfig &config)
    : agent_(agent), executor_(std::move(executor)), ssl_context_(ssl_context), config_(config),
      id_(config.host + ":" + std::to_string(config.port)), stopped_event_(executor_)
{
}

void NodeConnection::start()
{
    operations_.emplace(executor_);
    try
    {
        asio::co_spawn(executor_, run(), asio::detached);
    }
    catch (...)
    {
        operations_.reset();
        throw;
    }
}

asio::awaitable<void> NodeConnection::run()
{
    try
    {
        auto &operations = *operations_;
        auto reconnect_delay = config_.reconnect_initial_delay;
        while (running_)
        {
            std::string failure;
            const char *stage = "resolve control endpoint";
            std::shared_ptr<TLSChannel> channel;
            try
            {
                PROXY_DEBUG_PRINT("Control connecting -> %s:%u", config_.host.c_str(),
                                  static_cast<unsigned int>(config_.port));
                auto endpoints = co_await operations.resolver.async_resolve(
                    config_.host, std::to_string(config_.port),
                    asio::cancel_after(config_.connect_timeout, asio::use_awaitable));
                if (!running_)
                {
                    throw asio::system_error(asio::error::operation_aborted);
                }
                stage = "connect control endpoint";
                const auto endpoint = co_await asio::async_connect(
                    operations.socket, endpoints, asio::cancel_after(config_.connect_timeout, asio::use_awaitable));
                if (!running_)
                {
                    throw asio::system_error(asio::error::operation_aborted);
                }

                PROXY_DEBUG_PRINT("Control TCP connected -> %s:%u", endpoint.address().to_string().c_str(),
                                  static_cast<unsigned int>(endpoint.port()));
                stage = "start control TLS channel";
                channel = std::make_shared<TLSChannel>(std::move(operations.socket), ssl_context_, TLSChannelRole::C,
                                                       config_.channel);
                channel_ = channel;
                co_await channel->start(config_.host, config_.server_name);

                stage = "identify server node";
                channel->send(CtrlMessage{CtrlCommand::ServerIdentify, njson::object()});
                auto identified = co_await channel->async_receive(config_.channel.handshake_timeout);
                if (!running_)
                {
                    throw asio::system_error(asio::error::operation_aborted);
                }
                if (identified.type() != CtrlCommand::ServerIdentified)
                {
                    throw std::invalid_argument("Expected server.identified");
                }

                const auto identified_node =
                    config::required_string(config::message_params(identified), "node_id", "server.identified");
                node_id_ = identified_node;
                connected_ = true;
                reconnect_delay = config_.reconnect_initial_delay;
                agent_.connection_ready(*this);
                PROXY_INFO_PRINT("Control [+] node=%s peer=%s", node_id_.c_str(), id_.c_str());

                stage = "receive control message";
                for (;;)
                {
                    auto message = co_await channel->async_receive();
                    if (!running_)
                    {
                        break;
                    }
                    agent_.handle_control_message(*this, std::move(message));
                }
            }
            catch (const asio::system_error &exception)
            {
                if (exception.code() != asio::experimental::error::channel_cancelled &&
                    exception.code() != asio::experimental::error::channel_closed)
                {
                    failure = std::string(stage) + " failed: " + exception.what();
                }
            }
            catch (...)
            {
                failure = std::string(stage) + " failed: " + exception_description(std::current_exception());
            }

            channel_.reset();
            if (connected_)
            {
                PROXY_INFO_PRINT("Control [x] node=%s peer=%s reason=%s", node_id_.c_str(), id_.c_str(),
                                 !running_         ? "connection stopping"
                                 : failure.empty() ? "channel closed"
                                                   : "error");
                connected_ = false;
                if (running_)
                {
                    agent_.connection_closed();
                }
            }

            if (channel)
            {
                co_await channel->async_disconnect();
            }

            if (!running_)
            {
                break;
            }

            if (!failure.empty())
            {
                PROXY_ERROR_PRINT("Control failed node=%s peer=%s reason=%s retry=%lldms", node_id_.c_str(),
                                  id_.c_str(), failure.c_str(),
                                  static_cast<long long>(
                                      std::chrono::duration_cast<std::chrono::milliseconds>(reconnect_delay).count()));
            }
            else
            {
                PROXY_DEBUG_PRINT("Control retry node=%s peer=%s delay=%lldms", node_id_.c_str(), id_.c_str(),
                                  static_cast<long long>(
                                      std::chrono::duration_cast<std::chrono::milliseconds>(reconnect_delay).count()));
            }

            operations.retry.expires_after(reconnect_delay);
            const auto [error] = co_await operations.retry.async_wait(use_nothrow_awaitable);
            if (error)
            {
                break;
            }

            reconnect_delay = std::min(reconnect_delay * 2, config_.reconnect_max_delay);
        }
    }
    catch (...)
    {
        if (running_)
        {
            agent_.connection_closed();
        }
        PROXY_ERROR_PRINT("Control stopped reason=%s", exception_description(std::current_exception()).c_str());
    }
    connected_ = false;
    running_ = false;
    channel_.reset();
    operations_.reset();
    stopped_event_.notify_all();
}

void NodeConnection::stop()
{
    running_ = false;
    if (operations_)
    {
        operations_->cancel();
    }
    else
    {
        stopped_event_.notify_all();
    }

    if (auto channel = channel_.lock())
    {
        channel->disconnect();
    }
}

void NodeConnection::send(CtrlMessage message) const
{
    channel_.lock()->send(std::move(message));
}

bool NodeConnection::ready() const noexcept
{
    return running_ && connected_;
}

const std::string &NodeConnection::node_id() const noexcept
{
    return node_id_;
}

ServerRoute NodeConnection::route() const
{
    return ServerRoute{channel_, node_id_};
}

asio::awaitable<void> NodeConnection::close()
{
    if (!co_await stopped_event_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}
