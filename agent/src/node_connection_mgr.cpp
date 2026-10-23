#include "node_connection_mgr.h"
#include "relay_agent.h"

NodeConnectionMgr::NodeConnectionMgr(RelayAgent &agent, asio::any_io_executor executor,
                                     asio::ssl::context &ssl_context, const AgentConfig &config)
    : agent_(agent), executor_(std::move(executor)), ssl_context_(ssl_context), config_(config),
      primary_connection_id_(config.host + ":" + std::to_string(config.port)), stopped_event_(executor_)
{
}

void NodeConnectionMgr::start()
{
    ensure_connection({}, config_.host, config_.port);
}

std::shared_ptr<NodeConnection> NodeConnectionMgr::ensure_connection(std::string node_id, std::string host, std::uint16_t port)
{
    if (auto connection = find_node(node_id))
    {
        return connection;
    }

    const auto id = host + ":" + std::to_string(port);
    if (const auto existing = connections_.find_primary(id))
    {
        if (!node_id.empty())
        {
            if (!(*existing)->node_id().empty() && (*existing)->node_id() != node_id)
            {
                throw std::invalid_argument("Server endpoint belongs to a different node_id");
            }

            connections_.set_secondary(id, node_id);
            (*existing)->node_id_ = std::move(node_id);
        }
        (*existing)->idle_since_.reset();
        return *existing;
    }

    auto secondary = node_id.empty() ? std::optional<std::string>{} : std::optional<std::string>{node_id};
    auto connection = std::make_shared<NodeConnection>(std::move(node_id), std::move(host), port);
    connections_.insert(id, std::move(secondary), connection);

    try
    {
        asio::co_spawn(executor_, connection->run(ssl_context_, config_, *this, agent_),
                       [this, connection = connection.get()](std::exception_ptr failure) {
                           connection_finished(*connection, failure);
                       });
    }
    catch (...)
    {
        connections_.erase_primary(id);
        throw;
    }
    return connection;
}

void NodeConnectionMgr::collect_idle_connections(std::chrono::steady_clock::time_point now)
{
    connections_.for_each([&](const std::string &id, const std::shared_ptr<NodeConnection> &connection) {
        if (id == primary_connection_id_ || !connection->running_)
        {
            return;
        }
        if (connection.use_count() != 1)
        {
            connection->idle_since_.reset();
        }
        else if (!connection->idle_since_)
        {
            connection->idle_since_ = now;
        }
        else if (now - *connection->idle_since_ >= CONNECTION_IDLE_TIMEOUT)
        {
            connection->stop();
        }
    });
}

std::shared_ptr<NodeConnection> NodeConnectionMgr::find_node(const std::string &node)
{
    if (const auto connection = connections_.find_secondary(node))
    {
        (*connection)->idle_since_.reset();
        return *connection;
    }
    return {};
}

const NodeConnection *NodeConnectionMgr::primary() const
{
    const auto connection = connections_.find_primary(primary_connection_id_);
    return connection ? connection->get() : nullptr;
}

bool NodeConnectionMgr::is_primary(const NodeConnection &connection) const
{
    return connection.id() == primary_connection_id_;
}

void NodeConnectionMgr::connection_ready(NodeConnection &connection)
{
    if (!connections_.set_secondary(connection.id(), connection.node_id()))
    {
        connection.stop();
        throw std::invalid_argument("Server node_id is already connected through another endpoint");
    }
    agent_.connection_ready(connection);
}

void NodeConnectionMgr::connection_finished(NodeConnection &connection, std::exception_ptr failure)
{
    if (!stopping_ && (failure || connection.running_))
    {
        agent_.connection_closed(connection);
    }
    agent_.connection_finished(connection);
    connections_.erase_primary(connection.id());
    if (failure)
    {
        PROXY_ERROR_PRINT("Control stopped reason=%s", exception_description(failure).c_str());
    }
    if (stopping_ && connections_.size() == 0)
    {
        stopped_event_.notify_all();
    }
}

void NodeConnectionMgr::stop()
{
    stopping_ = true;
    connections_.for_each([](const std::string &, const std::shared_ptr<NodeConnection> &connection) {
        connection->stop();
    });
    if (connections_.size() == 0)
    {
        stopped_event_.notify_all();
    }
}

asio::awaitable<void> NodeConnectionMgr::close()
{
    if (!co_await stopped_event_.wait())
    {
        throw asio::system_error(asio::error::operation_aborted);
    }
}
