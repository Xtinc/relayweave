#include "cluster_mgr.h"
#include "relay_node.h"
#include <algorithm>
#include <asio/experimental/awaitable_operators.hpp>
#include <random>
#include <set>

class ClusterParticipant
{
  public:
    virtual ~ClusterParticipant() = default;
    virtual std::string_view node_id() const noexcept = 0;
    virtual std::string_view address() const noexcept = 0;
    virtual void deliver(CtrlMessage message) = 0;
    virtual void stop() = 0;
};

class ClusterRoom
{
  public:
    ClusterRoom(asio::any_io_executor executor, std::string master_id, std::uint64_t epoch)
        : master_id_(std::move(master_id)), epoch_(epoch), changed_(executor)
    {
    }

    void join(const std::shared_ptr<ClusterParticipant> &participant)
    {
        participants_.insert(participant);
        if (!participant->node_id().empty())
        {
            publish_members();
        }
    }

    void ready()
    {
        publish_members();
    }

    bool available(std::string_view node_id) const
    {
        return !node_id.empty() && node_id != cluster_broadcast_target &&
               std::ranges::none_of(participants_,
                                    [&](const auto &participant) { return participant->node_id() == node_id; });
    }

    void leave(const std::shared_ptr<ClusterParticipant> &participant)
    {
        const auto was_ready = !participant->node_id().empty();
        participants_.erase(participant);
        changed_.cancel();
        if (was_ready && !stopping_)
        {
            publish_members();
        }
    }

    std::size_t size() const noexcept
    {
        return participants_.size();
    }

    void deliver(std::string_view source, CtrlMessage message)
    {
        if (message.command.starts_with("cluster."))
        {
            throw std::invalid_argument("cluster.* commands are reserved");
        }

        const auto target = config::required_string(config::message_params(message), "target", message.command);
        message.params->erase("target");
        (*message.params)["source"] = source;
        static_cast<void>(WireMessage::pack(message));

        if (target != cluster_broadcast_target)
        {
            const auto participant = std::ranges::find_if(
                participants_, [&](const auto &candidate) { return candidate->node_id() == target; });
            if (participant != participants_.end())
            {
                (*participant)->deliver(std::move(message));
            }
            return;
        }

        for (const auto &participant : participants_)
        {
            if (!participant->node_id().empty())
            {
                participant->deliver(message);
            }
        }
    }

    asio::awaitable<void> async_stop()
    {
        stopping_ = true;
        for (const auto &participant : participants_)
        {
            participant->stop();
        }
        while (participants_.size() > 1)
        {
            changed_.expires_at(std::chrono::steady_clock::time_point::max());
            co_await changed_.async_wait(use_nothrow_awaitable);
        }
        participants_.clear();
    }

  private:
    void publish_members()
    {
        njson members = njson::array();
        for (const auto &participant : participants_)
        {
            if (!participant->node_id().empty())
            {
                members.push_back(njson{{"node_id", participant->node_id()}, {"address", participant->address()}});
            }
        }
        std::sort(members.begin(), members.end(), [](const njson &left, const njson &right) {
            return left.at("node_id").get_ref<const std::string &>() <
                   right.at("node_id").get_ref<const std::string &>();
        });

        ++version_;
        CtrlMessage message{CtrlCommand::TopologyMembers, njson{{"epoch", epoch_},
                                                                {"version", version_},
                                                                {"master", master_id_},
                                                                {"members", std::move(members)},
                                                                {"target", cluster_broadcast_target}}};
        deliver(master_id_, std::move(message));
    }

    std::set<std::shared_ptr<ClusterParticipant>> participants_;
    std::string master_id_;
    std::uint64_t epoch_;
    std::uint64_t version_ = 0;
    asio::steady_timer changed_;
    bool stopping_ = false;
};

class ClusterSession : public ClusterParticipant, public std::enable_shared_from_this<ClusterSession>
{
  public:
    ClusterSession(asio::ip::tcp::socket socket, asio::ssl::context &context, ClusterRoom &room,
                   TLSChannelConfig config)
        : channel_(std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::S, config)), room_(room),
          join_timeout_(config.handshake_timeout)
    {
    }

    void start()
    {
        std::weak_ptr<ClusterSession> weak = shared_from_this();
        asio::co_spawn(channel_->executor(), reader(), [weak](std::exception_ptr) {
            if (auto self = weak.lock())
            {
                self->room_.leave(self);
            }
        });
    }

    std::string_view node_id() const noexcept override
    {
        return node_id_;
    }

    std::string_view address() const noexcept override
    {
        return address_;
    }

    void deliver(CtrlMessage message) override
    {
        channel_->send(std::move(message));
    }

    void stop() override
    {
        stopping_ = true;
        asio::co_spawn(channel_->executor(), channel_->async_disconnect(), asio::detached);
    }

  private:
    asio::awaitable<void> reader()
    {
        try
        {
            co_await channel_->start({});
            auto join = co_await channel_->async_receive(join_timeout_);
            if (join.type() != CtrlCommand::ClusterJoin)
            {
                throw std::invalid_argument("Expected cluster.join");
            }

            auto node_id = config::required_string(config::message_params(join), "node_id", "cluster.join");
            if (!room_.available(node_id))
            {
                deliver(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "duplicate node_id"}}});
                co_await channel_->async_receive(join_timeout_);
            }
            else
            {
                node_id_ = std::move(node_id);
                const auto &params = config::message_params(join);
                address_ = config::optional_string(params, "address", node_id_);
                if (address_.empty())
                {
                    address_ = node_id_;
                }
                deliver(CtrlMessage{CtrlCommand::ClusterJoined});
                room_.ready();
                PROXY_INFO_PRINT("Cluster [+] node=%s peer=%.*s address=%s", node_id_.c_str(),
                                 static_cast<int>(channel_->peer().size()), channel_->peer().data(), address_.c_str());
                while (!stopping_)
                {
                    room_.deliver(node_id_, co_await channel_->async_receive());
                }
            }
        }
        catch (const asio::system_error &exception)
        {
            if (exception.code() != asio::experimental::error::channel_cancelled &&
                exception.code() != asio::experimental::error::channel_closed && !stopping_)
            {
                PROXY_ERROR_PRINT("Cluster session failed node=%s reason=%s", node_id_.c_str(), exception.what());
            }
        }
        catch (const std::exception &exception)
        {
            if (!stopping_)
                PROXY_ERROR_PRINT("Cluster session failed node=%s reason=%s", node_id_.c_str(), exception.what());
        }
        co_await channel_->async_disconnect();
        if (!node_id_.empty())
        {
            PROXY_INFO_PRINT("Cluster [x] node=%s peer=%.*s address=%s", node_id_.c_str(),
                             static_cast<int>(channel_->peer().size()), channel_->peer().data(), address_.c_str());
        }
    }

    std::shared_ptr<TLSChannel> channel_;
    ClusterRoom &room_;
    std::chrono::steady_clock::duration join_timeout_;
    std::string node_id_;
    std::string address_;
    bool stopping_ = false;
};

class ClusterMgr::LocalParticipant : public ClusterParticipant
{
  public:
    LocalParticipant(std::string node_id, std::string address, ClusterMgr &manager)
        : node_id_(std::move(node_id)), address_(std::move(address)), manager_(manager)
    {
    }

    std::string_view address() const noexcept override
    {
        return address_;
    }

    std::string_view node_id() const noexcept override
    {
        return node_id_;
    }

    void deliver(CtrlMessage message) override
    {
        manager_.receive(std::move(message));
    }

    void stop() override
    {
    }

  private:
    std::string node_id_;
    std::string address_;
    ClusterMgr &manager_;
};

class ClusterMgr::Connector
{
    using tcp = asio::ip::tcp;

    struct ConnectOperations
    {
        explicit ConnectOperations(asio::any_io_executor executor)
            : resolver(executor), socket(executor), retry(executor)
        {
        }

        void cancel()
        {
            resolver.cancel();
            asio::error_code ignored;
            socket.close(ignored);
            retry.cancel();
        }

        tcp::resolver resolver;
        tcp::socket socket;
        asio::steady_timer retry;
    };

  public:
    Connector(asio::any_io_executor executor, asio::ssl::context &context, const ClusterConfig &config,
              const TLSChannelConfig &channel_config, ClusterMgr &manager)
        : executor_(executor), context_(context), config_(config), channel_config_(channel_config), manager_(manager)
    {
    }

    void send(CtrlMessage message)
    {
        if (auto channel = channel_.lock(); channel && joined_)
            channel->send(std::move(message));
        else
            manager_.receive(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "cluster disconnected"}}});
    }

    void stop()
    {
        running_ = false;
        if (joined_)
        {
            PROXY_INFO_PRINT("Cluster [x] node=%s peer=%s:%u reason=node stopping", config_.node_id.c_str(),
                             config_.address.c_str(), static_cast<unsigned int>(config_.port));
        }
        joined_ = false;
        if (operations_)
        {
            operations_->cancel();
        }
        if (auto channel = channel_.lock())
        {
            asio::co_spawn(executor_, channel->async_disconnect(), asio::detached);
        }
    }

    asio::awaitable<void> run()
    {
        ConnectOperations operations(executor_);
        operations_ = &operations;
        ScopeGuard clear_operations([this]() noexcept { operations_ = nullptr; });
        constexpr auto retry_interval = std::chrono::seconds(5);
        while (running_)
        {
            if (channel_.expired())
            {
                joined_ = false;
                std::shared_ptr<TLSChannel> channel;
                try
                {
                    PROXY_DEBUG_PRINT("Cluster connecting -> %s:%u", config_.address.c_str(),
                                      static_cast<unsigned int>(config_.port));
                    auto endpoints = co_await operations.resolver.async_resolve(
                        config_.address, std::to_string(config_.port),
                        asio::cancel_after(std::chrono::seconds(5), asio::use_awaitable));
                    if (!running_)
                    {
                        break;
                    }

                    operations.socket = tcp::socket(executor_);
                    co_await asio::async_connect(operations.socket, endpoints,
                                                 asio::cancel_after(std::chrono::seconds(5), asio::use_awaitable));
                    if (!running_)
                    {
                        break;
                    }

                    channel = std::make_shared<TLSChannel>(std::move(operations.socket), context_, TLSChannelRole::C,
                                                           channel_config_);
                    channel_ = channel;
                    co_await channel->start(config_.address);
                    channel->send(CtrlMessage{CtrlCommand::ClusterJoin, njson{{"node_id", config_.node_id},
                                                                              {"address", config_.advertise_address}}});
                    auto joined = co_await channel->async_receive(channel_config_.handshake_timeout);
                    if (joined.type() == CtrlCommand::ClusterJoined)
                    {
                        joined_ = true;
                        PROXY_INFO_PRINT("Cluster [+] node=%s peer=%s:%u", config_.node_id.c_str(),
                                         config_.address.c_str(), static_cast<unsigned int>(config_.port));
                        manager_.receive(CtrlMessage{CtrlCommand::ClusterJoined});
                        while (running_)
                        {
                            manager_.receive(co_await channel->async_receive());
                        }
                    }
                    else
                    {
                        if (joined.type() != CtrlCommand::ClusterError)
                            throw std::invalid_argument("Expected cluster.joined or cluster.error");
                        manager_.receive(std::move(joined));
                    }
                }
                catch (const std::exception &exception)
                {
                    if (running_)
                    {
                        const auto *error = dynamic_cast<const asio::system_error *>(&exception);
                        if (error && (error->code() == asio::experimental::error::channel_cancelled ||
                                      error->code() == asio::experimental::error::channel_closed))
                        {
                            PROXY_DEBUG_PRINT("Cluster retry node=%s peer=%s:%u delay=5000ms", config_.node_id.c_str(),
                                              config_.address.c_str(), static_cast<unsigned int>(config_.port));
                        }
                        else
                        {
                            PROXY_ERROR_PRINT("Cluster failed node=%s peer=%s:%u reason=%s retry=5000ms",
                                              config_.node_id.c_str(), config_.address.c_str(),
                                              static_cast<unsigned int>(config_.port), exception.what());
                        }
                    }
                    asio::error_code ignored;
                    operations.socket.close(ignored);
                }

                if (joined_)
                {
                    PROXY_INFO_PRINT("Cluster [x] node=%s peer=%s:%u reason=%s", config_.node_id.c_str(),
                                     config_.address.c_str(), static_cast<unsigned int>(config_.port),
                                     running_ ? "channel closed" : "node stopping");
                }
                joined_ = false;
                if (channel)
                {
                    co_await channel->async_disconnect();
                }
            }

            if (!running_)
            {
                break;
            }

            operations.retry.expires_after(retry_interval);
            auto [error] = co_await operations.retry.async_wait(use_nothrow_awaitable);
            if (error || !running_)
            {
                break;
            }
        }
    }

  private:
    asio::any_io_executor executor_;
    asio::ssl::context &context_;
    const ClusterConfig &config_;
    const TLSChannelConfig &channel_config_;
    ClusterMgr &manager_;
    std::weak_ptr<TLSChannel> channel_;
    ConnectOperations *operations_ = nullptr;
    bool running_ = true;
    bool joined_ = false;
};

ClusterMgr::ClusterMgr(RelayNode &server, asio::any_io_executor executor, asio::ssl::context &context,
                       ClusterConfig config, TLSChannelConfig channel, std::size_t max_connections)
    : server_(server), executor_(executor), context_(context), config_(std::move(config)), channel_config_(channel),
      max_connections_(max_connections), acceptor_(executor), loop_done_(executor, 1),
      outbound_channel_(executor, channel.max_queued_messages)
{
    if (config_.node_id.empty() || config_.address.empty())
    {
        throw std::invalid_argument("cluster node_id and address are required");
    }
    if (config_.node_id == cluster_broadcast_target)
    {
        throw std::invalid_argument("cluster node_id must not be all");
    }
    if (config_.advertise_address.empty())
    {
        config_.advertise_address = config_.address;
    }
    if (config_.role == ClusterConfig::Role::Master)
    {
        std::random_device random;
        do
        {
            epoch_ = (static_cast<std::uint64_t>(random()) << 32U) | random();
        } while (epoch_ == 0);
    }
}

void ClusterMgr::start()
{
    if (running_)
    {
        throw std::logic_error("Cluster already started");
    }

    if (config_.role == ClusterConfig::Role::Master)
    {
        ScopeGuard close_acceptor([this]() noexcept {
            asio::error_code ignored;
            acceptor_.close(ignored);
        });
        tcp::endpoint endpoint(asio::ip::make_address(config_.address), config_.port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
        close_acceptor.dismiss();
    }

    running_ = true;
    ScopeGuard rollback([this]() noexcept {
        running_ = false;
        asio::error_code ignored;
        acceptor_.close(ignored);
        outbound_channel_.close();
    });
    std::weak_ptr<ClusterMgr> weak = shared_from_this();
    auto loop = config_.role == ClusterConfig::Role::Master ? accept_loop() : slave_loop();
    asio::co_spawn(executor_, std::move(loop), [weak](std::exception_ptr error) {
        if (auto self = weak.lock())
        {
            self->loop_done_.try_send(asio::error_code{}, std::move(error));
        }
    });
    rollback.dismiss();
}

asio::awaitable<void> ClusterMgr::async_stop()
{
    if (!running_)
    {
        co_return;
    }

    running_ = false;
    if (config_.role == ClusterConfig::Role::Master)
    {
        asio::error_code ignored;
        acceptor_.close(ignored);
        outbound_channel_.close();
    }

    if (connector_)
    {
        connector_->stop();
    }

    auto loop_error = co_await loop_done_.async_receive(asio::use_awaitable);
    if (loop_error)
    {
        std::rethrow_exception(loop_error);
    }
}

asio::awaitable<void> ClusterMgr::accept_loop()
{
    ClusterRoom room(executor_, config_.node_id, epoch_);
    room.join(std::make_shared<LocalParticipant>(config_.node_id, config_.advertise_address, *this));

    std::exception_ptr error;
    try
    {
        using namespace asio::experimental::awaitable_operators;
        co_await (accept_sessions(room) && deliver_messages(room));
    }
    catch (...)
    {
        error = std::current_exception();
    }
    co_await room.async_stop();
    if (error)
        std::rethrow_exception(error);
}

asio::awaitable<void> ClusterMgr::slave_loop()
{
    if (!running_)
    {
        co_return;
    }

    Connector connector(executor_, context_, config_, channel_config_, *this);
    connector_ = &connector;
    ScopeGuard clear_connector([this]() noexcept { connector_ = nullptr; });
    co_await connector.run();
}

asio::awaitable<void> ClusterMgr::accept_sessions(ClusterRoom &room)
{
    while (running_)
    {
        auto [error, socket] = co_await acceptor_.async_accept(use_nothrow_awaitable);
        if (!running_ || error == asio::error::operation_aborted)
            co_return;
        if (error)
        {
            PROXY_ERROR_PRINT("Cluster accept failed reason=%s", error.message().c_str());
            continue;
        }
        if (room.size() >= max_connections_ + 1)
        {
            PROXY_ERROR_PRINT("Cluster rejected peer=%s limit=%zu", socket_peer(socket).c_str(), max_connections_);
            continue;
        }

        auto session = std::make_shared<ClusterSession>(std::move(socket), context_, room, channel_config_);
        room.join(session);
        session->start();
    }
}

asio::awaitable<void> ClusterMgr::deliver_messages(ClusterRoom &room)
{
    for (;;)
    {
        auto [error, message] = co_await outbound_channel_.async_receive(use_nothrow_awaitable);
        if (error)
        {
            co_return;
        }

        try
        {
            room.deliver(config_.node_id, std::move(message));
        }
        catch (const std::exception &exception)
        {
            receive(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", exception.what()}}});
        }
    }
}

void ClusterMgr::send(std::string target, CtrlMessage message)
{
    std::weak_ptr<ClusterMgr> weak = shared_from_this();
    asio::post(executor_, [weak, target = std::move(target), message = std::move(message)]() mutable {
        auto self = weak.lock();
        if (!self || !self->running_)
        {
            return;
        }

        try
        {
            if (target.empty())
            {
                throw std::invalid_argument("cluster target must not be empty");
            }
            if (!message.params)
            {
                message.params = njson::object();
            }
            if (!message.params->is_object())
            {
                throw std::invalid_argument("Cluster message params must be an object");
            }
            if (message.command.starts_with("cluster."))
                throw std::invalid_argument("cluster.* commands are reserved");
            (*message.params)["target"] = std::move(target);
        }
        catch (const std::exception &exception)
        {
            self->receive(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", exception.what()}}});
            return;
        }

        if (self->config_.role == ClusterConfig::Role::Master)
        {
            if (!self->outbound_channel_.try_send(asio::error_code{}, std::move(message)))
            {
                self->receive(
                    CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "cluster outbound queue is full"}}});
            }
        }
        else if (self->connector_)
        {
            self->connector_->send(std::move(message));
        }
        else
        {
            self->receive(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "cluster disconnected"}}});
        }
    });
}

void ClusterMgr::broadcast(CtrlMessage message)
{
    send(std::string(cluster_broadcast_target), std::move(message));
}

void ClusterMgr::receive(CtrlMessage message)
{
    if (!running_)
    {
        return;
    }

    try
    {
        const auto command = message.type();
        if (command != CtrlCommand::ClusterJoined && command != CtrlCommand::ClusterError)
        {
            static_cast<void>(config::required_string(config::message_params(message), "source", message.command));
        }
        server_.handle_cluster_message(std::move(message));
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("Cluster message rejected reason=%s", exception.what());
    }
}
