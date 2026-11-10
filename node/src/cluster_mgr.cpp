#include "cluster_mgr.h"
#include "relay_node.h"
#include <algorithm>
#include <asio/experimental/awaitable_operators.hpp>
#include <random>
#include <vector>

/**
 * @brief Define a master-room participant on control_io.
 * @details The room retains participants until their connection tasks leave; delivery only
 * queues a message, and stop() requests shutdown without waiting for connection drainage.
 */
class ClusterParticipant
{
  public:
    /** @brief Destroy a participant after its owned connection work has drained. */
    virtual ~ClusterParticipant() = default;
    /** @brief Borrow the joined node identity, or an empty identity before join completes. */
    virtual std::string_view node_id() const noexcept = 0;
    /** @brief Borrow the participant's advertised Node address. */
    virtual std::string_view address() const noexcept = 0;
    /** @brief Queue a control message for this participant on control_io. */
    virtual void deliver(CtrlMessage message) = 0;
    /** @brief Request participant shutdown on control_io. */
    virtual void stop() = 0;
};

/**
 * @brief Own master participants, publish membership, and authenticate message sources.
 * @details All access requires control_io. The master accept loop owns this room and drains
 * remote sessions before destruction; sessions borrow the room until they leave.
 */
class ClusterRoom
{
  public:
    /** @brief Initialize a master room with one membership epoch and an empty participant set. */
    ClusterRoom(asio::any_io_executor executor, std::string master_id, std::uint64_t epoch)
        : master_id_(std::move(master_id)), epoch_(epoch), changed_(executor)
    {
    }

    /** @brief Retain a participant and publish membership if its identity is already known. */
    void join(std::unique_ptr<ClusterParticipant> participant)
    {
        const bool ready = !participant->node_id().empty();
        participants_.push_back(std::move(participant));
        if (ready)
        {
            publish_members();
        }
    }

    /** @brief Publish membership after a remote participant finishes joining. */
    void ready()
    {
        publish_members();
    }

    /** @brief Return whether a nonempty, nonreserved node identity is unused. */
    bool available(std::string_view node_id) const
    {
        return !node_id.empty() && node_id.size() <= cluster_max_node_id_length &&
               node_id != cluster_broadcast_target && std::ranges::none_of(participants_, [&](const auto &participant) {
                   return participant->node_id() == node_id;
               });
    }

    /** @brief Release a participant, wake shutdown, and publish membership when appropriate. */
    void leave(ClusterParticipant &participant)
    {
        const auto was_ready = !participant.node_id().empty();
        std::erase_if(participants_, [leaving = &participant](const auto &owned) { return owned.get() == leaving; });
        changed_.cancel();
        if (was_ready && !stopping_)
        {
            publish_members();
        }
    }

    /** @brief Return participant count, including unjoined sessions and the local master. */
    std::size_t size() const noexcept
    {
        return participants_.size();
    }

    /** @brief Route unicast by target and broadcast to joined members other than the authenticated source. */
    void deliver(std::string_view source, CtrlMessage message)
    {
        if (message.command.starts_with("cluster."))
        {
            throw std::invalid_argument("cluster.* commands are reserved");
        }

        try
        {
            WireMessage::validate(message, cluster_max_message_length);
        }
        catch (const std::length_error &exception)
        {
            PROXY_ERROR_PRINT("Cluster message rejected source=%.*s reason=%s", static_cast<int>(source.size()),
                              source.data(), exception.what());
            return;
        }

        const auto target = config::required_string(config::message_params(message), "target", message.command);
        message.params->erase("target");
        (*message.params)["source"] = source;

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
            if (!participant->node_id().empty() && participant->node_id() != source)
            {
                participant->deliver(message);
            }
        }
    }

    /** @brief Stop remote participants and await their removal before releasing the room. */
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
                                                                {"target", master_id_}}};
        deliver(master_id_, message);
        (*message.params)["target"] = cluster_broadcast_target;
        deliver(master_id_, std::move(message));
    }

    std::vector<std::unique_ptr<ClusterParticipant>> participants_;
    std::string master_id_;
    std::uint64_t epoch_;
    std::uint64_t version_ = 0;
    asio::steady_timer changed_;
    bool stopping_ = false;
};

/**
 * @brief Own one remote member's TLS channel and join/read lifecycle on the master.
 * @details The room retains this participant; reader completion removes it only after TLS
 * drainage. The room, TLS context and listener blacklist must outlive the session task.
 */
class ClusterSession : public ClusterParticipant
{
  public:
    /** @brief Adopt an accepted socket and borrow its master room and TLS context. */
    ClusterSession(asio::ip::tcp::socket socket, asio::ssl::context &context, ClusterRoom &room,
                   TLSChannelConfig config)
        : channel_(std::make_shared<TLSChannel>(std::move(socket), context, TLSChannelRole::S, config)), room_(room),
          join_timeout_(config.handshake_timeout)
    {
    }

    /** @brief Launch the reader with its borrowed listener blacklist and leave the room on completion. */
    void start(PeerBlacklist &blacklist, asio::ip::address_v4 source)
    {
        asio::co_spawn(channel_->executor(), reader(blacklist, source),
                       [this](std::exception_ptr) { room_.leave(*this); });
    }

    /** @copydoc ClusterParticipant::node_id */
    std::string_view node_id() const noexcept override
    {
        return node_id_;
    }

    /** @copydoc ClusterParticipant::address */
    std::string_view address() const noexcept override
    {
        return address_;
    }

    /** @copydoc ClusterParticipant::deliver */
    void deliver(CtrlMessage message) override
    {
        channel_->send(std::move(message));
    }

    /** @copydoc ClusterParticipant::stop */
    void stop() override
    {
        stopping_ = true;
        channel_->disconnect();
    }

  private:
    asio::awaitable<void> reader(PeerBlacklist &blacklist, asio::ip::address_v4 source)
    {
        bool authenticated = false;
        try
        {
            co_await channel_->start({});
            authenticated = true;
            auto join = co_await channel_->async_receive(join_timeout_);
            if (join.type() != CtrlCommand::ClusterJoin)
            {
                throw std::invalid_argument("Expected cluster.join");
            }

            auto node_id = config::required_string(config::message_params(join), "node_id", "cluster.join");
            if (!room_.available(node_id))
            {
                deliver(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "invalid or duplicate node_id"}}});
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
        if (!authenticated && !stopping_)
        {
            blacklist.record_failure(source, "cluster authentication incomplete");
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

/** @brief Represent the master locally on control_io, delivering directly to its borrowed ClusterMgr. */
class ClusterMgr::LocalParticipant : public ClusterParticipant
{
  public:
    /** @brief Retain master identity and borrow the manager that receives local messages. */
    LocalParticipant(std::string node_id, std::string address, ClusterMgr &manager)
        : node_id_(std::move(node_id)), address_(std::move(address)), manager_(manager)
    {
    }

    /** @copydoc ClusterParticipant::address */
    std::string_view address() const noexcept override
    {
        return address_;
    }

    /** @copydoc ClusterParticipant::node_id */
    std::string_view node_id() const noexcept override
    {
        return node_id_;
    }

    /** @copydoc ClusterParticipant::deliver */
    void deliver(CtrlMessage message) override
    {
        manager_.receive(std::move(message));
    }

    /** @brief Keep the local participant until room drainage releases it; no socket is owned. */
    void stop() override
    {
    }

  private:
    std::string node_id_;
    std::string address_;
    ClusterMgr &manager_;
};

ClusterMgr::ClusterMgr(RelayNode &server, asio::any_io_executor executor, asio::ssl::context &context,
                       ClusterConfig config, TLSChannelConfig channel, std::size_t max_connections)
    : server_(server), executor_(executor), context_(context), config_(std::move(config)), channel_config_(channel),
      max_connections_(max_connections), acceptor_(executor), resolver_(executor), connect_socket_(executor),
      retry_timer_(executor), loop_done_(executor), outbound_channel_(executor, channel.max_queued_messages)
{
    if (config_.node_id.empty() || config_.address.empty())
    {
        throw std::invalid_argument("cluster node_id and address are required");
    }

    if (config_.node_id.size() > cluster_max_node_id_length)
    {
        throw std::invalid_argument("cluster node_id must not exceed 256 bytes");
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
    if (started_)
    {
        throw std::logic_error("Cluster already started");
    }

    ScopeGuard rollback([this]() noexcept {
        running_ = false;
        asio::error_code ignored;
        acceptor_.close(ignored);
    });
    if (config_.role == ClusterConfig::Role::Master)
    {
        tcp::endpoint endpoint(asio::ip::make_address_v4(config_.address), config_.control_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
    }

    running_ = true;
    std::weak_ptr<ClusterMgr> weak = shared_from_this();
    auto loop = config_.role == ClusterConfig::Role::Master ? accept_loop() : slave_loop();
    asio::co_spawn(executor_, std::move(loop), [weak](std::exception_ptr error) {
        if (auto self = weak.lock())
        {
            self->drain_error_ = error;
            if (error)
            {
                PROXY_ERROR_PRINT("Cluster close failed reason=%s", exception_description(error).c_str());
            }
            if (self->running_ && self->config_.role == ClusterConfig::Role::Master)
            {
                self->fail(error ? exception_description(error) : "master control loop exited");
            }
            self->loop_done_.notify_all();
        }
    });
    started_ = true;
    rollback.dismiss();
}

asio::awaitable<bool> ClusterMgr::async_stop()
{
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    if (!started_)
    {
        co_return true;
    }

    running_ = false;
    if (config_.role == ClusterConfig::Role::Master)
    {
        asio::error_code error;
        acceptor_.close(error);
        if (error)
        {
            stop_ok_ = false;
            PROXY_ERROR_PRINT("Cluster listener close failed reason=%s", error.message().c_str());
        }
        outbound_channel_.close();
    }
    else
    {
        resolver_.cancel();
        asio::error_code ignored;
        connect_socket_.close(ignored);
        retry_timer_.cancel();
        if (joined_)
        {
            PROXY_INFO_PRINT("Cluster [x] node=%s peer=%s:%u reason=node stopping", config_.node_id.c_str(),
                             config_.address.c_str(), static_cast<unsigned int>(config_.control_port));
            joined_ = false;
        }
        if (auto channel = master_channel_.lock())
        {
            channel->disconnect();
        }
    }

    co_await loop_done_.wait();
    if (drain_error_)
    {
        std::rethrow_exception(drain_error_);
    }
    co_return stop_ok_;
}

asio::awaitable<void> ClusterMgr::accept_loop()
{
    ClusterRoom room(executor_, config_.node_id, epoch_);
    room.join(std::make_unique<LocalParticipant>(config_.node_id, config_.advertise_address, *this));

    try
    {
        using namespace asio::experimental::awaitable_operators;
        co_await (accept_sessions(room) && deliver_messages(room));
    }
    catch (...)
    {
        PROXY_ERROR_PRINT("Cluster failed reason=%s", exception_description(std::current_exception()).c_str());
    }
    co_await room.async_stop();
}

asio::awaitable<void> ClusterMgr::slave_loop()
{
    constexpr auto retry_interval = std::chrono::seconds(5);
    while (running_)
    {
        {
            std::shared_ptr<TLSChannel> channel;
            try
            {
                PROXY_DEBUG_PRINT("Cluster connecting -> %s:%u", config_.address.c_str(),
                                  static_cast<unsigned int>(config_.control_port));
                auto endpoints =
                    co_await resolver_.async_resolve(config_.address, std::to_string(config_.control_port),
                                                     asio::cancel_after(std::chrono::seconds(5), asio::use_awaitable));
                if (!running_)
                {
                    break;
                }

                co_await asio::async_connect(connect_socket_, endpoints,
                                             asio::cancel_after(std::chrono::seconds(5), asio::use_awaitable));
                if (!running_)
                {
                    break;
                }

                channel = std::make_shared<TLSChannel>(std::move(connect_socket_), context_, TLSChannelRole::C,
                                                       channel_config_);
                master_channel_ = channel;
                co_await channel->start(config_.address);
                channel->send(CtrlMessage{CtrlCommand::ClusterJoin,
                                          njson{{"node_id", config_.node_id}, {"address", config_.advertise_address}}});
                auto joined = co_await channel->async_receive(channel_config_.handshake_timeout);
                if (joined.type() == CtrlCommand::ClusterJoined)
                {
                    joined_ = true;
                    PROXY_INFO_PRINT("Cluster [+] node=%s peer=%s:%u", config_.node_id.c_str(), config_.address.c_str(),
                                     static_cast<unsigned int>(config_.control_port));
                    while (running_)
                    {
                        receive(co_await channel->async_receive());
                    }
                }
                else
                {
                    if (joined.type() != CtrlCommand::ClusterError)
                    {
                        throw std::invalid_argument("Expected cluster.joined or cluster.error");
                    }
                    receive(std::move(joined));
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
                                          config_.address.c_str(), static_cast<unsigned int>(config_.control_port));
                    }
                    else
                    {
                        PROXY_ERROR_PRINT("Cluster failed node=%s peer=%s:%u reason=%s retry=5000ms",
                                          config_.node_id.c_str(), config_.address.c_str(),
                                          static_cast<unsigned int>(config_.control_port), exception.what());
                    }
                }
                asio::error_code ignored;
                connect_socket_.close(ignored);
            }

            if (joined_)
            {
                PROXY_INFO_PRINT("Cluster [x] node=%s peer=%s:%u reason=%s", config_.node_id.c_str(),
                                 config_.address.c_str(), static_cast<unsigned int>(config_.control_port),
                                 running_ ? "channel closed" : "node stopping");
                joined_ = false;
                if (running_)
                {
                    receive(CtrlMessage{CtrlCommand::ClusterError, njson{{"reason", "cluster control disconnected"}}});
                }
            }
            if (channel)
            {
                co_await channel->async_disconnect();
            }
            master_channel_.reset();
        }

        if (!running_)
        {
            break;
        }

        retry_timer_.expires_after(retry_interval);
        auto [error] = co_await retry_timer_.async_wait(use_nothrow_awaitable);
        if (error)
        {
            break;
        }
    }
}

asio::awaitable<void> ClusterMgr::accept_sessions(ClusterRoom &room)
{
    while (running_)
    {
        tcp::endpoint source;
        auto [error, socket] = co_await acceptor_.async_accept(source, use_nothrow_awaitable);
        if (!running_ || error == asio::error::operation_aborted)
            co_return;
        if (error)
        {
            PROXY_ERROR_PRINT("Cluster accept failed reason=%s", error.message().c_str());
            continue;
        }
        if (blacklist_.blocked(source.address().to_v4()))
        {
            continue;
        }
        if (room.size() >= max_connections_ + 1)
        {
            PROXY_ERROR_PRINT("Cluster rejected peer=%s limit=%zu", socket_peer(source).c_str(), max_connections_);
            continue;
        }

        auto session = std::make_unique<ClusterSession>(std::move(socket), context_, room, channel_config_);
        auto *participant = session.get();
        room.join(std::move(session));
        ScopeGuard rollback([&room, participant] { room.leave(*participant); });
        participant->start(blacklist_, source.address().to_v4());
        rollback.dismiss();
    }
}

asio::awaitable<void> ClusterMgr::deliver_messages(ClusterRoom &room)
{
    for (;;)
    {
        auto [error, message] = co_await outbound_channel_.async_receive(use_nothrow_awaitable);
        if (error || !running_)
        {
            co_return;
        }

        const auto command = message.command;
        try
        {
            room.deliver(config_.node_id, std::move(message));
        }
        catch (const std::exception &exception)
        {
            PROXY_ERROR_PRINT("Cluster delivery failed command=%s reason=%s", command.c_str(), exception.what());
        }
    }
}

void ClusterMgr::fail(std::string reason)
{
    running_ = false;
    stop_ok_ = false;
    asio::error_code ignored;
    acceptor_.close(ignored);
    outbound_channel_.reset();
    outbound_channel_.close();
    // Notify after the caller leaves its current business-container iteration.
    asio::post(executor_, [self = shared_from_this(), reason = std::move(reason)]() mutable {
        self->server_.cluster_failed(std::move(reason));
    });
}

ClusterMgr::SendResult ClusterMgr::send(std::string target, CtrlMessage message)
{
    if (!running_)
    {
        return SendResult::Stopped;
    }

    const auto command = message.command;
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
        {
            throw std::invalid_argument("cluster.* commands are reserved");
        }
        (*message.params)["target"] = target;
        WireMessage::validate(message, cluster_max_message_length);
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("Cluster send rejected target=%s command=%s reason=%s", target.c_str(), command.c_str(),
                          exception.what());
        return SendResult::InvalidMessage;
    }

    if (config_.role == ClusterConfig::Role::Master)
    {
        if (outbound_channel_.try_send(asio::error_code{}, std::move(message)))
        {
            return SendResult::Submitted;
        }
        fail("master control outbound queue full");
        return SendResult::QueueFull;
    }
    if (auto channel = master_channel_.lock(); channel && joined_)
    {
        channel->send(std::move(message));
        return SendResult::Submitted;
    }
    return SendResult::NotConnected;
}

ClusterMgr::SendResult ClusterMgr::broadcast(CtrlMessage message)
{
    return send(std::string(cluster_broadcast_target), std::move(message));
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
        if (command != CtrlCommand::ClusterError)
        {
            static_cast<void>(config::required_string(config::message_params(message), "source", message.command));
        }
        server_.handle_cluster_message(command, std::move(message));
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("Cluster message rejected reason=%s", exception.what());
    }
}
