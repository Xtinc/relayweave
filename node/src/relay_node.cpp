#include "relay_node.h"
#include "node_session.h"
#include <algorithm>
#include <cassert>
#include <future>

RelayNode::RelayNode(asio::io_context &control_io, asio::io_context &transfer_tcp_io, asio::io_context &transfer_udp_io,
                     asio::io_context &cluster_data_io, asio::ssl::context &ssl_context, NodeConfig config)
    : control_executor_(control_io.get_executor()), transfer_tcp_executor_(transfer_tcp_io.get_executor()),
      transfer_udp_executor_(transfer_udp_io.get_executor()), cluster_data_executor_(cluster_data_io.get_executor()),
      control_probe_timer_(control_executor_), transfer_tcp_probe_timer_(transfer_tcp_executor_),
      transfer_udp_probe_timer_(transfer_udp_executor_), traffic_sample_timer_(control_executor_),
      control_sessions_done_(control_executor_), control_acceptor_(control_executor_), ssl_context_(ssl_context),
      config_(std::move(config)),
      relay_id_allocator_(std::make_shared<RelayIdAllocator>()),
      tcp_pipeline_(std::make_shared<TcpPipeline>(
          transfer_tcp_executor_, ssl_context_, relay_id_allocator_, config_.tcp.address, config_.tcp.port,
          config_.tcp.max_setup_connections, config_.tcp.max_relays, config_.tcp.setup_timeout, config_.tcp.traffic)),
      tls_pipeline_(std::make_shared<TlsPipeline>(
          transfer_tcp_executor_, ssl_context_, relay_id_allocator_, config_.tls.address, config_.tls.port,
          config_.tls.max_setup_connections, config_.tls.max_relays, config_.tls.setup_timeout, config_.tls.traffic)),
      datagram_mgr_(std::make_shared<DatagramMgr>(
          transfer_udp_executor_, relay_id_allocator_,
          asio::ip::udp::endpoint(asio::ip::make_address(config_.datagram.address), config_.datagram.port),
          config_.datagram.max_relays, config_.datagram.traffic)),
      registry_(config_.control.max_connections, config_.control.max_services, config_.control.max_services_per_session),
      relays_done_(control_executor_)
{
    control_sessions_done_.expires_at(std::chrono::steady_clock::time_point::max());
    if (&control_io == &transfer_tcp_io || &control_io == &transfer_udp_io || &transfer_tcp_io == &transfer_udp_io ||
        &cluster_data_io == &control_io || &cluster_data_io == &transfer_tcp_io || &cluster_data_io == &transfer_udp_io)
    {
        throw std::invalid_argument(
            "Control, TCP transfer, UDP transfer, and cluster data require distinct io_context instances");
    }

    cluster_context_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                 asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                 asio::ssl::context::no_tlsv1_1);
    cluster_context_.use_certificate_chain_file(config_.certificate_chain.string());
    cluster_context_.use_private_key_file(config_.private_key.string(), asio::ssl::context::pem);
    if (SSL_CTX_check_private_key(cluster_context_.native_handle()) != 1)
    {
        throw std::invalid_argument("Cluster certificate and private key do not match");
    }
    cluster_context_.load_verify_file(config_.server_ca_file.string());

    config_.cluster.advertise_address = config_.control.advertise_address;
    cluster_mgr_ = std::make_shared<ClusterMgr>(*this, control_executor_, cluster_context_, config_.cluster,
                                                config_.channel, config_.control.max_connections);
    topology_ = std::make_unique<Topology>(
        control_io, config_.cluster.node_id, config_.cluster.role == ClusterConfig::Role::Master, *cluster_mgr_,
        control_queue_delay_us_, transfer_tcp_queue_delay_us_, transfer_udp_queue_delay_us_);
    nodelink_mgr_ =
        std::make_unique<NodeLinkMgr>(control_executor_, cluster_data_executor_, config_.cluster, config_.tcp.address,
                                      config_.datagram.address, *cluster_mgr_, *topology_);

}

RelayNode::~RelayNode() = default;

void RelayNode::start()
{
    if (state_.load() != State::Created)
    {
        throw std::logic_error("RelayWeave Node can only be started once");
    }

    try
    {
        const tcp::endpoint endpoint(asio::ip::make_address(config_.control.address), config_.control.port);
        control_acceptor_.open(endpoint.protocol());
        control_acceptor_.set_option(asio::socket_base::reuse_address(true));
        control_acceptor_.bind(endpoint);
        control_acceptor_.listen();
        tcp_pipeline_->start();
        tls_pipeline_->start();
        datagram_mgr_->start();
        nodelink_mgr_->start();
        started_at_ = std::chrono::steady_clock::now();
        cluster_mgr_->start();
        topology_->start();
        nodelink_mgr_->activate();
        state_.store(State::Running);
    }
    catch (...)
    {
        state_.store(State::Stopped);
        asio::error_code ignored;
        control_acceptor_.close(ignored);
        nodelink_mgr_->rollback();
        tcp_pipeline_->stop();
        tls_pipeline_->stop();
        datagram_mgr_->stop();
        throw;
    }

    PROXY_INFO_PRINT("Node listening control=%s:%u tcp=%s:%u tls=%s:%u udp=%s:%u", config_.control.address.c_str(),
                     static_cast<unsigned int>(config_.control.port), config_.tcp.address.c_str(),
                     static_cast<unsigned int>(config_.tcp.port), config_.tls.address.c_str(),
                     static_cast<unsigned int>(config_.tls.port), config_.datagram.address.c_str(),
                     static_cast<unsigned int>(config_.datagram.port));
    asio::co_spawn(
        control_executor_,
        [self = shared_from_this()] { return self->control_accept_loop(); },
        asio::detached);
    schedule_queue_probe(control_probe_timer_, control_queue_delay_us_);
    schedule_queue_probe(transfer_tcp_probe_timer_, transfer_tcp_queue_delay_us_);
    schedule_queue_probe(transfer_udp_probe_timer_, transfer_udp_queue_delay_us_);
    schedule_traffic_sample();
}

void RelayNode::stop()
{
    if (control_executor_.running_in_this_thread() || transfer_tcp_executor_.running_in_this_thread() ||
        transfer_udp_executor_.running_in_this_thread() || cluster_data_executor_.running_in_this_thread())
    {
        throw std::logic_error("Synchronous server stop must be called outside the server executors");
    }

    std::call_once(stop_once_, [this]() {
        const auto current = state_.load();
        if (current == State::Created)
        {
            state_.store(State::Stopped);
            PROXY_INFO_PRINT("Node stopped");
            return;
        }
        if (current != State::Running)
        {
            throw std::logic_error("Server is not running");
        }
        state_.store(State::Stopping);

        const auto save_stop_error = [this](auto &completion) {
            try
            {
                completion.get();
            }
            catch (...)
            {
                if (!stop_error_)
                {
                    stop_error_ = std::current_exception();
                }
            }
        };

        auto self = shared_from_this();
        auto control_stopped = asio::co_spawn(
            control_executor_,
            [self]() -> asio::awaitable<void> {
                self->control_probe_timer_.cancel();
                self->traffic_sample_timer_.cancel();
                asio::error_code ignored;
                self->control_acceptor_.close(ignored);
                self->registry_.stop();

                co_await self->stop_relays();
                co_await self->nodelink_mgr_->stop();

                std::exception_ptr topology_error;
                try
                {
                    co_await self->topology_->close();
                }
                catch (...)
                {
                    topology_error = std::current_exception();
                }

                std::exception_ptr cluster_error;
                try
                {
                    co_await self->cluster_mgr_->async_stop();
                }
                catch (...)
                {
                    cluster_error = std::current_exception();
                }

                if (self->active_control_sessions_ != 0)
                {
                    co_await self->control_sessions_done_.async_wait(use_nothrow_awaitable);
                }
                if (topology_error)
                {
                    std::rethrow_exception(topology_error);
                }
                if (cluster_error)
                {
                    std::rethrow_exception(cluster_error);
                }
            },
            asio::use_future);
        save_stop_error(control_stopped);

        auto transfer_tcp_stopped = asio::post(
            transfer_tcp_executor_,
            asio::use_future([self] {
                self->transfer_tcp_probe_timer_.cancel();
                self->tcp_pipeline_->stop();
                self->tls_pipeline_->stop();
            }));
        save_stop_error(transfer_tcp_stopped);
        tcp_pipeline_->wait_for_pending();
        tls_pipeline_->wait_for_pending();

        auto transfer_udp_stopped = asio::post(
            transfer_udp_executor_,
            asio::use_future([self] {
                self->transfer_udp_probe_timer_.cancel();
                self->datagram_mgr_->stop();
            }));
        save_stop_error(transfer_udp_stopped);

        state_.store(State::Stopped);
        PROXY_INFO_PRINT("Node stopped");
    });

    if (stop_error_)
    {
        std::rethrow_exception(stop_error_);
    }
}

asio::awaitable<void> RelayNode::control_accept_loop()
{
    for (;;)
    {
        auto [error, socket] = co_await control_acceptor_.async_accept(use_nothrow_awaitable);
        const auto state = state_.load();
        if (error)
        {
            if (state != State::Running || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Control accept failed reason=%s category=%s", error.message().c_str(),
                              error.category().name());
            continue;
        }
        if (state != State::Running)
        {
            co_return;
        }
        if (registry_.full())
        {
            PROXY_ERROR_PRINT("Control rejected active=%zu limit=%zu", registry_.session_count(),
                              config_.control.max_connections);
            continue;
        }

        auto session =
            std::make_shared<ControlSession>(std::move(socket), ssl_context_, TLSChannelRole::S, config_.channel);
        const auto session_id = allocate_session_id();
        registry_.add(session_id, session);
        ++active_control_sessions_;
        ScopeGuard task_rollback([this, session_id]() noexcept {
            registry_.remove(session_id);
            --active_control_sessions_;
        });
        asio::co_spawn(
            control_executor_,
            [self = shared_from_this(), session_id, session] {
                return self->run_control_session(session_id, session);
            },
            asio::detached);
        task_rollback.dismiss();
    }
}

asio::awaitable<void> RelayNode::run_control_session(SessionId id, ControlSessionPtr session)
{
    ScopeGuard session_done([this]() noexcept {
        assert(active_control_sessions_ != 0);
        --active_control_sessions_;
        if (state_.load() == State::Stopping && active_control_sessions_ == 0)
        {
            try
            {
                control_sessions_done_.cancel();
            }
            catch (...)
            {
            }
        }
    });
    std::string failure;
    bool connected = false;
    try
    {
        co_await session->start({});
        connected = true;
        PROXY_INFO_PRINT("Control [+] session_id=%llu peer=%.*s", static_cast<unsigned long long>(id),
                         static_cast<int>(session->peer().size()), session->peer().data());
        for (;;)
        {
            auto message = co_await session->async_receive();
            if (state_.load() == State::Running)
            {
                handle_control_message(id, session, std::move(message));
            }
        }
    }
    catch (const asio::system_error &exception)
    {
        if (exception.code() != asio::experimental::error::channel_cancelled &&
            exception.code() != asio::experimental::error::channel_closed)
        {
            failure = exception.what();
        }
    }
    catch (const std::exception &exception)
    {
        failure = exception.what();
    }
    catch (...)
    {
        failure = "unknown exception";
    }

    cancel_relays("control session disconnected", session);
    registry_.remove(id);
    co_await session->async_disconnect();
    if (connected)
    {
        PROXY_INFO_PRINT("Control [x] session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                         static_cast<int>(session->peer().size()), session->peer().data(),
                         state_.load() != State::Running ? "node stopping"
                         : failure.empty()               ? "channel closed"
                                                         : "error");
    }
    if (!failure.empty() && state_.load() == State::Running)
    {
        PROXY_ERROR_PRINT("Control failed session_id=%llu peer=%.*s reason=%s", static_cast<unsigned long long>(id),
                          static_cast<int>(session->peer().size()), session->peer().data(), failure.c_str());
    }
}

void RelayNode::handle_cluster_message(CtrlMessage message)
{
    if (message.type() == CtrlCommand::TopologyMembers)
    {
        topology_->update_members(config::message_params(message));
        if (state_.load() == State::Running)
        {
            nodelink_mgr_->members_changed();
            invalidate_relays();
        }
        return;
    }
    if (message.type() == CtrlCommand::TopologyReport)
    {
        topology_->update_report(config::message_params(message));
        return;
    }
    if (message.command.starts_with("link.") || message.command.starts_with("flow."))
    {
        // Drain existing acknowledgements while relay cleanup is running; reject new remote opens during stop.
        if (state_.load() == State::Running || message.command != "flow.open")
        {
            nodelink_mgr_->handle(std::move(message));
        }
        return;
    }
    if (message.type() == CtrlCommand::ClusterError)
    {
        nodelink_mgr_->control_failed(config::message_params(message).value("reason", "cluster control failed"));
        invalidate_relays("cluster control failed");
        return;
    }
    if (message.command.starts_with("relay.peer."))
    {
        if (state_.load() == State::Running)
        {
            handle_relay_peer(std::move(message));
        }
        return;
    }
    handle_control_cluster_message(std::move(message));
}

void RelayNode::schedule_queue_probe(asio::steady_timer &timer, std::atomic<std::uint32_t> &queue_delay_us)
{
    static constexpr auto interval = std::chrono::seconds(1);
    static constexpr auto invalid = std::numeric_limits<std::uint32_t>::max();

    timer.expires_after(interval);
    const auto deadline = timer.expiry();
    timer.async_wait([self = shared_from_this(), &timer, &queue_delay_us, deadline](const asio::error_code &error) {
        if (error || self->state_.load() != State::Running)
        {
            return;
        }
        const auto elapsed = std::chrono::steady_clock::now() - deadline;
        const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        const auto max_valid = static_cast<std::chrono::microseconds::rep>(invalid - 1);
        const auto delay = microseconds < 0 ? invalid : static_cast<std::uint32_t>(std::min(microseconds, max_valid));
        queue_delay_us.store(delay, std::memory_order_relaxed);
        self->schedule_queue_probe(timer, queue_delay_us);
    });
}

void RelayNode::schedule_traffic_sample()
{
    traffic_sample_timer_.expires_after(std::chrono::seconds(1));
    traffic_sample_timer_.async_wait([self = shared_from_this()](const asio::error_code &error) {
        if (error || self->state_.load() != State::Running)
        {
            return;
        }
        self->registry_.sample_traffic();
        self->schedule_traffic_sample();
    });
}
