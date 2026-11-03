#include "pipeline_mgr.h"
#include "flow_protocol.h"
#include "xfr_channel.h"
#include <array>
#include <exception>
#include <openssl/rand.h>

StreamPipeline::StreamPipeline(asio::any_io_executor executor,
                                          std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address,
                                          std::uint16_t data_port, std::size_t max_setup_connections,
                                          std::size_t capacity, std::chrono::steady_clock::duration setup_timeout,
                                          TrafficLimitConfig config)
    : executor_(std::move(executor)), id_allocator_(std::move(id_allocator)),
      listen_address_(std::move(listen_address)), data_port_(data_port), max_setup_connections_(max_setup_connections),
      capacity_(capacity), setup_timeout_(setup_timeout), config_(std::move(config)), acceptor_(executor_)
{
}

void StreamPipeline::start()
{
    if (started_ || stopped_)
        throw std::logic_error("TCP pipeline can only be started once");

    const tcp::endpoint endpoint(asio::ip::make_address(listen_address_), data_port_);
    acceptor_.open(endpoint.protocol());
    ScopeGuard close_acceptor([this]() noexcept {
        asio::error_code ignored;
        acceptor_.close(ignored);
    });
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    started_ = true;
    close_acceptor.dismiss();

    asio::co_spawn(executor_, [self = shared_from_this()] { return self->accept_loop(); },
        [self = shared_from_this()](std::exception_ptr error) {
            if (error)
            {
                PROXY_ERROR_PRINT("Data listener failed reason=%s", exception_description(error).c_str());
                self->stop();
            }
        });
}

asio::awaitable<void> StreamPipeline::accept_loop()
{
    for (;;)
    {
        auto [error, socket] = co_await acceptor_.async_accept(use_nothrow_awaitable);
        if (error)
        {
            if (stopped_ || error == asio::error::operation_aborted)
                co_return;
            PROXY_ERROR_PRINT("Transfer accept failed TCP reason=%s category=%s",
                              error.message().c_str(), error.category().name());
            continue;
        }
        if (stopped_)
        {
            co_return;
        }

        const auto pending = unbound_.size();
        if (pending >= max_setup_connections_)
        {
            PROXY_ERROR_PRINT("Transfer rejected TCP setting_up=%zu limit=%zu", pending,
                              max_setup_connections_);
            continue;
        }

        ConnectionId id;
        do
        {
            if (RAND_bytes(id.data(), id.size()) != 1)
            {
                throw std::runtime_error("connection ID generation failed");
            }
        } while (unbound_.contains(id));
        auto connection = std::make_shared<PendingConnection>(std::move(socket),
            std::chrono::steady_clock::now() + setup_timeout_);
        unbound_.emplace(id, connection);
        setup_tasks_.fetch_add(1);
        asio::co_spawn(executor_,
            [self = shared_from_this(), id, connection] { return self->run_transfer_session(id, connection); },
            asio::detached);
    }
}

asio::awaitable<void> StreamPipeline::run_transfer_session(ConnectionId id,
                                                          std::shared_ptr<PendingConnection> pending)
{
    ScopeGuard cleanup([&]() noexcept {
        unbound_.erase(id);
        asio::error_code ignored;
        pending->socket.close(ignored);
        pending->announced.notify_all();
        setup_tasks_.fetch_sub(1);
        setup_tasks_.notify_all();
    });
    try
    {
        co_await asio::async_write(pending->socket, asio::buffer(id),
            asio::cancel_at(pending->deadline, asio::use_awaitable));
        pending->announced.notify_all();
        if (unbound_.contains(id))
        {
            co_await pending->socket.async_wait(tcp::socket::wait_read,
                asio::cancel_at(pending->deadline, use_nothrow_awaitable));
        }
    }
    catch (const std::exception &error)
    {
        if (!stopped_)
        {
            PROXY_DEBUG_PRINT("Data greeting failed reason=%s", error.what());
        }
    }
}

asio::awaitable<void> StreamPipeline::bind(RelayAttach request)
{
    if (stopped_ || !request.connection_id ||
        (request.role != RelayAttach::Producer && request.role != RelayAttach::Consumer))
    {
        throw std::invalid_argument("invalid stream attachment");
    }
    const auto id = *request.connection_id;
    const auto unbound_it = unbound_.find(id);
    std::shared_ptr<PendingConnection> pending;
    if (unbound_it != unbound_.end())
    {
        pending = unbound_it->second;
        if (!co_await pending->announced.wait())
        {
            throw asio::system_error(asio::error::operation_aborted);
        }
    }
    std::optional<tcp::socket> *slot;
    ConnectionId *bound_id;
    std::shared_ptr<RelayPair> pair;
    bool attached = true;
    if (const auto it = remote_pairs_.find(request.uuid); it != remote_pairs_.end())
    {
        auto &endpoint = *it->second;
        if (request.role != endpoint.role || request.ticket != endpoint.ticket)
        {
            throw std::invalid_argument("invalid endpoint ticket or role");
        }
        slot = &endpoint.stream;
        bound_id = &endpoint.connection_id;
        pair = it->second;
    }
    else if (const auto it = local_pairs_.find(request.uuid); it != local_pairs_.end())
    {
        auto &local = *it->second;
        const bool producer = request.role == RelayAttach::Producer;
        if (request.ticket != (producer ? local.producer_ticket : local.consumer_ticket))
        {
            throw std::invalid_argument("invalid pair ticket");
        }
        slot = producer ? &local.producer_stream : &local.consumer_stream;
        bound_id = producer ? &local.producer_id : &local.consumer_id;
        pair = it->second;
        attached = (producer ? local.consumer_stream : local.producer_stream).has_value();
    }
    else
    {
        throw std::invalid_argument("relay attachment expired");
    }
    if (pair->closed)
    {
        throw std::invalid_argument("relay attachment closed");
    }
    if (*slot)
    {
        if (*bound_id == id)
        {
            co_return;
        }
        throw std::invalid_argument("relay role already bound");
    }
    if (!pending || !unbound_.contains(id) || !pending->socket.is_open() ||
        std::chrono::steady_clock::now() >= pending->deadline)
    {
        throw std::invalid_argument("connection ID unknown or expired");
    }
    pending->socket.cancel();
    slot->emplace(std::move(pending->socket));
    *bound_id = id;
    unbound_.erase(id);
    if (attached)
    {
        pair->attached.notify_all();
    }
}

AttachPair StreamPipeline::install_local_pair()
{
    if (stopped_ || local_pairs_.size() + remote_pairs_.size() >= capacity_)
    {
        throw std::runtime_error(stopped_ ? "server stopping" : "relay capacity reached");
    }
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    auto pair = std::make_shared<LocalPair>(executor_);
    local_pairs_.emplace(uuid, pair);
    rollback.dismiss();
    return {{uuid, pair->producer_ticket, data_port_}, {uuid, pair->consumer_ticket, data_port_}};
}

asio::awaitable<void> StreamPipeline::prepare_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic,
                                                                    std::string accessor)
{
    auto pair = local_pairs_.at(uuid);
    co_await pair->prepare(config_, std::move(traffic), std::move(accessor));
}

asio::awaitable<void> StreamPipeline::run_local_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    if (!pair->active)
        throw std::runtime_error("local pair unavailable");
    co_await relay_tcp(*pair->producer_stream, *pair->consumer_stream, &pair->rx_limiter, &pair->tx_limiter,
                      &pair->traffic->rx, &pair->traffic->tx);
}

void StreamPipeline::close_local_pair(std::uint64_t uuid)
{
    const auto it = local_pairs_.find(uuid);
    if (it == local_pairs_.end())
        return;
    auto pair = it->second;
    local_pairs_.erase(it);
    pair->close();
    id_allocator_->release(uuid);
}

AttachInfo StreamPipeline::install_remote_pair(int role)
{
    if (role != RelayAttach::Consumer && role != RelayAttach::Producer)
    {
        throw std::invalid_argument("invalid relay endpoint role");
    }
    if (stopped_ || local_pairs_.size() + remote_pairs_.size() >= capacity_)
    {
        throw std::runtime_error("relay endpoint capacity reached or pipeline stopped");
    }
    const auto uuid = id_allocator_->allocate();
    ScopeGuard rollback([this, uuid] { id_allocator_->release(uuid); });
    auto endpoint = std::make_shared<RemotePair>(executor_, role);
    remote_pairs_.emplace(uuid, endpoint);
    rollback.dismiss();
    return {uuid, endpoint->ticket, data_port_};
}

asio::awaitable<void> StreamPipeline::prepare_remote_pair(std::uint64_t uuid, FlowProtocol &channel,
                                                                     std::uint64_t epoch, std::uint64_t flow_id,
                                                                     SRVTrafficPtr traffic, std::string accessor)
{
    const auto it = remote_pairs_.find(uuid);
    if (it == remote_pairs_.end())
    {
        throw std::runtime_error("relay endpoint closed before attach");
    }
    auto endpoint = it->second;
    co_await endpoint->prepare(channel, epoch, flow_id, executor_, config_, std::move(traffic), std::move(accessor));
}

asio::awaitable<void> StreamPipeline::read_remote_pair(std::shared_ptr<RemotePair> endpoint)
{
    auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->rx_limiter : endpoint->tx_limiter;
    BytesBuf read_buffer(32 * 1024);
    for (;;)
    {
        const auto [error, received] =
            co_await endpoint->stream->async_read_some(asio::buffer(read_buffer), use_nothrow_awaitable);
        if (error && error != asio::error::eof)
        {
            throw asio::system_error(error);
        }
        lnk::PooledBuffer data(endpoint->flow->pool, std::span(read_buffer).first(received));
        const auto size = data.size();
        if (size == 0)
        {
            co_await endpoint->flow->finish_send();
            co_return;
        }
        if (const auto deadline = limiter.reserve(size))
        {
            co_await limiter.wait_until(*deadline);
        }
        co_await endpoint->flow->send(std::move(data));
        endpoint->count(true, size);
    }
}

asio::awaitable<void> StreamPipeline::write_remote_pair(std::shared_ptr<RemotePair> endpoint)
{
    auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->tx_limiter : endpoint->rx_limiter;
    for (;;)
    {
        auto data = co_await endpoint->flow->receive();
        if (!data)
        {
            asio::error_code error;
            endpoint->stream->shutdown(tcp::socket::shutdown_send, error);
            if (error && error != asio::error::not_connected)
            {
                throw asio::system_error(error);
            }
            co_return;
        }
        if (const auto deadline = limiter.reserve(data->size()))
        {
            co_await limiter.wait_until(*deadline);
        }
        co_await asio::async_write(*endpoint->stream, asio::buffer(data->data(), data->size()), asio::use_awaitable);
        endpoint->count(false, data->size());
        data.reset();
        endpoint->flow->complete_receive();
    }
}

void StreamPipeline::activate_remote_pair(std::uint64_t uuid)
{
    auto endpoint = remote_pairs_.at(uuid);
    endpoint->activate(endpoint->stream.has_value() && endpoint->flow);
}

asio::awaitable<void> StreamPipeline::run_remote_pair(std::uint64_t uuid)
{
    auto endpoint = remote_pairs_.at(uuid);
    if (!endpoint->active)
    {
        throw std::runtime_error("relay bridge unavailable");
    }
    const auto accessor = endpoint->track_accessor();
    std::exception_ptr failure;
    try
    {
        co_await await_transfers(read_remote_pair(endpoint), write_remote_pair(endpoint));
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    const auto cancellation = co_await asio::this_coro::cancellation_state;
    if (failure && !endpoint->closed && cancellation.cancelled() == asio::cancellation_type::none)
    {
        try
        {
            auto reason = exception_description(failure);
            endpoint->flow->reset(std::move(reason));
        }
        catch (const std::exception &)
        {
        }
    }
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}

void StreamPipeline::close_remote_pair(std::uint64_t uuid)
{
    const auto it = remote_pairs_.find(uuid);
    if (it == remote_pairs_.end())
    {
        return;
    }
    auto endpoint = it->second;
    remote_pairs_.erase(it);
    endpoint->close();
    id_allocator_->release(uuid);
}

void StreamPipeline::stop()
{
    stopped_ = true;
    asio::error_code ignored;
    acceptor_.close(ignored);
    for (auto &[id, pending] : unbound_)
    {
        pending->socket.close(ignored);
    }
    while (!remote_pairs_.empty())
    {
        close_remote_pair(remote_pairs_.begin()->first);
    }
    while (!local_pairs_.empty())
        close_local_pair(local_pairs_.begin()->first);
}

void StreamPipeline::wait_for_setup_tasks() noexcept
{
    auto pending = setup_tasks_.load();
    while (pending != 0)
    {
        setup_tasks_.wait(pending);
        pending = setup_tasks_.load();
    }
}
