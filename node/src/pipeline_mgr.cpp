#include "pipeline_mgr.h"
#include "flow_protocol.h"
#include "frame_io.h"
#include "xfr_channel.h"
#include <exception>

static auto remaining_timeout(std::chrono::steady_clock::time_point deadline)
{
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero())
        throw asio::system_error(asio::error::timed_out, "relay transfer connection setup timed out");
    return remaining;
}

TcpTransport::TcpTransport(asio::ssl::context &) noexcept
{
}

asio::awaitable<TcpTransport::Stream> TcpTransport::prepare(Stream socket, std::chrono::steady_clock::time_point) const
{
    co_return std::move(socket);
}

asio::awaitable<void> TcpTransport::relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                          TokenBucket &tx_limiter, ServiceTraffic &traffic)
{
    return relay_tcp(producer, consumer, &rx_limiter, &tx_limiter, &traffic.rx, &traffic.tx);
}

TlsTransport::TlsTransport(asio::ssl::context &context) noexcept : context_(context)
{
}

asio::awaitable<TlsTransport::Stream> TlsTransport::prepare(asio::ip::tcp::socket socket,
                                                            std::chrono::steady_clock::time_point deadline) const
{
    Stream stream(std::move(socket), context_);
    configure_tls_server(stream);
    co_await stream.async_handshake(asio::ssl::stream_base::server,
                                    asio::cancel_after(remaining_timeout(deadline), asio::use_awaitable));
    co_return std::move(stream);
}

asio::awaitable<void> TlsTransport::relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                          TokenBucket &tx_limiter, ServiceTraffic &traffic)
{
    return relay_tls(producer, consumer, &rx_limiter, &tx_limiter, &traffic.rx, &traffic.tx);
}

template <typename Transport>
StreamPipeline<Transport>::StreamPipeline(asio::any_io_executor executor, asio::ssl::context &ssl_context,
                                          std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address,
                                          std::uint16_t data_port, std::size_t max_setup_connections,
                                          std::size_t capacity, std::chrono::steady_clock::duration setup_timeout,
                                          TrafficLimitConfig config)
    : executor_(std::move(executor)), transport_(ssl_context), id_allocator_(std::move(id_allocator)),
      listen_address_(std::move(listen_address)), data_port_(data_port), max_setup_connections_(max_setup_connections),
      capacity_(capacity), setup_timeout_(setup_timeout), config_(std::move(config)), acceptor_(executor_)
{
}

template <typename Transport> void StreamPipeline<Transport>::start()
{
    if (started_ || stopped_)
        throw std::logic_error(std::string(Transport::name) + " pipeline can only be started once");

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

    asio::co_spawn(executor_, [self = this->shared_from_this()] { return self->accept_loop(); }, asio::detached);
}

template <typename Transport> asio::awaitable<void> StreamPipeline<Transport>::accept_loop()
{
    for (;;)
    {
        auto [error, socket] = co_await acceptor_.async_accept(use_nothrow_awaitable);
        if (error)
        {
            if (stopped_ || error == asio::error::operation_aborted)
                co_return;
            PROXY_ERROR_PRINT("Transfer accept failed %s reason=%s category=%s", Transport::name.data(),
                              error.message().c_str(), error.category().name());
            continue;
        }
        if (stopped_)
        {
            co_return;
        }

        const auto pending = pending_sockets_.load();
        if (pending >= max_setup_connections_)
        {
            PROXY_ERROR_PRINT("Transfer rejected %s setting_up=%zu limit=%zu", Transport::name.data(), pending,
                              max_setup_connections_);
            continue;
        }

        pending_sockets_.fetch_add(1);
        asio::co_spawn(
            executor_,
            [self = this->shared_from_this(), socket = std::move(socket)]() mutable {
                return self->run_transfer_session(std::move(socket));
            },
            asio::detached);
    }
}

template <typename Transport> asio::awaitable<void> StreamPipeline<Transport>::run_transfer_session(tcp::socket socket)
{
    ScopeGuard pending_guard([this]() noexcept {
        pending_sockets_.fetch_sub(1);
        pending_sockets_.notify_all();
    });

    try
    {
        const auto deadline = std::chrono::steady_clock::now() + setup_timeout_;
        auto stream = co_await transport_.prepare(std::move(socket), deadline);
        const auto frame = co_await read_ctrl_frame(stream, deadline);
        if constexpr (std::is_same_v<Transport, TlsTransport>)
        {
            if (config::message_params(frame).value("tls_framing", std::string{}) != tls_data_framing)
            {
                throw std::invalid_argument("Unsupported TLS data framing");
            }
        }
        const auto message = RelayAttach::from_msg(frame);
        attach(message.role, message.uuid, message.ticket, std::move(stream));
    }
    catch (const std::exception &exception)
    {
        if (!stopped_)
            PROXY_ERROR_PRINT("Transfer rejected %s reason=%s", Transport::name.data(), exception.what());
    }
    catch (...)
    {
        if (!stopped_)
            PROXY_ERROR_PRINT("Transfer rejected %s reason=unknown exception", Transport::name.data());
    }
}

template <typename Transport> AttachPair StreamPipeline<Transport>::install_local_pair()
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

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::prepare_local_pair(std::uint64_t uuid, SRVTrafficPtr traffic,
                                                                    std::string accessor)
{
    auto pair = local_pairs_.at(uuid);
    co_await pair->prepare(config_, std::move(traffic), std::move(accessor));
}

template <typename Transport> asio::awaitable<void> StreamPipeline<Transport>::run_local_pair(std::uint64_t uuid)
{
    auto pair = local_pairs_.at(uuid);
    if (!pair->active)
        throw std::runtime_error("local pair unavailable");
    co_await Transport::relay(*pair->producer_stream, *pair->consumer_stream, pair->rx_limiter, pair->tx_limiter,
                              *pair->traffic);
}

template <typename Transport> void StreamPipeline<Transport>::close_local_pair(std::uint64_t uuid)
{
    const auto it = local_pairs_.find(uuid);
    if (it == local_pairs_.end())
        return;
    auto pair = it->second;
    local_pairs_.erase(it);
    pair->close();
    id_allocator_->release(uuid);
}

template <typename Transport> AttachInfo StreamPipeline<Transport>::install_remote_pair(int role)
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

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::prepare_remote_pair(std::uint64_t uuid, FlowProtocol &channel,
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

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::read_remote_pair(std::shared_ptr<RemotePair> endpoint)
{
    std::array<std::uint8_t, lnk::FlowStream::maximum_chunk_size> buffer;
    auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->rx_limiter : endpoint->tx_limiter;
    for (;;)
    {
        std::size_t size;
        if constexpr (std::is_same_v<Transport, TlsTransport>)
        {
            size = co_await read_tls_data(*endpoint->stream, buffer);
        }
        else
        {
            const auto [error, received] =
                co_await endpoint->stream->async_read_some(asio::buffer(buffer), use_nothrow_awaitable);
            if (error && error != asio::error::eof)
            {
                throw asio::system_error(error);
            }
            size = received;
        }
        if (size == 0)
        {
            co_await endpoint->flow->finish_send();
            co_return;
        }
        co_await limiter.consume(size);
        co_await endpoint->flow->send(endpoint->flow->copy_payload({buffer.data(), size}));
        endpoint->count(true, size);
    }
}

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::write_remote_pair(std::shared_ptr<RemotePair> endpoint)
{
    auto &limiter = endpoint->role == RelayAttach::Producer ? endpoint->tx_limiter : endpoint->rx_limiter;
    for (;;)
    {
        auto data = co_await endpoint->flow->receive();
        if (!data)
        {
            if constexpr (std::is_same_v<Transport, TlsTransport>)
            {
                co_await finish_tls_data(*endpoint->stream);
            }
            else
            {
                asio::error_code error;
                endpoint->stream->shutdown(asio::ip::tcp::socket::shutdown_send, error);
                if (error && error != asio::error::not_connected)
                {
                    throw asio::system_error(error);
                }
            }
            co_return;
        }
        co_await limiter.consume(data->size());
        if constexpr (std::is_same_v<Transport, TlsTransport>)
        {
            co_await write_tls_data(*endpoint->stream, data->bytes());
            endpoint->count(false, data->size());
        }
        else
        {
            const auto written = co_await asio::async_write(*endpoint->stream, asio::buffer(data->data(), data->size()),
                                                            asio::use_awaitable);
            endpoint->count(false, written);
        }
        data.reset();
        endpoint->flow->complete_receive();
    }
}

template <typename Transport> void StreamPipeline<Transport>::activate_remote_pair(std::uint64_t uuid)
{
    auto endpoint = remote_pairs_.at(uuid);
    endpoint->activate(endpoint->stream.has_value() && endpoint->flow);
}

template <typename Transport> asio::awaitable<void> StreamPipeline<Transport>::run_remote_pair(std::uint64_t uuid)
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

template <typename Transport> void StreamPipeline<Transport>::close_remote_pair(std::uint64_t uuid)
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

template <typename Transport>
bool StreamPipeline<Transport>::attach_remote_pair(int role, std::uint64_t uuid, std::uint64_t ticket, Stream &stream)
{
    const auto it = remote_pairs_.find(uuid);
    if (it == remote_pairs_.end())
    {
        return false;
    }
    auto &endpoint = *it->second;
    if (endpoint.stream || role != endpoint.role || ticket != endpoint.ticket)
    {
        throw std::invalid_argument("relay.attach invalid or duplicate path endpoint");
    }
    endpoint.stream.emplace(std::move(stream));
    endpoint.attached.notify_all();
    return true;
}

template <typename Transport>
void StreamPipeline<Transport>::attach(int role, std::uint64_t uuid, std::uint64_t ticket, Stream stream)
{
    if (stopped_)
        return;
    if (attach_remote_pair(role, uuid, ticket, stream))
    {
        return;
    }
    const auto iterator = local_pairs_.find(uuid);
    if (iterator == local_pairs_.end())
        throw std::invalid_argument("relay.attach uuid is unknown or expired");
    const auto &pair = iterator->second;
    auto &slot = role == RelayAttach::Producer ? pair->producer_stream : pair->consumer_stream;
    const auto expected = role == RelayAttach::Producer ? pair->producer_ticket : pair->consumer_ticket;
    if (ticket != expected || slot)
        throw std::invalid_argument("relay.attach invalid ticket or duplicate role");
    slot.emplace(std::move(stream));
    if (pair->producer_stream && pair->consumer_stream)
        pair->attached.notify_all();
}

template <typename Transport> void StreamPipeline<Transport>::stop()
{
    stopped_ = true;
    asio::error_code ignored;
    acceptor_.close(ignored);
    while (!remote_pairs_.empty())
    {
        close_remote_pair(remote_pairs_.begin()->first);
    }
    while (!local_pairs_.empty())
        close_local_pair(local_pairs_.begin()->first);
}

template <typename Transport> void StreamPipeline<Transport>::wait_for_pending() noexcept
{
    auto pending = pending_sockets_.load();
    while (pending != 0)
    {
        pending_sockets_.wait(pending);
        pending = pending_sockets_.load();
    }
}

template class StreamPipeline<TcpTransport>;
template class StreamPipeline<TlsTransport>;
