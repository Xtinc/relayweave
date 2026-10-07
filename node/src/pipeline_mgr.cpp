#include "pipeline_mgr.h"
#include "frame_io.h"
#include <vector>

namespace
{
auto remaining_timeout(std::chrono::steady_clock::time_point deadline)
{
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero())
        throw asio::system_error(asio::error::timed_out, "relay transfer connection setup timed out");
    return remaining;
}

} // namespace

TcpTransport::TcpTransport(asio::ssl::context &) noexcept
{
}

asio::awaitable<TcpTransport::Stream> TcpTransport::prepare(Stream socket,
                                                            std::chrono::steady_clock::time_point) const
{
    co_return std::move(socket);
}

asio::awaitable<void> TcpTransport::relay(Stream &producer, Stream &consumer, TokenBucket &rx_limiter,
                                          TokenBucket &tx_limiter, ServiceTraffic &traffic)
{
    co_await relay_tcp(producer, consumer, &rx_limiter, &tx_limiter, &traffic.rx, &traffic.tx);
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
    co_await relay_tls(producer, consumer, &rx_limiter, &tx_limiter, &traffic.rx, &traffic.tx);
}

template <typename Transport>
StreamPipeline<Transport>::Relay::Relay(asio::any_io_executor executor, std::uint64_t uuid,
                                        std::uint64_t producer_ticket, std::uint64_t consumer_ticket,
                                        std::string service, const ControlSessionPtr &producer,
                                        const ControlSessionPtr &consumer, std::uint64_t request_id,
                                        const TrafficLimitConfig &config, SRVTrafficPtr traffic)
    : uuid(uuid), producer_ticket(producer_ticket), consumer_ticket(consumer_ticket), service(std::move(service)),
      producer(producer), consumer(consumer), request_id(request_id), accessor(consumer->peer()),
      setup_timer(std::move(executor)),
      rx_limiter(config.rx_bytes_per_second, config.rx_burst_bytes),
      tx_limiter(config.tx_bytes_per_second, config.tx_burst_bytes), traffic(std::move(traffic))
{
}

template <typename Transport>
bool StreamPipeline<Transport>::Relay::belongs_to(const ControlSessionPtr &session) const
{
    return producer.lock() == session || consumer.lock() == session;
}

template <typename Transport>
bool StreamPipeline<Transport>::Relay::complete() const noexcept
{
    return producer_stream && consumer_stream;
}

template <typename Transport>
void StreamPipeline<Transport>::Relay::cancel() noexcept
{
    try
    {
        setup_timer.cancel();
    }
    catch (...)
    {
    }

    asio::error_code ignored;
    if (producer_stream)
        producer_stream->lowest_layer().cancel(ignored);
    if (consumer_stream)
        consumer_stream->lowest_layer().cancel(ignored);
}

template <typename Transport>
StreamPipeline<Transport>::StreamPipeline(asio::any_io_executor executor, asio::ssl::context &ssl_context,
                                          std::shared_ptr<RelayIdAllocator> id_allocator, std::string listen_address,
                                          std::uint16_t data_port, std::size_t max_setup_connections,
                                          std::size_t capacity, std::chrono::steady_clock::duration setup_timeout,
                                          TrafficLimitConfig config)
    : executor_(std::move(executor)), transport_(ssl_context), id_allocator_(std::move(id_allocator)),
      listen_address_(std::move(listen_address)), data_port_(data_port),
      max_setup_connections_(max_setup_connections), capacity_(capacity), setup_timeout_(setup_timeout),
      config_(std::move(config)), acceptor_(executor_)
{
}

template <typename Transport>
void StreamPipeline<Transport>::start()
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

    asio::co_spawn(executor_,
                   [self = this->shared_from_this()]() -> asio::awaitable<void> { co_await self->accept_loop(); },
                   asio::detached);
}

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::accept_loop()
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
            asio::error_code ignored;
            socket.close(ignored);
            co_return;
        }

        const auto pending = pending_sockets_.load();
        if (pending >= max_setup_connections_)
        {
            asio::error_code ignored;
            socket.close(ignored);
            PROXY_ERROR_PRINT("Transfer rejected %s setting_up=%zu limit=%zu", Transport::name.data(),
                              pending, max_setup_connections_);
            continue;
        }

        pending_sockets_.fetch_add(1);
        asio::co_spawn(
            executor_,
            [self = this->shared_from_this(), socket = std::move(socket)]() mutable -> asio::awaitable<void> {
                co_await self->run_transfer_session(std::move(socket));
            },
            asio::detached);
    }
}

template <typename Transport>
asio::awaitable<void> StreamPipeline<Transport>::run_transfer_session(tcp::socket socket)
{
    ScopeGuard pending_guard([this]() noexcept {
        pending_sockets_.fetch_sub(1);
        pending_sockets_.notify_all();
    });

    try
    {
        const auto deadline = std::chrono::steady_clock::now() + setup_timeout_;
        auto stream = co_await transport_.prepare(std::move(socket), deadline);
        const auto message = RelayAttach::from_msg(co_await read_ctrl_frame(stream, deadline));
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

template <typename Transport>
void StreamPipeline<Transport>::open(const ControlSessionPtr &producer, const ControlSessionPtr &consumer,
                                     std::string service, std::uint64_t request_id, SRVTrafficPtr traffic)
{
    try
    {
        do_open(producer, consumer, service, request_id, std::move(traffic));
    }
    catch (const std::exception &exception)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError,
                                   njson{{"request_id", request_id}, {"service", service},
                                         {"protocol", relay_protocol_name(Transport::protocol)},
                                         {"reason", exception.what()}}});
    }
    catch (...)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError,
                                   njson{{"request_id", request_id}, {"service", service},
                                         {"protocol", relay_protocol_name(Transport::protocol)},
                                         {"reason", "unknown relay error"}}});
    }
}

template <typename Transport>
void StreamPipeline<Transport>::do_open(const ControlSessionPtr &producer, const ControlSessionPtr &consumer,
                                        std::string service, std::uint64_t request_id, SRVTrafficPtr traffic)
{
    if (stopped_ || relays_.size() >= capacity_)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError,
                                   njson{{"request_id", request_id}, {"service", service},
                                         {"protocol", relay_protocol_name(Transport::protocol)},
                                         {"reason", stopped_ ? "server stopping" : "relay capacity reached"}}});
        return;
    }

    const auto uuid = id_allocator_->allocate();
    ScopeGuard id_rollback([this, uuid]() noexcept { id_allocator_->release(uuid); });
    const auto producer_ticket = generate_random_id();
    std::uint64_t consumer_ticket;
    do
        consumer_ticket = generate_random_id();
    while (consumer_ticket == producer_ticket);

    auto relay = std::make_shared<Relay>(executor_, uuid, producer_ticket, consumer_ticket, std::move(service),
                                         producer, consumer, request_id, config_, std::move(traffic));
    relays_.emplace(uuid, relay);
    ScopeGuard relay_rollback([&]() noexcept { erase_stream(relay); });
    id_rollback.dismiss();

    relay->setup_timer.expires_after(setup_timeout_);
    relay->setup_timer.async_wait(
        [weak_self = std::weak_ptr<StreamPipeline>(this->shared_from_this()), weak_relay = std::weak_ptr<Relay>(relay)](
            const asio::error_code &error) {
            if (!error)
                if (auto self = weak_self.lock())
                    if (auto relay = weak_relay.lock())
                        self->terminate_stream(std::move(relay), "relay setup timed out");
        });

    const njson common{{"uuid", uuid}, {"service", relay->service},
                       {"protocol", relay_protocol_name(Transport::protocol)}, {"data_port", data_port_}};
    auto consumer_params = common;
    consumer_params["request_id"] = request_id;
    consumer_params["ticket"] = consumer_ticket;
    auto producer_params = common;
    producer_params["ticket"] = producer_ticket;
    consumer->send(CtrlMessage{CtrlCommand::RelayOpened, std::move(consumer_params)});
    producer->send(CtrlMessage{CtrlCommand::RelayOffer, std::move(producer_params)});
    relay_rollback.dismiss();
}

template <typename Transport>
void StreamPipeline<Transport>::attach(int role, std::uint64_t uuid, std::uint64_t ticket, Stream stream)
{
    if (stopped_)
        return;
    const auto iterator = relays_.find(uuid);
    if (iterator == relays_.end())
        throw std::invalid_argument("relay.attach uuid is unknown or expired");

    const auto &relay = iterator->second;
    if (relay->relaying)
        throw std::invalid_argument("relay.attach is only valid before stream relaying starts");

    std::optional<Stream> *slot;
    std::uint64_t expected_ticket;
    if (role == RelayAttach::Producer)
    {
        slot = &relay->producer_stream;
        expected_ticket = relay->producer_ticket;
    }
    else if (role == RelayAttach::Consumer)
    {
        slot = &relay->consumer_stream;
        expected_ticket = relay->consumer_ticket;
    }
    else
        throw std::invalid_argument("relay.attach contains an invalid role");

    if (ticket != expected_ticket)
        throw std::invalid_argument("relay.attach ticket is invalid for role");
    if (*slot)
        throw std::invalid_argument("relay.attach role is already connected");

    PROXY_DEBUG_PRINT("Relay attached %s %s service=%s uuid=%llu peer=%s", Transport::name.data(),
                      role == RelayAttach::Producer ? "producer" : "consumer", relay->service.c_str(),
                      static_cast<unsigned long long>(uuid), socket_peer(stream).c_str());
    slot->emplace(std::move(stream));
    if (relay->complete())
        start_stream(relay);
}

template <typename Transport>
bool StreamPipeline<Transport>::reject(const ControlSessionPtr &session, std::uint64_t uuid, std::string reason)
{
    if (stopped_)
        return false;
    const auto relay = relays_.find(uuid);
    if (relay == relays_.end() || relay->second->relaying || relay->second->producer.lock() != session)
        return false;
    terminate_stream(relay->second, std::move(reason));
    return true;
}

template <typename Transport>
bool StreamPipeline<Transport>::cancel(const ControlSessionPtr &session, std::optional<std::uint64_t> uuid,
                                       std::optional<std::uint64_t> request_id)
{
    if (stopped_)
        return false;
    for (const auto &[relay_uuid, relay] : relays_)
    {
        if (!relay->relaying && relay->belongs_to(session) &&
            ((uuid && *uuid == relay_uuid) || (request_id && *request_id == relay->request_id)))
        {
            terminate_stream(relay, "relay cancelled");
            return true;
        }
    }
    return false;
}

template <typename Transport>
void StreamPipeline<Transport>::disconnect(const ControlSessionPtr &session)
{
    if (stopped_)
        return;
    std::vector<std::shared_ptr<Relay>> relays;
    for (const auto &[uuid, relay] : relays_)
        if (relay->belongs_to(session))
            relays.push_back(relay);
    for (auto &relay : relays)
        terminate_stream(relay,
                         relay->relaying ? std::nullopt : std::optional<std::string>("control session disconnected"));
}

template <typename Transport>
void StreamPipeline<Transport>::stop()
{
    if (stopped_)
        return;
    stopped_ = true;
    asio::error_code ignored;
    acceptor_.close(ignored);
    auto relays = std::move(relays_);
    for (auto &[uuid, relay] : relays)
    {
        relay->cancel();
        if (relay->relaying)
        {
            PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu reason=node stopping", Transport::name.data(),
                             relay->service.c_str(), static_cast<unsigned long long>(relay->uuid));
            relay->traffic->remove_accessor(relay->accessor);
        }
        id_allocator_->release(uuid);
    }
}

template <typename Transport>
void StreamPipeline<Transport>::wait_for_pending()
{
    auto pending = pending_sockets_.load();
    while (pending != 0)
    {
        pending_sockets_.wait(pending);
        pending = pending_sockets_.load();
    }
}

template <typename Transport>
void StreamPipeline<Transport>::start_stream(const std::shared_ptr<Relay> &relay)
{
    auto producer = relay->producer.lock();
    auto consumer = relay->consumer.lock();
    if (!producer || !consumer)
    {
        terminate_stream(relay, "control session disconnected");
        return;
    }

    relay->traffic->add_accessor(relay->accessor);
    relay->setup_timer.cancel();
    relay->relaying = true;
    PROXY_INFO_PRINT("Relay [+] %s service=%s uuid=%llu", Transport::name.data(),
                     relay->service.c_str(), static_cast<unsigned long long>(relay->uuid));
    const njson ready{{"uuid", relay->uuid}, {"protocol", relay_protocol_name(Transport::protocol)}};
    producer->send(CtrlMessage{CtrlCommand::RelayReady, ready});
    consumer->send(CtrlMessage{CtrlCommand::RelayReady, ready});

    asio::co_spawn(
        executor_,
        [self = this->shared_from_this(), relay]() -> asio::awaitable<void> {
            try
            {
                co_await Transport::relay(*relay->producer_stream, *relay->consumer_stream, relay->rx_limiter,
                                          relay->tx_limiter, *relay->traffic);
            }
            catch (const std::exception &exception)
            {
                PROXY_ERROR_PRINT("Relay failed %s service=%s uuid=%llu reason=%s", Transport::name.data(),
                                  relay->service.c_str(), static_cast<unsigned long long>(relay->uuid), exception.what());
            }
            catch (...)
            {
                PROXY_ERROR_PRINT("Relay failed %s service=%s uuid=%llu reason=unknown exception",
                                  Transport::name.data(), relay->service.c_str(),
                                  static_cast<unsigned long long>(relay->uuid));
            }
            self->erase_stream(relay);
        },
        asio::detached);
}

template <typename Transport>
bool StreamPipeline<Transport>::erase_stream(const std::shared_ptr<Relay> &relay) noexcept
{
    const auto iterator = relays_.find(relay->uuid);
    if (iterator == relays_.end() || iterator->second != relay)
        return false;
    relay->cancel();
    if (relay->relaying)
    {
        PROXY_INFO_PRINT("Relay [x] %s service=%s uuid=%llu", Transport::name.data(),
                         relay->service.c_str(), static_cast<unsigned long long>(relay->uuid));
        relay->traffic->remove_accessor(relay->accessor);
    }
    relays_.erase(iterator);
    id_allocator_->release(relay->uuid);
    return true;
}

template <typename Transport>
void StreamPipeline<Transport>::terminate_stream(std::shared_ptr<Relay> relay, std::optional<std::string> reason)
{
    const bool active = relay->relaying;
    if (!erase_stream(relay) || active || !reason)
        return;
    PROXY_DEBUG_PRINT("Relay setup closed %s service=%s uuid=%llu reason=%s", Transport::name.data(),
                      relay->service.c_str(), static_cast<unsigned long long>(relay->uuid), reason->c_str());
    if (auto consumer = relay->consumer.lock())
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError,
                                   njson{{"request_id", relay->request_id}, {"uuid", relay->uuid},
                                         {"service", relay->service},
                                         {"protocol", relay_protocol_name(Transport::protocol)},
                                         {"reason", std::move(*reason)}}});
    }
}

template class StreamPipeline<TcpTransport>;
template class StreamPipeline<TlsTransport>;
