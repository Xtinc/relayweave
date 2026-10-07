#include "tls_channel.h"
#include "asio/experimental/parallel_group.hpp"

static void configure_tls_socket(TLSStream &stream)
{
    asio::error_code error;
    auto &tcp_stream = stream.lowest_layer();
    tcp_stream.set_option(asio::ip::tcp::no_delay(true), error);
    if (error)
    {
        throw std::runtime_error("TLS configuration failed: operation=set TCP_NODELAY, error=" + error.message() +
                                 ", category=" + error.category().name());
    }

    tcp_stream.set_option(asio::socket_base::keep_alive(true), error);
    if (error)
    {
        throw std::runtime_error("TLS configuration failed: operation=enable TCP keepalive, error=" + error.message() +
                                 ", category=" + error.category().name());
    }
}

void configure_tls_client(TLSStream &stream, const std::string &verify_host, const std::optional<std::string> &sni_name)
{
    configure_tls_socket(stream);
    if (verify_host.empty())
    {
        throw std::runtime_error("TLS configuration failed: role=client, verify_host is empty");
    }

    const std::string sni_value = sni_name.value_or(verify_host);
    if (!sni_value.empty() && SSL_set_tlsext_host_name(stream.native_handle(), sni_value.c_str()) != 1)
    {
        throw std::runtime_error("TLS configuration failed: operation=set SNI, sni=" + sni_value);
    }

    asio::error_code error;
    stream.set_verify_mode(asio::ssl::verify_peer, error);
    if (error)
    {
        throw std::runtime_error("TLS configuration failed: operation=set client verify mode, error=" +
                                 error.message() + ", category=" + error.category().name());
    }
    stream.set_verify_callback(asio::ssl::host_name_verification(verify_host), error);
    if (error)
    {
        throw std::runtime_error("TLS configuration failed: operation=set host-name verification callback, "
                                 "verify_host=" +
                                 verify_host + ", error=" + error.message() + ", category=" + error.category().name());
    }
}

void configure_tls_server(TLSStream &stream)
{
    configure_tls_socket(stream);
    asio::error_code error;
    stream.set_verify_mode(asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert, error);
    if (error)
    {
        throw std::runtime_error("TLS configuration failed: operation=set server verify mode, error=" +
                                 error.message() + ", category=" + error.category().name());
    }
}

TLSChannel::TLSChannel(tcp::socket &&socket, asio::ssl::context &ssl_context, TLSChannelRole role,
                       TLSChannelConfig config)
    : executor_(socket.get_executor()), stream_(std::move(socket), ssl_context), role_(role),
      config_(std::move(config)), write_channel_(executor_, config_.max_queued_messages),
      receive_channel_(executor_, config_.max_queued_messages), close_channel_(executor_, 1),
      run_done_channel_(executor_, 1)
{
    asio::error_code error;
    const auto endpoint = stream_.lowest_layer().remote_endpoint(error);
    std::string peer = "unknown";
    if (!error)
    {
        const auto address = endpoint.address().to_string();
        peer = endpoint.address().is_v6() ? "[" + address + "]:" + std::to_string(endpoint.port())
                                          : address + ":" + std::to_string(endpoint.port());
    }
    log_context_ = "[" + peer + "]";
}

TLSChannel::~TLSChannel()
{
    finalize_close();
}

asio::awaitable<void> TLSChannel::start(std::string verify_host, std::optional<std::string> sni_name)
{
    auto self = shared_from_this();
    co_await asio::dispatch(self->executor(), asio::use_awaitable);

    if (self->state_ != State::Created)
    {
        throw std::logic_error("TLS channel start rejected: state=" + std::string(state_name(self->state_)));
    }

    self->state_ = State::Handshaking;
    try
    {
        self->configure_tls(verify_host, sni_name);

        co_await self->stream_.async_handshake(
            self->role_ == TLSChannelRole::S ? asio::ssl::stream_base::server : asio::ssl::stream_base::client,
            asio::cancel_after(self->config_.handshake_timeout, asio::use_awaitable));

        if (self->state_ != State::Handshaking)
        {
            throw asio::system_error(asio::error::operation_aborted, "TLS channel start cancelled");
        }

        PROXY_DEBUG_PRINT("TLS handshake complete role=%s peer=%.*s version=%s cipher=%s",
                          self->role_ == TLSChannelRole::C ? "client" : "server", static_cast<int>(self->peer().size()),
                          self->peer().data(), SSL_get_version(self->stream_.native_handle()),
                          SSL_get_cipher_name(self->stream_.native_handle()));
        self->state_ = State::Connected;
        asio::co_spawn(self->executor(), [self]() -> asio::awaitable<void> { co_await self->run(); }, asio::detached);
    }
    catch (...)
    {
        self->finalize_close();
        // The caller reports the handshake failure with its connection context.
        throw;
    }
}

asio::awaitable<void> TLSChannel::async_disconnect()
{
    auto self = shared_from_this();
    co_await asio::dispatch(self->executor(), asio::use_awaitable);

    switch (self->state_)
    {
    case State::Created:
    case State::Handshaking:
        self->finalize_close();
        co_return;
    case State::Connected:
        self->request_close();
        break;
    case State::Closing:
        break;
    case State::Closed:
        co_return;
    }

    co_await self->async_wait_closed();
}

asio::awaitable<void> TLSChannel::async_wait_closed()
{
    auto self = shared_from_this();
    co_await asio::dispatch(self->executor(), asio::use_awaitable);

    if (self->state_ == State::Closed)
    {
        co_return;
    }

    auto [ec] = co_await self->run_done_channel_.async_receive(use_nothrow_awaitable);
    if (ec && ec != asio::experimental::error::channel_closed)
    {
        throw asio::system_error(ec, "TLS channel close wait failed");
    }
}

asio::awaitable<CtrlMessage> TLSChannel::async_receive(std::chrono::steady_clock::duration timeout)
{
    auto self = shared_from_this();
    co_await asio::dispatch(self->executor(), asio::use_awaitable);

    if (timeout == std::chrono::steady_clock::duration::max())
        co_return co_await self->receive_channel_.async_receive(asio::use_awaitable);
    co_return co_await self->receive_channel_.async_receive(asio::cancel_after(timeout, asio::use_awaitable));
}

void TLSChannel::send(CtrlMessage message)
{
    asio::post(executor(), [self = shared_from_this(), message = std::move(message)]() mutable {
        if (self->state_ != State::Connected)
        {
            PROXY_ERROR_PRINT("TLS send rejected command=%s state=%s %s", message.command.c_str(),
                              state_name(self->state_), self->log_context_.c_str());
            return;
        }
        const auto command = message.command;
        try
        {
            self->enqueue_message(WireMessage::pack(std::move(message)));
        }
        catch (const std::exception &exception)
        {
            PROXY_ERROR_PRINT("TLS encode failed command=%s reason=%s %s", command.c_str(), exception.what(),
                              self->log_context_.c_str());
            self->request_close();
        }
    });
}

asio::any_io_executor TLSChannel::executor() const noexcept
{
    return executor_;
}

std::string_view TLSChannel::peer() const noexcept
{
    return std::string_view(log_context_).substr(1, log_context_.size() - 2);
}

asio::awaitable<void> TLSChannel::run()
{
    try
    {
        time_point deadline = std::chrono::steady_clock::now() + config_.heartbeat_timeout;
        {
            auto [completion_order, read_exception, write_exception, keepalive_exception, close_exception] =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(executor(), rloop(deadline), asio::deferred),
                    asio::co_spawn(executor(), wloop(), asio::deferred),
                    asio::co_spawn(executor(), keepalive(deadline), asio::deferred),
                    asio::co_spawn(executor(), wait_close_request(), asio::deferred))
                    .async_wait(asio::experimental::wait_for_one(), asio::use_awaitable);

            const std::array<std::exception_ptr, 4> exceptions{read_exception, write_exception, keepalive_exception,
                                                               close_exception};
            if (exceptions[completion_order[0]])
            {
                std::rethrow_exception(exceptions[completion_order[0]]);
            }
        }

        state_ = State::Closing;
        auto [ec] =
            co_await stream_.async_shutdown(asio::cancel_after(config_.disconnect_timeout, use_nothrow_awaitable));
        if (ec && ec != asio::error::eof)
        {
            PROXY_ERROR_PRINT("TLS close failed reason=%s category=%s %s", ec.message().c_str(), ec.category().name(),
                              log_context_.c_str());
        }
    }
    catch (const asio::system_error &exception)
    {
        if (exception.code() == asio::error::operation_aborted && state_ == State::Closing)
            PROXY_DEBUG_PRINT("TLS cancelled %s", log_context_.c_str());
        else
            PROXY_ERROR_PRINT("TLS failed reason=%s category=%s %s", exception.what(),
                              exception.code().category().name(), log_context_.c_str());
    }
    catch (const std::exception &exception)
    {
        PROXY_ERROR_PRINT("TLS failed reason=%s %s", exception.what(), log_context_.c_str());
    }
    catch (...)
    {
        PROXY_ERROR_PRINT("TLS failed reason=unknown exception %s", log_context_.c_str());
    }

    finalize_close();
}

asio::awaitable<void> TLSChannel::rloop(time_point &deadline)
{
    MessageReceiver receiver;
    for (;;)
    {
        WireMessage::Header header{};
        auto [ec1, n1] = co_await asio::async_read(stream_, asio::buffer(header), use_nothrow_awaitable);
        if (ec1)
        {
            if (ec1 == asio::error::eof)
            {
                state_ = State::Closing;
                PROXY_DEBUG_PRINT("TLS peer closed %s", log_context_.c_str());
                co_return;
            }
            throw asio::system_error(ec1, "TLS channel read failed");
        }

        BytesBuf payload(WireMessage::decode_length(header));
        auto [ec2, n2] = co_await asio::async_read(stream_, asio::buffer(payload), use_nothrow_awaitable);
        if (ec2)
        {
            if (ec2 == asio::error::eof)
            {
                state_ = State::Closing;
                PROXY_DEBUG_PRINT("TLS peer closed mid-frame %s", log_context_.c_str());
                co_return;
            }
            throw asio::system_error(ec2, "TLS channel read failed");
        }

        deadline = std::chrono::steady_clock::now() + config_.heartbeat_timeout;

        auto complete = receiver.receive(payload);
        if (!complete)
            continue;
        auto message = std::move(*complete);
        const auto command = message.type();
        if (command == CtrlCommand::Ping)
        {
            enqueue_message(WireMessage::pack(CtrlMessage{CtrlCommand::Pong}));
        }
        else if (command == CtrlCommand::Pong)
        {
            continue;
        }
        else if (!receive_channel_.try_send(asio::error_code{}, std::move(message)))
        {
            throw std::runtime_error("TLS channel receive queue is full");
        }
    }
}

asio::awaitable<void> TLSChannel::wloop()
{
    for (;;)
    {
        auto message = co_await write_channel_.async_receive(asio::use_awaitable);
        co_await asio::async_write(stream_, asio::buffer(message), asio::use_awaitable);
    }
}

asio::awaitable<void> TLSChannel::keepalive(time_point &deadline)
{
    asio::steady_timer timer(executor());
    auto next_ping = std::chrono::steady_clock::now() + config_.heartbeat_interval;
    for (;;)
    {
        timer.expires_at(next_ping < deadline ? next_ping : deadline);
        co_await timer.async_wait(asio::use_awaitable);
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            throw asio::system_error(asio::error::timed_out, "TLS channel heartbeat timed out");
        }
        if (now >= next_ping)
        {
            if (state_ == State::Connected)
            {
                enqueue_message(WireMessage::pack(CtrlMessage{CtrlCommand::Ping}));
            }
            next_ping = now + config_.heartbeat_interval;
        }
    }
}

asio::awaitable<void> TLSChannel::wait_close_request()
{
    co_await close_channel_.async_receive(asio::use_awaitable);
}

void TLSChannel::configure_tls(const std::string &verify_host, const std::optional<std::string> &sni_name)
{
    if (role_ == TLSChannelRole::C)
    {
        configure_tls_client(stream_, verify_host, sni_name);
    }
    else
    {
        configure_tls_server(stream_);
    }
}

void TLSChannel::enqueue_message(BytesBuf message)
{
    if (!write_channel_.try_send(asio::error_code{}, std::move(message)))
    {
        PROXY_ERROR_PRINT("TLS queue full capacity=%zu open=%d %s", write_channel_.capacity(), write_channel_.is_open(),
                          log_context_.c_str());
        request_close();
    }
}

void TLSChannel::request_close()
{
    if (state_ != State::Connected)
    {
        return;
    }

    state_ = State::Closing;
    if (!close_channel_.try_send(asio::error_code{}))
    {
        PROXY_ERROR_PRINT("TLS close signal failed state=%s open=%d %s", state_name(state_), close_channel_.is_open(),
                          log_context_.c_str());
        asio::error_code cancel_error;
        stream_.lowest_layer().cancel(cancel_error);
        if (cancel_error)
        {
            PROXY_ERROR_PRINT("TLS cancel failed reason=%s category=%s %s", cancel_error.message().c_str(),
                              cancel_error.category().name(), log_context_.c_str());
        }
    }
}

void TLSChannel::finalize_close() noexcept
{
    state_ = State::Closing;

    if (write_channel_.is_open())
    {
        write_channel_.reset();
        write_channel_.close();
    }
    if (close_channel_.is_open())
    {
        close_channel_.reset();
        close_channel_.close();
    }
    if (receive_channel_.is_open())
    {
        receive_channel_.reset();
        receive_channel_.close();
    }

    auto &socket = stream_.lowest_layer();
    if (socket.is_open())
    {
        asio::error_code ignored;
        socket.close(ignored);
    }

    state_ = State::Closed;
    run_done_channel_.close();
}

const char *TLSChannel::state_name(State state) noexcept
{
    switch (state)
    {
    case State::Created:
        return "created";
    case State::Handshaking:
        return "handshaking";
    case State::Connected:
        return "connected";
    case State::Closing:
        return "closing";
    case State::Closed:
        return "closed";
    }
    return "unknown";
}

TLSChannelConfig parse_channel_config(const njson &root)
{
    using namespace config;
    TLSChannelConfig result;
    const auto iterator = root.find("channel");
    if (iterator == root.end())
    {
        return result;
    }

    const auto &channel = *iterator;
    reject_unknown_fields(channel,
                          {"handshake_timeout_ms", "disconnect_timeout_ms", "heartbeat_interval_ms",
                           "heartbeat_timeout_ms", "max_queued_messages"},
                          "channel");
    result.handshake_timeout = optional_duration(channel, "handshake_timeout_ms", result.handshake_timeout, "channel");
    result.disconnect_timeout =
        optional_duration(channel, "disconnect_timeout_ms", result.disconnect_timeout, "channel");
    result.heartbeat_interval =
        optional_duration(channel, "heartbeat_interval_ms", result.heartbeat_interval, "channel");
    result.heartbeat_timeout = optional_duration(channel, "heartbeat_timeout_ms", result.heartbeat_timeout, "channel");
    result.max_queued_messages = static_cast<std::size_t>(
        optional_unsigned(channel, "max_queued_messages", result.max_queued_messages, 1, max_queue_size, "channel"));

    if (result.heartbeat_interval >= result.heartbeat_timeout)
    {
        throw std::runtime_error("channel.heartbeat_interval_ms must be less than channel.heartbeat_timeout_ms");
    }
    return result;
}
