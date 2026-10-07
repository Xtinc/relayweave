#ifndef PROXY_TLS_CHANNEL_H
#define PROXY_TLS_CHANNEL_H

#include "asio.hpp"
#include "asio/experimental/channel.hpp"
#include "asio/ssl.hpp"
#include "message.h"

inline constexpr auto use_nothrow_awaitable = asio::as_tuple(asio::use_awaitable);

template <typename F> class ScopeGuard
{
  public:
    explicit ScopeGuard(F &&f) : f_(std::forward<F>(f)), active_(true)
    {
    }
    ScopeGuard(ScopeGuard &&other) noexcept : f_(std::move(other.f_)), active_(std::exchange(other.active_, false))
    {
    }
    ScopeGuard(const ScopeGuard &) = delete;
    ScopeGuard &operator=(const ScopeGuard &) = delete;
    ~ScopeGuard()
    {
        if (active_)
        {
            f_();
        }
    }

    void dismiss()
    {
        active_ = false;
    }

  private:
    F f_;
    bool active_;
};

struct TLSChannelConfig
{
    std::chrono::steady_clock::duration handshake_timeout = std::chrono::seconds(5);
    std::chrono::steady_clock::duration disconnect_timeout = std::chrono::seconds(5);
    std::chrono::steady_clock::duration heartbeat_interval = std::chrono::seconds(5);
    std::chrono::steady_clock::duration heartbeat_timeout = std::chrono::seconds(30);
    std::size_t max_queued_messages = 100;
};

enum class TLSChannelRole
{
    C,
    S,
};

using TLSStream = asio::ssl::stream<asio::ip::tcp::socket>;

void configure_tls_client(TLSStream &stream, const std::string &verify_host,
                          const std::optional<std::string> &sni_name = std::nullopt);
void configure_tls_server(TLSStream &stream);

class TLSChannel : public std::enable_shared_from_this<TLSChannel>
{
    using tcp = asio::ip::tcp;
    using time_point = std::chrono::steady_clock::time_point;
    using write_channel = asio::experimental::channel<void(asio::error_code, BytesBuf)>;
    using receive_channel = asio::experimental::channel<void(asio::error_code, CtrlMessage)>;
    using signal_channel = asio::experimental::channel<void(asio::error_code)>;

    enum class State
    {
        Created,
        Handshaking,
        Connected,
        Closing,
        Closed,
    };

  public:
    TLSChannel(tcp::socket &&socket, asio::ssl::context &ssl_context, TLSChannelRole role,
               TLSChannelConfig config = {});
    ~TLSChannel();

    asio::awaitable<void> start(std::string verify_host, std::optional<std::string> sni_name = std::nullopt);
    asio::awaitable<void> async_disconnect();
    asio::awaitable<void> async_wait_closed();
    asio::awaitable<CtrlMessage> async_receive(
        std::chrono::steady_clock::duration timeout = std::chrono::steady_clock::duration::max());
    void send(CtrlMessage message);
    asio::any_io_executor executor() const noexcept;
    std::string_view peer() const noexcept;

  private:
    asio::awaitable<void> run();
    asio::awaitable<void> rloop(time_point &deadline);
    asio::awaitable<void> wloop();
    asio::awaitable<void> keepalive(time_point &deadline);
    asio::awaitable<void> wait_close_request();

    void configure_tls(const std::string &verify_host, const std::optional<std::string> &sni_name);
    void enqueue_message(BytesBuf message);
    void request_close();
    void finalize_close() noexcept;
    static const char *state_name(State state) noexcept;

  private:
    asio::any_io_executor executor_;
    TLSStream stream_;
    TLSChannelRole role_;
    TLSChannelConfig config_;
    write_channel write_channel_;
    receive_channel receive_channel_;
    signal_channel close_channel_;
    signal_channel run_done_channel_;
    std::string log_context_;

    State state_ = State::Created;
};

TLSChannelConfig parse_channel_config(const njson &root);

using ControlSession = TLSChannel;
using ControlSessionPtr = std::shared_ptr<ControlSession>;

#endif // PROXY_TLS_CHANNEL_H
