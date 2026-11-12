#ifndef PROXY_TLS_CHANNEL_H
#define PROXY_TLS_CHANNEL_H

#include "asio.hpp"
#include "asio/experimental/channel.hpp"
#include "asio/ssl.hpp"
#include "message.h"
#include <map>

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

/**
 * @brief Own bounded IPv4 admission penalties for one TCP listener on its single-threaded executor.
 * @details Access advances one shared minute clock and ages the whole table. Entries contain only
 * counters, never individual timestamps. IPv4 loopback (127.0.0.0/8) is never tracked or blocked.
 * No tasks or external dependencies are retained.
 */
class PeerBlacklist
{
  public:
    using Clock = std::chrono::steady_clock;
    static constexpr auto round_duration = std::chrono::minutes(1);
    static constexpr std::uint8_t failure_limit = 5;
    static constexpr std::uint16_t initial_rounds = 30;
    static constexpr std::uint16_t maximum_rounds = 720;
    static constexpr std::size_t capacity = 16384;

    /**
     * @brief Initialize the listener's common aging clock without starting a timer.
     * @param listener Listener name copied for ban and capacity diagnostics.
     * @param now Initial steady-clock round boundary; injectable for deterministic tests.
     */
    explicit PeerBlacklist(std::string_view listener, Clock::time_point now = Clock::now());

    /**
     * @brief Age all entries, then reject and extend an active ban by one round, capped at 720.
     * @param address IPv4 source of a newly accepted TCP socket, independent of its source port.
     * @param now Current steady-clock time; must not precede previous calls.
     * @return True when the socket must be rejected; false for loopback, an allowed or expired source.
     * @pre Call exactly once per accepted socket, on this listener's executor.
     */
    bool blocked(asio::ip::address_v4 address, Clock::time_point now = Clock::now());

    /**
     * @brief Record one connection ending before authentication or authorization, banning on the fifth this round.
     * @param address IPv4 source captured before transferring ownership of the accepted socket.
     * @param reason Diagnostic reason used only when a new ban is installed.
     * @param now Current steady-clock time; must not precede previous calls.
     * @return True only for a newly installed ban; false for loopback, below threshold, while banned, or at capacity.
     * @pre Call at most once per failed connection, on this listener's executor.
     */
    bool record_failure(asio::ip::address_v4 address, std::string_view reason,
                        Clock::time_point now = Clock::now());

  private:
    struct Entry
    {
        std::uint8_t failures = 0;
        std::uint16_t rounds = 0;
    };

    /** @brief Clear all suspicion counters and subtract elapsed rounds from bans in one bounded scan. */
    void age(Clock::time_point now);

    std::string listener_;
    Clock::time_point round_at_;
    std::map<std::uint32_t, Entry> entries_;
    bool capacity_reported_ = false;
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

/**
 * @brief Configure a TLS 1.3 business context for mutual, pinned raw-public-key authentication.
 * @param context Separate business context, owned until every business stream has drained.
 * @param identity Existing X.509 context supplying the shared certificate and matching private key.
 * @throws std::runtime_error If credentials are absent, mismatched, or RPK configuration fails.
 * @details Copies credential references into context; verification pins its certificate's public key.
 * Session resumption and early data are disabled. Control context verification remains unchanged.
 */
void configure_tls_rpk(asio::ssl::context &context, asio::ssl::context &identity);

template <typename Stream>
asio::awaitable<CtrlMessage> read_ctrl_frame(Stream &stream, std::chrono::steady_clock::time_point deadline)
{
    const auto remaining = [&]() {
        const auto value = deadline - std::chrono::steady_clock::now();
        if (value <= std::chrono::steady_clock::duration::zero())
        {
            throw asio::system_error(asio::error::timed_out, "frame read deadline");
        }
        return value;
    };
    WireMessage::Header header{};
    co_await asio::async_read(stream, asio::buffer(header), asio::cancel_after(remaining(), asio::use_awaitable));
    BytesBuf payload(WireMessage::decode_length(header));
    co_await asio::async_read(stream, asio::buffer(payload), asio::cancel_after(remaining(), asio::use_awaitable));
    co_return CtrlMessage::deserialize(payload);
}

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
    // Request closure without starting a waiter; the connection owner drains it.
    void disconnect();
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
