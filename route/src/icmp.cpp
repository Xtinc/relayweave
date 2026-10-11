#include "icmp.h"

#include <algorithm>
#include <array>
#include <random>
#include <stdexcept>

namespace
{
constexpr auto use_nothrow_awaitable = asio::as_tuple(asio::use_awaitable);
using ICMPProtocol = asio::ip::icmp;

constexpr std::size_t icmp_header_size = 8;
constexpr std::size_t payload_size = 12;
constexpr std::size_t echo_size = icmp_header_size + payload_size;
constexpr std::array<std::uint8_t, 4> payload_magic{'R', 'W', 'I', 'P'};

std::uint16_t read_u16(std::span<const std::uint8_t> bytes, std::size_t offset)
{
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) | bytes[offset + 1]);
}

std::uint64_t read_u64(std::span<const std::uint8_t> bytes, std::size_t offset)
{
    std::uint64_t value = 0;
    for (std::size_t index = 0; index != sizeof(value); ++index)
    {
        value = (value << 8U) | bytes[offset + index];
    }
    return value;
}

void write_u16(std::span<std::uint8_t> bytes, std::size_t offset, std::uint16_t value)
{
    bytes[offset] = static_cast<std::uint8_t>(value >> 8U);
    bytes[offset + 1] = static_cast<std::uint8_t>(value);
}

void write_u64(std::span<std::uint8_t> bytes, std::size_t offset, std::uint64_t value)
{
    for (std::size_t index = 0; index != sizeof(value); ++index)
    {
        bytes[offset + sizeof(value) - 1 - index] = static_cast<std::uint8_t>(value);
        value >>= 8U;
    }
}

std::uint16_t checksum(std::span<const std::uint8_t> bytes)
{
    std::uint32_t sum = 0;
    std::size_t index = 0;
    for (; index + 1 < bytes.size(); index += 2)
    {
        sum += static_cast<std::uint32_t>(bytes[index] << 8U) | bytes[index + 1];
    }
    if (index < bytes.size())
    {
        sum += static_cast<std::uint32_t>(bytes[index] << 8U);
    }
    while ((sum >> 16U) != 0)
    {
        sum = (sum & 0xffffU) + (sum >> 16U);
    }
    return static_cast<std::uint16_t>(~sum);
}

std::array<std::uint8_t, echo_size> make_echo(std::uint16_t sequence, std::uint64_t instance)
{
    std::array<std::uint8_t, echo_size> echo{};
    echo[0] = 8;
    write_u16(echo, 4, static_cast<std::uint16_t>(instance));
    write_u16(echo, 6, sequence);
    std::copy(payload_magic.begin(), payload_magic.end(), echo.begin() + icmp_header_size);
    write_u64(echo, icmp_header_size + 4, instance);
    write_u16(echo, 2, checksum(echo));
    return echo;
}

void cancel_timer_noexcept(asio::steady_timer &timer) noexcept
{
    try
    {
        timer.cancel();
    }
    catch (...)
    {
    }
}
} // namespace

ICMP::Session::Session(asio::io_context &io_context) : timer(io_context)
{
}

ICMP::ICMP(asio::io_context &io_context, Duration interval, Duration timeout)
    : io_context_(io_context), interval_(interval), timeout_(timeout), socket_(io_context),
      finished_wait_(io_context)
{
    if (interval_ <= Duration::zero())
    {
        throw std::invalid_argument("ICMP interval must be positive");
    }
    if (timeout_ <= Duration::zero() || timeout_ >= interval_)
    {
        throw std::invalid_argument("ICMP timeout must be positive and shorter than the interval");
    }
    std::random_device device;
    std::seed_seq seed{device(), device(), device(), device()};
    std::mt19937_64 random(seed);
    instance_ = random();
}

void ICMP::run(std::vector<asio::ip::address_v4> destinations, const History &history)
{
    if (stopping_)
    {
        throw asio::system_error(asio::error::operation_aborted, "ICMP is closed");
    }

    if (!sessions_.empty())
    {
        throw std::logic_error("ICMP has already been started");
    }

    if (destinations.empty())
    {
        throw std::invalid_argument("ICMP requires at least one destination");
    }

    for (const auto &destination : destinations)
    {
        if (destination.is_unspecified() || destination.is_multicast() || destination.to_uint() == 0xffffffffU)
        {
            throw std::invalid_argument("ICMP destination must be a unicast IPv4 address");
        }
    }
    std::sort(destinations.begin(), destinations.end());
    if (std::adjacent_find(destinations.begin(), destinations.end()) != destinations.end())
    {
        throw std::invalid_argument("ICMP destinations must be unique");
    }

    try
    {
        for (const auto &destination : destinations)
        {
            auto &session = sessions_.try_emplace(destination, io_context_).first->second;
            if (const auto previous = history.find(destination); previous != history.end())
            {
                session.state = previous->second;
            }
        }
    }
    catch (...)
    {
        sessions_.clear();
        throw;
    }

    asio::error_code open_error;
    socket_.open(ICMPProtocol::v4(), open_error);
    if (open_error)
    {
        sessions_.clear();
        throw asio::system_error(open_error, "ICMP socket open failed");
    }
    // Set up the local endpoint before starting the receive coroutine.
    asio::error_code bind_error;
    socket_.bind(ICMPProtocol::endpoint(asio::ip::address_v4::any(), 0), bind_error);
    if (bind_error)
    {
        asio::error_code ignored;
        socket_.close(ignored);
        sessions_.clear();
        throw asio::system_error(bind_error, "ICMP socket bind failed");
    }

    receiving_ = true;
    finished_wait_.expires_at(Clock::time_point::max());
    try
    {
        asio::co_spawn(io_context_, receive_loop(), asio::detached);
    }
    catch (...)
    {
        receiving_ = false;
        stop();
        throw;
    }
    for (const auto &entry : sessions_)
    {
        ++running_probes_;
        try
        {
            asio::co_spawn(io_context_, probe_loop(entry.first), asio::detached);
        }
        catch (...)
        {
            --running_probes_;
            stop();
            throw;
        }
    }
}

asio::awaitable<void> ICMP::probe_loop(asio::ip::address_v4 destination)
{
    auto &session = sessions_.at(destination);
    auto next_send_at = Clock::now();
    try
    {
        while (!stopping_)
        {
            const auto now = Clock::now();
            if (now < next_send_at)
            {
                session.timer.expires_at(next_send_at);
                auto [wait_error] = co_await session.timer.async_wait(use_nothrow_awaitable);
                if (stopping_)
                {
                    break;
                }
                if (wait_error)
                {
                    throw asio::system_error(wait_error, "ICMP interval wait failed");
                }
            }

            const auto sent_at = Clock::now();
            do
            {
                next_send_at += interval_;
            } while (next_send_at <= sent_at);

            ++session.state.transmitted;
            ++session.sequence;
            session.sent_at = sent_at;
            session.in_flight = true;
            session.timer.expires_after(timeout_);

            const auto echo = make_echo(session.sequence, instance_);
            auto [send_error, sent] = co_await socket_.async_send_to(
                asio::buffer(echo), ICMPProtocol::endpoint(destination, 0), use_nothrow_awaitable);
            if (!send_error && sent != echo.size())
            {
                send_error = asio::error::message_size;
            }

            if (send_error)
            {
                if (stopping_)
                {
                    session.in_flight = false;
                    break;
                }

                if (session.in_flight)
                {
                    session.in_flight = false;
                    session.state.quality.record(false, 0.0, Clock::now());
                }
                continue;
            }

            if (session.in_flight && !stopping_)
            {
                auto [wait_error] = co_await session.timer.async_wait(use_nothrow_awaitable);
                if (wait_error && wait_error != asio::error::operation_aborted)
                {
                    throw asio::system_error(wait_error, "ICMP reply wait failed");
                }
                if (wait_error == asio::error::operation_aborted && session.in_flight && !stopping_)
                {
                    throw asio::system_error(wait_error, "ICMP reply wait cancelled");
                }
            }

            if (stopping_)
            {
                session.in_flight = false;
                break;
            }

            if (session.in_flight)
            {
                session.in_flight = false;
                session.state.quality.record(false, 0.0, Clock::now());
            }
        }
    }
    catch (...)
    {
        session.in_flight = false;
        stop();
    }
    --running_probes_;
    notify_finished();
}

std::vector<ICMP::Metrics> ICMP::metrics() const
{
    const auto now = Clock::now();
    std::vector<Metrics> result;
    result.reserve(sessions_.size());
    for (const auto &entry : sessions_)
    {
        const auto &session = entry.second;
        result.push_back({entry.first, session.state.transmitted, session.state.quality.assess(now)});
    }
    return result;
}

ICMP::History ICMP::history() const
{
    History result;
    for (const auto &[destination, session] : sessions_)
    {
        result.emplace(destination, session.state);
    }
    return result;
}

asio::awaitable<void> ICMP::close()
{
    co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
    co_await asio::co_spawn(io_context_, close_on_executor(), asio::use_awaitable);
}

asio::awaitable<void> ICMP::close_on_executor()
{
    stop();

    while (receiving_ || running_probes_ != 0)
    {
        auto [error] = co_await finished_wait_.async_wait(use_nothrow_awaitable);
        if (error && error != asio::error::operation_aborted)
        {
            throw asio::system_error(error, "ICMP close wait failed");
        }
    }
}

asio::awaitable<void> ICMP::receive_loop()
{
    ICMPProtocol::endpoint sender;
    std::array<std::uint8_t, 65536> buffer{};
    try
    {
        for (;;)
        {
            auto [error, size] = co_await socket_.async_receive_from(asio::buffer(buffer), sender, use_nothrow_awaitable);
            if (error)
            {
                stop();
                break;
            }

            if (sender.address().is_v4())
            {
                handle_packet(std::span<const std::uint8_t>(buffer.data(), size), sender.address().to_v4());
            }
        }
    }
    catch (...)
    {
        stop();
    }

    receiving_ = false;
    notify_finished();
}

void ICMP::handle_packet(std::span<const std::uint8_t> packet, asio::ip::address_v4 source) noexcept
{
    const auto sequence = parse_reply(packet);
    if (!sequence)
    {
        return;
    }

    const auto found = sessions_.find(source);
    if (found == sessions_.end())
    {
        return;
    }

    auto &session = found->second;
    const auto now = Clock::now();
    if (!session.in_flight || session.sequence != *sequence || now >= session.timer.expiry())
    {
        return;
    }

    session.in_flight = false;
    session.state.quality.record(true, std::chrono::duration<double, std::milli>(now - session.sent_at).count(), now);
    cancel_timer_noexcept(session.timer);
}

std::optional<std::uint16_t> ICMP::parse_reply(std::span<const std::uint8_t> packet) const noexcept
{
    std::size_t ip_header_size = 0;
    if (!packet.empty() && (packet[0] >> 4U) == 4)
    {
        if (packet.size() < 20)
        {
            return std::nullopt;
        }

        ip_header_size = static_cast<std::size_t>(packet[0] & 0x0fU) * 4;
        if (ip_header_size < 20 || ip_header_size > 60 || packet.size() < ip_header_size + echo_size ||
            packet[9] != 1 || (read_u16(packet, 6) & 0x3fffU) != 0)
        {
            return std::nullopt;
        }

        const auto total_size = read_u16(packet, 2);
        if (total_size < ip_header_size + echo_size || total_size > packet.size())
        {
            return std::nullopt;
        }
        packet = packet.first(total_size);
    }

    const auto message = packet.subspan(ip_header_size);
    if (message.size() < echo_size || message[0] != 0 || message[1] != 0 ||
        read_u16(message, 4) != static_cast<std::uint16_t>(instance_) || checksum(message) != 0)
    {
        return std::nullopt;
    }

    const auto payload = message.subspan(icmp_header_size);
    if (!std::equal(payload_magic.begin(), payload_magic.end(), payload.begin()) || read_u64(payload, 4) != instance_)
    {
        return std::nullopt;
    }
    return read_u16(message, 6);
}

void ICMP::stop() noexcept
{
    if (!stopping_)
    {
        stopping_ = true;
        asio::error_code ignored;
        socket_.close(ignored);
        for (auto &entry : sessions_)
        {
            cancel_timer_noexcept(entry.second.timer);
        }
    }
    notify_finished();
}

void ICMP::notify_finished() noexcept
{
    if (!receiving_ && running_probes_ == 0)
    {
        cancel_timer_noexcept(finished_wait_);
    }
}
