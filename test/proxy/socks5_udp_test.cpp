#include "proxy_server.h"
#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

class RunningProxy
{
  public:
    explicit RunningProxy(std::size_t targets = 256, std::chrono::milliseconds ttl = 60s, bool ipv6 = false)
        : config_(make_config(targets, ttl, ipv6)), server_(server_io_, config_)
    {
        server_.start();
        worker_ = std::thread([this] { server_io_.run(); });
    }

    ~RunningProxy()
    {
        stop();
    }

    void stop()
    {
        if (!worker_.joinable())
        {
            return;
        }
        asio::post(server_io_, [this] { server_.stop(); });
        worker_.join();
    }

    std::uint16_t port() const
    {
        return config_.socks5.port;
    }

  private:
    static ProxyConfig make_config(std::size_t targets, std::chrono::milliseconds ttl, bool ipv6)
    {
        asio::io_context io;
        const asio::ip::address address = ipv6 ? asio::ip::address(asio::ip::address_v6::loopback())
                                              : asio::ip::address(asio::ip::address_v4::loopback());
        tcp::acceptor socks(io, {address, 0});
        tcp::acceptor http(io, {asio::ip::address_v4::loopback(), 0});
        while (http.local_endpoint().port() == socks.local_endpoint().port())
        {
            http.close();
            http.open(tcp::v4());
            http.bind({asio::ip::address_v4::loopback(), 0});
        }
        ProxyConfig result;
        result.socks5 = {address.to_string(), socks.local_endpoint().port()};
        result.http.address = "127.0.0.1";
        result.http.port = http.local_endpoint().port();
        result.udp_max_targets = targets;
        result.udp_target_timeout = ttl;
        result.connect_timeout = 250ms;
        return result;
    }

    asio::io_context server_io_;
    ProxyConfig config_;
    ProxyServer server_;
    std::thread worker_;
};

void read_exact(asio::io_context &io, tcp::socket &socket, std::span<std::uint8_t> buffer)
{
    io.restart();
    auto result = asio::async_read(socket, asio::buffer(buffer.data(), buffer.size()),
                                  asio::cancel_after(2s, asio::use_future));
    io.run();
    result.get();
}

std::vector<std::uint8_t> address_bytes(const udp::endpoint &endpoint, bool domain = false)
{
    std::vector<std::uint8_t> result;
    if (domain)
    {
        const std::string name = "localhost";
        result.push_back(3);
        result.push_back(static_cast<std::uint8_t>(name.size()));
        result.insert(result.end(), name.begin(), name.end());
    }
    else if (endpoint.address().is_v4())
    {
        result.push_back(1);
        const auto bytes = endpoint.address().to_v4().to_bytes();
        result.insert(result.end(), bytes.begin(), bytes.end());
    }
    else
    {
        result.push_back(4);
        const auto bytes = endpoint.address().to_v6().to_bytes();
        result.insert(result.end(), bytes.begin(), bytes.end());
    }
    result.push_back(static_cast<std::uint8_t>(endpoint.port() >> 8));
    result.push_back(static_cast<std::uint8_t>(endpoint.port()));
    return result;
}

udp::endpoint associate(asio::io_context &io, tcp::socket &control, std::uint16_t port,
                        udp::endpoint expected = udp::endpoint(asio::ip::address_v4::any(), 0),
                        std::uint8_t expected_reply = 0, bool ipv6 = false)
{
    const asio::ip::address peer_address = ipv6 ? asio::ip::address(asio::ip::address_v6::loopback())
                                               : asio::ip::address(asio::ip::address_v4::loopback());
    control.connect({peer_address, port});
    constexpr std::array<std::uint8_t, 3> greeting{5, 1, 0};
    asio::write(control, asio::buffer(greeting));
    std::array<std::uint8_t, 2> method;
    read_exact(io, control, method);
    require(method == std::array<std::uint8_t, 2>{5, 0}, "No-auth negotiation failed");
    std::vector<std::uint8_t> request{5, 3, 0};
    const auto address = address_bytes(expected);
    request.insert(request.end(), address.begin(), address.end());
    asio::write(control, asio::buffer(request));
    std::array<std::uint8_t, 4> header;
    read_exact(io, control, header);
    require(header[0] == 5 && header[1] == expected_reply && header[2] == 0, "Unexpected UDP ASSOCIATE reply");
    asio::ip::address bound;
    if (header[3] == 1)
    {
        asio::ip::address_v4::bytes_type bytes;
        read_exact(io, control, bytes);
        bound = asio::ip::address_v4(bytes);
    }
    else
    {
        require(header[3] == 4, "Invalid UDP relay address type");
        asio::ip::address_v6::bytes_type bytes;
        read_exact(io, control, bytes);
        bound = asio::ip::address_v6(bytes);
    }
    std::array<std::uint8_t, 2> bound_port;
    read_exact(io, control, bound_port);
    const auto result_port = static_cast<std::uint16_t>((static_cast<unsigned int>(bound_port[0]) << 8) | bound_port[1]);
    if (expected_reply == 0)
    {
        require(!bound.is_unspecified() && result_port != 0, "Proxy advertised an unusable UDP relay");
    }
    return {bound, result_port};
}

struct Received
{
    std::vector<std::uint8_t> data;
    udp::endpoint source;
};

std::optional<Received> receive(asio::io_context &io, udp::socket &socket, std::chrono::milliseconds timeout = 2s)
{
    io.restart();
    Received result;
    result.data.resize(65536);
    auto completed = socket.async_receive_from(asio::buffer(result.data), result.source,
                                               asio::cancel_after(timeout, asio::use_future));
    io.run();
    try
    {
        result.data.resize(completed.get());
        return result;
    }
    catch (const asio::system_error &failure)
    {
        if (failure.code() == asio::error::operation_aborted)
        {
            return std::nullopt;
        }
        throw;
    }
}

Received required_receive(asio::io_context &io, udp::socket &socket)
{
    auto received = receive(io, socket);
    require(received.has_value(), "UDP receive timed out");
    return std::move(*received);
}

std::vector<std::uint8_t> packet(const udp::endpoint &target, std::span<const std::uint8_t> payload,
                                bool domain = false)
{
    std::vector<std::uint8_t> result{0, 0, 0};
    const auto address = address_bytes(target, domain);
    result.insert(result.end(), address.begin(), address.end());
    result.insert(result.end(), payload.begin(), payload.end());
    return result;
}

void exchange(asio::io_context &io, udp::socket &client, const udp::endpoint &relay, udp::socket &origin,
              std::span<const std::uint8_t> payload, bool domain = false)
{
    const auto request = packet(origin.local_endpoint(), payload, domain);
    client.send_to(asio::buffer(request), relay);
    const auto forwarded = required_receive(io, origin);
    require(forwarded.data == std::vector<std::uint8_t>(payload.begin(), payload.end()), "UDP payload was corrupted");
    origin.send_to(asio::buffer(forwarded.data), forwarded.source);
    const auto reply = required_receive(io, client);
    require(reply.source == relay && reply.data == packet(origin.local_endpoint(), payload),
            "UDP reply lost its payload or source endpoint");
}

void verify_datagrams()
{
    RunningProxy proxy;
    asio::io_context io;
    tcp::socket control(io);
    const auto relay = associate(io, control, proxy.port());
    udp::socket client(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket origin(io, {asio::ip::address_v4::loopback(), 0});
    const std::array<std::uint8_t, 4> small{'P', 'I', 'N', 'G'};
    exchange(io, client, relay, origin, small);
    exchange(io, client, relay, origin, small, true);
    exchange(io, client, relay, origin, {});
    std::vector<std::uint8_t> largest(65507 - 10, 0xa5);
    exchange(io, client, relay, origin, largest);

    udp::socket ipv6(io);
    asio::error_code ipv6_error;
    ipv6.open(udp::v6(), ipv6_error);
    if (!ipv6_error)
    {
        ipv6.bind({asio::ip::address_v6::loopback(), 0}, ipv6_error);
    }
    if (!ipv6_error)
    {
        exchange(io, client, relay, ipv6, small);
    }
    else
    {
        std::cout << "[SKIP] IPv6 UDP origin unavailable: " << ipv6_error.message() << '\n';
    }
}

void verify_ipv6_relay()
{
    asio::io_context io;
    tcp::acceptor capability(io);
    asio::error_code error;
    capability.open(tcp::v6(), error);
    if (!error)
    {
        capability.bind({asio::ip::address_v6::loopback(), 0}, error);
    }
    if (error)
    {
        std::cout << "[SKIP] IPv6 TCP listener unavailable: " << error.message() << '\n';
        return;
    }
    RunningProxy proxy(256, 60s, true);
    tcp::socket control(io);
    const auto relay = associate(io, control, proxy.port(), {asio::ip::address_v6::any(), 0}, 0, true);
    require(relay.address() == asio::ip::address_v6::loopback(), "IPv6 relay advertised an incorrect address");
    udp::socket client(io, {asio::ip::address_v6::loopback(), 0});
    udp::socket ipv4_origin(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket ipv6_origin(io, {asio::ip::address_v6::loopback(), 0});
    const std::array<std::uint8_t, 4> data{1, 2, 3, 4};
    exchange(io, client, relay, ipv4_origin, data);
    exchange(io, client, relay, ipv6_origin, data);
    exchange(io, client, relay, ipv4_origin, data, true);
    exchange(io, client, relay, ipv6_origin, {});
    std::vector<std::uint8_t> largest(65507 - 22, 0x5a);
    exchange(io, client, relay, ipv6_origin, largest);
}

void verify_invalid_packets_and_pinning()
{
    RunningProxy proxy;
    asio::io_context io;
    tcp::socket control(io);
    const auto relay = associate(io, control, proxy.port());
    udp::socket client(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket stranger(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket origin(io, {asio::ip::address_v4::loopback(), 0});
    const std::array<std::uint8_t, 1> data{42};
    const auto valid = packet(origin.local_endpoint(), data);
    std::vector<std::vector<std::uint8_t>> invalid{{}, {0}, {0, 0, 0}, {0, 0, 0, 1},
        {0, 0, 0, 4, 0}, {0, 0, 0, 3, 0}, {0, 0, 0, 3, 4, 'a'}, {0, 0, 0, 0xff}};
    auto reserved = valid;
    reserved[0] = 1;
    invalid.push_back(reserved);
    reserved[0] = 0;
    reserved[1] = 1;
    invalid.push_back(reserved);
    auto fragmented = valid;
    fragmented[2] = 1;
    invalid.push_back(fragmented);
    fragmented[2] = 0x80;
    invalid.push_back(fragmented);
    auto invalid_domain = packet(origin.local_endpoint(), data, true);
    invalid_domain[5] = 0;
    invalid.push_back(invalid_domain);
    invalid_domain[5] = 0x80;
    invalid.push_back(invalid_domain);
    auto zero_port = valid;
    zero_port[8] = 0;
    zero_port[9] = 0;
    invalid.push_back(zero_port);
    for (const auto &bad : invalid)
    {
        stranger.send_to(asio::buffer(bad), relay);
    }
    require(!receive(io, origin, 80ms), "Malformed datagram reached its target");
    udp::socket other_ip(io);
    asio::error_code other_ip_error;
    other_ip.open(udp::v4());
    other_ip.bind({asio::ip::make_address_v4("127.0.0.2"), client.local_endpoint().port()}, other_ip_error);
    if (!other_ip_error)
    {
        other_ip.send_to(asio::buffer(valid), relay);
        require(!receive(io, origin, 80ms), "A different source IP hijacked the association");
    }
    exchange(io, client, relay, origin, data);
    stranger.send_to(asio::buffer(valid), relay);
    require(!receive(io, origin, 80ms), "A different source port hijacked the association");
    exchange(io, client, relay, origin, data);

    tcp::socket fixed_control(io);
    const auto fixed_relay = associate(io, fixed_control, proxy.port(), client.local_endpoint());
    stranger.send_to(asio::buffer(valid), fixed_relay);
    require(!receive(io, origin, 80ms), "Explicit client port was ignored");
    exchange(io, client, fixed_relay, origin, data);
    tcp::socket denied(io);
    associate(io, denied, proxy.port(), {asio::ip::make_address("192.0.2.1"), 0}, 2);
}

void verify_isolation_and_reply_filter()
{
    RunningProxy proxy;
    asio::io_context io;
    tcp::socket first_control(io);
    tcp::socket second_control(io);
    const auto first_relay = associate(io, first_control, proxy.port());
    const auto second_relay = associate(io, second_control, proxy.port());
    require(first_relay != second_relay, "UDP associations share a relay endpoint");
    udp::socket first(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket second(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket origin(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket stranger(io, {asio::ip::address_v4::loopback(), 0});
    const std::array<std::uint8_t, 1> a{1};
    const std::array<std::uint8_t, 1> b{2};
    const auto a_packet = packet(origin.local_endpoint(), a);
    const auto b_packet = packet(origin.local_endpoint(), b);
    first.send_to(asio::buffer(a_packet), first_relay);
    second.send_to(asio::buffer(b_packet), second_relay);
    const auto one = required_receive(io, origin);
    const auto two = required_receive(io, origin);
    require(one.source != two.source, "Associations share an outbound UDP endpoint");
    stranger.send_to(asio::buffer(a), one.source);
    stranger.send_to(asio::buffer(a), two.source);
    require(!receive(io, first, 80ms) && !receive(io, second, 80ms), "Unsolicited target reply was forwarded");
    origin.send_to(asio::buffer(one.data), one.source);
    origin.send_to(asio::buffer(two.data), two.source);
    require(required_receive(io, first).data == a_packet, "First association received another client's reply");
    require(required_receive(io, second).data == b_packet, "Second association received another client's reply");

    first.send_to(asio::buffer(a_packet), first_relay);
    const auto oversized_target = required_receive(io, origin);
    const std::vector<std::uint8_t> oversized(65507, 0);
    origin.send_to(asio::buffer(oversized), oversized_target.source);
    require(!receive(io, first, 80ms), "Reply exceeding SOCKS UDP datagram limit was forwarded");
    exchange(io, first, first_relay, origin, a);
}

void verify_target_eviction_and_expiry()
{
    RunningProxy proxy(1, 150ms);
    asio::io_context io;
    tcp::socket control(io);
    const auto relay = associate(io, control, proxy.port());
    udp::socket client(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket old_target(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket new_target(io, {asio::ip::address_v4::loopback(), 0});
    const std::array<std::uint8_t, 1> data{1};
    const auto old_packet = packet(old_target.local_endpoint(), data);
    const auto new_packet = packet(new_target.local_endpoint(), data);
    client.send_to(asio::buffer(old_packet), relay);
    const auto old_forwarded = required_receive(io, old_target);
    client.send_to(asio::buffer(new_packet), relay);
    const auto new_forwarded = required_receive(io, new_target);
    old_target.send_to(asio::buffer(data), old_forwarded.source);
    require(!receive(io, client, 60ms), "Evicted target remained authorized");
    new_target.send_to(asio::buffer(data), new_forwarded.source);
    required_receive(io, client);
    io.restart();
    asio::steady_timer expiry(io, 180ms);
    expiry.async_wait([](asio::error_code) {});
    io.run();
    new_target.send_to(asio::buffer(data), new_forwarded.source);
    require(!receive(io, client, 60ms), "Expired target remained authorized");
    exchange(io, client, relay, new_target, data);
}

void verify_control_lifetime_and_stop()
{
    RunningProxy proxy;
    asio::io_context io;
    tcp::socket control(io);
    const auto relay = associate(io, control, proxy.port());
    control.shutdown(tcp::socket::shutdown_send);
    udp::socket released(io);
    bool rebound = false;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        asio::error_code error;
        released.open(relay.protocol());
        released.bind(relay, error);
        if (!error)
        {
            rebound = true;
            break;
        }
        released.close();
        std::this_thread::sleep_for(1ms);
    }
    require(rebound, "TCP closure did not release UDP relay socket");
    tcp::socket active(io);
    const auto active_relay = associate(io, active, proxy.port());
    udp::socket client(io, {asio::ip::address_v4::loopback(), 0});
    udp::socket origin(io, {asio::ip::address_v4::loopback(), 0});
    const std::array<std::uint8_t, 1> payload{1};
    const auto request = packet(origin.local_endpoint(), payload);
    client.send_to(asio::buffer(request), active_relay);
    required_receive(io, origin);
    proxy.stop();
    udp::socket stopped(io, active_relay);
    std::array<std::uint8_t, 1> byte;
    asio::error_code error;
    active.read_some(asio::buffer(byte), error);
    require(error == asio::error::eof || error == asio::error::connection_reset,
            "Proxy stop did not close the UDP control connection");
}
}

int main()
{
    try
    {
        verify_datagrams();
        verify_ipv6_relay();
        verify_invalid_packets_and_pinning();
        verify_isolation_and_reply_filter();
        verify_target_eviction_and_expiry();
        verify_control_lifetime_and_stop();
        std::cout << "[PASS] SOCKS5 UDP forwarding, validation, isolation, limits and lifecycle\n";
        return 0;
    }
    catch (const std::exception &failure)
    {
        std::cerr << "[FAIL] " << failure.what() << '\n';
        return 1;
    }
}
