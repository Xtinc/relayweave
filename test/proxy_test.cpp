#include "message.h"
#include "proxy_server.h"

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

struct TempFile
{
    ~TempFile()
    {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

    std::filesystem::path path;
};

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

std::uint16_t unused_port()
{
    asio::io_context io;
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
}

std::uint16_t different_port(std::uint16_t other)
{
    auto result = unused_port();
    while (result == other)
    {
        result = unused_port();
    }
    return result;
}

ProxyConfig test_config()
{
    ProxyConfig config;
    config.max_connections = 16;
    config.handshake_timeout = 1s;
    config.connect_timeout = 1s;
    config.socks5 = {"127.0.0.1", unused_port()};
    config.http.address = "127.0.0.1";
    config.http.port = different_port(config.socks5.port);
    config.http.max_header_bytes = 4096;
    return config;
}

std::string read_all(tcp::socket &socket)
{
    std::string result;
    std::array<char, 4096> buffer{};
    for (;;)
    {
        asio::error_code error;
        const auto size = socket.read_some(asio::buffer(buffer), error);
        result.append(buffer.data(), size);
        if (error)
        {
            require(error == asio::error::eof || error == asio::error::connection_reset,
                    "Unexpected socket read error: " + error.message());
            return result;
        }
    }
}

void read_exact(tcp::socket &socket, void *data, std::size_t size)
{
    asio::read(socket, asio::buffer(data, size));
}

class RunningProxy
{
  public:
    explicit RunningProxy(ProxyConfig config)
        : config(std::move(config)), server(std::make_shared<ProxyServer>(io, this->config))
    {
        server->start();
        thread = std::thread([this]() { io.run(); });
    }

    ~RunningProxy()
    {
        if (thread.joinable())
        {
            std::promise<void> stopped;
            auto result = stopped.get_future();
            asio::post(io, [this, &stopped]() {
                server->stop();
                stopped.set_value();
            });
            result.wait();
            thread.join();
        }
    }

    ProxyConfig config;
    asio::io_context io;
    std::shared_ptr<ProxyServer> server;
    std::thread thread;
};

void verify_config()
{
    const auto directory = std::filesystem::temp_directory_path();
    const auto path = directory / ("relayweave_proxy_test_" + std::to_string(unused_port()) + ".json");
    const auto write_config = [&](const njson &value) {
        std::ofstream output(path);
        require(static_cast<bool>(output), "Could not create proxy configuration fixture");
        output << value.dump(2);
    };
    TempFile cleanup{path};

    njson value = {
        {"log", {{"debug_enable", false}}},
        {"proxy", {{"max_connections", 12}, {"handshake_timeout_ms", 750}, {"connect_timeout_ms", 900}}},
        {"socks5", {{"address", "127.0.0.1"}, {"port", 11080}}},
        {"http", {{"address", "127.0.0.1"}, {"port", 11081}, {"max_header_bytes", 8192}}},
    };
    write_config(value);
    const auto parsed = load_proxy_config(path);
    require(parsed.max_connections == 12 && parsed.handshake_timeout == 750ms && parsed.connect_timeout == 900ms,
            "Proxy limits were not parsed");
    require(parsed.socks5.port == 11080 && parsed.http.port == 11081 && parsed.http.max_header_bytes == 8192,
            "Proxy listeners were not parsed");

    const auto require_rejected = [&](njson candidate, std::string_view description) {
        write_config(candidate);
        bool rejected = false;
        try
        {
            static_cast<void>(load_proxy_config(path));
        }
        catch (const std::exception &)
        {
            rejected = true;
        }
        require(rejected, "Invalid proxy configuration was accepted: " + std::string(description));
    };
    auto invalid = value;
    invalid["unknown"] = true;
    require_rejected(std::move(invalid), "unknown root field");
    invalid = value;
    invalid["http"]["address"] = "localhost";
    require_rejected(std::move(invalid), "non-numeric listen address");
    invalid = value;
    invalid["http"]["port"] = 11080;
    require_rejected(std::move(invalid), "duplicate listen port");
    invalid = value;
    invalid["proxy"]["max_connections"] = 0;
    require_rejected(std::move(invalid), "zero connection limit");
    invalid = value;
    invalid["http"]["max_header_bytes"] = 100;
    require_rejected(std::move(invalid), "small HTTP header limit");
    invalid = value;
    invalid["log"]["debug_enable"] = "yes";
    require_rejected(std::move(invalid), "non-boolean log.debug_enable");
}

void verify_http_forward(RunningProxy &proxy)
{
    asio::io_context origin_io;
    tcp::acceptor origin(origin_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto origin_port = origin.local_endpoint().port();
    std::promise<std::string> received_promise;
    auto received = received_promise.get_future();
    std::thread origin_thread([&]() {
        try
        {
            tcp::socket socket(origin_io);
            origin.accept(socket);
            std::string request;
            asio::read_until(socket, asio::dynamic_buffer(request), "\r\n\r\n");
            while (request.size() < request.find("\r\n\r\n") + 4 + 4)
            {
                std::array<char, 4> body{};
                const auto count = socket.read_some(asio::buffer(body));
                request.append(body.data(), count);
            }
            received_promise.set_value(request);
            const std::string response =
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
            asio::write(socket, asio::buffer(response));
        }
        catch (...)
        {
            received_promise.set_exception(std::current_exception());
        }
    });

    asio::io_context client_io;
    tcp::socket client(client_io);
    client.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    const auto request = "POST http://127.0.0.1:" + std::to_string(origin_port) +
                         "/upload?x=1 HTTP/1.1\r\nHost: ignored.example\r\n"
                         "Proxy-Authorization: Basic secret\r\nProxy-Connection: keep-alive\r\n"
                         "Connection: keep-alive, X-Hop\r\nX-Hop: remove\r\n"
                         "Content-Length: 4\r\nX-Test: yes\r\n\r\nDATA";
    asio::write(client, asio::buffer(request));
    const auto response = read_all(client);
    origin_thread.join();
    const auto upstream_request = received.get();
    require(upstream_request.starts_with("POST /upload?x=1 HTTP/1.1\r\n"),
            "HTTP absolute URI was not rewritten to origin form");
    require(upstream_request.find("Host: 127.0.0.1:" + std::to_string(origin_port)) != std::string::npos,
            "HTTP Host header was not rewritten");
    require(upstream_request.find("Proxy-Authorization") == std::string::npos &&
                upstream_request.find("Proxy-Connection") == std::string::npos &&
                upstream_request.find("X-Hop") == std::string::npos,
            "Hop-by-hop HTTP headers leaked upstream");
    require(upstream_request.find("Connection: close") != std::string::npos && upstream_request.ends_with("DATA"),
            "HTTP body or close semantics were not forwarded");
    require(response.find("200 OK") != std::string::npos && response.ends_with("OK"),
            "HTTP response was not returned transparently");
}

void verify_http_origin_form(RunningProxy &proxy)
{
    asio::io_context origin_io;
    tcp::acceptor origin(origin_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto origin_port = origin.local_endpoint().port();
    std::thread origin_thread([&]() {
        tcp::socket socket(origin_io);
        origin.accept(socket);
        std::string request;
        asio::read_until(socket, asio::dynamic_buffer(request), "\r\n\r\n");
        const std::string response = "HTTP/1.0 204 No Content\r\nConnection: close\r\n\r\n";
        asio::write(socket, asio::buffer(response));
    });

    asio::io_context client_io;
    tcp::socket client(client_io);
    client.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    const auto request = "GET /health HTTP/1.0\r\nHost: 127.0.0.1:" + std::to_string(origin_port) + "\r\n\r\n";
    asio::write(client, asio::buffer(request));
    const auto response = read_all(client);
    origin_thread.join();
    require(response.find("204 No Content") != std::string::npos, "Origin-form HTTP request failed");
}

void verify_http_connect(RunningProxy &proxy)
{
    asio::io_context origin_io;
    tcp::acceptor origin(origin_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto origin_port = origin.local_endpoint().port();
    std::thread origin_thread([&]() {
        tcp::socket socket(origin_io);
        origin.accept(socket);
        std::array<char, 4> request{};
        read_exact(socket, request.data(), request.size());
        require(std::string_view(request.data(), request.size()) == "PING", "CONNECT prefetched data was lost");
        asio::write(socket, asio::buffer("PONG", 4));
    });

    asio::io_context client_io;
    tcp::socket client(client_io);
    client.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    const auto request = "CONNECT 127.0.0.1:" + std::to_string(origin_port) +
                         " HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\nPING";
    asio::write(client, asio::buffer(request));
    std::string response;
    const auto header_size = asio::read_until(client, asio::dynamic_buffer(response), "\r\n\r\n");
    require(response.substr(0, header_size).find("200 Connection Established") != std::string::npos,
            "CONNECT did not return success");
    while (response.size() - header_size < 4)
    {
        std::array<char, 4> data{};
        const auto count = client.read_some(asio::buffer(data));
        response.append(data.data(), count);
    }
    require(response.substr(header_size, 4) == "PONG", "CONNECT response payload was not relayed");
    origin_thread.join();
}

void verify_http_errors(RunningProxy &proxy)
{
    asio::io_context io;
    tcp::socket malformed(io);
    malformed.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    asio::write(malformed, asio::buffer("GET relative HTTP/1.1\r\n\r\n", 25));
    require(read_all(malformed).find("400 Bad Request") != std::string::npos,
            "Malformed HTTP request did not return 400");

    tcp::socket fragment(io);
    fragment.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    constexpr std::string_view fragment_request =
        "GET http://example.test?query#fragment HTTP/1.1\r\nHost: example.test\r\n\r\n";
    asio::write(fragment, asio::buffer(fragment_request));
    require(read_all(fragment).find("400 Bad Request") != std::string::npos,
            "HTTP URI fragment was not rejected");

    tcp::socket invalid_ipv6(io);
    invalid_ipv6.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    constexpr std::string_view invalid_ipv6_request =
        "CONNECT [not-an-ipv6-address]:443 HTTP/1.1\r\nHost: ignored\r\n\r\n";
    asio::write(invalid_ipv6, asio::buffer(invalid_ipv6_request));
    require(read_all(invalid_ipv6).find("400 Bad Request") != std::string::npos,
            "Invalid bracketed IPv6 authority was not rejected");

    tcp::socket oversized(io);
    oversized.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    std::string request = "GET / HTTP/1.1\r\nHost: example.test\r\nX-Large: ";
    request.append(proxy.config.http.max_header_bytes, 'x');
    asio::write(oversized, asio::buffer(request));
    require(read_all(oversized).find("431 Request Header Fields Too Large") != std::string::npos,
            "Oversized HTTP request did not return 431");

    const auto closed_port = unused_port();
    tcp::socket unavailable(io);
    unavailable.connect({asio::ip::address_v4::loopback(), proxy.config.http.port});
    const auto connect =
        "CONNECT 127.0.0.1:" + std::to_string(closed_port) + " HTTP/1.1\r\nHost: ignored\r\n\r\n";
    asio::write(unavailable, asio::buffer(connect));
    require(read_all(unavailable).find("502 Bad Gateway") != std::string::npos,
            "Unavailable HTTP upstream did not return 502");
}

void read_socks_success(tcp::socket &client)
{
    std::array<std::uint8_t, 4> header{};
    read_exact(client, header.data(), header.size());
    require(header[0] == 0x05 && header[1] == 0x00, "SOCKS5 CONNECT failed");
    std::size_t remaining = 0;
    if (header[3] == 0x01)
    {
        remaining = 4 + 2;
    }
    else if (header[3] == 0x04)
    {
        remaining = 16 + 2;
    }
    else
    {
        throw std::runtime_error("SOCKS5 success reply used an invalid address type");
    }
    std::vector<std::uint8_t> rest(remaining);
    read_exact(client, rest.data(), rest.size());
}

void verify_socks5(RunningProxy &proxy)
{
    asio::io_context origin_io;
    tcp::acceptor origin(origin_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const auto origin_port = origin.local_endpoint().port();
    std::thread origin_thread([&]() {
        tcp::socket socket(origin_io);
        origin.accept(socket);
        std::array<char, 4> request{};
        read_exact(socket, request.data(), request.size());
        require(std::string_view(request.data(), request.size()) == "PING", "SOCKS5 payload was corrupted");
        asio::write(socket, asio::buffer("PONG", 4));
    });

    asio::io_context client_io;
    tcp::socket client(client_io);
    client.connect({asio::ip::address_v4::loopback(), proxy.config.socks5.port});
    const std::array<std::uint8_t, 4> greeting{0x05, 0x02, 0x02, 0x00};
    asio::write(client, asio::buffer(greeting));
    std::array<std::uint8_t, 2> method{};
    read_exact(client, method.data(), method.size());
    require(method == std::array<std::uint8_t, 2>{0x05, 0x00}, "SOCKS5 no-auth method was not selected");

    constexpr std::string_view host = "127.0.0.1";
    std::vector<std::uint8_t> request{0x05, 0x01, 0x00, 0x03, static_cast<std::uint8_t>(host.size())};
    request.insert(request.end(), host.begin(), host.end());
    request.push_back(static_cast<std::uint8_t>(origin_port >> 8));
    request.push_back(static_cast<std::uint8_t>(origin_port));
    asio::write(client, asio::buffer(request));
    read_socks_success(client);
    asio::write(client, asio::buffer("PING", 4));
    std::array<char, 4> response{};
    read_exact(client, response.data(), response.size());
    require(std::string_view(response.data(), response.size()) == "PONG", "SOCKS5 response was not relayed");
    origin_thread.join();

    tcp::socket rejected(client_io);
    rejected.connect({asio::ip::address_v4::loopback(), proxy.config.socks5.port});
    const std::array<std::uint8_t, 3> password_only{0x05, 0x01, 0x02};
    asio::write(rejected, asio::buffer(password_only));
    read_exact(rejected, method.data(), method.size());
    require(method == std::array<std::uint8_t, 2>{0x05, 0xff}, "SOCKS5 authentication was not rejected");

    tcp::socket unsupported(client_io);
    unsupported.connect({asio::ip::address_v4::loopback(), proxy.config.socks5.port});
    const std::array<std::uint8_t, 3> no_auth{0x05, 0x01, 0x00};
    asio::write(unsupported, asio::buffer(no_auth));
    read_exact(unsupported, method.data(), method.size());
    const std::array<std::uint8_t, 10> udp_associate{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 1};
    asio::write(unsupported, asio::buffer(udp_associate));
    std::array<std::uint8_t, 4> failure{};
    read_exact(unsupported, failure.data(), failure.size());
    require(failure[1] == 0x07, "SOCKS5 UDP ASSOCIATE was not rejected as unsupported");
}

void verify_socks_ip_address(RunningProxy &proxy, bool ipv6)
{
    asio::io_context origin_io;
    tcp::acceptor origin(origin_io);
    asio::error_code error;
    const auto address = ipv6 ? asio::ip::address(asio::ip::address_v6::loopback())
                              : asio::ip::address(asio::ip::address_v4::loopback());
    origin.open(ipv6 ? tcp::v6() : tcp::v4(), error);
    if (error && ipv6)
    {
        return;
    }
    require(!error, "Could not open IP address test listener: " + error.message());
    if (ipv6)
    {
        origin.set_option(asio::ip::v6_only(true));
    }
    origin.bind({address, 0}, error);
    if (error && ipv6)
    {
        return;
    }
    require(!error, "Could not bind IP address test listener: " + error.message());
    origin.listen();
    const auto origin_port = origin.local_endpoint().port();
    std::thread origin_thread([&]() {
        tcp::socket socket(origin_io);
        origin.accept(socket);
        std::array<char, 2> request{};
        read_exact(socket, request.data(), request.size());
        asio::write(socket, asio::buffer("OK", 2));
    });

    asio::io_context client_io;
    tcp::socket client(client_io);
    client.connect({asio::ip::address_v4::loopback(), proxy.config.socks5.port});
    constexpr std::array<std::uint8_t, 3> greeting{0x05, 0x01, 0x00};
    asio::write(client, asio::buffer(greeting));
    std::array<std::uint8_t, 2> method{};
    read_exact(client, method.data(), method.size());

    std::vector<std::uint8_t> request{0x05, 0x01, 0x00, static_cast<std::uint8_t>(ipv6 ? 0x04 : 0x01)};
    if (ipv6)
    {
        const auto bytes = asio::ip::address_v6::loopback().to_bytes();
        request.insert(request.end(), bytes.begin(), bytes.end());
    }
    else
    {
        const auto bytes = asio::ip::address_v4::loopback().to_bytes();
        request.insert(request.end(), bytes.begin(), bytes.end());
    }
    request.push_back(static_cast<std::uint8_t>(origin_port >> 8));
    request.push_back(static_cast<std::uint8_t>(origin_port));
    asio::write(client, asio::buffer(request));
    read_socks_success(client);
    asio::write(client, asio::buffer("IP", 2));
    std::array<char, 2> response{};
    read_exact(client, response.data(), response.size());
    require(std::string_view(response.data(), response.size()) == "OK",
            std::string("SOCKS5 ") + (ipv6 ? "IPv6" : "IPv4") + " CONNECT failed");
    origin_thread.join();
}

void verify_capacity_and_timeout()
{
    auto config = test_config();
    config.max_connections = 1;
    config.handshake_timeout = 200ms;
    RunningProxy proxy(config);
    asio::io_context io;
    tcp::socket first(io);
    first.connect({asio::ip::address_v4::loopback(), config.http.port});
    std::this_thread::sleep_for(25ms);
    tcp::socket excess(io);
    excess.connect({asio::ip::address_v4::loopback(), config.socks5.port});
    std::array<char, 1> byte{};
    asio::error_code error;
    excess.read_some(asio::buffer(byte), error);
    require(error && error != asio::error::would_block, "Proxy connection capacity was not enforced");

    first.read_some(asio::buffer(byte), error);
    require(error == asio::error::eof || error == asio::error::connection_reset,
            "Incomplete proxy handshake did not time out");
}

void verify_listener_rollback()
{
    asio::io_context io;
    auto config = test_config();
    tcp::acceptor occupied(io, {asio::ip::address_v4::loopback(), config.http.port});
    auto server = std::make_shared<ProxyServer>(io, config);
    bool failed = false;
    try
    {
        server->start();
    }
    catch (const std::exception &)
    {
        failed = true;
    }
    require(failed, "Proxy unexpectedly started with an occupied HTTP port");
    tcp::acceptor socks_probe(io, {asio::ip::address_v4::loopback(), config.socks5.port});
}

} // namespace

int main()
{
    try
    {
        verify_config();
        verify_listener_rollback();
        RunningProxy proxy(test_config());
        verify_http_forward(proxy);
        verify_http_origin_form(proxy);
        verify_http_connect(proxy);
        verify_http_errors(proxy);
        verify_socks5(proxy);
        verify_socks_ip_address(proxy, false);
        verify_socks_ip_address(proxy, true);
        verify_capacity_and_timeout();
        std::cout << "[PASS] Proxy configuration, HTTP, CONNECT, SOCKS5, and lifecycle\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
