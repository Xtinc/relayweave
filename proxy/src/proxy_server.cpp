#include "proxy_server.h"

#include "message.h"
#include "tls_channel.h"
#include "xfr_channel.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <stdexcept>
#include <string_view>

namespace
{
using tcp = asio::ip::tcp;

enum class SessionProtocol
{
    Http,
    Socks5,
};

struct HttpFailure : std::runtime_error
{
    HttpFailure(unsigned int status, std::string reason) : std::runtime_error(reason), status(status)
    {
    }

    unsigned int status;
};

struct Authority
{
    std::string host;
    std::string header;
    std::uint16_t port = 0;
};

struct ParsedHttpRequest
{
    std::string method;
    Authority authority;
    std::string outbound_header;
    bool connect = false;
};

bool ascii_iequal(std::string_view left, std::string_view right)
{
    return left.size() == right.size() && std::ranges::equal(left, right, [](unsigned char a, unsigned char b) {
               return std::tolower(a) == std::tolower(b);
           });
}

bool ascii_istarts_with(std::string_view value, std::string_view prefix)
{
    return value.size() >= prefix.size() && ascii_iequal(value.substr(0, prefix.size()), prefix);
}

std::string_view trim_ows(std::string_view value)
{
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
    {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
    {
        value.remove_suffix(1);
    }
    return value;
}

bool valid_token(std::string_view value)
{
    constexpr std::string_view punctuation = "!#$%&'*+-.^_`|~";
    return !value.empty() && std::ranges::all_of(value, [punctuation](unsigned char ch) {
        return std::isalnum(ch) || punctuation.find(static_cast<char>(ch)) != std::string_view::npos;
    });
}

std::uint16_t parse_port(std::string_view value)
{
    unsigned int parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed == 0 || parsed > 65535)
    {
        throw HttpFailure(400, "invalid target port");
    }
    return static_cast<std::uint16_t>(parsed);
}

Authority parse_authority(std::string_view value, std::optional<std::uint16_t> default_port)
{
    value = trim_ows(value);
    if (value.empty() || value.find('@') != std::string_view::npos)
    {
        throw HttpFailure(400, "invalid target authority");
    }

    Authority result;
    if (value.front() == '[')
    {
        const auto close = value.find(']');
        if (close == std::string_view::npos || close == 1)
        {
            throw HttpFailure(400, "invalid IPv6 authority");
        }
        result.host = std::string(value.substr(1, close - 1));
        asio::error_code address_error;
        static_cast<void>(asio::ip::make_address_v6(result.host, address_error));
        if (address_error)
        {
            throw HttpFailure(400, "invalid IPv6 authority");
        }
        result.header = std::string(value.substr(0, close + 1));
        const auto suffix = value.substr(close + 1);
        if (suffix.empty())
        {
            if (!default_port)
            {
                throw HttpFailure(400, "target port is required");
            }
            result.port = *default_port;
        }
        else
        {
            if (suffix.front() != ':')
            {
                throw HttpFailure(400, "invalid IPv6 authority");
            }
            result.port = parse_port(suffix.substr(1));
            result.header += std::string(suffix);
        }
    }
    else
    {
        const auto first_colon = value.find(':');
        const auto last_colon = value.rfind(':');
        if (first_colon != std::string_view::npos && first_colon != last_colon)
        {
            throw HttpFailure(400, "IPv6 addresses must be enclosed in brackets");
        }
        if (last_colon == std::string_view::npos)
        {
            if (!default_port)
            {
                throw HttpFailure(400, "target port is required");
            }
            result.host = std::string(value);
            result.header = std::string(value);
            result.port = *default_port;
        }
        else
        {
            result.host = std::string(value.substr(0, last_colon));
            result.header = std::string(value);
            result.port = parse_port(value.substr(last_colon + 1));
        }
    }

    if (result.host.empty() || std::ranges::any_of(result.host, [](unsigned char ch) {
            return ch <= 0x20 || ch == 0x7f || ch == '/' || ch == '?' || ch == '#';
        }))
    {
        throw HttpFailure(400, "invalid target host");
    }
    return result;
}

ParsedHttpRequest parse_http_request(std::string_view header)
{
    const auto request_end = header.find("\r\n");
    if (request_end == std::string_view::npos)
    {
        throw HttpFailure(400, "missing HTTP request line");
    }
    const auto request_line = header.substr(0, request_end);
    const auto first_space = request_line.find(' ');
    const auto second_space =
        first_space == std::string_view::npos ? std::string_view::npos : request_line.find(' ', first_space + 1);
    if (first_space == std::string_view::npos || second_space == std::string_view::npos ||
        request_line.find(' ', second_space + 1) != std::string_view::npos)
    {
        throw HttpFailure(400, "invalid HTTP request line");
    }

    const auto method = request_line.substr(0, first_space);
    const auto target = request_line.substr(first_space + 1, second_space - first_space - 1);
    const auto version = request_line.substr(second_space + 1);
    if (!valid_token(method) || target.empty() || (version != "HTTP/1.0" && version != "HTTP/1.1"))
    {
        throw HttpFailure(400, "unsupported HTTP request line");
    }

    std::vector<std::pair<std::string_view, std::string_view>> headers;
    std::optional<std::string_view> host_header;
    std::size_t cursor = request_end + 2;
    while (cursor < header.size())
    {
        const auto line_end = header.find("\r\n", cursor);
        if (line_end == std::string_view::npos)
        {
            throw HttpFailure(400, "invalid HTTP header termination");
        }
        if (line_end == cursor)
        {
            cursor += 2;
            break;
        }
        const auto line = header.substr(cursor, line_end - cursor);
        if (line.front() == ' ' || line.front() == '\t')
        {
            throw HttpFailure(400, "obsolete folded HTTP headers are not supported");
        }
        const auto colon = line.find(':');
        if (colon == std::string_view::npos || !valid_token(line.substr(0, colon)))
        {
            throw HttpFailure(400, "invalid HTTP header");
        }
        const auto name = line.substr(0, colon);
        const auto value = trim_ows(line.substr(colon + 1));
        if (std::ranges::any_of(value, [](unsigned char ch) { return (ch < 0x20 && ch != '\t') || ch == 0x7f; }))
        {
            throw HttpFailure(400, "invalid HTTP header value");
        }
        if (ascii_iequal(name, "Host"))
        {
            if (host_header && *host_header != value)
            {
                throw HttpFailure(400, "conflicting Host headers");
            }
            host_header = value;
        }
        headers.emplace_back(name, value);
        cursor = line_end + 2;
    }
    if (cursor != header.size())
    {
        throw HttpFailure(400, "invalid HTTP header block");
    }

    ParsedHttpRequest result;
    result.method = std::string(method);
    result.connect = method == "CONNECT";
    std::string outbound_target;
    if (result.connect)
    {
        result.authority = parse_authority(target, std::nullopt);
    }
    else if (ascii_istarts_with(target, "http://"))
    {
        auto remainder = target.substr(7);
        const auto delimiter = remainder.find_first_of("/?#");
        const auto authority = remainder.substr(0, delimiter);
        result.authority = parse_authority(authority, 80);
        if (delimiter == std::string_view::npos)
        {
            outbound_target = "/";
        }
        else if (remainder[delimiter] == '#')
        {
            throw HttpFailure(400, "HTTP request target must not contain a fragment");
        }
        else if (remainder[delimiter] == '?')
        {
            outbound_target = "/" + std::string(remainder.substr(delimiter));
            if (outbound_target.find('#') != std::string::npos)
            {
                throw HttpFailure(400, "HTTP request target must not contain a fragment");
            }
        }
        else
        {
            outbound_target = std::string(remainder.substr(delimiter));
            if (outbound_target.find('#') != std::string::npos)
            {
                throw HttpFailure(400, "HTTP request target must not contain a fragment");
            }
        }
    }
    else
    {
        if ((target.front() != '/' && target != "*") || !host_header)
        {
            throw HttpFailure(400, "HTTP proxy requests require an absolute URI or Host header");
        }
        result.authority = parse_authority(*host_header, 80);
        outbound_target = std::string(target);
    }

    if (!result.connect)
    {
        const auto named_by_connection = [&headers](std::string_view candidate) {
            for (const auto &[name, value] : headers)
            {
                if (!ascii_iequal(name, "Connection"))
                {
                    continue;
                }
                auto remaining = value;
                while (!remaining.empty())
                {
                    const auto comma = remaining.find(',');
                    if (ascii_iequal(trim_ows(remaining.substr(0, comma)), candidate))
                    {
                        return true;
                    }
                    if (comma == std::string_view::npos)
                    {
                        break;
                    }
                    remaining.remove_prefix(comma + 1);
                }
            }
            return false;
        };

        result.outbound_header.reserve(header.size() + 32);
        result.outbound_header = result.method + " " + outbound_target + " " + std::string(version) + "\r\n";
        result.outbound_header += "Host: " + result.authority.header + "\r\n";
        for (const auto &[name, value] : headers)
        {
            if (ascii_iequal(name, "Host") || ascii_iequal(name, "Connection") ||
                ascii_iequal(name, "Proxy-Connection") || ascii_iequal(name, "Proxy-Authorization") ||
                named_by_connection(name))
            {
                continue;
            }
            result.outbound_header.append(name);
            result.outbound_header.append(": ");
            result.outbound_header.append(value);
            result.outbound_header.append("\r\n");
        }
        result.outbound_header += "Connection: close\r\n\r\n";
    }
    return result;
}

std::string http_reason(unsigned int status)
{
    switch (status)
    {
    case 400:
        return "Bad Request";
    case 408:
        return "Request Timeout";
    case 431:
        return "Request Header Fields Too Large";
    case 502:
        return "Bad Gateway";
    case 504:
        return "Gateway Timeout";
    default:
        return "Proxy Error";
    }
}

std::uint8_t socks_reply_for(const asio::error_code &error)
{
    if (error == asio::error::network_unreachable)
    {
        return 0x03;
    }
    if (error == asio::error::host_unreachable || error == asio::error::host_not_found ||
        error == asio::error::host_not_found_try_again || error == asio::error::timed_out)
    {
        return 0x04;
    }
    if (error == asio::error::connection_refused)
    {
        return 0x05;
    }
    return 0x01;
}

} // namespace

class ProxySession
{
  public:
    ProxySession(tcp::socket client, SessionProtocol protocol, const ProxyConfig &config)
        : client_(std::move(client)), upstream_(client_.get_executor()), resolver_(client_.get_executor()),
          protocol_(protocol), config_(config), peer_(socket_peer(client_))
    {
    }

    asio::awaitable<void> run()
    {
        return protocol_ == SessionProtocol::Http ? run_http() : run_socks5();
    }

    const std::string &peer() const noexcept
    {
        return peer_;
    }

    const char *protocol_name() const noexcept
    {
        return protocol_ == SessionProtocol::Http ? "http" : "socks5";
    }

    void cancel() noexcept
    {
        resolver_.cancel();
        asio::error_code ignored;
        client_.close(ignored);
        upstream_.close(ignored);
    }

  private:
    asio::awaitable<void> connect_upstream(const std::string &host, std::uint16_t port)
    {
        auto endpoints = co_await resolver_.async_resolve(
            host, std::to_string(port), asio::cancel_after(config_.connect_timeout, asio::use_awaitable));
        co_await asio::async_connect(upstream_, endpoints,
                                     asio::cancel_after(config_.connect_timeout, asio::use_awaitable));
    }

    asio::awaitable<void> send_http_error(unsigned int status)
    {
        const auto reason = http_reason(status);
        const auto response =
            "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
        auto [error, _] = co_await asio::async_write(
            client_, asio::buffer(response),
            asio::cancel_after(config_.handshake_timeout, asio::as_tuple(asio::use_awaitable)));
        static_cast<void>(error);
    }

    asio::awaitable<void> run_http()
    {
        std::string input;
        input.reserve(std::min<std::size_t>(config_.http.max_header_bytes, 4096));
        std::size_t header_size = 0;
        unsigned int failure_status = 0;
        try
        {
            header_size = co_await asio::async_read_until(
                client_, asio::dynamic_buffer(input, config_.http.max_header_bytes), "\r\n\r\n",
                asio::cancel_after(config_.handshake_timeout, asio::use_awaitable));
        }
        catch (const asio::system_error &exception)
        {
            if (exception.code() == asio::error::not_found)
            {
                failure_status = 431;
            }
            else if (exception.code() == asio::error::timed_out)
            {
                failure_status = 408;
            }
        }
        if (failure_status != 0)
        {
            co_await send_http_error(failure_status);
            co_return;
        }
        if (header_size == 0)
        {
            co_return;
        }

        ParsedHttpRequest request;
        try
        {
            request = parse_http_request(std::string_view(input).substr(0, header_size));
        }
        catch (const HttpFailure &failure)
        {
            PROXY_DEBUG_PRINT("Proxy request rejected http reason=%s", failure.what());
            failure_status = failure.status;
        }
        if (failure_status != 0)
        {
            co_await send_http_error(failure_status);
            co_return;
        }

        asio::error_code connect_error;
        try
        {
            co_await connect_upstream(request.authority.host, request.authority.port);
        }
        catch (const asio::system_error &exception)
        {
            PROXY_ERROR_PRINT("Proxy connect failed http -> %s:%u reason=%s", request.authority.host.c_str(),
                              static_cast<unsigned int>(request.authority.port), exception.what());
            connect_error = exception.code();
        }
        if (connect_error)
        {
            co_await send_http_error(connect_error == asio::error::timed_out ? 504 : 502);
            co_return;
        }

        PROXY_INFO_PRINT("Proxy [+] http peer=%s target=%s:%u", peer_.c_str(), request.authority.host.c_str(),
                         static_cast<unsigned int>(request.authority.port));
        ScopeGuard log_closed([this, &request]() noexcept {
            PROXY_INFO_PRINT("Proxy [x] http peer=%s target=%s:%u", peer_.c_str(), request.authority.host.c_str(),
                             static_cast<unsigned int>(request.authority.port));
        });
        PROXY_DEBUG_PRINT("Proxy request http peer=%s method=%s", peer_.c_str(), request.method.c_str());
        const std::string_view extra(input.data() + header_size, input.size() - header_size);
        if (request.connect)
        {
            constexpr std::string_view established = "HTTP/1.1 200 Connection Established\r\n\r\n";
            co_await asio::async_write(client_, asio::buffer(established), asio::use_awaitable);
        }
        else
        {
            co_await asio::async_write(upstream_, asio::buffer(request.outbound_header), asio::use_awaitable);
        }
        if (!extra.empty())
        {
            co_await asio::async_write(upstream_, asio::buffer(extra), asio::use_awaitable);
        }
        co_await relay_tcp(client_, upstream_);
    }

    template <std::size_t N> asio::awaitable<void> read_socks(std::array<std::uint8_t, N> &buffer)
    {
        co_await asio::async_read(client_, asio::buffer(buffer),
                                  asio::cancel_after(config_.handshake_timeout, asio::use_awaitable));
    }

    asio::awaitable<void> send_socks_reply(std::uint8_t reply)
    {
        std::vector<std::uint8_t> response{0x05, reply, 0x00};
        if (reply == 0x00)
        {
            const auto endpoint = upstream_.local_endpoint();
            if (endpoint.address().is_v6())
            {
                response.push_back(0x04);
                const auto bytes = endpoint.address().to_v6().to_bytes();
                response.insert(response.end(), bytes.begin(), bytes.end());
            }
            else
            {
                response.push_back(0x01);
                const auto bytes = endpoint.address().to_v4().to_bytes();
                response.insert(response.end(), bytes.begin(), bytes.end());
            }
            response.push_back(static_cast<std::uint8_t>(endpoint.port() >> 8));
            response.push_back(static_cast<std::uint8_t>(endpoint.port()));
        }
        else
        {
            response.insert(response.end(), {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        }
        co_await asio::async_write(client_, asio::buffer(response), asio::use_awaitable);
    }

    asio::awaitable<void> run_socks5()
    {
        std::array<std::uint8_t, 2> greeting{};
        co_await read_socks(greeting);
        if (greeting[0] != 0x05 || greeting[1] == 0)
        {
            co_return;
        }
        std::vector<std::uint8_t> methods(greeting[1]);
        co_await asio::async_read(client_, asio::buffer(methods),
                                  asio::cancel_after(config_.handshake_timeout, asio::use_awaitable));
        if (std::ranges::find(methods, std::uint8_t{0x00}) == methods.end())
        {
            constexpr std::array<std::uint8_t, 2> rejected{0x05, 0xff};
            co_await asio::async_write(client_, asio::buffer(rejected), asio::use_awaitable);
            co_return;
        }
        constexpr std::array<std::uint8_t, 2> accepted{0x05, 0x00};
        co_await asio::async_write(client_, asio::buffer(accepted), asio::use_awaitable);

        std::array<std::uint8_t, 4> request{};
        co_await read_socks(request);
        if (request[0] != 0x05 || request[2] != 0x00)
        {
            co_return;
        }
        if (request[1] != 0x01)
        {
            co_await send_socks_reply(0x07);
            co_return;
        }

        std::string host;
        if (request[3] == 0x01)
        {
            std::array<std::uint8_t, 4> address{};
            co_await read_socks(address);
            host = asio::ip::address_v4(address).to_string();
        }
        else if (request[3] == 0x04)
        {
            std::array<std::uint8_t, 16> address{};
            co_await read_socks(address);
            host = asio::ip::address_v6(address).to_string();
        }
        else if (request[3] == 0x03)
        {
            std::array<std::uint8_t, 1> length{};
            co_await read_socks(length);
            if (length[0] == 0)
            {
                co_await send_socks_reply(0x08);
                co_return;
            }
            std::vector<std::uint8_t> name(length[0]);
            co_await asio::async_read(client_, asio::buffer(name),
                                      asio::cancel_after(config_.handshake_timeout, asio::use_awaitable));
            if (std::ranges::any_of(name, [](std::uint8_t ch) { return ch <= 0x20 || ch >= 0x7f; }))
            {
                co_await send_socks_reply(0x08);
                co_return;
            }
            host.assign(name.begin(), name.end());
        }
        else
        {
            co_await send_socks_reply(0x08);
            co_return;
        }

        std::array<std::uint8_t, 2> port_bytes{};
        co_await read_socks(port_bytes);
        const auto port = static_cast<std::uint16_t>((static_cast<unsigned int>(port_bytes[0]) << 8) | port_bytes[1]);
        if (port == 0)
        {
            co_await send_socks_reply(0x01);
            co_return;
        }

        asio::error_code connect_error;
        try
        {
            co_await connect_upstream(host, port);
        }
        catch (const asio::system_error &exception)
        {
            PROXY_ERROR_PRINT("Proxy connect failed socks5 -> %s:%u reason=%s", host.c_str(),
                              static_cast<unsigned int>(port), exception.what());
            connect_error = exception.code();
        }
        if (connect_error)
        {
            co_await send_socks_reply(socks_reply_for(connect_error));
            co_return;
        }

        PROXY_INFO_PRINT("Proxy [+] socks5 peer=%s target=%s:%u", peer_.c_str(), host.c_str(),
                         static_cast<unsigned int>(port));
        ScopeGuard log_closed([this, &host, port]() noexcept {
            PROXY_INFO_PRINT("Proxy [x] socks5 peer=%s target=%s:%u", peer_.c_str(), host.c_str(),
                             static_cast<unsigned int>(port));
        });
        co_await send_socks_reply(0x00);
        co_await relay_tcp(client_, upstream_);
    }

    tcp::socket client_;
    tcp::socket upstream_;
    tcp::resolver resolver_;
    SessionProtocol protocol_;
    const ProxyConfig &config_;
    std::string peer_;
};

ProxyConfig load_proxy_config(const std::filesystem::path &path)
{
    using namespace config;
    const auto root = load_json(path);
    reject_unknown_fields(root, {"log", "proxy", "socks5", "http"}, "root");
    const auto &proxy = required_object(root, "proxy", "root");
    const auto &socks5 = required_object(root, "socks5", "root");
    const auto &http = required_object(root, "http", "root");
    reject_unknown_fields(proxy, {"max_connections", "handshake_timeout_ms", "connect_timeout_ms"}, "proxy");
    reject_unknown_fields(socks5, {"address", "port"}, "socks5");
    reject_unknown_fields(http, {"address", "port", "max_header_bytes"}, "http");

    ProxyConfig result;
    result.max_connections = static_cast<std::size_t>(
        optional_unsigned(proxy, "max_connections", result.max_connections, 1, max_connection_count, "proxy"));
    result.handshake_timeout = optional_duration(proxy, "handshake_timeout_ms", result.handshake_timeout, "proxy");
    result.connect_timeout = optional_duration(proxy, "connect_timeout_ms", result.connect_timeout, "proxy");
    result.socks5.address = required_string(socks5, "address", "socks5");
    result.socks5.port = required_port(socks5, "port", "socks5");
    result.http.address = required_string(http, "address", "http");
    result.http.port = required_port(http, "port", "http");
    result.http.max_header_bytes = static_cast<std::size_t>(
        optional_unsigned(http, "max_header_bytes", result.http.max_header_bytes, 1024, 1024 * 1024, "http"));

    try
    {
        static_cast<void>(asio::ip::make_address(result.socks5.address));
    }
    catch (const std::exception &)
    {
        throw std::runtime_error("socks5.address must be a numeric IP address");
    }
    try
    {
        static_cast<void>(asio::ip::make_address(result.http.address));
    }
    catch (const std::exception &)
    {
        throw std::runtime_error("http.address must be a numeric IP address");
    }
    if (result.socks5.port == result.http.port)
    {
        throw std::runtime_error("socks5.port and http.port must be different");
    }
    initialize_logger_config(root);
    return result;
}

ProxyServer::ProxyServer(asio::io_context &io, ProxyConfig config)
    : executor_(io.get_executor()), config_(std::move(config)), http_acceptor_(executor_), socks5_acceptor_(executor_)
{
}

void ProxyServer::start()
{
    if (started_)
    {
        throw std::logic_error("RelayWeave Proxy can only be started once");
    }
    try
    {
        const tcp::endpoint socks5_endpoint(asio::ip::make_address(config_.socks5.address), config_.socks5.port);
        socks5_acceptor_.open(socks5_endpoint.protocol());
        socks5_acceptor_.set_option(asio::socket_base::reuse_address(true));
        socks5_acceptor_.bind(socks5_endpoint);
        socks5_acceptor_.listen();

        const tcp::endpoint http_endpoint(asio::ip::make_address(config_.http.address), config_.http.port);
        http_acceptor_.open(http_endpoint.protocol());
        http_acceptor_.set_option(asio::socket_base::reuse_address(true));
        http_acceptor_.bind(http_endpoint);
        http_acceptor_.listen();
    }
    catch (...)
    {
        asio::error_code ignored;
        socks5_acceptor_.close(ignored);
        http_acceptor_.close(ignored);
        throw;
    }

    started_ = true;
    running_ = true;
    PROXY_INFO_PRINT("Proxy listening socks5 %s:%u", config_.socks5.address.c_str(),
                     static_cast<unsigned int>(config_.socks5.port));
    PROXY_INFO_PRINT("Proxy listening http %s:%u", config_.http.address.c_str(),
                     static_cast<unsigned int>(config_.http.port));
    asio::co_spawn(executor_, accept_loop(socks5_acceptor_, ListenerProtocol::Socks5),
                   [self = shared_from_this()](std::exception_ptr failure) {
                       if (failure && self->running_)
                       {
                           PROXY_ERROR_PRINT("Proxy accept stopped socks5 reason=%s",
                                             exception_description(failure).c_str());
                       }
                   });
    asio::co_spawn(executor_, accept_loop(http_acceptor_, ListenerProtocol::Http),
                   [self = shared_from_this()](std::exception_ptr failure) {
                       if (failure && self->running_)
                       {
                           PROXY_ERROR_PRINT("Proxy accept stopped http reason=%s",
                                             exception_description(failure).c_str());
                       }
                   });
}

void ProxyServer::stop()
{
    if (!running_)
    {
        return;
    }
    running_ = false;
    asio::error_code ignored;
    http_acceptor_.close(ignored);
    socks5_acceptor_.close(ignored);
    for (const auto &session : sessions_)
    {
        session->cancel();
    }
    PROXY_INFO_PRINT("Proxy stopping");
}

asio::awaitable<void> ProxyServer::accept_loop(tcp::acceptor &acceptor, ListenerProtocol protocol)
{
    while (running_)
    {
        auto [error, socket] = co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));
        if (error)
        {
            if (!running_ || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Proxy accept failed %s reason=%s",
                              protocol == ListenerProtocol::Http ? "http" : "socks5", error.message().c_str());
            continue;
        }
        if (sessions_.size() >= config_.max_connections)
        {
            PROXY_ERROR_PRINT("Proxy rejected active=%zu limit=%zu", sessions_.size(), config_.max_connections);
            asio::error_code ignored;
            socket.close(ignored);
            continue;
        }

        const auto session_protocol =
            protocol == ListenerProtocol::Http ? SessionProtocol::Http : SessionProtocol::Socks5;
        auto session = std::make_shared<ProxySession>(std::move(socket), session_protocol, config_);
        sessions_.insert(session);
        asio::co_spawn(executor_, session->run(), [self = shared_from_this(), session](std::exception_ptr failure) {
            self->session_finished(session, failure);
        });
    }
}

void ProxyServer::session_finished(const std::shared_ptr<ProxySession> &session, std::exception_ptr failure)
{
    session->cancel();
    sessions_.erase(session);
    if (failure && running_)
    {
        PROXY_ERROR_PRINT("Proxy session failed %s peer=%s reason=%s", session->protocol_name(),
                          session->peer().c_str(), exception_description(failure).c_str());
    }
}
