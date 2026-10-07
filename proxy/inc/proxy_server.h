#ifndef RELAYWEAVE_PROXY_SERVER_H
#define RELAYWEAVE_PROXY_SERVER_H

#include "asio.hpp"
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_set>

struct ProxyListenerConfig
{
    std::string address;
    std::uint16_t port = 0;
};

struct HttpProxyListenerConfig : ProxyListenerConfig
{
    std::size_t max_header_bytes = 32 * 1024;
};

struct ProxyConfig
{
    std::size_t max_connections = 512;
    std::chrono::steady_clock::duration handshake_timeout = std::chrono::seconds(10);
    std::chrono::steady_clock::duration connect_timeout = std::chrono::seconds(10);
    ProxyListenerConfig socks5;
    HttpProxyListenerConfig http;
};

ProxyConfig load_proxy_config(const std::filesystem::path &path);

class ProxySession;

class ProxyServer : public std::enable_shared_from_this<ProxyServer>
{
  public:
    ProxyServer(asio::io_context &io, ProxyConfig config);

    void start();
    void stop();

  private:
    enum class ListenerProtocol
    {
        Http,
        Socks5,
    };

    asio::awaitable<void> accept_loop(asio::ip::tcp::acceptor &acceptor, ListenerProtocol protocol);
    void session_finished(const std::shared_ptr<ProxySession> &session, std::exception_ptr failure);

    asio::any_io_executor executor_;
    ProxyConfig config_;
    asio::ip::tcp::acceptor http_acceptor_;
    asio::ip::tcp::acceptor socks5_acceptor_;
    std::unordered_set<std::shared_ptr<ProxySession>> sessions_;
    bool started_ = false;
    bool running_ = false;
};

#endif
