#include "test_data.h"
#include "tls_channel.h"

#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

struct DataFiles
{
    std::filesystem::path server_ca;
    std::filesystem::path server_certificate;
    std::filesystem::path server_private_key;
    std::filesystem::path client_ca;
    std::filesystem::path client_certificate;
    std::filesystem::path client_private_key;
};

struct SocketPair
{
    tcp::socket client;
    tcp::socket server;
};

class IoRunner
{
  public:
    explicit IoRunner(asio::io_context &io)
        : io_(io), guard_(asio::make_work_guard(io)), thread_([&io]() { io.run(); })
    {
    }

    ~IoRunner()
    {
        guard_.reset();
        io_.stop();
    }

  private:
    asio::io_context &io_;
    asio::executor_work_guard<asio::io_context::executor_type> guard_;
    std::jthread thread_;
};

void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

void require_file(const std::filesystem::path &path)
{
    require(std::filesystem::is_regular_file(path), "Missing test data file: " + path.string());
}

DataFiles load_data_files()
{
    const auto directory = test_data::directory();
    DataFiles files{directory / "tls_channel_test_ca.pem",
                    directory / "tls_channel_test_server.pem",
                    directory / "tls_channel_test_server.key",
                    directory / "tls_channel_test_client_ca.pem",
                    directory / "tls_channel_test_client.pem",
                    directory / "tls_channel_test_client.key"};
    require_file(files.server_ca);
    require_file(files.server_certificate);
    require_file(files.server_private_key);
    require_file(files.client_ca);
    require_file(files.client_certificate);
    require_file(files.client_private_key);
    return files;
}

void configure_common(asio::ssl::context &context)
{
    context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                        asio::ssl::context::no_tlsv1_1);
}

void configure_server(asio::ssl::context &context, const DataFiles &files)
{
    configure_common(context);
    context.use_certificate_chain_file(files.server_certificate.string());
    context.use_private_key_file(files.server_private_key.string(), asio::ssl::context::pem);
    context.load_verify_file(files.client_ca.string());
    require(SSL_CTX_check_private_key(context.native_handle()) == 1, "Server test certificate and key do not match");
}

void configure_client(asio::ssl::context &context, const DataFiles &files, bool with_certificate)
{
    configure_common(context);
    context.load_verify_file(files.server_ca.string());
    if (with_certificate)
    {
        context.use_certificate_chain_file(files.client_certificate.string());
        context.use_private_key_file(files.client_private_key.string(), asio::ssl::context::pem);
        require(SSL_CTX_check_private_key(context.native_handle()) == 1,
                "Client test certificate and key do not match");
    }
}

SocketPair make_socket_pair(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    tcp::socket client(io);
    client.connect(tcp::endpoint(asio::ip::address_v4::loopback(), acceptor.local_endpoint().port()));
    tcp::socket server = acceptor.accept();
    return {std::move(client), std::move(server)};
}

TLSChannelConfig channel_config()
{
    TLSChannelConfig config;
    config.handshake_timeout = 2s;
    config.disconnect_timeout = 2s;
    config.heartbeat_interval = 100ms;
    config.heartbeat_timeout = 2s;
    return config;
}

bool failed(std::future<void> &result)
{
    try
    {
        result.get();
        return false;
    }
    catch (...)
    {
        return true;
    }
}

bool failed_with_logic_error(std::future<void> &result)
{
    try
    {
        result.get();
        return false;
    }
    catch (const std::logic_error &)
    {
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void disconnect_pair(asio::io_context &io, const std::shared_ptr<TLSChannel> &client,
                     const std::shared_ptr<TLSChannel> &server)
{
    auto client_result = asio::co_spawn(io, client->async_disconnect(), asio::use_future);
    auto server_result = asio::co_spawn(io, server->async_disconnect(), asio::use_future);
    client_result.get();
    server_result.get();
}

void test_success_and_start_contract(const DataFiles &files)
{
    asio::ssl::context server_context(asio::ssl::context::tls_server);
    asio::ssl::context client_context(asio::ssl::context::tls_client);
    configure_server(server_context, files);
    configure_client(client_context, files, true);

    asio::io_context io;
    auto sockets = make_socket_pair(io);
    auto client =
        std::make_shared<TLSChannel>(std::move(sockets.client), client_context, TLSChannelRole::C, channel_config());
    auto server =
        std::make_shared<TLSChannel>(std::move(sockets.server), server_context, TLSChannelRole::S, channel_config());

    auto client_start = asio::co_spawn(io, client->start("localhost"), asio::use_future);
    auto server_start = asio::co_spawn(io, server->start({}), asio::use_future);
    IoRunner runner(io);
    client_start.get();
    server_start.get();

    auto closed = asio::co_spawn(io, client->async_wait_closed(), asio::use_future);
    require(closed.wait_for(50ms) == std::future_status::timeout,
            "start() did not return while the TLS channel remained connected");

    const CtrlMessage large{"test.large", njson{{"value", std::string(200000, 'x')}}};
    auto received = asio::co_spawn(io, server->async_receive(2s), asio::use_future);
    client->send(large);
    const auto complete = received.get();
    require(complete.command == large.command && complete.params == large.params,
            "TLS channel did not transparently assemble a large message");
    auto echoed = asio::co_spawn(io, client->async_receive(2s), asio::use_future);
    server->send(complete);
    require(echoed.get().params == large.params, "TLS channel large reply changed data");

    auto repeated_start = asio::co_spawn(io, client->start("localhost"), asio::use_future);
    require(failed_with_logic_error(repeated_start), "A second start() call did not throw std::logic_error");

    auto server_closed = asio::co_spawn(io, server->async_wait_closed(), asio::use_future);
    client->disconnect();
    client->disconnect();
    server->disconnect();
    closed.get();
    server_closed.get();
}

void test_missing_client_certificate(const DataFiles &files)
{
    asio::ssl::context server_context(asio::ssl::context::tls_server);
    asio::ssl::context client_context(asio::ssl::context::tls_client);
    configure_server(server_context, files);
    configure_client(client_context, files, false);

    asio::io_context io;
    auto sockets = make_socket_pair(io);
    auto client =
        std::make_shared<TLSChannel>(std::move(sockets.client), client_context, TLSChannelRole::C, channel_config());
    auto server =
        std::make_shared<TLSChannel>(std::move(sockets.server), server_context, TLSChannelRole::S, channel_config());
    auto client_start = asio::co_spawn(io, client->start("localhost"), asio::use_future);
    auto server_start = asio::co_spawn(io, server->start({}), asio::use_future);
    IoRunner runner(io);

    const bool client_failed = failed(client_start);
    const bool server_failed = failed(server_start);
    require(server_failed, "The server accepted a client that did not present a certificate");
    require(client_failed || server_failed, "The certificate-less mTLS handshake unexpectedly succeeded");
    disconnect_pair(io, client, server);
}

void test_host_name_mismatch(const DataFiles &files)
{
    asio::ssl::context server_context(asio::ssl::context::tls_server);
    asio::ssl::context client_context(asio::ssl::context::tls_client);
    configure_server(server_context, files);
    configure_client(client_context, files, true);

    asio::io_context io;
    auto sockets = make_socket_pair(io);
    auto client =
        std::make_shared<TLSChannel>(std::move(sockets.client), client_context, TLSChannelRole::C, channel_config());
    auto server =
        std::make_shared<TLSChannel>(std::move(sockets.server), server_context, TLSChannelRole::S, channel_config());
    auto client_start = asio::co_spawn(io, client->start("wrong-host.invalid"), asio::use_future);
    auto server_start = asio::co_spawn(io, server->start({}), asio::use_future);
    IoRunner runner(io);

    require(failed(client_start), "The client accepted a server certificate with the wrong host name");
    failed(server_start);
    disconnect_pair(io, client, server);
}

void test_disconnect_during_handshake(const DataFiles &files)
{
    asio::ssl::context client_context(asio::ssl::context::tls_client);
    configure_client(client_context, files, true);

    asio::io_context io;
    auto sockets = make_socket_pair(io);
    auto client =
        std::make_shared<TLSChannel>(std::move(sockets.client), client_context, TLSChannelRole::C, channel_config());
    auto start_result = asio::co_spawn(io, client->start("localhost"), asio::use_future);
    auto disconnect_result = asio::co_spawn(io, client->async_wait_closed(), asio::use_future);
    asio::post(io, [client] { client->disconnect(); });
    IoRunner runner(io);

    require(failed(start_result), "Disconnecting during the TLS handshake did not cancel start()");
    disconnect_result.get();
}

void test_send_encoding_failure_closes_channel(const DataFiles &files)
{
    asio::ssl::context server_context(asio::ssl::context::tls_server);
    asio::ssl::context client_context(asio::ssl::context::tls_client);
    configure_server(server_context, files);
    configure_client(client_context, files, true);

    asio::io_context io;
    auto sockets = make_socket_pair(io);
    auto client =
        std::make_shared<TLSChannel>(std::move(sockets.client), client_context, TLSChannelRole::C, channel_config());
    auto server =
        std::make_shared<TLSChannel>(std::move(sockets.server), server_context, TLSChannelRole::S, channel_config());
    auto client_start = asio::co_spawn(io, client->start("localhost"), asio::use_future);
    auto server_start = asio::co_spawn(io, server->start({}), asio::use_future);
    IoRunner runner(io);
    client_start.get();
    server_start.get();

    auto client_closed = asio::co_spawn(io, client->async_wait_closed(), asio::use_future);
    auto server_closed = asio::co_spawn(io, server->async_wait_closed(), asio::use_future);
    client->send(CtrlMessage{"invalid-command"});

    require(client_closed.wait_for(3s) == std::future_status::ready,
            "A control message encoding failure did not close the sending channel");
    client_closed.get();
    require(server_closed.wait_for(3s) == std::future_status::ready,
            "The peer did not observe closure after a control message encoding failure");
    server_closed.get();
}
} // namespace

int main()
{
    try
    {
        const auto files = load_data_files();
        test_success_and_start_contract(files);
        std::cout << "[PASS] successful mTLS and start contract\n";
        test_missing_client_certificate(files);
        std::cout << "[PASS] missing client certificate rejected\n";
        test_host_name_mismatch(files);
        std::cout << "[PASS] server host-name mismatch rejected\n";
        test_disconnect_during_handshake(files);
        std::cout << "[PASS] disconnect during handshake\n";
        test_send_encoding_failure_closes_channel(files);
        std::cout << "[PASS] send encoding failure closes channel\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
