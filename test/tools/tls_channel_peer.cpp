#include "test_data.h"
#include "tls_channel.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

constexpr std::uint16_t default_port = 18443;

struct Arguments
{
    TLSChannelRole role;
    std::string host = "localhost";
    std::uint16_t port = default_port;
};

void print_usage(std::string_view program)
{
    std::cerr << "Usage:\n"
              << "  " << program << " server [port]\n"
              << "  " << program << " client [host] [port]\n\n"
              << "Examples:\n"
              << "  " << program << " server 18443\n"
              << "  " << program << " client localhost 18443\n";
}

std::uint16_t parse_port(std::string_view text)
{
    unsigned int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value == 0 || value > 65535)
    {
        throw std::invalid_argument("port must be an integer in [1, 65535]");
    }
    return static_cast<std::uint16_t>(value);
}

Arguments parse_arguments(int argc, char *argv[])
{
    if (argc < 2)
    {
        throw std::invalid_argument("missing role");
    }

    const std::string_view role = argv[1];
    if (role == "server")
    {
        if (argc > 3)
        {
            throw std::invalid_argument("too many server arguments");
        }
        return Arguments{TLSChannelRole::S, "localhost", argc == 3 ? parse_port(argv[2]) : default_port};
    }
    if (role == "client")
    {
        if (argc > 4)
        {
            throw std::invalid_argument("too many client arguments");
        }
        Arguments result{TLSChannelRole::C};
        if (argc >= 3)
        {
            result.host = argv[2];
        }
        if (argc == 4)
        {
            result.port = parse_port(argv[3]);
        }
        return result;
    }

    throw std::invalid_argument("role must be 'server' or 'client'");
}

tcp::socket accept_client(asio::io_context &io, std::uint16_t port)
{
    tcp::acceptor acceptor(io);
    acceptor.open(tcp::v4());
    acceptor.set_option(asio::socket_base::reuse_address(true));
    acceptor.bind(tcp::endpoint(tcp::v4(), port));
    acceptor.listen();

    std::cout << "[LISTEN] 0.0.0.0:" << port << '\n'
              << "[WAIT] Waiting for the client TCP connection...\n"
              << std::flush;

    tcp::socket socket(io);
    acceptor.accept(socket);
    std::cout << "[TCP] Accepted " << socket.remote_endpoint() << '\n';
    return socket;
}

tcp::socket connect_to_server(asio::io_context &io, const std::string &host, std::uint16_t port)
{
    tcp::resolver resolver(io);
    const auto endpoints = resolver.resolve(host, std::to_string(port));
    tcp::socket socket(io);

    std::cout << "[CONNECT] " << host << ':' << port << "...\n" << std::flush;
    asio::connect(socket, endpoints);
    std::cout << "[TCP] Connected to " << socket.remote_endpoint() << '\n';
    return socket;
}

TLSChannelConfig debug_config()
{
    TLSChannelConfig config;
    config.handshake_timeout = 5s;
    config.disconnect_timeout = 5s;
    config.heartbeat_interval = 2s;
    config.heartbeat_timeout = 8s;
    return config;
}

void require_data_file(const std::filesystem::path &path)
{
    if (!std::filesystem::is_regular_file(path))
    {
        throw std::runtime_error("TLS debug data file is missing: " + path.string() +
                                 ". Check the test/data directory in the source tree.");
    }
}
} // namespace

int main(int argc, char *argv[])
{
    Arguments arguments{TLSChannelRole::S};
    try
    {
        arguments = parse_arguments(argc, argv);
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        print_usage(argc > 0 ? argv[0] : "test_tls_channel_peer");
        return 1;
    }

    try
    {
        const auto data_directory = test_data::directory();
        const auto debug_config_path = data_directory / "tls_channel_debug.json";
        const auto ca_path = data_directory / "tls_channel_test_ca.pem";
        const auto certificate_path = data_directory / "tls_channel_test_server.pem";
        const auto private_key_path = data_directory / "tls_channel_test_server.key";
        const auto client_ca_path = data_directory / "tls_channel_test_client_ca.pem";
        const auto client_certificate_path = data_directory / "tls_channel_test_client.pem";
        const auto client_private_key_path = data_directory / "tls_channel_test_client.key";

        std::cout << "[DATA] Loading TLS debug files from " << data_directory << '\n';
        require_data_file(debug_config_path);
        if (arguments.role == TLSChannelRole::S)
        {
            require_data_file(certificate_path);
            require_data_file(private_key_path);
            require_data_file(client_ca_path);
        }
        else
        {
            require_data_file(ca_path);
            require_data_file(client_certificate_path);
            require_data_file(client_private_key_path);
        }

        initialize_logger_config(config::load_json(debug_config_path));

        asio::io_context io;
        asio::ssl::context ssl_context(arguments.role == TLSChannelRole::S ? asio::ssl::context::tls_server
                                                                           : asio::ssl::context::tls_client);

        if (arguments.role == TLSChannelRole::S)
        {
            ssl_context.use_certificate_chain_file(certificate_path.string());
            ssl_context.use_private_key_file(private_key_path.string(), asio::ssl::context::pem);
            ssl_context.load_verify_file(client_ca_path.string());
        }
        else
        {
            ssl_context.load_verify_file(ca_path.string());
            ssl_context.use_certificate_chain_file(client_certificate_path.string());
            ssl_context.use_private_key_file(client_private_key_path.string(), asio::ssl::context::pem);
        }
        if (SSL_CTX_check_private_key(ssl_context.native_handle()) != 1)
        {
            throw std::runtime_error("TLS private key does not match its certificate");
        }

        tcp::socket socket = arguments.role == TLSChannelRole::S
                                 ? accept_client(io, arguments.port)
                                 : connect_to_server(io, arguments.host, arguments.port);

        auto channel = std::make_shared<TLSChannel>(std::move(socket), ssl_context, arguments.role, debug_config());
        auto start_future = asio::co_spawn(
            channel->executor(),
            channel->start(arguments.role == TLSChannelRole::C ? arguments.host : std::string{}), asio::use_future);
        std::jthread io_thread([&io]() { io.run(); });
        start_future.get();

        std::cout << "[TLS] " << (arguments.role == TLSChannelRole::S ? "Server" : "Client") << " connected.\n"
                  << std::flush;

        std::string line;
        while (std::getline(std::cin, line))
        {
            if (line == "quit")
            {
                break;
            }

            if (arguments.role == TLSChannelRole::C)
            {
                channel->send(CtrlMessage("echo", njson{{"data", line}}));
            }
        }

        auto disconnect_future =
            asio::co_spawn(channel->executor(), channel->async_disconnect(), asio::use_future);
        disconnect_future.get();
        channel.reset();

        std::cout << "[DONE] TLSChannel event loop stopped.\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
