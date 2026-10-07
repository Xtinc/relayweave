#include "relay_node.h"
#include "relayweave_version.h"
#include <csignal>
#include <iostream>

void print_usage(std::string_view program)
{
    std::cerr << "Usage: " << program << " <node-config.json>\n";
}

int main(int argc, char *argv[])
{
    if (argc == 2 && std::string_view(argv[1]) == "--help")
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-node");
        return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--version")
    {
        std::cout << "RelayWeave Node " << RELAYWEAVE_VERSION << '\n';
        return 0;
    }

    if (argc != 2)
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-node");
        return 1;
    }

    try
    {
        const std::filesystem::path config_path = argv[1];
        auto config = load_node_config(config_path);

        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        asio::io_context signal_io(1);
        asio::ssl::context ssl_context(asio::ssl::context::tls_server);
        ssl_context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                asio::ssl::context::no_tlsv1_1);
        ssl_context.use_certificate_chain_file(config.certificate_chain.string());
        ssl_context.use_private_key_file(config.private_key.string(), asio::ssl::context::pem);
        if (SSL_CTX_check_private_key(ssl_context.native_handle()) != 1)
        {
            throw std::runtime_error("TLS server private key does not match its certificate");
        }
        ssl_context.load_verify_file(config.ca_file.string());

        auto node =
            std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, ssl_context, std::move(config));
        node->start();

        asio::signal_set signals(signal_io, SIGINT, SIGTERM);
        signals.async_wait([node](const asio::error_code &ec, int) {
            if (!ec)
            {
                node->stop();
            }
        });

        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_tcp_thread([&transfer_tcp_io]() {
            set_current_thread_scheduler_policy();
            transfer_tcp_io.run();
        });
        std::thread transfer_udp_thread([&transfer_udp_io]() {
            set_current_thread_scheduler_policy();
            transfer_udp_io.run();
        });
        signal_io.run();

        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
