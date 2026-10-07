#include "relay_agent.h"
#include "relayweave_version.h"
#include "xfr_channel.h"
#include <csignal>
#include <cstdio>
#include <iostream>

void print_usage(std::string_view program)
{
    std::cerr << "Usage: " << program << " <agent-config.json>\n";
}

int main(int argc, char *argv[])
{
    if (argc == 2 && std::string_view(argv[1]) == "--help")
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-agent");
        return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--version")
    {
        std::cout << "RelayWeave Agent " << RELAYWEAVE_VERSION << '\n';
        return 0;
    }
    if (argc != 2)
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-agent");
        return 1;
    }

    try
    {
        const std::filesystem::path config_path = argv[1];
        setvbuf(stdout, nullptr, _IONBF, 0);
        setvbuf(stderr, nullptr, _IONBF, 0);
        auto config = load_agent_config(config_path);

        asio::io_context control_io(1);
        asio::io_context transfer_io(1);
        asio::io_context signal_io(1);
        asio::ssl::context ssl_context(asio::ssl::context::tls_client);
        ssl_context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                asio::ssl::context::no_tlsv1_1);
        ssl_context.load_verify_file(config.ca_file.string());
        ssl_context.use_certificate_chain_file(config.certificate_chain.string());
        ssl_context.use_private_key_file(config.private_key.string(), asio::ssl::context::pem);
        if (SSL_CTX_check_private_key(ssl_context.native_handle()) != 1)
        {
            throw std::runtime_error("TLS client private key does not match its certificate");
        }

        auto agent = std::make_shared<RelayAgent>(control_io, transfer_io, ssl_context, std::move(config));
        agent->start();

        asio::signal_set signals(signal_io, SIGINT, SIGTERM);
        signals.async_wait([agent, &control_io](const asio::error_code &error, int) {
            if (!error)
            {
                asio::co_spawn(control_io, agent->async_stop(), asio::detached);
            }
        });

        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_thread([&transfer_io]() {
            set_current_thread_scheduler_policy();
            transfer_io.run();
        });
        signal_io.run();

        control_thread.join();
        transfer_thread.join();
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
