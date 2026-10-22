#include "relay_node.h"
#include "relayweave_version.h"
#include <csignal>
#include <future>
#include <iostream>
#include <thread>

void print_usage(std::string_view program)
{
    std::cerr << "Usage: " << program << " <node-config.json>\n"
              << "       " << program << " --check-cluster-config <node-config.json>\n";
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

    if (argc == 3 && std::string_view(argv[1]) == "--check-cluster-config")
    {
        try
        {
            static_cast<void>(parse_cluster_config(config::load_json(argv[2])));
            return 0;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[FAIL] " << e.what() << '\n';
            return 1;
        }
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
        asio::io_context cluster_data_io(1);
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

        auto node = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data_io,
                                                ssl_context, std::move(config));
        asio::signal_set signals(signal_io, SIGINT, SIGTERM);
        auto signal = signals.async_wait(asio::use_future);
        std::exception_ptr failure;
        bool stopped = false;
        {
            auto control_work = asio::make_work_guard(control_io);
            auto tcp_work = asio::make_work_guard(transfer_tcp_io);
            auto udp_work = asio::make_work_guard(transfer_udp_io);
            auto data_work = asio::make_work_guard(cluster_data_io);
            std::vector<std::thread> workers;
            workers.reserve(4);
            ScopeGuard join_workers([&] {
                control_work.reset();
                tcp_work.reset();
                udp_work.reset();
                data_work.reset();
                for (auto &worker : workers)
                {
                    worker.join();
                }
            });
            workers.emplace_back([&] { control_io.run(); });
            workers.emplace_back([&] {
                set_current_thread_scheduler_policy();
                transfer_tcp_io.run();
            });
            workers.emplace_back([&] {
                set_current_thread_scheduler_policy();
                transfer_udp_io.run();
            });
            workers.emplace_back([&] { cluster_data_io.run(); });

            try
            {
                asio::post(control_io, asio::use_future([node] { node->start(); })).get();
                signal_io.run();
                signal.get();
            }
            catch (...)
            {
                failure = std::current_exception();
            }
            try
            {
                stopped = node->stop();
            }
            catch (...)
            {
                if (!failure)
                {
                    failure = std::current_exception();
                }
            }
        }
        if (failure)
        {
            std::rethrow_exception(failure);
        }
        return stopped ? 0 : 1;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
