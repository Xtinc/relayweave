#include "node_test_config.h"
#include <future>
#include <thread>
#include <iostream>
#include <stdexcept>
#include <string>

namespace
{
using namespace std::chrono_literals;
void require(bool condition, const std::string &message)
{
    if (!condition)
        throw std::runtime_error(message);
}

using tcp = asio::ip::tcp;

std::uint16_t unused_port(asio::io_context &io)
{
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
}

void verify_server_start_lifecycle(asio::ssl::context &server_context)
{
    {
        asio::io_context control_io(1);
        asio::io_context transfer_io(1);
        TestClusterDataIO cluster_data;
        NodeConfig config = make_test_node_config();
        config.control.address = "127.0.0.1";
        config.control.port = 1;
        config.tcp.address = "127.0.0.1";
        config.tcp.port = 1;
        config.tls.address = "127.0.0.1";
        config.tls.port = 2;
        config.datagram.address = "127.0.0.1";
        config.datagram.port = 1;

        bool failed = false;
        try
        {
            [[maybe_unused]] auto server =
                std::make_shared<RelayNode>(control_io, transfer_io, transfer_io, cluster_data.io, server_context, std::move(config));
        }
        catch (const std::invalid_argument &)
        {
            failed = true;
        }
        require(failed, "Server accepted a shared TCP/UDP transfer io_context");
    }

    {
        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        const auto control_port = unused_port(control_io);

        NodeConfig config = make_test_node_config();
        config.control.address = "127.0.0.1";
        config.control.port = control_port;
        config.tcp.address = "invalid-address";
        config.tcp.port = unused_port(transfer_tcp_io);
        config.tls.address = "127.0.0.1";
        config.tls.port = 0;
        config.datagram.address = "127.0.0.1";
        config.datagram.port = config.tcp.port;
        auto server = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data.io, server_context,
                                                  std::move(config));

        bool failed = false;
        try
        {
            server->start();
        }
        catch (const std::exception &)
        {
            failed = true;
        }
        require(failed, "Server start unexpectedly succeeded with an invalid data listener");
        tcp::acceptor control_probe(control_io, tcp::endpoint(asio::ip::address_v4::loopback(), control_port));
    }

    {
        asio::io_context control_io(1);
        asio::io_context transfer_tcp_io(1);
        asio::io_context transfer_udp_io(1);
        TestClusterDataIO cluster_data;
        const auto control_port = unused_port(control_io);
        auto transfer_port = unused_port(transfer_tcp_io);
        while (transfer_port == control_port)
        {
            transfer_port = unused_port(transfer_tcp_io);
        }

        NodeConfig config = make_test_node_config();
        config.control.address = "127.0.0.1";
        config.control.port = control_port;
        config.tcp.address = "127.0.0.1";
        config.tcp.port = transfer_port;
        config.tls.address = "127.0.0.1";
        config.tls.port = 0;
        config.datagram.address = "127.0.0.1";
        config.datagram.port = transfer_port;
        auto server = std::make_shared<RelayNode>(control_io, transfer_tcp_io, transfer_udp_io, cluster_data.io, server_context,
                                                  std::move(config));
        std::weak_ptr<RelayNode> weak_server = server;
        server->start();

        std::promise<bool> executor_stop_rejected;
        auto executor_stop_result = executor_stop_rejected.get_future();
        asio::post(control_io, [server, &executor_stop_rejected]() {
            try
            {
                server->stop();
                executor_stop_rejected.set_value(false);
            }
            catch (const std::logic_error &)
            {
                executor_stop_rejected.set_value(true);
            }
        });
        std::thread control_thread([&control_io]() { control_io.run(); });
        std::thread transfer_tcp_thread([&transfer_tcp_io]() { transfer_tcp_io.run(); });
        std::thread transfer_udp_thread([&transfer_udp_io]() { transfer_udp_io.run(); });
        const bool rejected_executor_stop = executor_stop_result.get();
        server.reset();
        auto running_server = weak_server.lock();
        const bool retained_by_accept_loops = static_cast<bool>(running_server);
        if (running_server)
        {
            running_server->stop();
        }
        else
        {
            control_io.stop();
            transfer_tcp_io.stop();
            transfer_udp_io.stop();
        }
        running_server.reset();
        control_thread.join();
        transfer_tcp_thread.join();
        transfer_udp_thread.join();
        require(rejected_executor_stop, "Server stop was accepted from the control executor");
        require(retained_by_accept_loops, "Accept loops did not retain the running server");
        require(weak_server.expired(), "Stopped server was retained after accept loops completed");
    }
}

} // namespace

int main()
{
    try
    {
        asio::ssl::context context(asio::ssl::context::tls_server);
        const auto config = make_test_node_config();
        context.use_certificate_chain_file(config.certificate_chain.string());
        context.use_private_key_file(config.private_key.string(), asio::ssl::context::pem);
        context.load_verify_file((test_data::directory() / "tls_channel_test_client_ca.pem").string());
        verify_server_start_lifecycle(context);
        std::cout << "[PASS] Node execution domains, listener rollback and shutdown ownership\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
