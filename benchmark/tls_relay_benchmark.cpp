#include "relay_workload.h"
#include "test_data.h"
#include "tls_rpk.h"

namespace
{
using namespace relay_bench;

void tls_relay(bench::Report &report)
{
    asio::io_context io(1);
    asio::ssl::context identity(asio::ssl::context::tls);
    const auto dir = test_data::directory();
    identity.use_certificate_chain_file((dir / "tls_channel_test_client.pem").string());
    identity.use_private_key_file((dir / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
    asio::ssl::context context(asio::ssl::context::tls);
    configure_tls_rpk(context, identity);
    TLS client(io, context);
    TLS server(io, context);
    tcp::socket app(io);
    tcp::socket local(io);
    connect(client.next_layer(), server.next_layer());
    connect(app, local);
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer handshake;
        co_await (client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
        report.add("handshake/tls13_rpk", 1, 0, handshake, 1,
                   {{"tls_version", SSL_get_version(client.native_handle())},
                    {"cipher", SSL_get_cipher_name(client.native_handle())}});
        constexpr std::size_t size = 32768;
        bench::Timer timer;
        co_await (relay_tls(local, server) && business(app, report.iterations, size, 0) &&
                  business(client, report.iterations, size, 1));
        report.add("relay/access_tcp_tls13_rpk", report.iterations * 2, report.iterations * size * 2,
                   timer, report.iterations * size * 2,
                   {{"chunk_bytes", size}, {"directions", 2}, {"includes_setup", false},
                    {"includes_integrity_check", true}});
    };
    run(io, task());
}
}

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 4096);
        for (unsigned i = 0; i < report.repeats; ++i)
        {
            tls_relay(report);
        }
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
