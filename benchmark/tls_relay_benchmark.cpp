#include "relay_workload.h"
#include "test_data.h"
#include "tls_channel.h"
#include <openssl/ssl.h>

namespace
{
using namespace relay_bench;
asio::ssl::context context(bool server, int version)
{
    asio::ssl::context result(server ? asio::ssl::context::tls_server : asio::ssl::context::tls_client);
    const auto dir = test_data::directory();
    const std::string prefix = server ? "tls_channel_test_server" : "tls_channel_test_client";
    result.use_certificate_chain_file((dir / (prefix + ".pem")).string());
    result.use_private_key_file((dir / (prefix + ".key")).string(), asio::ssl::context::pem);
    result.load_verify_file((dir / (server ? "tls_channel_test_client_ca.pem" : "tls_channel_test_ca.pem")).string());
    require(SSL_CTX_set_min_proto_version(result.native_handle(), version) == 1 &&
            SSL_CTX_set_max_proto_version(result.native_handle(), version) == 1, "cannot pin TLS version");
    return result;
}

void tls_relay(bench::Report &report, int version, bool access)
{
    asio::io_context io(1);
    auto client_context = context(false, version);
    auto server_context = context(true, version);
    TLS left_client(io, client_context), left_server(io, server_context);
    TLS right_client(io, client_context), right_server(io, server_context);
    tcp::socket app(io), local(io);
    connect(right_client.next_layer(), right_server.next_layer());
    configure_tls_client(right_client, "localhost");
    configure_tls_server(right_server);
    if (access)
        connect(app, local);
    else
    {
        connect(left_client.next_layer(), left_server.next_layer());
        configure_tls_client(left_client, "localhost");
        configure_tls_server(left_server);
    }
    const auto label = std::string(access ? "access_tcp_tls" : "tls_tls") + (version == TLS1_2_VERSION ? "/1.2" : "/1.3");
    auto task = [&]() -> asio::awaitable<void> {
        bench::Timer handshake;
        co_await (right_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                  right_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
        if (!access)
            co_await (left_client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable) &&
                      left_server.async_handshake(asio::ssl::stream_base::server, asio::use_awaitable));
        report.add("handshake/" + label, access ? 1 : 2, 0, handshake, access ? 1 : 2,
                   {{"tls_version", SSL_get_version(right_client.native_handle())}, {"cipher", SSL_get_cipher_name(right_client.native_handle())}});
        constexpr std::size_t size = 32768;
        TrafficCounter outgoing, incoming;
        bench::Timer timer;
        if (access)
            co_await (relay_tls(local, right_server) && business(app, report.iterations, size, 0) &&
                      business(right_client, report.iterations, size, 1));
        else
        {
            co_await (relay_tls(left_server, right_server, nullptr, nullptr, &outgoing, &incoming) &&
                      business(left_client, report.iterations, size, 0) && business(right_client, report.iterations, size, 1));
            require(outgoing.total() == report.iterations * size && incoming.total() == report.iterations * size,
                    "TLS counters include framing or lost payload");
        }
        report.add("relay/" + label, report.iterations * 2, report.iterations * size * 2, timer, report.iterations * size * 2,
                   {{"chunk_bytes", size}, {"directions", 2}, {"includes_setup", false},
                    {"includes_integrity_check", true}});
    };
    run(io, task());
}

} // namespace

int main(int argc, char **argv)
{
    try
    {
        bench::Report report(argc, argv, 4096);
        for (unsigned i = 0; i < report.repeats; ++i)
            for (const int version : {TLS1_2_VERSION, TLS1_3_VERSION})
                for (const bool access : {false, true})
                    tls_relay(report, version, access);
        report.save();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
