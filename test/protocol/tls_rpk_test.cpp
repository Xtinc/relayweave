#include "tls_rpk.h"
#include "test_data.h"
#include <iostream>
#include <memory>
#include <stdexcept>

namespace
{
using Connection = std::unique_ptr<SSL, decltype(&SSL_free)>;

void require(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
}

asio::ssl::context identity(bool other = false)
{
    asio::ssl::context result(asio::ssl::context::tls);
    const auto prefix = other ? "tls_channel_test_server" : "tls_channel_test_client";
    const auto path = test_data::directory() / prefix;
    result.use_certificate_chain_file(path.string() + ".pem");
    result.use_private_key_file(path.string() + ".key", asio::ssl::context::pem);
    return result;
}

/** @brief Exercise the production verifier with independent peers over a bounded memory BIO pair. */
void handshake(bool wrong_server, bool wrong_client, bool missing_client, bool x509_server)
{
    auto trusted = identity();
    auto other = identity(true);
    asio::ssl::context client_context(asio::ssl::context::tls);
    asio::ssl::context server_context(asio::ssl::context::tls);
    configure_tls_rpk(client_context, wrong_client ? other : trusted);
    configure_tls_rpk(server_context, wrong_server ? other : trusted);
    if (wrong_client)
    {
        SSL_CTX_set_cert_verify_callback(client_context.native_handle(), [](X509_STORE_CTX *, void *) { return 1; }, nullptr);
    }
    if (wrong_server)
    {
        SSL_CTX_set_cert_verify_callback(server_context.native_handle(), [](X509_STORE_CTX *, void *) { return 1; }, nullptr);
    }
    if (missing_client)
    {
        client_context = asio::ssl::context(asio::ssl::context::tls);
        const unsigned char types[] = {TLSEXT_cert_type_rpk};
        require(SSL_CTX_set1_server_cert_type(client_context.native_handle(), types, sizeof(types)) == 1,
                "Cannot configure certificate-less test peer");
        require(SSL_CTX_set1_client_cert_type(client_context.native_handle(), types, sizeof(types)) == 1,
                "Cannot configure certificate-less test peer");
    }
    if (x509_server)
    {
        server_context = identity();
    }
    Connection client(SSL_new(client_context.native_handle()), SSL_free);
    Connection server(SSL_new(server_context.native_handle()), SSL_free);
    require(client && server, "Cannot create TLS peers");
    BIO *left = nullptr;
    BIO *right = nullptr;
    require(BIO_new_bio_pair(&left, 0, &right, 0) == 1, "Cannot create BIO pair");
    SSL_set_bio(client.get(), left, left);
    SSL_set_bio(server.get(), right, right);
    SSL_set_connect_state(client.get());
    SSL_set_accept_state(server.get());
    bool failed = false;
    for (unsigned step = 0; step < 100 && !failed; ++step)
    {
        for (auto *peer : {client.get(), server.get()})
        {
            const auto result = SSL_do_handshake(peer);
            if (result != 1)
            {
                const auto error = SSL_get_error(peer, result);
                if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE)
                {
                    failed = true;
                    break;
                }
            }
        }
        if (SSL_is_init_finished(client.get()) && SSL_is_init_finished(server.get()))
        {
            break;
        }
    }
    const bool reject = wrong_server || wrong_client || missing_client || x509_server;
    require(failed == reject, "RPK authentication result differs from expected");
    if (!reject)
    {
        require(SSL_is_init_finished(client.get()) && SSL_is_init_finished(server.get()), "RPK handshake stalled");
        require(SSL_version(client.get()) == TLS1_3_VERSION, "RPK negotiated a different TLS version");
        require(SSL_get0_peer_rpk(client.get()) && SSL_get0_peer_rpk(server.get()), "Mutual RPK was not negotiated");
        require(!SSL_get0_peer_certificate(client.get()) && !SSL_get0_peer_certificate(server.get()),
                "RPK unexpectedly sent an X.509 certificate");
        require(SSL_get_negotiated_group(client.get()) != 0,
                "TLS did not negotiate an ephemeral key exchange group");
    }
}
}

int main()
{
    try
    {
        handshake(false, false, false, false);
        handshake(true, false, false, false);
        handshake(false, true, false, false);
        handshake(false, false, true, false);
        handshake(false, false, false, true);
        asio::ssl::context empty(asio::ssl::context::tls);
        asio::ssl::context context(asio::ssl::context::tls);
        bool rejected = false;
        try { configure_tls_rpk(context, empty); }
        catch (const std::runtime_error &) { rejected = true; }
        require(rejected, "Empty business credentials were accepted");
        std::cout << "[PASS] Mutual TLS 1.3 RPK, wrong/missing keys, X.509 fallback rejection\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
