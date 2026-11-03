#include "tls_rpk.h"
#include <stdexcept>

/** @brief Verify only the RPK against this context's pinned shared public key. */
static int verify_rpk(X509_STORE_CTX *store, void *argument)
{
    auto *context = static_cast<SSL_CTX *>(argument);
    auto *received = X509_STORE_CTX_get0_rpk(store);
    auto *expected = X509_get0_pubkey(SSL_CTX_get0_certificate(context));
    const bool valid = received && expected && EVP_PKEY_eq(received, expected) == 1;
    X509_STORE_CTX_set_error(store, valid ? X509_V_OK : X509_V_ERR_APPLICATION_VERIFICATION);
    return valid ? 1 : 0;
}

void configure_tls_rpk(asio::ssl::context &context, asio::ssl::context &identity)
{
    auto *ctx = context.native_handle();
    auto *certificate = SSL_CTX_get0_certificate(identity.native_handle());
    auto *key = SSL_CTX_get0_privatekey(identity.native_handle());
    const unsigned char types[] = {TLSEXT_cert_type_rpk};
    if (!certificate || !key || SSL_CTX_use_certificate(ctx, certificate) != 1 ||
        SSL_CTX_use_PrivateKey(ctx, key) != 1 || SSL_CTX_check_private_key(ctx) != 1 ||
        SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1 ||
        SSL_CTX_set1_client_cert_type(ctx, types, sizeof(types)) != 1 ||
        SSL_CTX_set1_server_cert_type(ctx, types, sizeof(types)) != 1 ||
        SSL_CTX_set_num_tickets(ctx, 0) != 1 || SSL_CTX_set_max_early_data(ctx, 0) != 1)
    {
        throw std::runtime_error("Cannot configure mutual TLS raw-public-key authentication");
    }
    SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
    SSL_CTX_set_options(ctx, SSL_OP_NO_TICKET);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    SSL_CTX_set_cert_verify_callback(ctx, verify_rpk, ctx);
}
