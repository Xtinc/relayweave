#ifndef RELAYWEAVE_TLS_RPK_H
#define RELAYWEAVE_TLS_RPK_H

#include "tls_channel.h"

/**
 * @brief Configure a TLS 1.3 business context for mutual, pinned raw-public-key authentication.
 * @param context Separate business context, owned until every business stream has drained.
 * @param identity Existing X.509 context supplying the shared certificate and matching private key.
 * @throws std::runtime_error If credentials are absent, mismatched, or RPK configuration fails.
 * @details Copies credential references into context; verification pins its certificate's public key.
 * Session resumption and early data are disabled. Control context verification remains unchanged.
 */
void configure_tls_rpk(asio::ssl::context &context, asio::ssl::context &identity);

#endif
