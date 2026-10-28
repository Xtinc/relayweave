#ifndef RELAYWEAVE_SOCKS5_UDP_H
#define RELAYWEAVE_SOCKS5_UDP_H

#include "asio.hpp"
#include "lru_cache.h"
#include <string>

struct ProxyConfig;

/**
 * @brief Own one SOCKS5 UDP association, its sockets and bounded target caches on the Proxy executor.
 * @details ProxySession owns this object and the borrowed TCP control connection through run().
 * Client packets enter a dedicated relay socket; target replies enter separate outbound sockets.
 * Closing the TCP connection or cancelling the owner terminates and drains all receive tasks.
 */
class Socks5UdpAssociation
{
  public:
    /**
     * @brief Prepare an association without opening sockets.
     * @param control Borrowed negotiated TCP connection, valid through run().
     * @param config Proxy limits, copied where needed during construction.
     */
    Socks5UdpAssociation(asio::ip::tcp::socket &control, const ProxyConfig &config);
    /**
     * @brief Validate the expected client endpoint and bind UDP relay sockets.
     * @param host Client UDP address from UDP ASSOCIATE; an unspecified address permits the TCP peer.
     * @param port Expected client UDP port, or zero to learn it from the first valid datagram.
     * @return SOCKS reply code; zero indicates that endpoint() is ready.
     */
    asio::awaitable<std::uint8_t> open(std::string host, std::uint16_t port);
    /** @brief Read the relay address advertised to the client. @return Bound UDP endpoint. */
    asio::ip::udp::endpoint endpoint() const;
    /**
     * @brief Run independent TCP lifetime, client receive and target receive chains, draining siblings on exit.
     * @pre Called on the Proxy executor after open() succeeds; the owner remains alive until completion.
     */
    asio::awaitable<void> run();
    /** @brief Cancel resolution and close every UDP socket on the Proxy executor. */
    void cancel() noexcept;

  private:
    /** @brief Await TCP closure and cancel the associated datagram operations. */
    asio::awaitable<void> watch_control();
    /** @brief Validate client datagrams, resolve destinations and send their payloads. */
    asio::awaitable<void> receive_client();
    /**
     * @brief Encapsulate replies from previously contacted targets.
     * @param socket Association-owned IPv4 or IPv6 outbound socket, valid through run().
     */
    asio::awaitable<void> receive_target(asio::ip::udp::socket &socket);

    asio::ip::tcp::socket &control_;
    asio::ip::udp::socket relay_;
    asio::ip::udp::socket outbound_v4_;
    asio::ip::udp::socket outbound_v6_;
    asio::ip::udp::resolver resolver_;
    asio::ip::udp::endpoint client_;
    std::chrono::steady_clock::duration resolve_timeout_;
    LRUCache<std::string, asio::ip::udp::endpoint> resolved_;
    LRUCache<asio::ip::udp::endpoint, bool> targets_;
};

#endif
