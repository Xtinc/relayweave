#ifndef PROXY_CLUSTER_MGR_H
#define PROXY_CLUSTER_MGR_H

#include "tls_channel.h"
#include "async_event.h"

class RelayNode;
class ClusterRoom;

inline constexpr std::string_view cluster_broadcast_target = "all";

/** @brief Configure the Node role, identity, master control endpoint, and local NodeLink ports. */
struct ClusterConfig
{
    enum class Role
    {
        Master,
        Slave
    };

    Role role = Role::Master;
    std::string node_id;
    std::string address;
    std::uint16_t control_port = 0;
    std::uint16_t tcp_port = 0;
    std::uint16_t udp_port = 0;
    std::string advertise_address;
};

/**
 * @brief Own cluster control transport and route authenticated messages on control_io.
 * @details A master hosts a participant room; a slave reconnects through a Connector.
 * The RelayNode and TLS context are borrowed and must outlive all owned tasks.
 */
class ClusterMgr : public std::enable_shared_from_this<ClusterMgr>
{
    using tcp = asio::ip::tcp;
    using message_channel = asio::experimental::channel<void(asio::error_code, CtrlMessage)>;

  public:
    /**
     * @brief Construct cluster control resources without starting the master or slave loop.
     * @param server Owning RelayNode receiving validated cluster messages.
     * @param executor Single-threaded control executor owning cluster sockets and member routing.
     * @param context TLS context borrowed for the lifetime of the connection resources.
     * @param config Local master/slave role, Node identity, control endpoint, and advertised address.
     * @param channel TLS handshake, heartbeat, and bounded-message-queue settings.
     * @param max_connections Maximum remote cluster sessions accepted by the master.
     */
    ClusterMgr(RelayNode &server, asio::any_io_executor executor, asio::ssl::context &context, ClusterConfig config,
               TLSChannelConfig channel, std::size_t max_connections);

    /** @brief Start once during bootstrap or on control_io; the owner must retain this manager until drainage. */
    void start();
    /**
     * @brief Cancel and drain the control loop and member sessions on control_io.
     * @return False for handled shutdown errors; every normal return guarantees tasks have drained.
     */
    asio::awaitable<bool> async_stop();
    /**
     * @brief Submit a unicast directly on control_io; delivery is not acknowledged by this call.
     * @param target Destination Node identity, or the reserved broadcast target where supported.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return True when submitted to the master outbox or a joined slave TLS channel; false on local failure.
     */
    bool send(std::string target, CtrlMessage message);
    /**
     * @brief Queue delivery to all joined participants, including the local master, on control_io.
     * @param message Control message to dispatch; ownership of its parameters may be transferred.
     * @return Local submission result, with the same meaning as send().
     */
    bool broadcast(CtrlMessage message);

  private:
    class LocalParticipant;
    class Connector;

    asio::awaitable<void> accept_loop();
    asio::awaitable<void> slave_loop();
    asio::awaitable<void> accept_sessions(ClusterRoom &room);
    asio::awaitable<void> deliver_messages(ClusterRoom &room);
    void receive(CtrlMessage message);

    RelayNode &server_;
    asio::any_io_executor executor_;
    asio::ssl::context &context_;
    ClusterConfig config_;
    TLSChannelConfig channel_config_;
    std::size_t max_connections_;
    tcp::acceptor acceptor_;
    AsyncEvent loop_done_;
    std::exception_ptr drain_error_;
    message_channel outbound_channel_;
    Connector *connector_ = nullptr;
    std::uint64_t epoch_ = 0;
    bool running_ = false;
    bool started_ = false;
    bool stop_ok_ = true;
};

#endif
