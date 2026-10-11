#ifndef RELAYWEAVE_AGENT_SESSION_H
#define RELAYWEAVE_AGENT_SESSION_H

#include "relay_agent.h"
#include "xfr_channel.h"
#include <variant>

class Forwarder;

// State and coroutine entry points belong to Forwarder's transfer executor.
class AgentSession : public std::enable_shared_from_this<AgentSession>
{
  public:
    AgentSession(Forwarder &owner, std::uint64_t request, ServiceKey service, std::string destination,
                 std::chrono::steady_clock::time_point deadline);
    asio::awaitable<void> run();
    void cancel(std::string cause);

  private:
    friend class Forwarder;

    struct StreamData
    {
        explicit StreamData(asio::any_io_executor executor) : resolver(executor), local(executor), transfer(executor)
        {
        }
        asio::ip::tcp::resolver resolver;
        asio::ip::tcp::socket local;
        asio::ip::tcp::socket transfer;
        std::optional<TLSStream> tls;
    };

    struct DatagramData
    {
        explicit DatagramData(asio::any_io_executor executor) : resolver(executor), transfer(executor)
        {
        }
        asio::ip::udp::resolver resolver;
        asio::ip::udp::socket transfer;
        std::optional<asio::ip::udp::socket> target;
        DatagramHeader::Buffer session_header{};
        std::size_t maximum_payload = DatagramHeader::maximum_user_payload;
    };

    bool producer() const
    {
        return target.has_value();
    }

    void handle(const CtrlMessage &message);
    void fail(std::string cause);
    void close_io();
    asio::awaitable<void> attach();
    asio::awaitable<void> receive_datagrams();

    Forwarder &owner;
    std::uint64_t request;
    ServiceKey service;
    std::string destination;
    std::chrono::steady_clock::time_point deadline;
    RelaySelection selection;
    njson endpoint;
    std::optional<std::size_t> forward;
    std::optional<AgentServiceConfig> target;
    using Data = std::variant<StreamData, DatagramData>;
    Data data;
    asio::steady_timer changed;
    bool ready = false;
    bool node_closed = false;
    std::string reason;
};

#endif
