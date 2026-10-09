#ifndef RELAYWEAVE_AGENT_RELAY_H
#define RELAYWEAVE_AGENT_RELAY_H

#include "relay_agent.h"
#include "xfr_channel.h"
#include <asio/cancellation_signal.hpp>
#include <variant>

class Forwarder;

// One business access, entirely confined to the owning Forwarder's transfer executor.
class AgentRelay : public std::enable_shared_from_this<AgentRelay>
{
  public:
    AgentRelay(Forwarder &owner, std::uint64_t request, ServiceKey service, std::string destination,
               std::chrono::steady_clock::time_point deadline);
    asio::awaitable<void> run();
    asio::awaitable<void> send_datagram(std::span<const std::uint8_t> payload);
    void cancel(std::string cause);

  private:
    friend class Forwarder;
    struct StreamData
    {
        explicit StreamData(asio::any_io_executor executor)
            : resolver(executor), local(executor), transfer(executor)
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
    };
    bool producer() const
    {
        return target.has_value();
    }
    void handle(const CtrlMessage &message);
    asio::awaitable<void> attach();
    asio::awaitable<void> receive_datagrams();
    asio::awaitable<void> finish();

    // Forwarder owns this instance and drains its task before destruction.
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
    asio::cancellation_signal cancellation;
    asio::steady_timer changed;
    bool ready = false;
    bool completed = false;
    bool cancelled = false;
    bool submitted = false;
    bool failed = false;
    std::string reason;
};

#endif
