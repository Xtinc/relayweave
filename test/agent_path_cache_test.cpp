#include "relay_agent.h"

#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <thread>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;

#ifdef _WIN32
constexpr auto duplicate_fd = &_dup;
constexpr auto redirect_fd = &_dup2;
constexpr auto close_fd = &_close;
constexpr auto file_fd = &_fileno;
#else
constexpr auto duplicate_fd = &dup;
constexpr auto redirect_fd = &dup2;
constexpr auto close_fd = &close;
constexpr auto file_fd = &fileno;
#endif

void require(bool condition, const std::string &message)
{
    if (!condition)
        throw std::runtime_error(message);
}

// Read through a separate file handle so inspecting logs does not change stdout's offset.
class LogCapture
{
  public:
    LogCapture()
        : path_(std::filesystem::temp_directory_path() /
                ("relayweave-path-cache-" + std::to_string(AgentRouting::Clock::now().time_since_epoch().count()) +
                 ".log"))
    {
        std::fflush(stdout);
        saved_ = duplicate_fd(file_fd(stdout));
        require(saved_ >= 0, "Cannot save stdout");
        auto *file = std::fopen(path_.string().c_str(), "wb");
        const bool redirected = file && redirect_fd(file_fd(file), file_fd(stdout)) >= 0;
        if (file)
            std::fclose(file);
        if (!redirected)
        {
            close_fd(saved_);
            throw std::runtime_error("Cannot capture stdout");
        }
    }

    ~LogCapture()
    {
        std::fflush(stdout);
        redirect_fd(saved_, file_fd(stdout));
        close_fd(saved_);
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }

    std::string read() const
    {
        std::fflush(stdout);
        std::ifstream file(path_);
        return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    }

    void expect_calculations(std::size_t expected) const
    {
        const auto log = read();
        const std::string marker = "Routes service=";
        std::size_t count = 0;
        for (auto offset = log.find(marker); offset != std::string::npos;
             offset = log.find(marker, offset + marker.size()))
            ++count;
        require(count == expected, "Expected " + std::to_string(expected) + " path calculations, got " +
                                       std::to_string(count) + "\n" + log);
    }

  private:
    std::filesystem::path path_;
    int saved_ = -1;
};

class IoRunner
{
  public:
    explicit IoRunner(asio::io_context &io) : io_(io), work_(asio::make_work_guard(io)), thread_([&io] { io.run(); })
    {
    }
    ~IoRunner()
    {
        io_.stop();
    }

  private:
    asio::io_context &io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    std::jthread thread_;
};

asio::awaitable<void> pause(std::chrono::steady_clock::duration duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor, duration);
    co_await timer.async_wait(asio::use_awaitable);
}

template <class Predicate>
asio::awaitable<void> wait_for(Predicate predicate, const char *reason, AgentRouting::Clock::duration timeout = 2s)
{
    const auto deadline = AgentRouting::Clock::now() + timeout;
    while (!predicate())
    {
        require(AgentRouting::Clock::now() < deadline, reason);
        co_await pause(10ms);
    }
}

// The fake control peer rejects relay opens, avoiding data-plane setup while exercising
// the real local accept loops, executor crossing, discovery, and reconnect behavior.
class ControlPeer
{
  public:
    ControlPeer(asio::io_context &io, asio::ssl::context &context) : acceptor(io), context_(context)
    {
        // IPv6 cannot be measured by the IPv4 ProbeSet: this makes empty candidate results
        // deterministic, without requiring raw sockets. Fall back where IPv6 is unavailable.
        asio::error_code error;
        acceptor.open(tcp::v6(), error);
        if (!error)
            acceptor.bind(tcp::endpoint(asio::ip::address_v6::loopback(), 0), error);
        if (error)
        {
            acceptor.close();
            acceptor.open(tcp::v4());
            acceptor.bind(tcp::endpoint(asio::ip::address_v4::loopback(), 0));
        }
        acceptor.listen();
        host = acceptor.local_endpoint().address().to_string();
    }

    asio::awaitable<void> run()
    {
        while (acceptor.is_open())
        {
            auto [error, socket] = co_await acceptor.async_accept(use_nothrow_awaitable);
            if (error)
                co_return;
            asio::co_spawn(acceptor.get_executor(), session(std::move(socket)), asio::detached);
        }
    }

    asio::awaitable<void> barrier(const LogCapture &logs)
    {
        const auto marker = "cache-test-barrier-" + std::to_string(++barriers_);
        channel->send(CtrlMessage{CtrlCommand::ServiceError, njson{{"reason", marker}}});
        co_await wait_for([&] { return logs.read().find(marker) != std::string::npos; },
                          "Agent did not process the control-message barrier");
    }

    njson snapshot() const
    {
        LinkQuality::Summary quality;
        quality.cost = 5.0;
        quality.confidence = 1.0;
        quality.usable = true;
        return {{"request_id", topology_request},
                {"epoch", 1u},
                {"snapshot_version", 1u},
                {"created_age_ms", 0u},
                {"nodes", njson::array({njson{{"node_id", "cache-node"}, {"address", host}, {"report_age_ms", 0u}},
                                        njson{{"node_id", "other-node"}, {"address", host}, {"report_age_ms", 0u}}})},
                {"links", njson::array({njson{{"source", "cache-node"},
                                              {"destination", "other-node"},
                                              {"age_ms", 0u},
                                              {"quality", quality_json(quality)}}})}};
    }

    tcp::acceptor acceptor;
    std::string host;
    std::shared_ptr<TLSChannel> channel;
    std::size_t lookups = 0;
    std::size_t identifications = 0;
    std::size_t opens = 0;
    std::uint64_t topology_request = 0;
    bool stale_next_open = false;

  private:
    asio::awaitable<void> session(tcp::socket socket)
    {
        auto current = std::make_shared<TLSChannel>(std::move(socket), context_, TLSChannelRole::S);
        try
        {
            co_await current->start({});
            channel = current;
            for (;;)
            {
                const auto message = co_await current->async_receive();
                const auto &params = config::message_params(message);
                switch (message.type())
                {
                case CtrlCommand::ServerIdentify:
                    current->send(CtrlMessage{CtrlCommand::ServerIdentified, njson{{"node_id", "cache-node"}}});
                    ++identifications;
                    break;
                case CtrlCommand::ServiceLookup: {
                    auto located = params;
                    located["node_id"] = "cache-node";
                    located["address"] = host;
                    located["port"] = acceptor.local_endpoint().port();
                    current->send(CtrlMessage{CtrlCommand::ServiceLocated, std::move(located)});
                    ++lookups;
                    break;
                }
                case CtrlCommand::TopologyQuery:
                    topology_request = config::require_unsigned(params, "request_id", true);
                    break; // Hold the response so expiry is not masked by automatic refresh.
                case CtrlCommand::RelayOpen: {
                    auto failure = params;
                    failure["reason"] =
                        std::exchange(stale_next_open, false) ? "service unavailable" : "test rejection";
                    current->send(CtrlMessage{CtrlCommand::RelayError, std::move(failure)});
                    ++opens;
                    break;
                }
                default:
                    break;
                }
            }
        }
        catch (const asio::system_error &)
        {
            // Expected on deliberate disconnect and Agent shutdown.
        }
        co_await current->async_disconnect();
    }

    asio::ssl::context &context_;
    std::size_t barriers_ = 0;
};

asio::awaitable<void> open_batch(const std::array<std::uint16_t, 2> &ports, std::size_t count = 8)
{
    const auto executor = co_await asio::this_coro::executor;
    std::vector<tcp::socket> sockets;
    for (std::size_t index = 0; index < count; ++index)
    {
        sockets.emplace_back(executor);
        co_await sockets.back().async_connect(
            tcp::endpoint(asio::ip::address_v4::loopback(), ports[index % ports.size()]),
            asio::cancel_after(2s, asio::use_awaitable));
    }
    for (auto &socket : sockets)
    {
        std::array<char, 1> data{};
        const auto [error, length] =
            co_await socket.async_read_some(asio::buffer(data), asio::cancel_after(2s, use_nothrow_awaitable));
        require(length == 0 && (error == asio::error::eof || error == asio::error::connection_reset),
                "Rejected local relay did not close");
    }
}

asio::awaitable<void> verify_cache(ControlPeer &peer, std::shared_ptr<RelayAgent> agent,
                                   std::array<std::uint16_t, 2> ports, const LogCapture &logs)
{
    co_await wait_for([&] { return peer.lookups == 2 && peer.topology_request != 0; }, "Service discovery timed out");
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    require(peer.opens == 8, "Batch did not reach both service accept loops");
    logs.expect_calculations(1);
    const auto output = logs.read();
    require(output.find("Control [+] node=cache-node") != std::string::npos,
            "INF did not report the authenticated node");
    require(output.find("Service located service=first/tcp") != std::string::npos,
            "INF did not report service discovery");
    require(output.find("[INF] calculate_service_paths") == std::string::npos,
            "Candidate route details leaked into INF");
    require(output.find("[DEB] calculate_service_paths") != std::string::npos, "Missing DEB route diagnostics");
    require(output.find("request_id=") != std::string::npos &&
                output.find("reason=test rejection") != std::string::npos,
            "Relay rejection lost its correlation ID or reason");
    require(output.find(" server=") == std::string::npos,
            "Relay logs still imply that the connection ingress is the service destination");
    if (peer.host == "::1")
        require(logs.read().find("cost=unavailable") != std::string::npos,
                "Unmeasured IPv6 ingress invented candidate paths");

    // Service removal and rediscovery call update_probe_targets with unchanged targets.
    peer.stale_next_open = true;
    co_await open_batch(ports, 1);
    co_await wait_for([&] { return peer.lookups == 3; }, "Service rediscovery timed out");
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    logs.expect_calculations(1);

    auto snapshot = peer.snapshot();
    auto unrelated = snapshot;
    unrelated["request_id"] = peer.topology_request + 1000;
    peer.channel->send(CtrlMessage{CtrlCommand::TopologySnapshot, std::move(unrelated)});
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    logs.expect_calculations(1);

    peer.channel->send(CtrlMessage{CtrlCommand::TopologySnapshot, snapshot});
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    logs.expect_calculations(1);
    const auto cached_at = AgentRouting::Clock::now();

    peer.channel->send(CtrlMessage{CtrlCommand::TopologySnapshot, snapshot});
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    logs.expect_calculations(1);

    // Accept fresh metrics at two regular five-second polls. They must neither
    // invalidate the destination cache nor renew its fifteen-second TTL.
    auto request = peer.topology_request;
    for (unsigned update = 1; update <= 2; ++update)
    {
        co_await wait_for([&] { return peer.topology_request != request; }, "Waiting for metric refresh", 6s);
        request = peer.topology_request;
        auto metrics = peer.snapshot();
        metrics["snapshot_version"] = update + 1;
        metrics["created_age_ms"] = 10u;
        metrics["nodes"][0]["report_age_ms"] = update * 100;
        metrics["links"][0]["age_ms"] = update * 100;
        metrics["links"][0]["quality"]["cost"] = 10.0 * update;
        peer.channel->send(CtrlMessage{CtrlCommand::TopologySnapshot, std::move(metrics)});
        co_await peer.barrier(logs);
        co_await open_batch(ports);
        logs.expect_calculations(1);
    }
    asio::steady_timer expiry(co_await asio::this_coro::executor, cached_at + 15100ms);
    co_await expiry.async_wait(asio::use_awaitable);
    co_await open_batch(ports);
    logs.expect_calculations(2);

    co_await wait_for([&] { return peer.topology_request != request; }, "Next topology request timed out");
    auto malformed = peer.snapshot();
    malformed["snapshot_version"] = 4u;
    malformed["nodes"].push_back(malformed["nodes"].front());
    peer.channel->send(CtrlMessage{CtrlCommand::TopologySnapshot, std::move(malformed)});
    co_await peer.barrier(logs);
    require(logs.read().find("Topology rejected") != std::string::npos,
            "Malformed topology did not exercise rejection");
    co_await open_batch(ports);
    logs.expect_calculations(2);

    const auto identifications = peer.identifications;
    co_await peer.channel->async_disconnect();
    co_await pause(50ms);
    co_await open_batch(ports, 2);
    logs.expect_calculations(2);
    co_await wait_for([&] { return peer.identifications == identifications + 1; }, "Control reconnect timed out");
    co_await peer.barrier(logs);
    co_await open_batch(ports);
    logs.expect_calculations(3);

    co_await agent->async_stop();
    peer.acceptor.close();
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        require(argc > 0, "Missing executable path");
        const auto data = std::filesystem::absolute(argv[0]).parent_path() / "data";
        asio::ssl::context server_context(asio::ssl::context::tls_server);
        server_context.use_certificate_chain_file((data / "tls_channel_test_server.pem").string());
        server_context.use_private_key_file((data / "tls_channel_test_server.key").string(), asio::ssl::context::pem);
        server_context.load_verify_file((data / "tls_channel_test_client_ca.pem").string());
        asio::ssl::context client_context(asio::ssl::context::tls_client);
        client_context.use_certificate_chain_file((data / "tls_channel_test_client.pem").string());
        client_context.use_private_key_file((data / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
        client_context.load_verify_file((data / "tls_channel_test_ca.pem").string());

        asio::io_context control_io(1), transfer_io(1);
        ControlPeer peer(control_io, server_context);
        std::array<std::uint16_t, 2> ports;
        {
            tcp::acceptor first(transfer_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
            tcp::acceptor second(transfer_io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
            ports = {first.local_endpoint().port(), second.local_endpoint().port()};
        }
        AgentConfig config;
        config.host = peer.host;
        config.port = peer.acceptor.local_endpoint().port();
        config.reconnect_initial_delay = 300ms;
        config.reconnect_max_delay = 300ms;
        config.channel.disconnect_timeout = 500ms;
        config.forwards = {{"first", "127.0.0.1", ports[0], RelayProtocol::Tcp},
                           {"second", "127.0.0.1", ports[1], RelayProtocol::Tls}};
        auto agent = std::make_shared<RelayAgent>(control_io, transfer_io, client_context, std::move(config));
        {
            LogCapture logs;
            initialize_logger_config(njson::object());
            PROXY_DEBUG_PRINT("debug-filter-hidden");
            PROXY_INFO_PRINT("info-filter-visible");
            require(logs.read().find("debug-filter-hidden") == std::string::npos,
                    "DEB appeared when debug was disabled");
            require(logs.read().find("info-filter-visible") != std::string::npos,
                    "INF was hidden when debug was disabled");
            initialize_logger_config(njson{{"log", {{"debug_enable", true}}}});
            agent->start();
            asio::co_spawn(control_io, peer.run(), asio::detached);
            auto result = asio::co_spawn(control_io, verify_cache(peer, agent, ports, logs), asio::use_future);
            IoRunner control_runner(control_io), transfer_runner(transfer_io);
            result.get();
        }
        std::cout << "[PASS] Agent destination cache sharing, empty results, TTL and invalidation\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
