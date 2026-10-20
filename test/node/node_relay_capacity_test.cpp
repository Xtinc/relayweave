#include "member_access.h"
#include "node_test_config.h"
#include "node_session.h"
#include <array>
#include <future>
#include <iostream>

TEST_MEMBER(RelayNode, start_relay)
TEST_MEMBER(RelayNode, relay_sessions_)
TEST_MEMBER(RelayNode, stop_relays)

using namespace std::chrono_literals;

static void require_capacity(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

int main()
{
    try
    {
        asio::io_context control(1), tcp_io(1), udp_io(1), data(1);
        asio::ssl::context ssl(asio::ssl::context::tls_server);
        auto config = make_test_node_config();
        config.control.address = config.tcp.address = config.tls.address = config.datagram.address = "127.0.0.1";
        config.tcp.max_relays = 1;
        config.tls.max_relays = 2;
        config.datagram.max_relays = 3;
        constexpr std::array cases{
            std::pair{RelayProtocol::Tcp, 1}, std::pair{RelayProtocol::Tls, 2}, std::pair{RelayProtocol::Udp, 3}};
        auto node = std::make_shared<RelayNode>(control, tcp_io, udp_io, data, ssl, config);
        auto session = std::make_shared<TLSChannel>(asio::ip::tcp::socket(control), ssl, TLSChannelRole::S);
        auto make_relay = [&](RelayProtocol protocol) {
            return std::make_shared<NodeSession>(*node, session, session,
                njson{{"request_id", 1}, {"service", "capacity"}, {"protocol", relay_protocol_name(protocol)}},
                std::make_shared<ServiceTraffic>());
        };
        auto poll_control = [&] { control.restart(); control.poll(); };
        auto reject = [&](RelayProtocol protocol) {
            bool rejected = false;
            try
            {
                test_access::RelayNode_start_relay(*node, make_relay(protocol));
            }
            catch (const std::runtime_error &error)
            {
                rejected = std::string_view(error.what()) == "relay capacity reached";
            }
            require_capacity(rejected, "pending or closing Relay bypassed max_relays");
        };
        auto admit = [&] {
            for (auto [protocol, capacity] : cases)
            {
                for (int i = 0; i < capacity; ++i)
                {
                    test_access::RelayNode_start_relay(*node, make_relay(protocol));
                }
                for (int i = 0; i < 20; ++i)
                {
                    reject(protocol);
                }
            }
        };
        auto drain = [&] {
            auto stopped = asio::co_spawn(control, test_access::RelayNode_stop_relays(*node), asio::use_future);
            for (unsigned step = 0; step < 128 && stopped.wait_for(0s) != std::future_status::ready; ++step)
            {
                for (auto *io : {&control, &tcp_io, &udp_io, &data})
                {
                    io->restart();
                    io->poll();
                }
            }
            require_capacity(stopped.wait_for(0s) == std::future_status::ready, "closing Relays did not drain");
            stopped.get();
            require_capacity(test_access::RelayNode_relay_sessions_(*node).empty(), "drained Relays retained count capacity");
        };
        // Each protocol has its own quota. Do not pump either transfer executor:
        // all admitted requests must remain counted while install is pending.
        asio::post(control, admit);
        poll_control();
        require_capacity(test_access::RelayNode_relay_sessions_(*node).size() == 6, "protocol quotas were combined or exceeded");
        asio::post(control, [&] {
            for (const auto &relay : test_access::RelayNode_relay_sessions_(*node))
            {
                relay->cancel("test", "cancelled");
            }
            for (const auto &entry : cases)
            {
                reject(entry.first);
            }
        });
        poll_control();
        drain();
        asio::post(control, admit);
        poll_control();
        require_capacity(test_access::RelayNode_relay_sessions_(*node).size() == 6, "released quotas were not reusable");
        drain();
        std::cout << "[PASS] Relay count includes pending, active and draining tasks per protocol\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
