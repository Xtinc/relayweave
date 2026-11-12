#include "node_fixture.h"
#include <future>
#include <iostream>

using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;
TEST_MEMBER(RelayNode, topology_)
TEST_MEMBER(NodeLinkMgr, local_links_)
TEST_MEMBER(NodeLinkMgr, local_flows_)

namespace
{
void require(bool condition, const char *message)
{
    if (!condition)
        throw std::runtime_error(message);
}

asio::awaitable<void> pause()
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(10ms);
    co_await timer.async_wait(asio::use_awaitable);
}

struct Cluster
{
    asio::io_context control{1}, transfer{1}, datagram{1}, data{1};
    asio::executor_work_guard<asio::io_context::executor_type> control_work{control.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> tcp_work{transfer.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> udp_work{datagram.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> data_work{data.get_executor()};
    asio::ssl::context ssl{asio::ssl::context::tls_server};
    std::vector<std::shared_ptr<RelayNode>> nodes;
    std::vector<std::thread> workers;

    Cluster()
    {
        auto settings = make_test_node_config();
        ssl.use_certificate_chain_file(settings.certificate_chain.string());
        ssl.use_private_key_file(settings.private_key.string(), asio::ssl::context::pem);
        ssl.load_verify_file(settings.server_ca_file.string());
        tcp::acceptor control_reservation(control, {asio::ip::address_v4::loopback(), 0});
        tcp::acceptor tcp_reservation(data, {asio::ip::address_v4::loopback(), 0});
        udp::socket udp_reservation(data, {asio::ip::address_v4::loopback(), 0});
        const auto cp = control_reservation.local_endpoint().port();
        const auto tp = tcp_reservation.local_endpoint().port();
        const auto up = udp_reservation.local_endpoint().port();
        control_reservation.close();
        tcp_reservation.close();
        udp_reservation.close();
        for (int i = 0; i < 3; ++i)
        {
            auto config = settings;
            const auto address = "127.0.0." + std::to_string(i + 1);
            config.control.address = config.tcp.address = config.datagram.address = address;
            config.control.advertise_address = i == 0 ? "localhost" : i == 1 ? "127.0.1.2" : address;
            config.cluster = {i == 0 ? ClusterConfig::Role::Master : ClusterConfig::Role::Slave,
                              std::string(1, 'a' + i), "127.0.0.1", cp, tp, up};
            config.cluster.label = i < 2 ? "vpc-a" : "vpc-b";
            config.cluster.label_address = i < 2 ? address : "127.0.1.3";
            config.channel.disconnect_timeout = 100ms;
            auto node = std::make_shared<RelayNode>(control, transfer, datagram, data, ssl, config);
            node->start();
            nodes.push_back(std::move(node));
        }
        workers.emplace_back([this] { control.run(); });
        workers.emplace_back([this] { transfer.run(); });
        workers.emplace_back([this] { datagram.run(); });
        workers.emplace_back([this] { data.run(); });
    }

    ~Cluster()
    {
        for (auto &node : nodes)
            node->stop();
        control_work.reset();
        tcp_work.reset();
        udp_work.reset();
        data_work.reset();
        for (auto &worker : workers)
            worker.join();
    }

    Topology &topology(std::size_t index)
    {
        return *test_access::RelayNode_topology_(*nodes[index]);
    }

    auto shared(const std::string &right, RelayProtocol transport)
    {
        return test_access::NodeLinkMgr_shared_links_(test_node::links(nodes[0])).at({"a", right, transport});
    }
};

asio::awaitable<void> verify(Cluster &f)
{
    for (int attempt = 0; attempt < 300 &&
         (f.topology(0).members().size() != 3 || f.topology(1).members().size() != 3 ||
          f.topology(2).members().size() != 3); ++attempt)
        co_await pause();
    require(f.topology(0).members().size() == 3 && f.topology(1).members().size() == 3 &&
                f.topology(2).members().size() == 3, "Label members did not join");
    for (const auto transport : {RelayProtocol::Tcp, RelayProtocol::Udp})
    {
        const auto ab = co_await test_node::ensure_link(f.nodes[0], "a", "b", transport);
        const auto ac = co_await test_node::ensure_link(f.nodes[0], "a", "c", transport);
        require(bool(ab) && bool(ac), "Label/public links did not become ready");
        const auto label_link = f.shared("b", transport);
        const auto public_link = f.shared("c", transport);
        const auto label_params = label_link->params.to_params();
        const auto public_params = public_link->params.to_params();
        require(label_params.at("left_address") == "127.0.0.1" &&
                    label_params.at("right_address") == "127.0.0.2" &&
                    public_params.at("left_address") == "localhost" &&
                    public_params.at("right_address") == "127.0.0.3",
                "One node did not publish different addresses to different peers");
        require(label_link->params.token != public_link->params.token && ab.id != ac.id,
                "Different node pairs shared credentials");
        const auto reverse = co_await test_node::ensure_link(f.nodes[0], "b", "a", transport);
        require(reverse.id == ab.id, "Reverse pair did not reuse its label link");
        std::vector<FlowResult> flows;
        for (int i = 0; i < 2; ++i)
        {
            auto flow = co_await test_node::open_flow(f.nodes[0], {"b", "a", "c"}, transport);
            require(bool(flow), "Mixed label/public Flow failed");
            flows.push_back(flow);
            require(co_await test_node::send_data(f.nodes[1], flow.epoch, flow.id, false, {1, 2, 3}),
                    "Forward mixed-path send failed");
            require(co_await test_node::receive_flow(f.nodes[2], flow.epoch, flow.id) == BytesBuf({1, 2, 3}),
                    "Forward mixed-path payload changed");
            require(co_await test_node::send_data(f.nodes[2], flow.epoch, flow.id, true, {4, 5, 6}),
                    "Reverse mixed-path send failed");
            require(co_await test_node::receive_flow(f.nodes[1], flow.epoch, flow.id) == BytesBuf({4, 5, 6}),
                    "Reverse mixed-path payload changed");
        }
        require(f.shared("b", transport)->params.id == ab.id && f.shared("c", transport)->params.id == ac.id,
                "Multiple Flows did not reuse both path links");
        for (const auto &flow : flows)
            co_await test_node::close_flow(f.nodes[0], flow.epoch, flow.id);
    }
    require(f.shared("b", RelayProtocol::Tcp)->params.token != f.shared("b", RelayProtocol::Udp)->params.token,
            "TCP and UDP shared credentials");

    // Invalid addresses must not install a Link at an endpoint.
    const auto old = f.shared("b", RelayProtocol::Tcp)->params;
    for (const auto *field : {"peer_address", "left_address", "right_address"})
    {
        auto prepare = old.to_params("b");
        const auto id = generate_random_id();
        prepare["id"] = id;
        prepare["source"] = "a";
        prepare[field] = "127.0.1.99";
        test_node::links(f.nodes[1]).handle(CtrlCommand::LinkPrepare, {CtrlCommand::LinkPrepare, prepare});
        require(!test_access::NodeLinkMgr_local_links_(test_node::links(f.nodes[1])).contains(id),
                "Forged selected address was accepted");
    }
    auto flow = co_await test_node::open_flow(f.nodes[0], {"b", "a", "c"}, RelayProtocol::Tcp);
    require(bool(flow), "Invalidation Flow failed");
    const auto unaffected = f.shared("c", RelayProtocol::Tcp)->params.id;
    njson members = njson::array();
    for (const auto &[id, address] : f.topology(0).members())
    {
        auto member = address.to_params();
        member["node_id"] = id;
        if (id == "b")
        {
            member["address"] = "127.0.0.2";
            member["label"] = "vpc-b";
        }
        members.push_back(std::move(member));
    }
    auto publish = [&](unsigned version) {
        for (std::size_t i = 0; i < f.nodes.size(); ++i)
        {
            f.topology(i).update_members({{"source", "a"}, {"master", "a"}, {"epoch", old.epoch},
                                         {"version", version}, {"members", members}});
            test_node::links(f.nodes[i]).members_changed();
        }
    };
    publish(100);
    for (int attempt = 0; attempt < 100 &&
         test_access::NodeLinkMgr_local_flows_(test_node::links(f.nodes[1])).contains(flow.id); ++attempt)
        co_await pause();
    require(!test_access::NodeLinkMgr_local_flows_(test_node::links(f.nodes[1])).contains(flow.id),
            "Invalidated link retained its Flow");
    auto rebuilt = co_await test_node::ensure_link(f.nodes[0], "a", "b", RelayProtocol::Tcp);
    require(bool(rebuilt) && rebuilt.id != old.id && f.shared("b", RelayProtocol::Tcp)->params.token != old.token &&
                f.shared("b", RelayProtocol::Tcp)->params.to_params().at("left_address") == "localhost" &&
                f.shared("c", RelayProtocol::Tcp)->params.id == unaffected,
            "Member change reused stale credentials or invalidated an unrelated pair");
    tcp::socket retired(co_await asio::this_coro::executor);
    co_await retired.async_connect({asio::ip::make_address_v4("127.0.0.2"), old.tcp_port}, asio::use_awaitable);
    const auto retired_attach = WireMessage::pack(CtrlMessage{CtrlCommand::LinkAttach,
        njson{{"id", old.id}, {"epoch", old.epoch}, {"node", "a"}, {"token", old.token},
         {"data_version", LnkFrameHeader::version}}});
    co_await asio::async_write(retired, asio::buffer(retired_attach), asio::use_awaitable);
    std::array<std::uint8_t, 4> response{};
    const auto [error, received] = co_await retired.async_read_some(
        asio::buffer(response), asio::cancel_after(1s, use_nothrow_awaitable));
    require(received == 0 && (error == asio::error::eof || error == asio::error::connection_reset),
            "Retired Link credentials were accepted or left pending");
    auto stale = old.to_params("b");
    stale["id"] = generate_random_id();
    stale["source"] = "a";
    test_node::links(f.nodes[1]).handle(CtrlCommand::LinkPrepare, {CtrlCommand::LinkPrepare, stale});
    require(!test_access::NodeLinkMgr_local_links_(test_node::links(f.nodes[1])).contains(
                stale.at("id").get<std::uint64_t>()),
            "Stale selected addresses were accepted");
    // An unused label IP change must also invalidate cached authorization.
    const auto before = f.shared("b", RelayProtocol::Tcp)->params;
    members[1]["label_address"] = "127.0.1.22";
    publish(101);
    rebuilt = co_await test_node::ensure_link(f.nodes[0], "a", "b", RelayProtocol::Tcp);
    require(bool(rebuilt) && rebuilt.id != before.id && f.shared("b", RelayProtocol::Tcp)->params.token != before.token,
            "Label metadata change retained a cached Link");
    // A label-only change switches back to the private route without touching the public address.
    const auto public_id = rebuilt.id;
    members[1]["label"] = "vpc-a";
    members[1]["label_address"] = "127.0.0.2";
    publish(102);
    rebuilt = co_await test_node::ensure_link(f.nodes[0], "a", "b", RelayProtocol::Tcp);
    require(bool(rebuilt) && rebuilt.id != public_id &&
                f.shared("b", RelayProtocol::Tcp)->params.to_params().at("left_address") == "127.0.0.1" &&
                f.shared("c", RelayProtocol::Tcp)->params.id == unaffected,
            "Label-only change did not switch routes or disturbed an unrelated pair");
}
} // namespace

int main()
{
    try
    {
        Cluster cluster;
        asio::co_spawn(cluster.control, verify(cluster), asio::use_future).get();
        std::cout << "[PASS] mixed label/public links, Flows, reuse and stale authorization\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
