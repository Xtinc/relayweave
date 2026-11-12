#include "frame_io.h"
#include "node_fixture.h"
#include <array>
#include <future>
#include <iostream>

TEST_MEMBER(RelayNode, control_acceptor_)
TEST_MEMBER(RelayNode, control_blacklist_)
TEST_MEMBER(RelayNode, tcp_pipeline_)
TEST_MEMBER(StreamPipeline, acceptor_)
TEST_MEMBER(StreamPipeline, blacklist_)
TEST_MEMBER(StreamPipeline, bind)
TEST_MEMBER(ClusterMgr, acceptor_)
TEST_MEMBER(ClusterMgr, blacklist_)
TEST_MEMBER(LnkChannel, acceptor_)
TEST_MEMBER(LnkChannel, blacklist_)
TEST_MEMBER(PeerBlacklist, entries_)

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
const auto loopback = asio::ip::address_v4::loopback();

void require(bool condition, const char *reason)
{
    if (!condition)
    {
        throw std::runtime_error(reason);
    }
}

asio::awaitable<void> pause(std::chrono::milliseconds duration)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(duration);
    co_await timer.async_wait(asio::use_awaitable);
}

struct State
{
    int failures = 0;
    int rounds = 0;
};

/** @brief Own a real four-executor Node and drain it before joining worker threads. */
struct Fixture
{
    asio::io_context control{1};
    asio::io_context transfer{1};
    asio::io_context datagram{1};
    asio::io_context data{1};
    asio::executor_work_guard<asio::io_context::executor_type> control_work{control.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> transfer_work{transfer.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> datagram_work{datagram.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> data_work{data.get_executor()};
    asio::ssl::context server_context{asio::ssl::context::tls};
    asio::ssl::context client_context{asio::ssl::context::tls};
    asio::ssl::context anonymous_context{asio::ssl::context::tls};
    std::shared_ptr<RelayNode> node;
    std::array<tcp::endpoint, 4> endpoints;
    std::vector<std::thread> workers;

    Fixture()
    {
        auto config = make_test_node_config();
        config.control.address = config.tcp.address = config.datagram.address = "127.0.0.1";
        config.channel.handshake_timeout = 500ms;
        config.channel.disconnect_timeout = 50ms;
        config.tcp.setup_timeout = 200ms;
        server_context.use_certificate_chain_file(config.certificate_chain.string());
        server_context.use_private_key_file(config.private_key.string(), asio::ssl::context::pem);
        const auto files = test_data::directory();
        server_context.load_verify_file((files / "tls_channel_test_client_ca.pem").string());
        client_context.load_verify_file(config.server_ca_file.string());
        client_context.use_certificate_chain_file((files / "tls_channel_test_client.pem").string());
        client_context.use_private_key_file((files / "tls_channel_test_client.key").string(), asio::ssl::context::pem);
        anonymous_context.load_verify_file(config.server_ca_file.string());
        node = std::make_shared<RelayNode>(control, transfer, datagram, data, server_context, config);
        node->start();
        endpoints = {test_access::RelayNode_control_acceptor_(*node).local_endpoint(),
                     test_access::StreamPipeline_acceptor_(pipeline()).local_endpoint(),
                     test_access::ClusterMgr_acceptor_(test_node::cluster(node)).local_endpoint(),
                     test_access::LnkChannel_acceptor_(links()).local_endpoint()};
        workers.emplace_back([this] { control.run(); });
        workers.emplace_back([this] { transfer.run(); });
        workers.emplace_back([this] { datagram.run(); });
        workers.emplace_back([this] { data.run(); });
    }

    ~Fixture()
    {
        node->stop();
        control_work.reset();
        transfer_work.reset();
        datagram_work.reset();
        data_work.reset();
        for (auto &worker : workers)
        {
            worker.join();
        }
    }

    StreamPipeline &pipeline()
    {
        return *test_access::RelayNode_tcp_pipeline_(*node);
    }

    LnkChannel &links()
    {
        return test_node::links(node).protocol().links();
    }

    asio::any_io_executor executor(int index)
    {
        if (index == 1)
        {
            return transfer.get_executor();
        }
        if (index == 3)
        {
            return data.get_executor();
        }
        return control.get_executor();
    }

    PeerBlacklist &blacklist(int index)
    {
        if (index == 0)
        {
            return test_access::RelayNode_control_blacklist_(*node);
        }
        if (index == 1)
        {
            return test_access::StreamPipeline_blacklist_(pipeline());
        }
        if (index == 2)
        {
            return test_access::ClusterMgr_blacklist_(test_node::cluster(node));
        }
        return test_access::LnkChannel_blacklist_(links());
    }

    asio::awaitable<State> state(int index, asio::ip::address_v4 source = loopback)
    {
        auto read = [this, index, source]() -> asio::awaitable<State> {
            const auto &entries = test_access::PeerBlacklist_entries_(blacklist(index));
            const auto found = entries.find(source.to_uint());
            if (found == entries.end())
            {
                co_return State{};
            }
            co_return State{found->second.failures, found->second.rounds};
        };
        co_return co_await asio::co_spawn(executor(index), read(), asio::use_awaitable);
    }

    asio::awaitable<void> expect(int index, int failures, int rounds, asio::ip::address_v4 source = loopback)
    {
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        for (;;)
        {
            const auto value = co_await state(index, source);
            if (value.failures == failures && value.rounds == rounds)
            {
                co_return;
            }
            require(std::chrono::steady_clock::now() < deadline, "Listener penalty did not match expected state");
            co_await pause(1ms);
        }
    }

    asio::awaitable<tcp::socket> connect(int index, asio::ip::address_v4 source = loopback)
    {
        tcp::socket socket(co_await asio::this_coro::executor);
        socket.open(tcp::v4());
        socket.bind({source, 0});
        co_await socket.async_connect(endpoints[index], asio::cancel_after(1s, asio::use_awaitable));
        co_return std::move(socket);
    }
};

template <class Stream> asio::awaitable<std::size_t> drain(Stream &stream)
{
    std::array<std::uint8_t, 256> buffer{};
    std::size_t total = 0;
    for (;;)
    {
        const auto [error, size] = co_await stream.async_read_some(
            asio::buffer(buffer), asio::cancel_after(1500ms, use_nothrow_awaitable));
        total += size;
        require(total <= 4096, "Rejected connection produced unexpected traffic");
        if (error)
        {
            require(error != asio::error::operation_aborted && error != asio::error::timed_out,
                    "Rejected connection did not close within the test deadline");
            co_return total;
        }
    }
}

asio::awaitable<void> unfinished_connections(Fixture &fixture)
{
    const auto source = asio::ip::make_address_v4("127.0.0.3");
    for (int index = 0; index < 4; ++index)
    {
        for (const bool reset : {false, true})
        {
            auto socket = co_await fixture.connect(index, source);
            if (index == 1)
            {
                RelayAttach::ConnectionId id{};
                co_await asio::async_read(socket, asio::buffer(id), asio::cancel_after(1s, asio::use_awaitable));
            }
            if (reset)
            {
                socket.set_option(asio::socket_base::linger(true, 0));
            }
            socket.close();
            co_await fixture.expect(index, 0, 0, source);
        }
    }
    for (int index = 0; index < 3; ++index)
    {
        auto socket = co_await fixture.connect(index, source);
        co_await drain(socket);
        co_await fixture.expect(index, 0, 0, source);
    }
    auto unknown = co_await fixture.connect(3, source);
    const auto frame = WireMessage::pack(CtrlMessage(CtrlCommand::LinkAttach, njson{{"id", 987654321U}}));
    co_await asio::async_write(unknown, asio::buffer(frame), asio::use_awaitable);
    co_await drain(unknown);
    co_await fixture.expect(3, 0, 0, source);
    for (int index = 0; index < 4; ++index)
    {
        co_await fixture.expect(index, 0, 0);
    }
}

asio::awaitable<void> authorized_data(Fixture &fixture)
{
    auto socket = co_await fixture.connect(1);
    auto install = [&fixture]() -> asio::awaitable<AttachInfo> {
        co_return fixture.pipeline().install_remote_pair(RelayAttach::Consumer);
    };
    const auto info = co_await asio::co_spawn(fixture.transfer, install(), asio::use_awaitable);
    RelayAttach request{RelayAttach::Consumer, info.uuid, info.ticket, RelayAttach::ConnectionId{}};
    co_await asio::async_read(socket, asio::buffer(*request.connection_id),
                              asio::cancel_after(1s, asio::use_awaitable));
    co_await asio::co_spawn(fixture.transfer, test_access::StreamPipeline_bind(fixture.pipeline(), request),
                            asio::use_awaitable);
    const std::array<std::uint8_t, 1> byte{42};
    co_await asio::async_write(socket, asio::buffer(byte), asio::use_awaitable);
    auto release = [&fixture, uuid = info.uuid]() -> asio::awaitable<void> {
        fixture.pipeline().close_remote_pair(uuid);
        co_return;
    };
    co_await asio::co_spawn(fixture.transfer, release(), asio::use_awaitable);
    co_await fixture.expect(1, 0, 0);
}

asio::awaitable<void> stale_link(Fixture &fixture)
{
    auto prepare = [&fixture]() -> asio::awaitable<void> {
        lnk::LinkSpec spec;
        spec.id = 41;
        spec.epoch = 7;
        spec.peer = "peer";
        spec.token = "test-token";
        fixture.links().prepare(std::move(spec));
        co_return;
    };
    co_await asio::co_spawn(fixture.data, prepare(), asio::use_awaitable);
    const auto source = asio::ip::make_address_v4("127.0.0.3");
    auto socket = co_await fixture.connect(3, source);
    const auto frame = WireMessage::pack(CtrlMessage(
        CtrlCommand::LinkAttach,
        njson{{"id", 41U}, {"epoch", 6U}, {"node", "peer"}, {"token", "test-token"},
              {"data_version", LnkFrameHeader::version}}));
    co_await asio::async_write(socket, asio::buffer(frame), asio::use_awaitable);
    co_await drain(socket);
    co_await fixture.expect(3, 0, 0, source);
}

asio::awaitable<void> violate(Fixture &fixture, int index, int attempt)
{
    auto socket = co_await fixture.connect(index);
    if (index == 1)
    {
        RelayAttach::ConnectionId id{};
        co_await asio::async_read(socket, asio::buffer(id), asio::cancel_after(1s, asio::use_awaitable));
        const std::array<std::uint8_t, 1> byte{42};
        co_await asio::async_write(socket, asio::buffer(byte), asio::use_awaitable);
    }
    else if (index == 3)
    {
        BytesBuf frame;
        if (attempt == 1)
        {
            frame = {0, 0, 0, 0};
        }
        else if (attempt == 2)
        {
            frame = {0, 0, 0, 1, 0xff};
        }
        else if (attempt == 3)
        {
            frame = WireMessage::pack(CtrlMessage(CtrlCommand::Ping));
        }
        else
        {
            frame = WireMessage::pack(CtrlMessage(
                CtrlCommand::LinkAttach,
                njson{{"id", 41U}, {"epoch", 7U}, {"node", "peer"},
                      {"token", attempt == 4 ? "bad-token" : "test-token"},
                      {"data_version", attempt == 5 ? 0 : LnkFrameHeader::version}}));
        }
        co_await asio::async_write(socket, asio::buffer(frame), asio::use_awaitable);
    }
    else
    {
        const std::string request = "GET / HTTP/1.0\r\n\r\n";
        co_await asio::async_write(socket, asio::buffer(request), asio::use_awaitable);
    }
    co_await drain(socket);
}

asio::awaitable<void> bad_certificate(Fixture &fixture, int index)
{
    TLSStream stream(co_await fixture.connect(index), index == 0 ? fixture.anonymous_context : fixture.client_context);
    configure_tls_client(stream, "127.0.0.1");
    const auto [error] = co_await stream.async_handshake(asio::ssl::stream_base::client,
                                                       asio::cancel_after(1s, use_nothrow_awaitable));
    require(error != asio::error::operation_aborted, "Bad-certificate handshake stalled");
    if (!error)
    {
        co_await drain(stream);
    }
}

asio::awaitable<void> scenario(Fixture &fixture, std::vector<tcp::socket> &pending)
{
    co_await unfinished_connections(fixture);
    co_await authorized_data(fixture);
    co_await stale_link(fixture);
    TLSStream existing(co_await fixture.connect(0), fixture.client_context);
    configure_tls_client(existing, "127.0.0.1");
    co_await existing.async_handshake(asio::ssl::stream_base::client, asio::cancel_after(1s, asio::use_awaitable));
    for (int index = 0; index < 4; ++index)
    {
        for (int attempt = 1; attempt <= 5; ++attempt)
        {
            if ((index == 0 || index == 2) && attempt == 5)
            {
                co_await bad_certificate(fixture, index);
            }
            else
            {
                co_await violate(fixture, index, attempt);
            }
            co_await fixture.expect(index, 0, 0);
        }
        auto admitted = co_await fixture.connect(index);
        if (index == 3)
        {
            const std::array<std::uint8_t, 4> invalid_frame{};
            co_await asio::async_write(admitted, asio::buffer(invalid_frame), asio::use_awaitable);
        }
        require(co_await drain(admitted) == (index == 1 ? sizeof(RelayAttach::ConnectionId) : 0),
                "Loopback connection did not follow normal authentication or authorization");
        co_await fixture.expect(index, 0, 0);
        for (int other = index + 1; other < 4; ++other)
        {
            co_await fixture.expect(other, 0, 0);
        }
    }
    const auto ping = WireMessage::pack(CtrlMessage(CtrlCommand::Ping));
    co_await asio::async_write(existing, asio::buffer(ping), asio::cancel_after(1s, asio::use_awaitable));
    require((co_await read_ctrl_frame(existing, std::chrono::steady_clock::now() + 1s)).type() == CtrlCommand::Pong,
            "Loopback failures interrupted an authenticated connection");
    existing.lowest_layer().close();

    TLSStream fresh(co_await fixture.connect(0), fixture.client_context);
    configure_tls_client(fresh, "127.0.0.1");
    co_await fresh.async_handshake(asio::ssl::stream_base::client, asio::cancel_after(1s, asio::use_awaitable));
    co_await asio::async_write(fresh, asio::buffer(ping), asio::cancel_after(1s, asio::use_awaitable));
    require((co_await read_ctrl_frame(fresh, std::chrono::steady_clock::now() + 1s)).type() == CtrlCommand::Pong,
            "Loopback failures prevented a new authenticated connection");
    fresh.lowest_layer().close();

    const auto other_source = asio::ip::make_address_v4("127.0.0.2");
    pending.push_back(co_await fixture.connect(3, other_source));
    pending.push_back(co_await fixture.connect(0, other_source));
    pending.push_back(co_await fixture.connect(1, other_source));
    RelayAttach::ConnectionId id{};
    co_await asio::async_read(pending.back(), asio::buffer(id), asio::cancel_after(1s, asio::use_awaitable));
    require((co_await fixture.state(1, other_source)).rounds == 0, "Loopback source was penalized");
}
} // namespace

int main()
{
    try
    {
        asio::io_context client;
        Fixture fixture;
        std::vector<tcp::socket> pending;
        auto result = asio::co_spawn(client, scenario(fixture, pending), asio::use_future);
        client.run();
        result.get();
        require(fixture.node->stop(), "Node did not drain authenticated or pending connections");
        for (const int index : {0, 1, 2, 3})
        {
            auto check = [&fixture, index]() -> asio::awaitable<void> {
                const auto &entries = test_access::PeerBlacklist_entries_(fixture.blacklist(index));
                require(entries.empty(), "Listener recorded a loopback failure or shutdown penalty");
                co_return;
            };
            asio::co_spawn(fixture.executor(index), check(), asio::use_future).get();
        }
        std::cout << "node_blacklist_test passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
