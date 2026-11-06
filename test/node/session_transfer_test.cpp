#include "node_session.h"
#include "datagram_mgr.h"
#include "pipeline_mgr.h"
#include "member_access.h"
#include <future>
#include <iostream>
#include <thread>

TEST_MEMBER(StreamPipeline, local_pairs_)
TEST_MEMBER(StreamPipeline, remote_pairs_)
TEST_MEMBER(DatagramMgr, local_pairs_)
TEST_MEMBER(DatagramMgr, remote_pairs_)
TEST_MEMBER(DatagramMgr, bindings_)
TEST_MEMBER(RelayIdAllocator, active_ids_)
TEST_MEMBER(RelayIdAllocator, mutex_)
TEST_MEMBER(SessionTransfer, cancellation_)

namespace
{
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using udp = asio::ip::udp;

void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

bool is_cancelled(const asio::system_error &error)
{
    return error.code() == asio::error::operation_aborted ||
           error.code() == asio::experimental::error::channel_cancelled;
}

struct Fixture
{
    asio::io_context control{1};
    asio::io_context transfer{1};
    asio::executor_work_guard<asio::io_context::executor_type> work{transfer.get_executor()};
    std::shared_ptr<RelayIdAllocator> ids = std::make_shared<RelayIdAllocator>();
    std::shared_ptr<ServiceTraffic> traffic = std::make_shared<ServiceTraffic>();
    std::shared_ptr<StreamPipeline> streams;
    std::shared_ptr<DatagramMgr> datagrams;
    std::uint16_t tcp_port;
    std::uint16_t udp_port;
    std::thread worker;

    Fixture()
    {
        tcp::acceptor reserve_tcp(control, {asio::ip::address_v4::loopback(), 0});
        udp::socket reserve_udp(control, {asio::ip::address_v4::loopback(), 0});
        tcp_port = reserve_tcp.local_endpoint().port();
        udp_port = reserve_udp.local_endpoint().port();
        reserve_tcp.close();
        reserve_udp.close();
        streams = std::make_shared<StreamPipeline>(transfer.get_executor(), ids, "127.0.0.1", tcp_port,
                                                    16, 16, 2s, TrafficLimitConfig{});
        datagrams = std::make_shared<DatagramMgr>(transfer.get_executor(), ids,
            udp::endpoint(asio::ip::address_v4::loopback(), udp_port), 16, TrafficLimitConfig{});
        streams->start();
        datagrams->start();
        worker = std::thread([this] { transfer.run(); });
    }

    ~Fixture()
    {
        asio::post(transfer, asio::use_future([this] {
            streams->stop();
            datagrams->stop();
        })).get();
        streams->wait_for_setup_tasks();
        work.reset();
        transfer.stop();
        worker.join();
    }

    asio::awaitable<void> empty()
    {
        co_await asio::co_spawn(transfer,
            [this]() -> asio::awaitable<void> {
                require(test_access::StreamPipeline_local_pairs_(*streams).empty() &&
                        test_access::StreamPipeline_remote_pairs_(*streams).empty(), "Stream pair survived stop");
                require(test_access::DatagramMgr_local_pairs_(*datagrams).empty() &&
                        test_access::DatagramMgr_remote_pairs_(*datagrams).empty() &&
                        test_access::DatagramMgr_bindings_(*datagrams).empty(), "UDP resources survived stop");
                std::lock_guard lock(test_access::RelayIdAllocator_mutex_(*ids));
                require(test_access::RelayIdAllocator_active_ids_(*ids).empty(), "Relay UUID survived stop");
                co_return;
            }, asio::use_awaitable);
        require(traffic->accessors().empty(), "Service accessor survived stop");
    }
};

using Scenario = std::function<asio::awaitable<void>(std::shared_ptr<SessionTransfer>, AttachPair)>;

asio::awaitable<void> run_case(Fixture &fixture, Scenario scenario, bool udp = false,
                              std::chrono::milliseconds timeout = 2s)
{
    SessionTransfer::Setup options{std::chrono::steady_clock::now() + timeout, fixture.traffic, "consumer"};
    RelayTransport &transport = udp ? static_cast<RelayTransport &>(*fixture.datagrams) : *fixture.streams;
    auto task = std::make_shared<SessionTransfer>(fixture.control.get_executor(), transport, std::move(options));
    task->start();
    std::exception_ptr failure;
    try
    {
        const auto attachment = co_await task->installed();
        co_await scenario(task, std::get<AttachPair>(attachment));
    }
    catch (...)
    {
        failure = std::current_exception();
    }
    co_await task->stop();
    co_await task->stop();
    co_await fixture.empty();
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}

asio::awaitable<tcp::socket> attach_stream(Fixture &fixture, const std::shared_ptr<SessionTransfer> &task,
                                          AttachInfo info, int role)
{
    tcp::socket socket(fixture.control);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), info.data_port}, asio::use_awaitable);
    RelayAttach request{role, info.uuid, info.ticket, RelayAttach::ConnectionId{}};
    co_await asio::async_read(socket, asio::buffer(*request.connection_id), asio::cancel_after(1s, asio::use_awaitable));
    task->attach(request);
    // Replays must never fill the authorization channel or leave extra bind tasks behind.
    for (int i = 0; i < 1000; ++i)
    {
        task->attach(request);
    }
    co_return std::move(socket);
}

asio::awaitable<void> exchange(tcp::socket &producer, tcp::socket &consumer)
{
    const std::string payload = "producer payload";
    co_await asio::async_write(producer, asio::buffer(payload), asio::use_awaitable);
    producer.shutdown(tcp::socket::shutdown_send);
    std::string received(payload.size(), '\0');
    co_await asio::async_read(consumer, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Data task corrupted forwarded bytes");
    std::array<char, 1> byte{};
    const auto [eof, size] = co_await consumer.async_read_some(asio::buffer(byte), use_nothrow_awaitable);
    require(eof == asio::error::eof && size == 0, "Producer FIN was not forwarded");
    co_await asio::async_write(consumer, asio::buffer(payload), asio::use_awaitable);
    consumer.shutdown(tcp::socket::shutdown_send);
    co_await asio::async_read(producer, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
    require(received == payload, "Reverse direction stopped after producer FIN");
    const auto [reverse_eof, reverse_size] = co_await producer.async_read_some(asio::buffer(byte), use_nothrow_awaitable);
    require(reverse_eof == asio::error::eof && reverse_size == 0, "Consumer FIN was not forwarded");
}

asio::awaitable<void> attach_datagram(udp::socket &socket, AttachInfo info, int role)
{
    auto frame = WireMessage::pack(RelayAttach::to_msg({role, info.uuid, info.ticket}));
    co_await socket.async_send_to(asio::buffer(frame), {asio::ip::address_v4::loopback(), info.data_port},
                                  asio::use_awaitable);
}

asio::awaitable<void> scenarios(Fixture &fixture)
{
    // Force cancellation before co_spawn enters run(): completion must still wake installed()/stop().
    {
        std::promise<void> paused;
        std::promise<void> resume;
        auto resumed = resume.get_future();
        asio::post(fixture.transfer, [&] {
            paused.set_value();
            resumed.wait();
        });
        paused.get_future().wait();
        ScopeGuard release([&] { resume.set_value(); });
        SessionTransfer::Setup options{std::chrono::steady_clock::now() + 2s, fixture.traffic, "consumer"};
        const auto owners = fixture.streams.use_count();
        auto task = std::make_shared<SessionTransfer>(fixture.control.get_executor(), *fixture.streams, std::move(options));
        require(fixture.streams.use_count() == owners, "Data task extended manager ownership");
        asio::post(fixture.transfer, [task] {
            test_access::SessionTransfer_cancellation_(*task).emit(asio::cancellation_type::all);
        });
        task->start();
        resume.set_value();
        release.dismiss();
        bool cancelled = false;
        try { co_await task->installed(); }
        catch (const asio::system_error &error) { cancelled = is_cancelled(error); }
        require(cancelled, "Pre-launch cancellation did not complete the data task");
        co_await task->stop();
        co_await fixture.empty();
    }

    // Stop may reach the transfer executor before installation has run or been observed by control.
    {
        SessionTransfer::Setup options{std::chrono::steady_clock::now() + 2s, fixture.traffic, "consumer"};
        auto task = std::make_shared<SessionTransfer>(fixture.control.get_executor(), *fixture.streams, std::move(options));
        task->start();
        co_await task->stop();
        co_await fixture.empty();
    }

    co_await run_case(fixture, [&](auto task, AttachPair pair) -> asio::awaitable<void> {
        auto producer = co_await attach_stream(fixture, task, pair.producer, RelayAttach::Producer);
        auto consumer = co_await attach_stream(fixture, task, pair.consumer, RelayAttach::Consumer);
        co_await task->prepared();
        require(fixture.traffic.use_count() == 2, "Prepared task retained accounting outside its pair");
        require(fixture.traffic->accessors().empty(), "Prepared data became active before activation");
        auto activation = task->activate();
        co_await asio::co_spawn(fixture.transfer, [&fixture]() -> asio::awaitable<void> {
            require(fixture.traffic->accessors().empty(), "Creating activation wait activated the pair");
            co_return;
        }, asio::use_awaitable);
        co_await std::move(activation);
        require(fixture.traffic->accessors().at("consumer") == 1, "Activation did not install accounting");

        auto forwarding = task->transfer();
        const std::string probe = "wait before forwarding";
        co_await asio::async_write(producer, asio::buffer(probe), asio::use_awaitable);
        std::string received(probe.size(), '\0');
        const auto [early_error, early_size] = co_await asio::async_read(consumer, asio::buffer(received),
            asio::cancel_after(30ms, use_nothrow_awaitable));
        require(early_error == asio::error::operation_aborted && early_size == 0,
                "Creating transfer wait started forwarding");
        auto round_trip = [&]() -> asio::awaitable<void> {
            co_await asio::async_read(consumer, asio::buffer(received), asio::cancel_after(1s, asio::use_awaitable));
            require(received == probe, "Deferred forwarding lost the buffered payload");
            co_await exchange(producer, consumer);
        };
        co_await await_transfers(std::move(forwarding), round_trip());
        // Normal completion must retain the pair until control finishes peer coordination and stops it.
        require(fixture.traffic->accessors().at("consumer") == 1, "Finished data closed before controller join");
    });

    co_await run_case(fixture, [](auto task, AttachPair pair) -> asio::awaitable<void> {
        RelayAttach request{RelayAttach::Producer, pair.producer.uuid, pair.producer.ticket,
                            RelayAttach::ConnectionId{}};
        task->attach(request);
        std::string failure;
        try { co_await task->prepared(); }
        catch (const std::exception &error) { failure = error.what(); }
        require(failure == "connection ID unknown or expired", "Binding failure lost its original diagnostic");
    });

    // One greeted connection can be claimed by only one role, even with both valid tickets.
    co_await run_case(fixture, [&](auto task, AttachPair pair) -> asio::awaitable<void> {
        tcp::socket socket(fixture.control);
        co_await socket.async_connect({asio::ip::address_v4::loopback(), pair.producer.data_port}, asio::use_awaitable);
        RelayAttach::ConnectionId id;
        co_await asio::async_read(socket, asio::buffer(id), asio::cancel_after(1s, asio::use_awaitable));
        task->attach({RelayAttach::Producer, pair.producer.uuid, pair.producer.ticket, id});
        task->attach({RelayAttach::Consumer, pair.consumer.uuid, pair.consumer.ticket, id});
        std::string failure;
        try { co_await task->prepared(); }
        catch (const std::invalid_argument &error) { failure = error.what(); }
        require(failure == "connection ID unknown or expired", "An already claimed socket was attached twice");
        std::array<char, 1> byte{};
        const auto [error, size] = co_await socket.async_read_some(asio::buffer(byte),
            asio::cancel_after(1s, use_nothrow_awaitable));
        require(error == asio::error::eof && size == 0, "Failed attachment retained the first claimed socket");
    });

    co_await run_case(fixture, [](auto task, AttachPair pair) -> asio::awaitable<void> {
        RelayAttach request{RelayAttach::Producer, pair.producer.uuid, pair.producer.ticket,
                            RelayAttach::ConnectionId{}};
        task->attach(request);
        (*request.connection_id)[0] = 1;
        bool rejected = false;
        try { task->attach(request); }
        catch (const std::invalid_argument &) { rejected = true; }
        require(rejected, "Conflicting role authorization was accepted");
        co_return;
    });

    // An abandoned controller must interrupt the request wait; pair.close alone cannot wake this wait.
    co_await run_case(fixture, [](auto, AttachPair) -> asio::awaitable<void> { co_return; });
    co_await run_case(fixture, [](auto, AttachPair) -> asio::awaitable<void> { co_return; }, true);

    for (bool udp : {false, true})
    {
        co_await run_case(fixture, [](auto task, AttachPair) -> asio::awaitable<void> {
            bool expired = false;
            try { co_await task->prepared(); }
            catch (const asio::system_error &error) {
                expired = is_cancelled(error);
            }
            require(expired, "Setup deadline did not interrupt the stage wait");
        }, udp, 60ms);
    }

    co_await run_case(fixture, [&](auto task, AttachPair pair) -> asio::awaitable<void> {
        auto producer = co_await attach_stream(fixture, task, pair.producer, RelayAttach::Producer);
        auto consumer = co_await attach_stream(fixture, task, pair.consumer, RelayAttach::Consumer);
        co_await task->prepared();
        // Match the session path: cancellation after Prepared must stop the next
        // co_await before its activation command, without another close-reason guard.
        asio::cancellation_signal cancellation;
        auto activate_cancelled = [&]() -> asio::awaitable<void> {
            cancellation.emit(asio::cancellation_type::all);
            co_await task->activate();
        };
        bool cancelled = false;
        try {
            co_await asio::co_spawn(fixture.control, activate_cancelled(),
                asio::bind_cancellation_slot(cancellation.slot(), asio::use_awaitable));
        }
        catch (const asio::system_error &error) { cancelled = is_cancelled(error); }
        require(cancelled, "Cancelled control coroutine entered activation");
        co_await asio::co_spawn(fixture.transfer, [&fixture]() -> asio::awaitable<void> {
            require(fixture.traffic->accessors().empty(), "Cancelled activation command reached the data task");
            co_return;
        }, asio::use_awaitable);
        co_await task->stop();
        require(fixture.traffic->accessors().empty(), "Cancelled prepared data was activated");
    });

    co_await run_case(fixture, [&](auto task, AttachPair pair) -> asio::awaitable<void> {
        auto producer = co_await attach_stream(fixture, task, pair.producer, RelayAttach::Producer);
        auto consumer = co_await attach_stream(fixture, task, pair.consumer, RelayAttach::Consumer);
        co_await task->prepared();
        co_await task->activate();
        bool cancelled = false;
        try {
            co_await asio::co_spawn(fixture.control, task->transfer(), asio::cancel_after(30ms, asio::use_awaitable));
        }
        catch (const asio::system_error &error) { cancelled = is_cancelled(error); }
        require(cancelled, "Control cancellation did not interrupt the transfer stage wait");
        co_await task->stop();
        std::array<char, 1> byte{};
        const auto [error, _] = co_await producer.async_read_some(asio::buffer(byte),
            asio::cancel_after(1s, use_nothrow_awaitable));
        require(error == asio::error::eof, "Stopping active data did not close the Agent stream");
    });

    co_await run_case(fixture, [&](auto task, AttachPair pair) -> asio::awaitable<void> {
        udp::socket producer(fixture.control, {asio::ip::address_v4::loopback(), 0});
        udp::socket consumer(fixture.control, {asio::ip::address_v4::loopback(), 0});
        co_await attach_datagram(producer, pair.producer, RelayAttach::Producer);
        co_await attach_datagram(consumer, pair.consumer, RelayAttach::Consumer);
        co_await task->prepared();
        require(fixture.traffic.use_count() == 2, "Prepared UDP task retained accounting outside its pair");
        const std::string payload = "udp payload";
        const auto header = DatagramHeader::encode(pair.producer.session_id);
        BytesBuf packet(header.begin(), header.end());
        packet.insert(packet.end(), payload.begin(), payload.end());
        const udp::endpoint node(asio::ip::address_v4::loopback(), pair.producer.data_port);
        co_await producer.async_send_to(asio::buffer(packet), node, asio::use_awaitable);
        std::array<std::uint8_t, 64> received{};
        auto [early_error, early_size] = co_await consumer.async_receive(asio::buffer(received),
            asio::cancel_after(30ms, use_nothrow_awaitable));
        require(early_error == asio::error::operation_aborted && early_size == 0,
                "UDP forwarded data before activation");
        co_await task->activate();
        co_await producer.async_send_to(asio::buffer(packet), node, asio::use_awaitable);
        const auto size = co_await consumer.async_receive(asio::buffer(received),
            asio::cancel_after(1s, asio::use_awaitable));
        require(size == DatagramHeader::length + payload.size() &&
                DatagramHeader::decode(std::span(received).first(DatagramHeader::length)) == pair.consumer.session_id &&
                std::string(received.begin() + DatagramHeader::length, received.begin() + size) == payload,
                "Activated UDP data was not routed to the consumer");
        bool cancelled = false;
        try {
            co_await asio::co_spawn(fixture.control, task->transfer(), asio::cancel_after(30ms, asio::use_awaitable));
        }
        catch (const asio::system_error &error) { cancelled = is_cancelled(error); }
        require(cancelled, "UDP control wait did not cancel");
    }, true);

    // Installation failure must complete through the same standard co_spawn handler as later failures.
    co_await asio::co_spawn(fixture.transfer, [&fixture]() -> asio::awaitable<void> {
        fixture.streams->stop();
        co_return;
    }, asio::use_awaitable);
    SessionTransfer::Setup options{std::chrono::steady_clock::now() + 2s, fixture.traffic, "consumer"};
    auto failed = std::make_shared<SessionTransfer>(fixture.control.get_executor(), *fixture.streams, std::move(options));
    failed->start();
    std::string failure;
    try { co_await failed->installed(); }
    catch (const std::runtime_error &error) { failure = error.what(); }
    require(failure == "server stopping", "Installation failure lost its original diagnostic");
    co_await failed->stop();
    co_await fixture.empty();
}
} // namespace

int main()
{
    try
    {
        Fixture fixture;
        auto result = asio::co_spawn(fixture.control, scenarios(fixture), asio::use_future);
        fixture.control.run();
        result.get();
        std::cout << "Session transfer tests passed\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
