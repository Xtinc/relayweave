#include "node_fixture.h"
#include <future>
#include <iostream>
#include <type_traits>

TEST_MEMBER(RelayNode, topology_)
TEST_MEMBER(RelayNode, config_)
TEST_MEMBER(RelayNode, state_)
TEST_MEMBER(RelayNode, tcp_pipeline_)
TEST_MEMBER(RelayNode, tls_pipeline_)
TEST_MEMBER(RelayNode, datagram_mgr_)
TEST_MEMBER(Topology, state_)
TEST_MEMBER(Topology, drain_error_)
TEST_MEMBER(ClusterMgr, loop_done_)
TEST_MEMBER(ClusterMgr, running_)
TEST_MEMBER(AsyncEvent, notified_)
TEST_MEMBER(FlowProtocol, spawn)
TEST_MEMBER(FlowProtocol, stopping_)
TEST_MEMBER(FlowProtocol, tasks_)
TEST_MEMBER(NodeLinkMgr, events_running_)
TEST_MEMBER(NodeLinkMgr, flow_tasks_)
TEST_MEMBER(TcpPipeline, stopped_)
TEST_MEMBER(TlsPipeline, stopped_)
TEST_MEMBER(DatagramMgr, stopped_)

using namespace std::chrono_literals;

static void require_shutdown(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

struct ShutdownNode
{
    asio::io_context control{1};
    asio::io_context tcp{1};
    asio::io_context udp{1};
    asio::io_context data{1};
    asio::executor_work_guard<asio::io_context::executor_type> control_work{control.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> tcp_work{tcp.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> udp_work{udp.get_executor()};
    asio::executor_work_guard<asio::io_context::executor_type> data_work{data.get_executor()};
    asio::ssl::context ssl{asio::ssl::context::tls};
    std::shared_ptr<RelayNode> node;
    std::vector<std::thread> workers;

    ShutdownNode()
    {
        auto config = make_test_node_config();
        config.control.address = config.tcp.address = config.tls.address = config.datagram.address = "127.0.0.1";
        config.channel.disconnect_timeout = 100ms;
        ssl.use_certificate_chain_file(config.certificate_chain.string());
        ssl.use_private_key_file(config.private_key.string(), asio::ssl::context::pem);
        ssl.load_verify_file(config.server_ca_file.string());
        node = std::make_shared<RelayNode>(control, tcp, udp, data, ssl, config);
        workers.emplace_back([this] { control.run(); });
        workers.emplace_back([this] { tcp.run(); });
        workers.emplace_back([this] { udp.run(); });
        workers.emplace_back([this] { data.run(); });
    }

    void start(std::function<void(std::string)> failure_handler = {})
    {
        asio::post(control, asio::use_future([this, handler = std::move(failure_handler)]() mutable {
            node->start(std::move(handler));
        })).get();
    }

    ~ShutdownNode()
    {
        try
        {
            node->stop();
        }
        catch (...)
        {
            // Keep the executors alive until teardown even if an unexpected drain failure escapes.
        }
        control_work.reset();
        tcp_work.reset();
        udp_work.reset();
        data_work.reset();
        for (auto &worker : workers)
        {
            worker.join();
        }
    }
};

static asio::awaitable<void> hold_data_task(std::shared_ptr<AsyncEvent> release)
{
    co_await release->wait();
}

static asio::awaitable<void> fail_data_task()
{
    throw std::runtime_error("injected data task failure");
    co_return;
}

static void stop_drains_all_domains(bool fail_during_stop)
{
    ShutdownNode f;
    f.start();
    auto &protocol = test_node::links(f.node).protocol();
    auto release = std::make_shared<AsyncEvent>(f.data.get_executor());
    std::future<bool> stopped;
    ScopeGuard unblock([&] { asio::post(f.data, [release] { release->notify_all(); }); });

    auto inject = [&]() -> asio::awaitable<void> {
        test_access::FlowProtocol_spawn(protocol, hold_data_task(release));
        if (!fail_during_stop)
        {
            test_access::FlowProtocol_spawn(protocol, fail_data_task());
            asio::steady_timer wait(f.data);
            const auto deadline = std::chrono::steady_clock::now() + 2s;
            while (!test_access::FlowProtocol_stopping_(protocol))
            {
                require_shutdown(std::chrono::steady_clock::now() < deadline, "Data task failure did not stop the service");
                wait.expires_after(1ms);
                co_await wait.async_wait(asio::use_awaitable);
            }
        }
    };
    asio::co_spawn(f.data, inject(), asio::use_future).get();

    stopped = std::async(std::launch::async, [&] { return f.node->stop(); });
    require_shutdown(stopped.wait_for(50ms) == std::future_status::timeout,
                     "Stop returned before the outstanding data task drained");
    if (fail_during_stop)
    {
        asio::post(f.data, asio::use_future([&] {
            require_shutdown(test_access::FlowProtocol_stopping_(protocol), "Shutdown did not reach the data service");
            test_access::FlowProtocol_spawn(protocol, fail_data_task());
        })).get();
    }
    asio::post(f.data, [release] { release->notify_all(); });
    const bool stop_ok = stopped.get();
    unblock.dismiss();
    require_shutdown(stop_ok == !fail_during_stop && f.node->stop() == stop_ok,
                     "Repeated stop did not preserve its shutdown result");
    asio::post(f.control, asio::use_future([&] {
        using State = decltype(test_access::RelayNode_state_(*f.node).load());
        auto &links = test_node::links(f.node);
        auto &topology = *test_access::RelayNode_topology_(*f.node);
        using TopologyState = std::remove_reference_t<decltype(test_access::Topology_state_(topology))>;
        require_shutdown(test_access::RelayNode_state_(*f.node).load() == State::Stopped &&
                             test_access::Topology_state_(topology) == TopologyState::Stopped &&
                             !test_access::NodeLinkMgr_events_running_(links) &&
                             test_access::NodeLinkMgr_flow_tasks_(links) == 0 &&
                             !test_access::ClusterMgr_running_(test_node::cluster(f.node)) &&
                             test_access::AsyncEvent_notified_(test_access::ClusterMgr_loop_done_(test_node::cluster(f.node))),
                         "A shutdown error skipped topology or cluster draining");
    })).get();
    asio::post(f.tcp, asio::use_future([&] {
        require_shutdown(test_access::TcpPipeline_stopped_(*test_access::RelayNode_tcp_pipeline_(*f.node)) &&
                             test_access::TlsPipeline_stopped_(*test_access::RelayNode_tls_pipeline_(*f.node)),
                         "A shutdown error skipped TCP/TLS shutdown");
    })).get();
    asio::post(f.udp, asio::use_future([&] {
        require_shutdown(test_access::DatagramMgr_stopped_(*test_access::RelayNode_datagram_mgr_(*f.node)),
                         "A shutdown error skipped UDP shutdown");
    })).get();
    asio::post(f.data, asio::use_future([&] {
        require_shutdown(test_access::FlowProtocol_tasks_(protocol) == 0, "Protocol tasks survived stop completion");
    })).get();
}

static void drain_failure_preserves_stopping()
{
    ShutdownNode f;
    f.start();
    auto &topology = *test_access::RelayNode_topology_(*f.node);
    require_shutdown(asio::co_spawn(f.control, topology.close(), asio::use_future).get(), "Topology did not close");
    asio::post(f.control, asio::use_future([&] {
        test_access::Topology_drain_error_(topology) =
            std::make_exception_ptr(std::runtime_error("injected drain failure"));
    })).get();
    ScopeGuard clear_failure([&] {
        asio::post(f.control, asio::use_future([&] { test_access::Topology_drain_error_(topology) = {}; })).get();
    });
    bool failed = false;
    try
    {
        f.node->stop();
    }
    catch (const std::runtime_error &error)
    {
        failed = std::string_view(error.what()) == "injected drain failure";
    }
    using State = decltype(test_access::RelayNode_state_(*f.node).load());
    require_shutdown(failed && test_access::RelayNode_state_(*f.node).load() == State::Stopping,
                     "An incomplete drain was converted to a completed shutdown result");
    asio::post(f.control, asio::use_future([&] {
        require_shutdown(test_access::ClusterMgr_running_(test_node::cluster(f.node)),
                         "An incomplete drain closed its cluster dependency");
        test_access::Topology_drain_error_(topology) = {};
    })).get();
    clear_failure.dismiss();
    require_shutdown(f.node->stop(), "Stop could not resume after the drain failure was resolved");
}

static void concurrent_module_close()
{
    ShutdownNode f;
    f.start();
    auto &cluster = test_node::cluster(f.node);
    auto &topology = *test_access::RelayNode_topology_(*f.node);
    asio::cancellation_signal cancellation;
    auto close_cluster = [&]() -> asio::awaitable<void> {
        require_shutdown(co_await cluster.async_stop(), "Cluster stop failed");
        require_shutdown(test_access::AsyncEvent_notified_(test_access::ClusterMgr_loop_done_(cluster)),
                         "Repeated cluster stop returned before its loop finished");
    };
    auto close_topology = [&]() -> asio::awaitable<void> {
        require_shutdown(co_await topology.close(), "Topology close failed");
        using State = std::remove_reference_t<decltype(test_access::Topology_state_(topology))>;
        require_shutdown(test_access::Topology_state_(topology) == State::Stopped,
                         "Topology close returned before its task finished");
    };
    auto first = asio::co_spawn(f.control, close_cluster(),
                               asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    auto second = asio::co_spawn(f.control, close_cluster(), asio::use_future);
    auto topo_first = asio::co_spawn(f.control, close_topology(), asio::use_future);
    auto topo_second = asio::co_spawn(f.control, close_topology(), asio::use_future);
    asio::post(f.control, [&] { cancellation.emit(asio::cancellation_type::all); });
    first.get();
    second.get();
    topo_first.get();
    topo_second.get();
    asio::co_spawn(f.control, close_cluster(), asio::use_future).get();
    asio::co_spawn(f.control, close_topology(), asio::use_future).get();
}

static void failed_start_drains_published_tasks()
{
    ShutdownNode f;
    auto &topology = *test_access::RelayNode_topology_(*f.node);
    // Reject Topology startup after listeners and the cluster loop have already been published.
    require_shutdown(asio::co_spawn(f.control, topology.close(), asio::use_future).get(), "Topology did not close");
    bool failed = false;
    try
    {
        f.start();
    }
    catch (const std::logic_error &error)
    {
        failed = std::string_view(error.what()) == "Topology can only be started once";
    }
    using State = decltype(test_access::RelayNode_state_(*f.node).load());
    require_shutdown(failed && test_access::RelayNode_state_(*f.node).load() == State::Stopping,
                     "Partial startup was marked stopped before its tasks drained");
    require_shutdown(f.node->stop(), "Failed startup did not drain normally");
    require_shutdown(test_access::RelayNode_state_(*f.node).load() == State::Stopped,
                     "Failed startup did not complete shutdown");
    const auto &config = test_access::RelayNode_config_(*f.node);
    asio::ip::tcp::acceptor tcp(f.data, {asio::ip::address_v4::loopback(), config.cluster.tcp_port});
    asio::ip::udp::socket udp(f.data, {asio::ip::address_v4::loopback(), config.cluster.udp_port});
}

static void master_overflow_stops_node(bool already_stopping)
{
    std::promise<std::string> failure;
    auto failed = failure.get_future();
    unsigned notifications = 0;
    ShutdownNode f;
    f.start([&](std::string reason) {
        if (++notifications == 1)
        {
            failure.set_value(std::move(reason));
        }
    });
    asio::post(f.control, asio::use_future([&] {
        auto &cluster = test_node::cluster(f.node);
        const auto limit = test_access::RelayNode_config_(*f.node).channel.max_queued_messages;
        auto result = ClusterMgr::SendResult::Submitted;
        // Stay in one control handler so the consumer cannot drain between submissions.
        for (std::size_t i = 0; i < limit + 2 && result == ClusterMgr::SendResult::Submitted; ++i)
        {
            result = cluster.broadcast(CtrlMessage{"overflow.notice"});
        }
        require_shutdown(result == ClusterMgr::SendResult::QueueFull, "Overflow did not return QueueFull");
        require_shutdown(cluster.broadcast(CtrlMessage{"after.overflow"}) == ClusterMgr::SendResult::Stopped,
                         "Failed master continued accepting messages");
        if (already_stopping)
        {
            // The owner can enter stop() before the posted failure notification runs.
            using State = decltype(test_access::RelayNode_state_(*f.node).load());
            test_access::RelayNode_state_(*f.node).store(State::Stopping);
        }
    })).get();
    require_shutdown(failed.wait_for(2s) == std::future_status::ready, "Fatal error did not notify the Node owner");
    require_shutdown(failed.get() == "master control outbound queue full", "Fatal error lost its cause");
    require_shutdown(!f.node->stop() && !f.node->stop(), "Fatal shutdown did not preserve its failure result");
    asio::post(f.control, asio::use_future([&] {
        using State = decltype(test_access::RelayNode_state_(*f.node).load());
        require_shutdown(notifications == 1 && test_access::RelayNode_state_(*f.node).load() == State::Stopped &&
                             test_access::AsyncEvent_notified_(test_access::ClusterMgr_loop_done_(test_node::cluster(f.node))),
                         "Fatal shutdown did not drain once to Stopped");
    })).get();
}

int main()
{
    try
    {
        stop_drains_all_domains(false);
        stop_drains_all_domains(true);
        drain_failure_preserves_stopping();
        concurrent_module_close();
        failed_start_drains_published_tasks();
        master_overflow_stops_node(false);
        master_overflow_stops_node(true);
        std::cout << "[PASS] Shutdown results, runtime failures, concurrent/cancelled drain and retry\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
