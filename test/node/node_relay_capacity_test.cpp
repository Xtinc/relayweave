#include "member_access.h"
#include "node_test_config.h"
#include "node_session.h"
#include <array>
#include <future>
#include <iostream>

TEST_MEMBER(RelayNode, start_relay)
TEST_MEMBER(RelayNode, relay_sessions_)
TEST_MEMBER(RelayNode, stop_relays)
TEST_MEMBER(RelayNode, handle_relay_peer)
TEST_MEMBER(RelayNode, handle_relay)
TEST_MEMBER(RelayNode, handle_cluster_message)
TEST_MEMBER(NodeSession, stage_)
TEST_MEMBER(NodeSession, close_reason_)
TEST_MEMBER(SingleSession, endpoint_)
TEST_MEMBER(MultiSession, endpoint_)

using namespace std::chrono_literals;

static void require_capacity(bool value, const char *reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

/** @brief Reject malformed business fields before dispatch even when there is no matching session. */
static void verify_relay_entry_validation(RelayNode &node, const ControlSessionPtr &agent)
{
    const std::array<njson, 5> invalid_requests{
        njson{{"request_id", 7.0}}, njson{{"uuid", 0U}}, njson{{"uuid", 41.0}},
        njson{{"reason", false}}, njson{{"reason", nullptr}}};
    for (const auto command : {CtrlCommand::RelayCancel, CtrlCommand::RelayReject})
    {
        for (const auto &params : invalid_requests)
        {
            bool rejected = false;
            try
            {
                test_access::RelayNode_handle_relay(node, command, agent, CtrlMessage(command, params));
            }
            catch (const std::invalid_argument &)
            {
                rejected = true;
            }
            require_capacity(rejected, "Malformed cancellation depended on finding a matching session");
        }
        test_access::RelayNode_handle_relay(node, command, agent, CtrlMessage(command, njson::object()));
    }
    for (const auto *field : {"stage", "reason"})
    {
        njson params{{"epoch", 17U}, {"flow_id", 39U}, {"source", "peer"}};
        params[field] = false;
        bool rejected = false;
        try
        {
            test_access::RelayNode_handle_relay_peer(node, CtrlCommand::RelayPeerClose,
                                                     CtrlMessage(CtrlCommand::RelayPeerClose, params));
        }
        catch (const std::invalid_argument &)
        {
            rejected = true;
        }
        require_capacity(rejected, "Malformed peer diagnostics depended on finding a matching session");
    }
    require_capacity(test_access::RelayNode_relay_sessions_(node).empty(), "Invalid business messages created a session");
}

/** @brief Preserve authenticated ownership, either-key matching and cancellation defaults through the real entry. */
static void verify_cancellation_dispatch(RelayNode &node, const ControlSessionPtr &consumer,
                                          const ControlSessionPtr &producer, const ControlSessionPtr &unrelated)
{
    auto &relays = test_access::RelayNode_relay_sessions_(node);
    auto local = std::make_unique<SingleSession>(node, consumer, producer,
                                                RelayRequest{"events", RelayProtocol::Tcp, 7}, SRVTrafficPtr{});
    test_access::SingleSession_endpoint_(*local) = {{41, 21, 4444}, {41, 22, 4444}};
    auto ingress = std::make_unique<MultiSession>(
        node, consumer, RelayRequest{"events", RelayProtocol::Tcp, 8, {"test-master", "peer"}, 17}, true);
    test_access::MultiSession_endpoint_(*ingress) = {42, 23, 4444};
    auto egress = std::make_unique<MultiSession>(
        node, producer, RelayRequest{"events", RelayProtocol::Tcp, 0, {}, 17, 39, "peer"}, false);
    test_access::MultiSession_endpoint_(*egress) = {43, 24, 4444};
    relays.push_back(std::move(local));
    relays.push_back(std::move(ingress));
    relays.push_back(std::move(egress));
    test_access::RelayNode_handle_relay_peer(node, CtrlCommand::RelayPeerOpen,
        CtrlMessage(CtrlCommand::RelayPeerOpen, njson{{"epoch", 17U}, {"flow_id", 39U}, {"source", "peer"}}));
    require_capacity(relays.size() == 3 && !test_access::NodeSession_close_reason_(*relays[2]),
                     "Duplicate peer open parsed creation fields before matching its existing session");
    const auto deliver = [&](CtrlCommand command, const ControlSessionPtr &agent, njson params) {
        test_access::RelayNode_handle_relay(node, command, agent, CtrlMessage(command, std::move(params)));
    };
    deliver(CtrlCommand::RelayCancel, unrelated, {{"request_id", 7U}, {"uuid", 42U}});
    deliver(CtrlCommand::RelayReject, consumer, {{"request_id", 7U}});
    for (const auto &relay : relays)
    {
        require_capacity(!test_access::NodeSession_close_reason_(*relay),
                         "Unrelated Agent or wrong-role rejection cancelled a session");
    }
    deliver(CtrlCommand::RelayCancel, consumer, {{"request_id", 7U}, {"uuid", 42U}, {"reason", ""}});
    require_capacity(test_access::NodeSession_close_reason_(*relays[0]) == std::optional<std::string>("") &&
                         !test_access::NodeSession_close_reason_(*relays[1]),
                     "Either-key matching changed precedence or lost an empty cancellation cause");
    deliver(CtrlCommand::RelayCancel, consumer, {{"uuid", 41U}, {"reason", "later failure"}});
    require_capacity(test_access::NodeSession_close_reason_(*relays[0]) == std::optional<std::string>(""),
                     "A late cancellation replaced the first cause");
    deliver(CtrlCommand::RelayCancel, consumer, {{"request_id", 999U}, {"uuid", 42U}});
    require_capacity(test_access::NodeSession_close_reason_(*relays[1]) ==
                         std::optional<std::string>("relay cancelled"),
                     "UUID cancellation lost either-key matching or its default reason");
    deliver(CtrlCommand::RelayReject, consumer, {{"uuid", 43U}});
    require_capacity(!test_access::NodeSession_close_reason_(*relays[2]), "Wrong Agent rejected a peer endpoint");
    deliver(CtrlCommand::RelayReject, producer, {{"uuid", 43U}});
    require_capacity(test_access::NodeSession_close_reason_(*relays[2]) ==
                         std::optional<std::string>("producer rejected relay"),
                     "Producer rejection lost its default reason");
    relays.push_back(std::make_unique<MultiSession>(
        node, producer, RelayRequest{"events", RelayProtocol::Tcp, 0, {}, 17, 40, "peer"}, false));
    test_access::RelayNode_handle_relay_peer(node, CtrlCommand::RelayPeerClose,
        CtrlMessage(CtrlCommand::RelayPeerClose, njson{{"epoch", 17U}, {"flow_id", 40U}, {"source", "peer"}}));
    require_capacity(test_access::NodeSession_close_reason_(*relays.back()) ==
                         std::optional<std::string>("peer closed") &&
                         test_access::NodeSession_stage_(*relays.back()) == "control",
                     "Peer close lost its default diagnostics");
    relays.clear();
    relays.push_back(std::make_unique<SingleSession>(
        node, consumer, producer, RelayRequest{"zero-request", RelayProtocol::Tcp, 0}, SRVTrafficPtr{}));
    deliver(CtrlCommand::RelayCancel, consumer, {{"request_id", 0U}, {"reason", ""}});
    require_capacity(test_access::NodeSession_close_reason_(*relays.front()) == std::optional<std::string>(""),
                     "A zero-valued Single request ID stopped matching");
    relays.clear();
}

// Exercise instance events on control_io before any endpoint or business task is installed.
static void verify_session_events(RelayNode &node, const ControlSessionPtr &consumer,
                                  const ControlSessionPtr &producer, const ControlSessionPtr &unrelated)
{
    for (const auto *field : {"epoch", "flow_id"})
    {
        njson invalid{{"epoch", 17U}, {"flow_id", 39U}, {"source", "peer"}};
        invalid.erase(field);
        bool rejected = false;
        try
        {
            test_access::RelayNode_handle_relay_peer(node, CtrlCommand::RelayPeerOpen,
                                                     CtrlMessage(CtrlCommand::RelayPeerOpen, invalid));
        }
        catch (const std::invalid_argument &error)
        {
            rejected = error.what() == std::string(field) + " is required";
        }
        require_capacity(rejected, "Invalid peer identity lost its original error while constructing a reply");
    }

    const RelayRequest request{"events", RelayProtocol::Tcp, 7, {"test-master", "peer"}, 17};
    MultiSession ingress(node, consumer, request, true);
    SingleSession single(node, consumer, producer,
                       RelayRequest{"events", RelayProtocol::Tcp, 7},
                       std::make_shared<ServiceTraffic>());
    unsigned ingress_cancelled = 0;
    unsigned single_cancelled = 0;
    ingress.cancellation_slot().assign([&](asio::cancellation_type) { ++ingress_cancelled; });
    single.cancellation_slot().assign([&](asio::cancellation_type) { ++single_cancelled; });
    require_capacity(ingress.handle_open(consumer, request) &&
                         ingress.handle_open(consumer, request),
                     "Opening duplicate was not consumed before endpoint installation");
    require_capacity(!ingress.handle_open(unrelated, request) &&
                         !single.handle_open(consumer, request),
                     "Opening request crossed session ownership or changed Single creation semantics");
    require_capacity(ingress_cancelled == 0 && single_cancelled == 0,
                     "Opening duplicate cancelled a business instance");
    for (const auto *field : {"path", "epoch", "service", "protocol"})
    {
        auto conflict = request;
        if (std::string_view(field) == "path")
        {
            conflict.path = {"test-master", "other"};
        }
        else if (std::string_view(field) == "epoch")
        {
            conflict.epoch = 18;
        }
        else if (std::string_view(field) == "protocol")
        {
            conflict.protocol = RelayProtocol::Tls;
        }
        else
        {
            conflict.service = "other-service";
        }
        bool rejected = false;
        try
        {
            ingress.handle_open(consumer, conflict);
        }
        catch (const std::invalid_argument &error)
        {
            rejected = std::string_view(error.what()) == "relay request conflicts with existing request";
        }
        require_capacity(rejected && ingress_cancelled == 0, "Conflicting duplicate changed the existing instance");
    }
    test_access::MultiSession_endpoint_(ingress) = {42, 23, 4444};
    auto by_uuid = request;
    by_uuid.request_id = 999;
    by_uuid.uuid = 42;
    require_capacity(ingress.handle_open(consumer, by_uuid), "Open retry stopped matching by UUID");

    ingress.invalidate(17, {});
    ingress.control_disconnected(unrelated);
    single.invalidate(18, "cluster control failed");
    single.control_disconnected(unrelated);
    require_capacity(ingress_cancelled == 0 && single_cancelled == 0,
                     "Unrelated disconnect or unchanged epoch cancelled a healthy instance");
    ingress.control_disconnected(consumer);
    single.control_disconnected(producer);
    require_capacity(ingress_cancelled == 1 && single_cancelled == 1,
                     "Owning control disconnect did not cancel its instance");

    for (const bool force : {false, true})
    {
        MultiSession invalidated(node, consumer, request, true);
        unsigned cancelled = 0;
        invalidated.cancellation_slot().assign([&](asio::cancellation_type) { ++cancelled; });
        invalidated.invalidate(force ? 17 : 18, force ? "cluster control failed" : "");
        invalidated.invalidate(18, "later failure");
        require_capacity(cancelled == 1, "Epoch or cluster failure did not cancel exactly once");
    }

    auto peer_params = request;
    peer_params.flow_id = 39;
    peer_params.peer = "peer";
    MultiSession egress(node, producer, peer_params, false);
    unsigned egress_cancelled = 0;
    egress.cancellation_slot().assign([&](asio::cancellation_type) { ++egress_cancelled; });
    const PeerEvent identity{17, 39, "peer", "control", "peer failure"};
    for (const auto *field : {"epoch", "flow_id", "source"})
    {
        auto wrong = identity;
        if (std::string_view(field) == "source")
        {
            wrong.source = "other-peer";
        }
        else if (std::string_view(field) == "epoch")
        {
            --wrong.epoch;
        }
        else
        {
            --wrong.flow_id;
        }
        require_capacity(!egress.handle_peer(CtrlCommand::RelayPeerClose, wrong) &&
                             egress_cancelled == 0,
                         "Unmatched peer message cancelled an unrelated instance");
    }
    for (const auto command : {CtrlCommand::RelayPeerReady, CtrlCommand::RelayPeerFinished})
    {
        require_capacity(egress.handle_peer(command, identity) && egress.handle_peer(command, identity) &&
                             egress_cancelled == 0,
                         "Duplicate peer notification cancelled its instance");
    }
    auto close_identity = identity;
    close_identity.reason.clear();
    require_capacity(!single.handle_peer(CtrlCommand::RelayPeerClose, close_identity), "Single consumed a peer message");
    require_capacity(egress.handle_peer(CtrlCommand::RelayPeerClose, close_identity) && egress_cancelled == 1,
                     "Authenticated matching peer close did not cancel its instance");
    egress.cancel("late", "later failure");
    const auto &peer_reason = test_access::NodeSession_close_reason_(egress);
    require_capacity(peer_reason && peer_reason->empty() && egress_cancelled == 1,
                     "An empty peer close reason was lost or cancellation was repeated");
    require_capacity(egress.handle_peer(CtrlCommand::RelayPeerOpen, identity) &&
                         egress_cancelled == 1,
                     "Closing instance released its peer identity before draining");
}

// One cluster failure must retain the same cause across every affected session type.
static void verify_cluster_failure_reason(RelayNode &node, const ControlSessionPtr &agent)
{
    const std::array cases{
        std::pair{njson{{"reason", "specific cluster failure"}}, std::string("specific cluster failure")},
        std::pair{njson::object(), std::string("cluster control failed")},
        std::pair{njson{{"reason", ""}}, std::string("cluster control failed")}};
    auto &relays = test_access::RelayNode_relay_sessions_(node);
    for (const auto &[params, expected] : cases)
    {
        relays.push_back(std::make_unique<SingleSession>(
            node, agent, agent, RelayRequest{"local", RelayProtocol::Tcp, 1}, std::make_shared<ServiceTraffic>()));
        relays.push_back(std::make_unique<SingleSession>(
            node, agent, AgentPeer{"peer", 42, "producer"}, RelayRequest{"remote", RelayProtocol::Tcp, 2},
            std::make_shared<ServiceTraffic>()));
        for (const bool ingress : {true, false})
        {
            relays.push_back(std::make_unique<MultiSession>(
                node, agent, RelayRequest{"multi", RelayProtocol::Tcp, 3, {"test-master", "peer"}, 17, 39, "peer"},
                ingress));
        }
        test_access::RelayNode_handle_cluster_message(node, CtrlCommand::ClusterError,
                                                      CtrlMessage(CtrlCommand::ClusterError, params));
        test_access::RelayNode_handle_cluster_message(
            node, CtrlCommand::ClusterError, CtrlMessage(CtrlCommand::ClusterError, njson{{"reason", "later failure"}}));
        require_capacity(!test_access::NodeSession_close_reason_(*relays.front()),
                         "Cluster failure cancelled a relay with only local Agents");
        for (std::size_t i = 1; i < relays.size(); ++i)
        {
            const auto &reason = test_access::NodeSession_close_reason_(*relays[i]);
            require_capacity(reason && *reason == expected && test_access::NodeSession_stage_(*relays[i]) == "control",
                             "Cluster failure lost its original cause or a later failure replaced it");
        }
        relays.clear();
    }
}


// A stopped control lifetime must finish without starting either transfer executor.
static asio::awaitable<void> verify_cancelled_before_start(RelayNode &node, const ControlSessionPtr &agent)
{
    for (const std::string reason : {"first stop", ""})
    {
        for (unsigned mode = 0; mode < 3; ++mode)
        {
            auto traffic = std::make_shared<ServiceTraffic>();
            std::weak_ptr<ServiceTraffic> retained = traffic;
            std::unique_ptr<NodeSession> relay;
            if (mode == 0)
            {
                relay = std::make_unique<SingleSession>(node, agent, agent,
                                                      RelayRequest{"early-stop", RelayProtocol::Tcp, 9}, traffic);
            }
            else
            {
                relay = std::make_unique<MultiSession>(
                    node, agent, RelayRequest{"early-stop", RelayProtocol::Tcp, 9, {"test-master", "peer"}, 17, 39, "peer"},
                    mode == 1, traffic);
            }
            relay->cancel("control", reason);
            relay->cancel("late", "later failure");
            co_await relay->run();
            const auto &first_reason = test_access::NodeSession_close_reason_(*relay);
            require_capacity(test_access::NodeSession_stage_(*relay) == "control" &&
                                 first_reason && *first_reason == reason,
                             "Cleanup replaced the original cancellation cause");
            relay.reset();
            traffic.reset();
            require_capacity(retained.expired(), "An unstarted relay retained its accounting record");
        }
    }
}

int main()
{
    try
    {
        asio::io_context control(1);
        asio::io_context tcp_io(1);
        asio::io_context udp_io(1);
        asio::io_context data(1);
        asio::ssl::context ssl(asio::ssl::context::tls_server);
        auto config = make_test_node_config();
        config.control.address = config.tcp.address = config.datagram.address = "127.0.0.1";
        config.control.max_connections = 2;
        config.tcp.max_relays = 1;

        config.datagram.max_relays = 3;
        constexpr std::array cases{
            std::pair{RelayProtocol::Tcp, 1}, std::pair{RelayProtocol::Tls, 0}, std::pair{RelayProtocol::Udp, 3}};
        auto node = std::make_shared<RelayNode>(control, tcp_io, udp_io, data, ssl, config);
        auto session = std::make_shared<TLSChannel>(asio::ip::tcp::socket(control), ssl, TLSChannelRole::S);
        auto producer = std::make_shared<TLSChannel>(asio::ip::tcp::socket(control), ssl, TLSChannelRole::S);
        auto unrelated = std::make_shared<TLSChannel>(asio::ip::tcp::socket(control), ssl, TLSChannelRole::S);
        auto early_stop = asio::co_spawn(control, verify_cancelled_before_start(*node, session), asio::use_future);
        control.poll();
        require_capacity(early_stop.wait_for(0s) == std::future_status::ready,
                         "Pre-cancelled relay required a data task to finish");
        early_stop.get();

        auto make_relay = [&](RelayProtocol protocol) {
            return std::make_unique<SingleSession>(*node, session, session,
                RelayRequest{"capacity", protocol, 1},
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
        // TCP and TLS share one quota; UDP has its own. Do not pump either transfer executor:
        // all admitted requests must remain counted while install is pending.
        asio::post(control, [&] {
            verify_relay_entry_validation(*node, session);
            verify_cancellation_dispatch(*node, session, producer, unrelated);
            verify_session_events(*node, session, producer, unrelated);
            verify_cluster_failure_reason(*node, session);
            admit();
        });
        poll_control();
        require_capacity(test_access::RelayNode_relay_sessions_(*node).size() == 4, "shared stream or UDP quota was exceeded");
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
        require_capacity(test_access::RelayNode_relay_sessions_(*node).size() == 4, "released quotas were not reusable");
        drain();

        auto make_multi = [&](bool ingress) {
            return std::make_unique<MultiSession>(
                *node, session,
                RelayRequest{"capacity", RelayProtocol::Udp, 7, {"test-master", "peer"}, 17, 39, "peer"},
                ingress);
        };
        auto reject_multi = [&](bool ingress) {
            bool rejected = false;
            try
            {
                test_access::RelayNode_start_relay(*node, make_multi(ingress));
            }
            catch (const std::runtime_error &error)
            {
                rejected = std::string_view(error.what()) == "relay path capacity reached";
            }
            require_capacity(rejected, "Ingress/egress bypassed the shared multi Relay quota");
        };
        for (unsigned attempt = 0; attempt < 2; ++attempt)
        {
            asio::post(control, [&] {
                auto ingress = make_multi(true);
                auto egress = make_multi(false);
                auto *ingress_session = ingress.get();
                auto *egress_session = egress.get();
                test_access::RelayNode_start_relay(*node, std::move(ingress));
                test_access::RelayNode_start_relay(*node, std::move(egress));
                reject_multi(true);
                reject_multi(false);
                ingress_session->cancel("test", "cancelled");
                egress_session->cancel("test", "cancelled");
                reject_multi(true);
                reject_multi(false);
                test_access::RelayNode_start_relay(*node, make_relay(RelayProtocol::Tcp));
                require_capacity(test_access::RelayNode_relay_sessions_(*node).size() == 3,
                                 "Multi quota affected Single admission or released closing instances early");
            });
            poll_control();
            drain();
        }
        std::cout << "[PASS] Relay count includes pending, active and draining tasks per protocol\n";
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
