#include "message.h"
#include "xfr_channel.h"

#include <chrono>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>

using DecodeLengthFunction = decltype(&WireMessage::decode_length);
static_assert(std::is_invocable_r_v<std::uint32_t, DecodeLengthFunction, const WireMessage::Header &>);
static_assert(!std::is_invocable_v<DecodeLengthFunction,
                                   const std::array<std::uint8_t, WireMessage::header_length - 1> &>);

namespace
{
void require(bool condition, const std::string &message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

void require_throws(const std::function<void()> &operation, const std::string &message)
{
    try
    {
        operation();
    }
    catch (const std::exception &)
    {
        return;
    }
    throw std::runtime_error(message);
}

CtrlMessage unpack_message(const BytesBuf &frame)
{
    require(frame.size() >= WireMessage::header_length, "Packed frame is missing its length header");
    const auto length =
        WireMessage::decode_length(std::span<const std::uint8_t>(frame).first<WireMessage::header_length>());
    require(frame.size() == WireMessage::header_length + length, "Packed frame length does not match its payload");
    return CtrlMessage::deserialize(std::span<const std::uint8_t>(frame.data() + WireMessage::header_length, length));
}

void test_control_commands()
{
    const CtrlMessage known{CtrlCommand::RelayOpen, njson{{"request_id", 42}}};
    require(known.command == "relay.open", "Control command enum has the wrong wire name");
    require(known.type() == CtrlCommand::RelayOpen, "Known control command was not parsed");
    require(known.params && known.params->at("request_id") == 42, "Control message params were changed");

    for (const auto &[command, name] : std::initializer_list<std::pair<CtrlCommand, std::string_view>>{
             {CtrlCommand::TopologyMembers, "topology.members"},
             {CtrlCommand::TopologyReport, "topology.report"},
             {CtrlCommand::TopologyQuery, "topology.query"},
             {CtrlCommand::TopologySnapshot, "topology.snapshot"}})
    {
        const CtrlMessage topology{command};
        require(topology.command == name && topology.type() == command, "Topology command mapping is inconsistent");
    }

    const CtrlMessage custom{"app.custom", njson{{"value", true}}};
    require(custom.type() == CtrlCommand::Unknown, "Custom control command was treated as a built-in command");
    require_throws([]() { static_cast<void>(CtrlMessage{CtrlCommand::Unknown}); },
                   "Unknown control command was serialized");
}

void test_common_json_validation()
{
    require(config::optional_string(njson::object(), "reason", "fallback") == "fallback",
            "Missing optional string did not use its fallback");
    require(config::optional_string(njson{{"reason", "closed"}}, "reason", "fallback") == "closed",
            "Optional string was not parsed");
    require_throws([]() { config::optional_string(njson{{"reason", 42}}, "reason", "fallback"); },
                   "Non-string optional field was accepted");

    initialize_logger_config(njson{{"log", {{"debug_enable", true}}}});
    require(proxy_is_debug_enabled(), "Logger debug flag was not enabled");
    initialize_logger_config(njson::object());
    require(!proxy_is_debug_enabled(), "Missing log config did not restore the default debug flag");
    require_throws([]() { initialize_logger_config(njson{{"log", {{"debug_enable", "yes"}}}}); },
                   "Non-boolean logger debug flag was accepted");
    require_throws([]() { initialize_logger_config(njson{{"log", {{"unknown", true}}}}); },
                   "Unknown logger field was accepted");
}

void test_round_trip(int role)
{
    constexpr uint64_t uuid = 42;
    constexpr uint64_t ticket = 0xfedcba9876543210ULL;
    const auto message = RelayAttach::to_msg({role, uuid, ticket});
    require(message.command == "relay.attach", "Unexpected relay attach command");
    require(message.params.has_value(), "Relay attach params are missing");
    require(message.params->at("role") == role, "Unexpected serialized relay role");
    require(message.params->at("uuid").is_number_unsigned(), "Relay UUID must be an unsigned integer");
    require(message.params->at("ticket").is_number_unsigned(), "Relay ticket must be an unsigned integer");

    const auto frame = WireMessage::pack(message);
    require(message.command == "relay.attach" && message.params.has_value(),
            "Packing an lvalue modified the original control message");
    const auto parsed = RelayAttach::from_msg(unpack_message(frame));
    require(parsed.role == role, "Relay role did not survive the framed CBOR round trip");
    require(parsed.uuid == uuid, "Relay UUID did not survive the framed CBOR round trip");
    require(parsed.ticket == ticket, "Relay ticket did not survive the framed CBOR round trip");
}

void test_attach_validation()
{
    constexpr uint64_t uuid = 42;
    constexpr uint64_t ticket = 99;
    const auto valid_params = njson{{"role", RelayAttach::Producer}, {"uuid", uuid}, {"ticket", ticket}};

    require_throws([&]() { RelayAttach::to_msg({99, uuid, ticket}); }, "Invalid relay role was accepted");
    require_throws([&]() { RelayAttach::to_msg({RelayAttach::Producer, 0, ticket}); },
                   "Zero relay UUID was accepted");
    require_throws([&]() { RelayAttach::to_msg({RelayAttach::Producer, uuid, 0}); },
                   "Zero ticket was accepted");
    require_throws([&]() { RelayAttach::from_msg(CtrlMessage{"relay.open", valid_params}); },
                   "Unexpected command was accepted as relay.attach");
    require_throws([&]() { RelayAttach::from_msg(CtrlMessage{"relay.attach"}); }, "Missing params were accepted");
    require_throws([&]() { RelayAttach::from_msg(CtrlMessage{"relay.attach", njson::array()}); },
                   "Non-object params were accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(CtrlMessage{"relay.attach", njson{{"uuid", uuid}, {"ticket", ticket}}});
        },
        "Missing role was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach", njson{{"role", "producer"}, {"uuid", uuid}, {"ticket", ticket}}});
        },
        "Non-integer role was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach", njson{{"role", 99}, {"uuid", uuid}, {"ticket", ticket}}});
        },
        "Unknown role was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach", njson{{"role", RelayAttach::Producer}, {"ticket", ticket}}});
        },
        "Missing relay UUID was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach",
                            njson{{"role", RelayAttach::Producer}, {"uuid", "not-a-number"}, {"ticket", ticket}}});
        },
        "Non-numeric relay UUID was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(CtrlMessage{
                "relay.attach", njson{{"role", RelayAttach::Producer}, {"uuid", -1}, {"ticket", ticket}}});
        },
        "Negative relay UUID was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(CtrlMessage{
                "relay.attach", njson{{"role", RelayAttach::Producer}, {"uuid", uint64_t{0}}, {"ticket", ticket}}});
        },
        "Zero relay UUID was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach", njson{{"role", RelayAttach::Producer}, {"uuid", uuid}}});
        },
        "Missing ticket was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(CtrlMessage{
                "relay.attach",
                njson{{"role", RelayAttach::Producer}, {"uuid", uuid}, {"ticket", "not-a-number"}}});
        },
        "Non-numeric ticket was accepted");
    require_throws(
        [&]() {
            RelayAttach::from_msg(
                CtrlMessage{"relay.attach",
                            njson{{"role", RelayAttach::Producer}, {"uuid", uuid}, {"ticket", uint64_t{0}}}});
        },
        "Zero ticket was accepted");

    auto extended_params = valid_params;
    extended_params["future_field"] = true;
    const auto extended = RelayAttach::from_msg(CtrlMessage{"relay.attach", std::move(extended_params)});
    require(extended.role == RelayAttach::Producer && extended.uuid == uuid && extended.ticket == ticket,
            "Additional relay.attach params should be ignored");
}

void test_frame_validation()
{
    const BytesBuf expected_ping_frame{0x00, 0x00, 0x00, 0x0e, 0xa1, 0x67, 0x63, 0x6f, 0x6d,
                                       0x6d, 0x61, 0x6e, 0x64, 0x64, 0x70, 0x69, 0x6e, 0x67};
    require(WireMessage::pack(CtrlMessage{CtrlCommand::Ping}) == expected_ping_frame,
            "The packed ping frame changed its wire representation");

    const CtrlMessage nested_message{
        "test.message", njson{{"nested", njson{{"enabled", true}, {"values", njson::array({1, 2, 3})}}}}};
    const auto nested_frame = WireMessage::pack(nested_message);
    const auto nested_round_trip = unpack_message(nested_frame);
    require(nested_round_trip.command == nested_message.command && nested_round_trip.params == nested_message.params,
            "Nested control message did not survive a framed CBOR round trip");

    WireMessage::Header empty_length{};
    require_throws([&]() { WireMessage::decode_length(empty_length); }, "Zero payload length was accepted");

    WireMessage::Header oversized_length{0, 1, 0, 1};
    require_throws([&]() { WireMessage::decode_length(oversized_length); }, "Oversized payload length was accepted");

    require_throws([]() { WireMessage::pack(CtrlMessage{"invalid-command"}); },
                   "Invalid control command was accepted");
    require_throws([]() { WireMessage::pack(CtrlMessage{"test.message", njson::array()}); },
                   "Non-object control params were accepted");
    require_throws(
        []() {
            WireMessage::pack(
                CtrlMessage{"test.message", njson{{"value", std::string(WireMessage::max_message_length, 'x')}}});
        },
        "Control message larger than 16 MiB was accepted");

    njson services = njson::array();
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    for (std::size_t index = 0; index < 256; ++index)
    {
        services.push_back(njson{{"service", std::string(64, static_cast<char>('a' + index % 26))},
                                 {"protocol", index % 2 == 0 ? "tcp" : "udp"},
                                 {"rx_bytes", maximum},
                                 {"tx_bytes", maximum},
                                 {"rx_bytes_per_second", maximum},
                                 {"tx_bytes_per_second", maximum}});
    }
    const auto maximum_traffic_response = WireMessage::pack(
        CtrlMessage{"server.traffic.reported", njson{{"request_id", maximum}, {"services", std::move(services)}}});
    const auto maximum_traffic_payload_length = WireMessage::decode_length(
        std::span<const std::uint8_t>(maximum_traffic_response).first<WireMessage::header_length>());
    require(maximum_traffic_payload_length > 32 * 1024,
            "Maximum traffic response did not exercise the expanded frame limit");
    require(maximum_traffic_payload_length <= WireMessage::max_payload_length,
            "Maximum traffic response does not fit in a 64 KiB frame");
    require(maximum_traffic_response.size() == WireMessage::header_length + maximum_traffic_payload_length,
            "Maximum traffic response has an invalid frame length");

    require_throws([&]() { CtrlMessage::deserialize(BytesBuf{0xff}); }, "Malformed CBOR was accepted");
}

std::vector<BytesBuf> frame_payloads(const BytesBuf &frames)
{
    std::vector<BytesBuf> result;
    for (std::size_t offset = 0; offset < frames.size();)
    {
        const auto remaining = std::span<const std::uint8_t>(frames).subspan(offset);
        require(remaining.size() >= WireMessage::header_length, "Truncated fragment header");
        const auto length = WireMessage::decode_length(remaining.first<WireMessage::header_length>());
        require(length <= remaining.size() - WireMessage::header_length, "Truncated fragment body");
        const auto payload = remaining.subspan(WireMessage::header_length, length);
        result.emplace_back(payload.begin(), payload.end());
        offset += WireMessage::header_length + length;
    }
    return result;
}

void test_message_fragmentation()
{
    // Exact encoded boundaries, including the CBOR string length header.
    for (const auto size : {WireMessage::max_payload_length - 1, WireMessage::max_payload_length,
                            WireMessage::max_payload_length + 1, 2 * WireMessage::fragment_payload_length, std::size_t(200000),
                            WireMessage::max_message_length})
    {
        njson params{{"value", std::string(size - 64, 'x')}};
        for (int attempt = 0; attempt < 2; ++attempt)
        {
            const auto encoded = njson::to_cbor(njson{{"command", "test.message"}, {"params", params}}).size();
            auto &value = params["value"].get_ref<std::string &>();
            value.resize(value.size() + size - encoded, 'x');
        }
        const CtrlMessage message{"test.message", params};
        const auto pages = frame_payloads(WireMessage::pack(message));
        require((pages.size() == 1) == (size <= WireMessage::max_payload_length), "Wrong fragmentation boundary");
        MessageReceiver receiver;
        for (std::size_t index = 0; index < pages.size(); ++index)
        {
            const auto complete = receiver.receive(pages[index]);
            require(complete.has_value() == (index + 1 == pages.size()), "A partial message was delivered");
            if (complete)
                require(complete->command == message.command && complete->params == params, "Large round trip changed data");
        }
        if (size == WireMessage::max_message_length)
        {
            MessageReceiver oversized;
            for (std::size_t index = 0; index + 1 < pages.size(); ++index)
                require(!oversized.receive(pages[index]), "Non-final page completed an oversized message");
            auto final = njson::from_cbor(pages.back());
            final["__fragment"][2].get_binary().push_back('x');
            require(!oversized.receive(njson::to_cbor(final)), "Final page exceeded the total message limit");
        }
    }
    const CtrlMessage message{"test.message", njson{{"value", std::string(200000, 'x')}}};
    const auto pages = frame_payloads(WireMessage::pack(message));
    require(pages.size() >= 3, "Missing-page test needs multiple fragments");
    MessageReceiver missing;
    require(!missing.receive(pages[0]), "First fragment was delivered");
    require(!missing.receive(pages.back()), "Final fragment with missing pages was delivered");
    const auto ping = frame_payloads(WireMessage::pack(CtrlMessage{CtrlCommand::Ping})).front();
    require(missing.receive(ping)->type() == CtrlCommand::Ping, "Discard broke the connection");
    std::optional<CtrlMessage> recovered;
    for (const auto &page : pages)
        recovered = missing.receive(page);
    require(recovered && recovered->params == message.params, "Discard prevented the next message");

    MessageReceiver final_only;
    require(!final_only.receive(pages.back()), "Final page alone completed a message");

    MessageReceiver interrupted;
    interrupted.receive(pages[0]);
    for (const auto &page : pages)
        recovered = interrupted.receive(page);
    require(recovered && recovered->params == message.params, "New batch did not replace an incomplete message");

    MessageReceiver inconsistent;
    inconsistent.receive(pages[0]);
    auto changed = njson::from_cbor(pages[1]);
    changed["__fragment"][1] = pages.size() + 1;
    require(!inconsistent.receive(njson::to_cbor(changed)), "Inconsistent page count was accepted");
    require(!inconsistent.receive(pages.back()), "Invalid batch resumed");

    MessageReceiver empty_final;
    for (std::size_t index = 0; index + 1 < pages.size(); ++index)
        empty_final.receive(pages[index]);
    auto final = njson::from_cbor(pages.back());
    final["__fragment"][2] = njson::binary(BytesBuf{});
    require(!empty_final.receive(njson::to_cbor(final)), "Empty final page was accepted");

    for (const auto &invalid : {njson::array({0u, 0u, njson::binary(BytesBuf{1})}),
                               njson::array({0u, 100000u, njson::binary(BytesBuf{1})}),
                               njson::array({0u, 2u, njson::binary(BytesBuf{1})}),
                               njson::array({2u, 2u, njson::binary(BytesBuf{1})})})
        require(!missing.receive(njson::to_cbor(njson{{"__fragment", invalid}})), "Invalid fragment was accepted");
    require_throws([&] { missing.receive(njson::to_cbor(njson{{"__fragment", "invalid"}})); },
                   "Malformed fragment envelope was accepted");
    require_throws([&] { missing.receive(njson::to_cbor(njson::array())); },
                   "Non-object control payload was accepted");

    // Reusing the parsed JSON must retain the same command/params validation.
    for (const auto &invalid : {njson::object(), njson{{"command", 1u}}, njson{{"command", "invalid-command"}},
                               njson{{"command", "ping"}, {"params", njson::array()}}})
    {
        const auto payload = njson::to_cbor(invalid);
        require_throws([&] { missing.receive(payload); }, "Plain message bypassed validation");
        require_throws([&] { CtrlMessage::deserialize(payload); }, "Direct message bypassed validation");
    }

    MessageReceiver malformed;
    auto first = njson::from_cbor(pages[0]);
    first["__fragment"][2].get_binary()[0] = 0xff;
    malformed.receive(njson::to_cbor(first));
    for (std::size_t index = 1; index + 1 < pages.size(); ++index)
        malformed.receive(pages[index]);
    require_throws([&] { malformed.receive(pages.back()); }, "Malformed assembled CBOR was accepted");
    for (const auto &page : pages)
        recovered = malformed.receive(page);
    require(recovered && recovered->params == message.params, "Decode failure left assembly state active");
}

void test_udp_session_header()
{
    constexpr std::uint64_t session_id = 0x0102030405060708ULL;
    const DatagramHeader::Buffer expected{1, 2, 3, 4, 5, 6, 7, 8};
    const auto encoded = DatagramHeader::encode(session_id);
    require(encoded == expected, "UDP session ID was not encoded in network byte order");
    const auto decoded = DatagramHeader::decode(encoded);
    require(decoded && *decoded == session_id, "UDP session ID did not survive a header round trip");

    DatagramHeader::Buffer zero{};
    require(!DatagramHeader::decode(zero), "Zero UDP session ID was accepted");
    require(!DatagramHeader::decode(std::span(encoded).first(7)), "Short UDP session header was accepted");
    require_throws([]() { DatagramHeader::encode(0); }, "Zero UDP session ID was encoded");
    require(DatagramHeader::maximum_user_payload + DatagramHeader::length ==
                DatagramHeader::maximum_wire_payload,
            "UDP payload limit does not reserve room for the session header");
}

void test_protocol_and_udp_limit()
{
    require(config::message_protocol(njson{{"protocol", "tcp"}}) == RelayProtocol::Tcp,
            "TCP protocol was not parsed");
    require(config::message_protocol(njson{{"protocol", "tls"}}) == RelayProtocol::Tls,
            "TLS protocol was not parsed");
    require(relay_protocol_name(RelayProtocol::Tls) == "tls", "TLS protocol name was not serialized");
    require(config::message_protocol(njson{{"protocol", "udp"}}) == RelayProtocol::Udp,
            "UDP protocol was not parsed");
    require_throws([]() { config::message_protocol(njson::object()); }, "Missing protocol was accepted");
    require_throws([]() { config::message_protocol(njson{{"protocol", "quic"}}); },
                   "Unknown protocol was accepted");
    require_throws([]() { config::required_protocol(njson::object(), "service"); },
                   "Missing configuration protocol was accepted");

    TokenBucket limiter(1000, 10);
    require(limiter.try_consume(6), "Initial UDP burst was rejected");
    require(!limiter.try_consume(5), "Over-limit UDP datagram was accepted");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    require(limiter.try_consume(5), "UDP tokens were not refilled");
    require(!limiter.try_consume(11), "Datagram larger than burst was accepted");

    TokenBucket unlimited;
    require(unlimited.try_consume(65535), "Disabled UDP limiter rejected a datagram");

    TrafficCounter traffic;
    traffic.add(6);
    traffic.add(4);
    require(traffic.total() == 10, "Traffic total did not accumulate bytes");
    require(traffic.take_interval() == 10 && traffic.take_interval() == 0,
            "Traffic sampling window was not exchanged atomically");
    traffic.add(std::numeric_limits<std::uint64_t>::max());
    traffic.add(1);
    require(traffic.total() == std::numeric_limits<std::uint64_t>::max(), "Traffic total did not saturate");
}

void test_stream_limit_accounting()
{
    TokenBucket burst(1, 10);
    require(burst.try_consume(6), "Initial stream burst was rejected");
    const auto now = TokenBucket::Clock::now();
    require(burst.reserve(4, now) == now, "Stream fast path did not leave the remaining burst available");

    // Keep refill time in the future so accounting assertions need no sleeps.
    TokenBucket waiting(1, 10);
    const auto future = now + std::chrono::hours(1);
    require(waiting.reserve(6, future) == future, "Initial stream reservation unexpectedly waited");
    require(!waiting.try_consume(5), "Insufficient stream tokens were accepted");
    require(waiting.reserve(5, future) == future + std::chrono::seconds(1),
            "Failed stream fast path consumed tokens or charged the reservation twice");
    require(!waiting.try_consume(1), "Stream fast path bypassed an outstanding reservation");
    require(waiting.reserve(11, future) == future + std::chrono::seconds(12),
            "Stream reservation lost outstanding debt or mishandled a write larger than the burst");
}
} // namespace

int main()
{
    try
    {
        test_control_commands();
        test_common_json_validation();
        test_round_trip(RelayAttach::Producer);
        test_round_trip(RelayAttach::Consumer);
        test_attach_validation();
        test_frame_validation();
        test_message_fragmentation();
        test_udp_session_header();
        test_protocol_and_udp_limit();
        test_stream_limit_accounting();
        std::cout << "[PASS] relay framing, UDP session header, and validation\n";
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
