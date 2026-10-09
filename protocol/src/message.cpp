#include "message.h"
#include <algorithm>
#include <fstream>
#include <string_view>
#include <utility>

std::string_view relay_protocol_name(RelayProtocol protocol) noexcept
{
    switch (protocol)
    {
    case RelayProtocol::Tcp:
        return "tcp";
    case RelayProtocol::Tls:
        return "tls";
    case RelayProtocol::Udp:
        return "udp";
    }
    return "unknown";
}

RelayProtocol parse_relay_protocol(std::string_view value)
{
    if (value == "tcp")
    {
        return RelayProtocol::Tcp;
    }
    if (value == "udp")
    {
        return RelayProtocol::Udp;
    }
    if (value == "tls")
    {
        return RelayProtocol::Tls;
    }
    throw std::invalid_argument("protocol must be tcp, tls, or udp");
}

static constexpr std::pair<CtrlCommand, std::string_view> ctrl_commands[] = {
    {CtrlCommand::Ping, "ping"},
    {CtrlCommand::Pong, "pong"},
    {CtrlCommand::RelayAttach, "relay.attach"},
    {CtrlCommand::ServerIdentify, "server.identify"},
    {CtrlCommand::ServerIdentified, "server.identified"},
    {CtrlCommand::ServiceRegister, "service.register"},
    {CtrlCommand::ServiceOk, "service.ok"},
    {CtrlCommand::ServiceError, "service.error"},
    {CtrlCommand::NodeLookup, "node.lookup"},
    {CtrlCommand::NodeLocated, "node.located"},
    {CtrlCommand::NodeError, "node.error"},
    {CtrlCommand::ServiceLookup, "service.lookup"},
    {CtrlCommand::ServiceLocated, "service.located"},
    {CtrlCommand::ServiceList, "service.list"},
    {CtrlCommand::ServiceListed, "service.listed"},
    {CtrlCommand::ServerLoad, "server.load"},
    {CtrlCommand::ServerLoaded, "server.loaded"},
    {CtrlCommand::ServerTraffic, "server.traffic"},
    {CtrlCommand::ServerTrafficReported, "server.traffic.reported"},
    {CtrlCommand::ServerCluster, "server.cluster"},
    {CtrlCommand::ServerStatusQuery, "server.status.query"},
    {CtrlCommand::ServerStatusReport, "server.status.report"},
    {CtrlCommand::ServerStatusReported, "server.status.reported"},
    {CtrlCommand::TopologyMembers, "topology.members"},
    {CtrlCommand::TopologyReport, "topology.report"},
    {CtrlCommand::TopologyQuery, "topology.query"},
    {CtrlCommand::TopologySnapshot, "topology.snapshot"},
    {CtrlCommand::RelayOpen, "relay.open"},
    {CtrlCommand::RelayOpened, "relay.opened"},
    {CtrlCommand::RelayOffer, "relay.offer"},
    {CtrlCommand::RelayReady, "relay.ready"},
    {CtrlCommand::RelayReject, "relay.reject"},
    {CtrlCommand::RelayCancel, "relay.cancel"},
    {CtrlCommand::RelayClosed, "relay.closed"},
    {CtrlCommand::RelayError, "relay.error"},
    {CtrlCommand::LinkPrepare, "link.prepare"},
    {CtrlCommand::LinkPrepared, "link.prepared"},
    {CtrlCommand::LinkConnect, "link.connect"},
    {CtrlCommand::LinkReady, "link.ready"},
    {CtrlCommand::LinkError, "link.error"},
    {CtrlCommand::LinkClose, "link.close"},
    {CtrlCommand::LinkClosed, "link.closed"},
    {CtrlCommand::LinkStatus, "link.status"},
    {CtrlCommand::LinkAttach, "link.attach"},
    {CtrlCommand::LinkAttached, "link.attached"},
    {CtrlCommand::FlowPrepare, "flow.prepare"},
    {CtrlCommand::FlowPrepared, "flow.prepared"},
    {CtrlCommand::FlowCommit, "flow.commit"},
    {CtrlCommand::FlowCommitted, "flow.committed"},
    {CtrlCommand::FlowError, "flow.error"},
    {CtrlCommand::FlowClose, "flow.close"},
    {CtrlCommand::FlowClosed, "flow.closed"},
    {CtrlCommand::ClusterJoin, "cluster.join"},
    {CtrlCommand::ClusterJoined, "cluster.joined"},
    {CtrlCommand::ClusterError, "cluster.error"},
};

std::string_view ctrl_command_name(CtrlCommand command) noexcept
{
    const auto entry =
        std::ranges::find_if(ctrl_commands, [command](const auto &item) { return item.first == command; });
    return entry == std::end(ctrl_commands) ? std::string_view{} : entry->second;
}

CtrlCommand parse_ctrl_command(std::string_view command) noexcept
{
    const auto entry =
        std::ranges::find_if(ctrl_commands, [command](const auto &item) { return item.second == command; });
    return entry == std::end(ctrl_commands) ? CtrlCommand::Unknown : entry->first;
}

CtrlMessage::CtrlMessage(std::string command_value, std::optional<njson> params_value)
    : command(std::move(command_value)), params(std::move(params_value))
{
}

CtrlMessage::CtrlMessage(CtrlCommand command_value, std::optional<njson> params_value)
    : command(ctrl_command_name(command_value)), params(std::move(params_value))
{
    if (command.empty())
        throw std::invalid_argument("Unknown control command");
}

CtrlCommand CtrlMessage::type() const noexcept
{
    return parse_ctrl_command(command);
}

namespace config
{
njson load_json(const std::filesystem::path &path)
{
    std::ifstream input(path);
    if (!input)
    {
        throw std::runtime_error("Cannot open configuration file: " + path.string());
    }

    try
    {
        auto root = njson::parse(input);
        if (!root.is_object())
        {
            throw std::runtime_error("Configuration root must be an object");
        }
        return root;
    }
    catch (const njson::exception &exception)
    {
        throw std::runtime_error("Invalid configuration file " + path.string() + ": " + exception.what());
    }
}

void reject_unknown_fields(const njson &object, std::initializer_list<std::string_view> allowed,
                           std::string_view location)
{
    if (!object.is_object())
    {
        throw std::runtime_error(std::string(location) + " must be an object");
    }

    for (const auto &[name, _] : object.items())
    {
        const auto known =
            std::ranges::any_of(allowed, [&name](std::string_view allowed_name) { return name == allowed_name; });
        if (!known)
        {
            throw std::runtime_error("Unknown configuration field: " + std::string(location) + "." + name);
        }
    }
}

const njson &required_object(const njson &parent, std::string_view name, std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end())
    {
        throw std::runtime_error("Missing configuration field: " + std::string(location) + "." + std::string(name));
    }
    if (!iterator->is_object())
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) + " must be an object");
    }
    return *iterator;
}

std::string required_string(const njson &parent, std::string_view name, std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end() || !iterator->is_string())
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) + " must be a string");
    }
    auto value = iterator->get<std::string>();
    if (value.empty())
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) + " must not be empty");
    }
    return value;
}

std::uint64_t optional_unsigned(const njson &parent, std::string_view name, std::uint64_t default_value,
                                std::uint64_t minimum, std::uint64_t maximum, std::string_view location)
{
    const auto iterator = parent.find(name);
    if (iterator == parent.end())
    {
        return default_value;
    }
    if (!iterator->is_number_unsigned())
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) + " must be an unsigned integer");
    }

    const auto value = iterator->get<std::uint64_t>();
    if (value < minimum || value > maximum)
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) + " is outside the valid range");
    }
    return value;
}

std::uint16_t required_port(const njson &parent, std::string_view name, std::string_view location)
{
    const auto value = optional_unsigned(parent, name, 0, 1, 65535, location);
    if (!parent.contains(name))
    {
        throw std::runtime_error("Missing configuration field: " + std::string(location) + "." + std::string(name));
    }
    return static_cast<std::uint16_t>(value);
}

RelayProtocol required_protocol(const njson &parent, std::string_view location)
{
    try
    {
        return parse_relay_protocol(required_string(parent, "protocol", location));
    }
    catch (const std::invalid_argument &)
    {
        throw std::runtime_error(std::string(location) + ".protocol must be tcp, tls, or udp");
    }
}

std::chrono::steady_clock::duration optional_duration(const njson &parent, std::string_view name,
                                                      std::chrono::steady_clock::duration default_value,
                                                      std::string_view location)
{
    const auto default_ms = std::chrono::duration_cast<std::chrono::milliseconds>(default_value).count();
    const auto value = optional_unsigned(parent, name, static_cast<std::uint64_t>(default_ms), 1,
                                         static_cast<std::uint64_t>(max_duration_ms), location);
    return std::chrono::milliseconds(value);
}

std::filesystem::path required_file(const njson &parent, std::string_view name,
                                    const std::filesystem::path &configuration_directory, std::string_view location)
{
    auto path = std::filesystem::path(required_string(parent, name, location));
    if (path.is_relative())
    {
        path = configuration_directory / path;
    }
    path = std::filesystem::absolute(path).lexically_normal();
    if (!std::filesystem::is_regular_file(path))
    {
        throw std::runtime_error(std::string(location) + "." + std::string(name) +
                                 " is not a regular file: " + path.string());
    }
    return path;
}

std::filesystem::path configuration_directory(const std::filesystem::path &path)
{
    return std::filesystem::absolute(path).lexically_normal().parent_path();
}

bool check_illegal_service_name(std::string_view name)
{
    return name.empty() || name.size() > 64 || !std::ranges::all_of(name, [](const char ch) {
               return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.' ||
                      ch == '_' || ch == '-';
           });
}

const njson &message_params(const CtrlMessage &message)
{
    if (!message.params || !message.params->is_object())
    {
        throw std::invalid_argument("Control message params must be an object");
    }
    return *message.params;
}

static const njson &require_field(const njson &params, std::string_view name)
{
    const auto iterator = params.find(name);
    if (iterator == params.end())
    {
        throw std::invalid_argument(std::string(name) + " is required");
    }
    return *iterator;
}

std::uint64_t require_unsigned(const njson &params, std::string_view name, bool reject_zero)
{
    const auto &value = require_field(params, name);
    if (!value.is_number_unsigned())
    {
        throw std::invalid_argument(std::string(name) + " must be an unsigned integer");
    }
    const auto result = value.get<std::uint64_t>();
    if (reject_zero && result == 0)
    {
        throw std::invalid_argument(std::string(name) + " must not be zero");
    }
    return result;
}

std::optional<std::uint64_t> optional_unsigned(const njson &params, std::string_view name, bool reject_zero)
{
    const auto iterator = params.find(name);
    if (iterator == params.end())
    {
        return std::nullopt;
    }
    if (!iterator->is_number_unsigned())
    {
        throw std::invalid_argument(std::string(name) + " must be an unsigned integer");
    }
    const auto value = iterator->get<std::uint64_t>();
    if (reject_zero && value == 0)
    {
        throw std::invalid_argument(std::string(name) + " must not be zero");
    }
    return value;
}

std::string optional_string(const njson &params, std::string_view name, std::string_view fallback)
{
    const auto iterator = params.find(name);
    if (iterator == params.end())
    {
        return std::string(fallback);
    }
    if (!iterator->is_string())
    {
        throw std::invalid_argument(std::string(name) + " must be a string");
    }
    return iterator->get<std::string>();
}

std::string message_service(const njson &params)
{
    const auto &value = require_field(params, "service");
    if (!value.is_string())
    {
        throw std::invalid_argument("service must be a string");
    }
    auto name = value.get<std::string>();
    if (check_illegal_service_name(name))
    {
        throw std::invalid_argument("service contains invalid characters or has an invalid length");
    }
    return name;
}

std::uint16_t message_data_port(const njson &params)
{
    const auto port = require_unsigned(params, "data_port");
    if (port == 0 || port > 65535)
    {
        throw std::invalid_argument("data_port is outside the valid range");
    }
    return static_cast<std::uint16_t>(port);
}

RelayProtocol message_protocol(const njson &params)
{
    const auto &value = require_field(params, "protocol");
    if (!value.is_string())
    {
        throw std::invalid_argument("protocol must be a string");
    }
    return parse_relay_protocol(value.get_ref<const std::string &>());
}
} // namespace config

std::string exception_description(std::exception_ptr failure)
{
    if (!failure)
        return "none";
    try
    {
        std::rethrow_exception(failure);
    }
    catch (const std::exception &error)
    {
        return error.what();
    }
    catch (...)
    {
        return "unknown exception";
    }
}

static bool message_debug_enabled = false;

bool proxy_is_debug_enabled()
{
    return message_debug_enabled;
}

void initialize_logger_config(const njson &root)
{
    message_debug_enabled = false;
    const auto log = root.find("log");
    if (log == root.end())
    {
        return;
    }

    config::reject_unknown_fields(*log, {"debug_enable"}, "log");
    const auto debug = log->find("debug_enable");
    if (debug != log->end())
    {
        if (!debug->is_boolean())
        {
            throw std::runtime_error("log.debug_enable must be a boolean");
        }
        message_debug_enabled = debug->get<bool>();
    }
}

static bool validate_command(std::string_view command)
{
    if (command.empty() || command.length() > CtrlMessage::max_command_length)
    {
        return false;
    }

    return std::ranges::all_of(command, [](char ch) {
        return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.';
    });
}

static njson make_message_json(CtrlMessage message)
{
    if (!validate_command(message.command))
    {
        throw std::invalid_argument("Invalid command: " + message.command);
    }

    if (message.params && !message.params->is_object())
    {
        throw std::invalid_argument("Message params must be an object");
    }

    njson root = njson::object();
    root["command"] = std::move(message.command);
    if (message.params)
    {
        root["params"] = std::move(*message.params);
    }
    return root;
}

static CtrlMessage parse_message_json(njson root)
{
    if (!root.is_object())
    {
        throw std::logic_error("Top level must be an object");
    }

    const auto command = root.find("command");
    if (command == root.end() || !command->is_string())
    {
        throw std::logic_error("CBOR root must contain a string field 'command'");
    }

    const auto &recv_cmd = command->get_ref<const njson::string_t &>();
    if (!validate_command(recv_cmd))
    {
        throw std::logic_error("Invalid command: " + recv_cmd);
    }

    const auto params = root.find("params");
    if (params != root.end() && !params->is_object())
    {
        throw std::logic_error("CBOR field 'params' must be an object");
    }

    CtrlMessage message{};
    message.command = std::move(command->get_ref<njson::string_t &>());
    if (params != root.end())
    {
        message.params.emplace(std::move(*params));
    }

    return message;
}

CtrlMessage CtrlMessage::deserialize(std::span<const uint8_t> payload)
{
    if (payload.empty())
    {
        throw std::invalid_argument("Payload is empty");
    }
    return parse_message_json(
        njson::from_cbor(payload.begin(), payload.end(), true, true, njson::cbor_tag_handler_t::ignore));
}

static constexpr WireMessage::Header encode_length(std::uint32_t length) noexcept
{
    return {static_cast<std::uint8_t>((length >> 24U) & 0xffU), static_cast<std::uint8_t>((length >> 16U) & 0xffU),
            static_cast<std::uint8_t>((length >> 8U) & 0xffU), static_cast<std::uint8_t>(length & 0xffU)};
}

std::uint32_t WireMessage::decode_length(std::span<const std::uint8_t, header_length> header)
{
    const auto length = (static_cast<std::uint32_t>(header[0]) << 24U) |
                        (static_cast<std::uint32_t>(header[1]) << 16U) | (static_cast<std::uint32_t>(header[2]) << 8U) |
                        static_cast<std::uint32_t>(header[3]);
    if (length == 0 || length > max_payload_length)
    {
        char buffer[256];
        snprintf(buffer, sizeof(buffer), "Invalid payload length: %u (valid range: [1, %zu])", length,
                 max_payload_length);
        throw std::runtime_error(buffer);
    }

    return length;
}

static void append_frame(BytesBuf &frames, const BytesBuf &payload)
{
    const auto header = encode_length(static_cast<std::uint32_t>(payload.size()));
    frames.insert(frames.end(), header.begin(), header.end());
    frames.insert(frames.end(), payload.begin(), payload.end());
}

BytesBuf WireMessage::pack(CtrlMessage message)
{
    const auto payload = njson::to_cbor(make_message_json(std::move(message)));
    if (payload.size() > max_message_length)
        throw std::invalid_argument("Control message exceeds 16 MiB");
    BytesBuf frames;
    if (payload.size() <= max_payload_length)
    {
        append_frame(frames, payload);
        return frames;
    }

    const auto count = (payload.size() + fragment_payload_length - 1) / fragment_payload_length;
    for (std::size_t index = 0; index < count; ++index)
    {
        const auto begin = payload.begin() + index * fragment_payload_length;
        const auto end = begin + std::min(fragment_payload_length, payload.size() - index * fragment_payload_length);
        append_frame(frames, njson::to_cbor(njson{
                                 {"__fragment", njson::array({index, count, njson::binary(BytesBuf(begin, end))})}}));
    }
    return frames;
}

void MessageReceiver::reset() noexcept
{
    assembled_.clear();
    next_page_ = page_count_ = 0;
}

std::optional<CtrlMessage> MessageReceiver::receive(std::span<const std::uint8_t> payload)
{
    if (payload.empty() || payload.size() > WireMessage::max_payload_length)
        throw std::invalid_argument("Invalid control frame payload size");
    auto root = njson::from_cbor(payload.begin(), payload.end(), true, true, njson::cbor_tag_handler_t::ignore);
    if (!root.contains("__fragment"))
    {
        reset();
        return parse_message_json(std::move(root));
    }
    const auto &fragment = root.at("__fragment");
    if (root.size() != 1 || !fragment.is_array() || fragment.size() != 3 || !fragment[0].is_number_unsigned() ||
        !fragment[1].is_number_unsigned() || !fragment[2].is_binary())
        throw std::invalid_argument("Invalid control message fragment envelope");
    const auto index = fragment[0].get<std::uint64_t>();
    const auto count = fragment[1].get<std::uint64_t>();
    const auto &bytes = fragment[2].get_binary();
    constexpr auto chunk = WireMessage::fragment_payload_length;
    constexpr auto max_count = (WireMessage::max_message_length + chunk - 1) / chunk;
    if (index == 0)
    {
        reset(); // A new TCP fragment batch replaces any unfinished message.
        if (count < 2 || count > max_count || bytes.size() != chunk)
        {
            return std::nullopt;
        }
        page_count_ = count;
        next_page_ = 1;
        // Full pages have a fixed length, so the page count bounds the required capacity.
        assembled_.reserve(std::min(page_count_ * chunk, WireMessage::max_message_length));
        assembled_.insert(assembled_.end(), bytes.begin(), bytes.end());
        return std::nullopt;
    }

    if (index != next_page_ || count != page_count_)
    {
        reset();
        return std::nullopt;
    }

    const auto final = index == count - 1;
    const auto valid_size =
        final ? !bytes.empty() && bytes.size() <= std::min(chunk, WireMessage::max_message_length - assembled_.size())
              : bytes.size() == chunk;
    if (!valid_size)
    {
        reset();
        return std::nullopt;
    }

    assembled_.insert(assembled_.end(), bytes.begin(), bytes.end());
    ++next_page_;
    if (!final)
    {
        return std::nullopt;
    }

    auto complete = std::move(assembled_);
    reset();
    return CtrlMessage::deserialize(complete);
}

DatagramHeader::Buffer DatagramHeader::encode(std::uint64_t session_id)
{
    if (session_id == 0)
    {
        throw std::invalid_argument("UDP session ID must not be zero");
    }

    Buffer header{};
    for (std::size_t index = 0; index < header.size(); ++index)
    {
        header[index] = static_cast<std::uint8_t>(session_id >> (56 - index * 8));
    }
    return header;
}

std::optional<std::uint64_t> DatagramHeader::decode(std::span<const std::uint8_t> header) noexcept
{
    if (header.size() != length)
    {
        return std::nullopt;
    }

    std::uint64_t session_id = 0;
    for (const auto byte : header)
    {
        session_id = (session_id << 8) | byte;
    }
    return session_id == 0 ? std::nullopt : std::optional(session_id);
}

CtrlMessage RelayAttach::to_msg(const RelayAttach &self)
{
    if (self.role != Producer && self.role != Consumer)
    {
        throw std::invalid_argument("Invalid relay role");
    }

    if (self.uuid == 0 || self.ticket == 0)
    {
        throw std::invalid_argument("relay.attach uuid and ticket must not be zero");
    }

    return CtrlMessage{CtrlCommand::RelayAttach,
                       njson{{"role", self.role}, {"uuid", self.uuid}, {"ticket", self.ticket}}};
}

RelayAttach RelayAttach::from_msg(const CtrlMessage &message)
{
    if (message.type() != CtrlCommand::RelayAttach)
    {
        throw std::invalid_argument("Expected relay.attach message");
    }
    const auto &params = config::message_params(message);

    const auto role_iterator = params.find("role");
    if (role_iterator == params.end() || !role_iterator->is_number_integer())
    {
        throw std::invalid_argument("relay.attach role must be an integer");
    }

    if (*role_iterator != Producer && *role_iterator != Consumer)
    {
        throw std::invalid_argument("Invalid relay.attach role");
    }
    const auto role = *role_iterator == Producer ? Producer : Consumer;

    return {role, config::require_unsigned(params, "uuid", true), config::require_unsigned(params, "ticket", true)};
}

namespace
{
void encode_lnk_integer(std::span<std::uint8_t> output, std::uint64_t value)
{
    for (std::size_t i = output.size(); i != 0; --i)
    {
        output[i - 1] = static_cast<std::uint8_t>(value);
        value >>= 8;
    }
}
std::uint64_t decode_lnk_integer(std::span<const std::uint8_t> input)
{
    std::uint64_t value = 0;
    for (const auto byte : input)
    {
        value = (value << 8) | byte;
    }
    return value;
}
} // namespace

void LnkFrameHeader::validate() const
{
    if (!epoch || body_length > maximum_payload)
    {
        throw std::invalid_argument("invalid node frame epoch or body length");
    }
    switch (kind)
    {
    case LnkFrType::Data:
    case LnkFrType::Fin:
    case LnkFrType::Reset:
        if (!id || sequence || (kind == LnkFrType::Fin && body_length) ||
            (kind == LnkFrType::Reset && body_length > maximum_reason))
        {
            throw std::invalid_argument("invalid NodeFlow header");
        }
        break;
    case LnkFrType::Ping:
    case LnkFrType::Pong:
        if (id || !sequence || body_length || reverse)
        {
            throw std::invalid_argument("invalid node heartbeat header");
        }
        break;
    case LnkFrType::Attach:
    case LnkFrType::Attached:
        if (id || sequence || !body_length || reverse)
        {
            throw std::invalid_argument("invalid node bootstrap header");
        }
        break;
    default:
        throw std::invalid_argument("unknown node frame kind");
    }
}

LnkFrameHeader::Buffer LnkFrameHeader::encode() const noexcept
{
    Buffer output{};
    output[0] = magic;
    output[1] = version;
    output[2] = static_cast<std::uint8_t>(kind);
    output[3] = reverse ? 1 : 0;
    auto bytes = std::span(output);
    encode_lnk_integer(bytes.subspan<4, 4>(), body_length);
    encode_lnk_integer(bytes.subspan<8, 8>(), epoch);
    encode_lnk_integer(bytes.subspan<16, 8>(), id);
    encode_lnk_integer(bytes.subspan<24, 8>(), sequence);
    return output;
}

LnkFrameHeader LnkFrameHeader::decode(std::span<const std::uint8_t, length> header)
{
    if (header[0] != magic || header[1] != version || header[3] > 1)
    {
        throw std::invalid_argument("invalid node frame magic, version or flags");
    }
    LnkFrameHeader result{static_cast<LnkFrType>(header[2]),
                          header[3] != 0,
                          static_cast<std::uint32_t>(decode_lnk_integer(header.subspan<4, 4>())),
                          decode_lnk_integer(header.subspan<8, 8>()),
                          decode_lnk_integer(header.subspan<16, 8>()),
                          decode_lnk_integer(header.subspan<24, 8>())};
    result.validate();
    return result;
}

bool FlowFrame::validate() const noexcept
{
    if (!epoch || !flow_id)
    {
        return false;
    }
    switch (kind)
    {
    case LnkFrType::Data:
        return reason.empty() && payload.size() <= LnkFrameHeader::maximum_payload;
    case LnkFrType::Fin:
        return payload.empty() && reason.empty();
    case LnkFrType::Reset:
        return payload.empty() && reason.size() <= LnkFrameHeader::maximum_reason;
    default:
        return false;
    }
}
