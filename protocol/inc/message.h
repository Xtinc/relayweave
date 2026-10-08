#ifndef PROXY_MESSAGE_H
#define PROXY_MESSAGE_H

#include "nlohmann/json.hpp"
#include <array>
#include <chrono>
#include <exception>
#include <filesystem>
#include <optional>
#include <span>
#include <string_view>

using njson = nlohmann::json;
using BytesBuf = std::vector<uint8_t>;

enum class RelayProtocol
{
    Tcp,
    Tls,
    Udp,
};

std::string_view relay_protocol_name(RelayProtocol protocol) noexcept;
RelayProtocol parse_relay_protocol(std::string_view value);

enum class CtrlCommand
{
    Unknown,
    Ping,
    Pong,
    RelayAttach,
    ServerIdentify,
    ServerIdentified,
    ServiceRegister,
    ServiceOk,
    ServiceError,
    ServiceLookup,
    ServiceLocated,
    ServiceList,
    ServiceListed,
    ServerLoad,
    ServerLoaded,
    ServerTraffic,
    ServerTrafficReported,
    ServerCluster,
    ServerStatusQuery,
    ServerStatusReport,
    ServerStatusReported,
    TopologyMembers,
    TopologyReport,
    TopologyQuery,
    TopologySnapshot,
    RelayOpen,
    RelayOpened,
    RelayOffer,
    RelayReady,
    RelayReject,
    RelayCancel,
    RelayClosed,
    RelayError,
    LinkPrepare,
    LinkPrepared,
    LinkConnect,
    LinkReady,
    LinkError,
    LinkClose,
    LinkClosed,
    LinkStatus,
    LinkAttach,
    LinkAttached,
    FlowPrepare,
    FlowPrepared,
    FlowCommit,
    FlowCommitted,
    FlowError,
    FlowClose,
    FlowClosed,
    ClusterJoin,
    ClusterJoined,
    ClusterError,
};

std::string_view ctrl_command_name(CtrlCommand command) noexcept;
CtrlCommand parse_ctrl_command(std::string_view command) noexcept;

#define PROXY_INFO_PRINT(fmt, ...)                                                                                     \
    do                                                                                                                 \
    {                                                                                                                  \
        printf("[INF] %s(%d): " fmt "\n", __FUNCTION__, __LINE__, ##__VA_ARGS__);                                      \
        fflush(stdout);                                                                                                \
    } while (0)
#define PROXY_ERROR_PRINT(fmt, ...)                                                                                    \
    do                                                                                                                 \
    {                                                                                                                  \
        printf("[ERR] %s(%d): " fmt "\n", __FUNCTION__, __LINE__, ##__VA_ARGS__);                                      \
        fflush(stdout);                                                                                                \
    } while (0)
#define PROXY_DEBUG_PRINT(fmt, ...)                                                                                    \
    do                                                                                                                 \
    {                                                                                                                  \
        if (proxy_is_debug_enabled())                                                                                  \
        {                                                                                                              \
            printf("[DEB] %s(%d): " fmt "\n", __FUNCTION__, __LINE__, ##__VA_ARGS__);                                  \
            fflush(stdout);                                                                                            \
        }                                                                                                              \
    } while (0)

bool proxy_is_debug_enabled();
std::string exception_description(std::exception_ptr failure);

void initialize_logger_config(const njson &root);

struct CtrlMessage
{
    static constexpr std::size_t max_command_length = 32;
    static CtrlMessage deserialize(std::span<const uint8_t> payload);

    CtrlMessage() = default;
    CtrlMessage(std::string command, std::optional<njson> params = std::nullopt);
    CtrlMessage(CtrlCommand command, std::optional<njson> params = std::nullopt);
    CtrlCommand type() const noexcept;

    std::string command;
    std::optional<njson> params;
};

struct WireMessage
{
    static constexpr std::size_t header_length = 4;
    static constexpr std::size_t max_payload_length = 64 * 1024;
    static constexpr std::size_t max_message_length = 16 * 1024 * 1024;
    static constexpr std::size_t fragment_payload_length = max_payload_length - 64;
    using Header = std::array<std::uint8_t, header_length>;
    static std::uint32_t decode_length(std::span<const std::uint8_t, header_length> header);
    static BytesBuf pack(CtrlMessage message);
};

class MessageReceiver
{
  public:
    std::optional<CtrlMessage> receive(std::span<const std::uint8_t> payload);

  private:
    void reset() noexcept;
    BytesBuf assembled_;
    std::size_t next_page_ = 0;
    std::size_t page_count_ = 0;
};

// Node data sockets use this fixed binary header after TCP bootstrap, and for every UDP packet.
enum class LnkFrType : std::uint8_t
{
    Data = 1,
    Fin = 2,
    Reset = 3,
    Ping = 4,
    Pong = 5,
    Attach = 7,
    Attached = 8
};

struct LnkFrameHeader
{
    static constexpr std::uint8_t magic = 0x4e;
    static constexpr std::uint8_t version = 1;
    static constexpr std::size_t length = 32;
    static constexpr std::size_t maximum_payload = 4096;
    static constexpr std::size_t maximum_reason = 512;
    using Buffer = std::array<std::uint8_t, length>;

    LnkFrType kind = LnkFrType::Data;
    bool reverse = false;
    std::uint32_t body_length = 0;
    std::uint64_t epoch = 0;
    std::uint64_t id = 0;       // Flow ID; zero for NodeLink control frames.
    std::uint64_t sequence = 0; // PING/PONG sequence; otherwise zero.

    // Pure encoding. Local callers validate their input before constructing this header.
    Buffer encode() const noexcept;
    static LnkFrameHeader decode(std::span<const std::uint8_t, length> header);

  private:
    void validate() const;
};

struct FlowFrame
{
    std::uint64_t epoch = 0;
    std::uint64_t flow_id = 0;
    bool reverse = false;
    LnkFrType kind = LnkFrType::Data;
    BytesBuf payload;
    std::string reason;
    [[nodiscard]] bool validate() const noexcept;
};

struct DatagramHeader
{
    static constexpr std::size_t length = 8;
    static constexpr std::size_t maximum_wire_payload = 65507;
    static constexpr std::size_t maximum_user_payload = maximum_wire_payload - length;
    using Buffer = std::array<std::uint8_t, length>;

    static Buffer encode(std::uint64_t session_id);
    static std::optional<std::uint64_t> decode(std::span<const std::uint8_t> header) noexcept;
};

struct RelayAttach
{
    static constexpr int Producer = 1;
    static constexpr int Consumer = 2;
    static CtrlMessage to_msg(const RelayAttach &self);
    static RelayAttach from_msg(const CtrlMessage &message);

    int role;
    std::uint64_t uuid;
    std::uint64_t ticket;
};

namespace config
{
inline constexpr std::int64_t max_duration_ms = 60LL * 60LL * 1000LL;
inline constexpr std::uint64_t max_queue_size = 10'000;
inline constexpr std::uint64_t max_connection_count = 1'000;

njson load_json(const std::filesystem::path &path);

void reject_unknown_fields(const njson &object, std::initializer_list<std::string_view> allowed,
                           std::string_view location);

const njson &required_object(const njson &parent, std::string_view name, std::string_view location);

std::string required_string(const njson &parent, std::string_view name, std::string_view location);

std::uint64_t optional_unsigned(const njson &parent, std::string_view name, std::uint64_t default_value,
                                std::uint64_t minimum, std::uint64_t maximum, std::string_view location);

std::uint16_t required_port(const njson &parent, std::string_view name, std::string_view location);

RelayProtocol required_protocol(const njson &parent, std::string_view location);

std::chrono::steady_clock::duration optional_duration(const njson &parent, std::string_view name,
                                                      std::chrono::steady_clock::duration default_value,
                                                      std::string_view location);

std::filesystem::path required_file(const njson &parent, std::string_view name,
                                    const std::filesystem::path &configuration_directory, std::string_view location);

std::filesystem::path configuration_directory(const std::filesystem::path &path);

bool check_illegal_service_name(std::string_view service_name);

const njson &message_params(const CtrlMessage &message);

std::uint64_t require_unsigned(const njson &params, std::string_view name, bool reject_zero = false);

std::optional<std::uint64_t> optional_unsigned(const njson &params, std::string_view name, bool reject_zero = false);

std::string optional_string(const njson &params, std::string_view name, std::string_view fallback);

std::string message_service(const njson &params);

std::uint16_t message_data_port(const njson &params);

RelayProtocol message_protocol(const njson &params);
} // namespace config

#endif // PROXY_MESSAGE_H
