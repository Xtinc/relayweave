#include "app_common.h"
#include <limits>
#include <iostream>
#include <stdexcept>
#include <string>

namespace
{
using namespace std::chrono_literals;
void require(bool condition, const std::string &message)
{
    if (!condition)
        throw std::runtime_error(message);
}

void verify_service_traffic_ema()
{
    ServiceTraffic traffic;
    const auto first_sample = ServiceTraffic::Clock::now() + 1s;
    traffic.rx.add(400);
    traffic.rx.add(600);
    traffic.tx.add(750);
    traffic.tx.add(1250);
    traffic.sample(first_sample);
    require(traffic.rx.total() == 1000 && traffic.tx.total() == 2000,
            "Service traffic totals were not preserved after sampling");
    require(traffic.rx_bytes_per_second() >= 999 && traffic.rx_bytes_per_second() <= 1000,
            "First RX bandwidth sample was not used to initialise the EMA");
    require(traffic.tx_bytes_per_second() >= 1999 && traffic.tx_bytes_per_second() <= 2000,
            "First TX bandwidth sample was not used to initialise the EMA");

    traffic.rx.add(3000);
    traffic.tx.add(4000);
    traffic.sample(first_sample + 1s);
    require(traffic.rx_bytes_per_second() >= 1999 && traffic.rx_bytes_per_second() <= 2000,
            "RX bandwidth EMA did not use alpha 0.5");
    require(traffic.tx_bytes_per_second() >= 2999 && traffic.tx_bytes_per_second() <= 3000,
            "TX bandwidth EMA did not use alpha 0.5");

    traffic.add_accessor("192.0.2.10:40000");
    traffic.add_accessor("192.0.2.10:40000");
    traffic.add_accessor("192.0.2.11:40001");
    auto accessors = traffic.accessors();
    require(accessors.size() == 2 && accessors.at("192.0.2.10:40000") == 2 &&
                accessors.at("192.0.2.11:40001") == 1,
            "Service accessors were not aggregated and sorted");
    traffic.remove_accessor("192.0.2.10:40000");
    traffic.remove_accessor("192.0.2.11:40001");
    accessors = traffic.accessors();
    require(accessors.size() == 1 && accessors.at("192.0.2.10:40000") == 1,
            "Service accessor connections were not removed correctly");
    traffic.remove_accessor("192.0.2.10:40000");
    require(traffic.accessors().empty(), "Service accessor remained after its last connection closed");

    traffic.sample(first_sample + 2s);
    require(traffic.rx_bytes_per_second() >= 999 && traffic.rx_bytes_per_second() <= 1000,
            "RX bandwidth EMA did not decay during an idle window");
    require(traffic.tx_bytes_per_second() >= 1499 && traffic.tx_bytes_per_second() <= 1500,
            "TX bandwidth EMA did not decay during an idle window");
}

void verify_status_fragmentation()
{
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    njson services = njson::array();
    for (std::size_t index = 0; index < 256; ++index)
    {
        auto prefix = "service-" + std::to_string(index) + "-";
        auto name = prefix + std::string(64 - prefix.size(), static_cast<char>('a' + index % 26));
        njson accessors = njson::object();
        for (std::size_t accessor = 0; accessor < 4; ++accessor)
        {
            accessors["203.0.113." + std::to_string((index * 4 + accessor) % 256) + ":" +
                      std::to_string(40000 + accessor)] = 1;
        }
        services.push_back(njson{{"service", std::move(name)},
                                 {"protocol", index % 2 == 0 ? "tcp" : "udp"},
                                 {"rx_bytes", maximum},
                                 {"tx_bytes", maximum},
                                 {"rx_bytes_per_second", maximum},
                                 {"tx_bytes_per_second", maximum},
                                 {"accessors", std::move(accessors)}});
    }

    njson base{{"request_id", maximum},
               {"node_id", "master-1"},
               {"uptime_ms", maximum}};
    base["services"] = services;
    const CtrlMessage message{CtrlCommand::ServerStatusReported, std::move(base)};
    const auto frames = WireMessage::pack(message);
    require(frames.size() > WireMessage::max_payload_length + WireMessage::header_length,
            "Oversized server status was not fragmented");
    MessageReceiver receiver;
    std::optional<CtrlMessage> complete;
    for (std::size_t offset = 0; offset < frames.size();)
    {
        const auto remaining = std::span<const std::uint8_t>(frames).subspan(offset);
        const auto length = WireMessage::decode_length(remaining.first<WireMessage::header_length>());
        complete = receiver.receive(remaining.subspan(WireMessage::header_length, length));
        offset += WireMessage::header_length + length;
        require(complete.has_value() == (offset == frames.size()), "Partial status reached the business layer");
    }
    require(complete && complete->params == message.params,
            "Server status fragmentation lost or duplicated data");
}

} // namespace

int main()
{
    try
    {
        verify_service_traffic_ema();
        verify_status_fragmentation();
        std::cout << "[PASS] Node traffic sampling, accessor lifetime and large status fragmentation\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
