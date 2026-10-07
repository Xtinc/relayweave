#include "icmp.h"
#include <cmath>
#include <charconv>
#include <exception>
#include <iomanip>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

namespace
{
using ICMPProtocol = asio::ip::icmp;

asio::awaitable<asio::ip::address_v4> resolve_target(std::string target)
{
    asio::error_code address_error;
    const auto address = asio::ip::make_address_v4(target, address_error);
    if (!address_error)
    {
        co_return address;
    }

    const auto executor = co_await asio::this_coro::executor;
    ICMPProtocol::resolver resolver(executor);
    const auto endpoints =
        co_await resolver.async_resolve(ICMPProtocol::v4(), target, "", asio::use_awaitable);
    for (const auto &entry : endpoints)
    {
        if (entry.endpoint().address().is_v4())
        {
            co_return entry.endpoint().address().to_v4();
        }
    }
    throw std::runtime_error("no IPv4 address found for " + target);
}

std::size_t parse_count(std::string_view text)
{
    std::size_t count = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), count);
    if (error != std::errc{} || end != text.data() + text.size() || count == 0)
    {
        throw std::invalid_argument("ping count must be a positive integer");
    }
    return count;
}

void print_metrics(std::string_view target, const ICMP::Metrics &metrics)
{
    const auto &long_ema = metrics.assessment.scales.back();
    std::cout << "\n--- " << target << " ping metrics ---\n"
              << metrics.transmitted << " packets transmitted, " << metrics.assessment.total_received << " received, "
              << std::fixed << std::setprecision(1) << long_ema.loss_rate * 100.0 << "% packet loss\n";
    if (long_ema.rtt_ms && long_ema.jitter_ms)
    {
        std::cout << std::setprecision(3) << "rtt_ema_30min = " << *long_ema.rtt_ms
                  << " ms, jitter_ema_30min = " << *long_ema.jitter_ms << " ms\n";
        if (metrics.assessment.quality.cost)
        {
            std::cout << "quality_score = " << 100.0 * std::exp(-*metrics.assessment.quality.cost / 100.0)
                      << ", confidence = " << metrics.assessment.quality.confidence << '\n';
        }
    }
}

asio::awaitable<void> start(std::string target, asio::io_context &io_context,
                            std::optional<std::size_t> count, std::optional<ICMP> &icmp)
{
    const auto destination = co_await resolve_target(target);
    std::cout << "PING " << target << " (" << destination.to_string() << ")" << std::endl;
    icmp.emplace(io_context, std::chrono::seconds(1), std::chrono::milliseconds(800), count);

    icmp->run({destination});
}
} // namespace

int main(int argc, char **argv)
{
    if (argc < 2 || argc > 3)
    {
        std::cerr << "usage: " << argv[0] << " <IPv4-or-hostname> [count]\n";
        return 2;
    }

    try
    {
        const auto count = argc == 3 ? parse_count(argv[2]) : 4;

        asio::io_context io_context(1);
        std::optional<ICMP> icmp;
        auto result = asio::co_spawn(io_context, start(argv[1], io_context, count, icmp), asio::use_future);
        io_context.run();
        result.get();
        print_metrics(argv[1], icmp->metrics().front());
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "ping: " << error.what() << '\n';
        return 1;
    }
}
