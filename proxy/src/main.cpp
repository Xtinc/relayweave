#include "proxy_server.h"
#include "message.h"
#include "relayweave_version.h"

#include <csignal>
#include <iostream>

namespace
{
void print_usage(std::string_view program)
{
    std::cerr << "Usage: " << program << " <proxy-config.json>\n";
}
} // namespace

int main(int argc, char *argv[])
{
    if (argc == 2 && std::string_view(argv[1]) == "--help")
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-proxy");
        return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--version")
    {
        std::cout << "RelayWeave Proxy " << RELAYWEAVE_VERSION << '\n';
        return 0;
    }
    if (argc != 2)
    {
        print_usage(argc > 0 ? argv[0] : "relayweave-proxy");
        return 1;
    }

    try
    {
        const std::filesystem::path config_path = argv[1];
        auto config = load_proxy_config(config_path);

        asio::io_context io(1);
        auto proxy = std::make_shared<ProxyServer>(io, std::move(config));
        proxy->start();

        asio::signal_set signals(io, SIGINT, SIGTERM);
        signals.async_wait([proxy](const asio::error_code &error, int) {
            if (!error)
            {
                proxy->stop();
            }
        });
        io.run();
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "[FAIL] " << exception.what() << '\n';
        return 1;
    }
}
