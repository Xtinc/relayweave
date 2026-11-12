#include "node_address_fixture.h"
#include "message.h"
#include <iostream>
#include <stdexcept>

namespace
{
void require(bool value, const char *reason)
{
    if (!value)
    {
        throw std::runtime_error(reason);
    }
}
}

int main()
{
    try
    {
        const auto a = test_node_address({{"vpc-a", "192.168.1.10"}, {"default", "node-a.example"},
                                          {"lo:host-a", "127.0.0.1"}, {"vpc-b", "10.0.0.1"}});
        const auto reordered = test_node_address({{"vpc-b", "10.0.0.1"}, {"lo:host-a", "127.0.0.1"},
                                                  {"default", "node-a.example"}, {"vpc-a", "192.168.1.10"}});
        const auto b = test_node_address({{"default", "192.0.2.2"}, {"vpc-b", "10.0.0.2"},
                                          {"vpc-a", "192.168.1.11"}, {"lo:host-b", "127.0.0.1"}});
        require(a.entries().size() == 4 && a == reordered, "Ordering changed address identity");
        require(a.address_for("lo:host-a") == "127.0.0.1" &&
                    a.address_for("lo:host-b") == "node-a.example" &&
                    a.address_for("VPC-A") == "node-a.example" && a.select("lo:host-b").label == "default",
                "Exact lookup or fallback failed");
        require(a.common_label(b) == "vpc-a" && b.common_label(a) == "vpc-a" &&
                    a.address_for(b) == "192.168.1.10" && b.address_for(a) == "192.168.1.11",
                "Common labels did not select symmetric lexical priority");
        require(test_node_address("localhost").address_for(a) == "localhost", "Default-only address failed");
        require(NodeAddress::from_params(a.to_params(), "round trip") == a, "Wire metadata lost addresses");
        const auto rejected = [](const njson &params) {
            try
            {
                NodeAddress::from_params(params, "test");
            }
            catch (const std::exception &)
            {
                return;
            }
            throw std::runtime_error("Invalid address metadata was accepted");
        };
        rejected(njson{{"address", "127.0.0.1"}});
        rejected(njson{{"network_ports", njson::array()}});
        rejected(njson{{"network_ports", njson::object()}});
        auto invalid = a.to_params();
        invalid["network_ports"].push_back({{"label", "fifth"}, {"address", "127.0.0.1"}});
        rejected(invalid);
        for (const auto &label : {"", "vpc-a"})
        {
            invalid = a.to_params();
            invalid["network_ports"][0]["label"] = label;
            rejected(invalid);
        }
        invalid = a.to_params();
        invalid["network_ports"][0]["label"] = "no-default";
        rejected(invalid);
        for (const auto *ip : {"", "0.0.0.0", "224.0.0.1", "255.255.255.255", "::1"})
        {
            invalid = a.to_params();
            invalid["network_ports"][0]["address"] = ip;
            rejected(invalid);
        }
        for (const auto *address : {"localhost", "invalid host", "999.0.0.1"})
        {
            invalid = a.to_params();
            invalid["network_ports"][1]["address"] = address;
            rejected(invalid);
        }
        invalid = a.to_params();
        invalid["network_ports"][0]["port"] = 1234;
        rejected(invalid);
        const auto alias = test_node_address({{"default", "127.0.0.1"}, {"lo:host-a", "127.0.0.1"}});
        require(alias.address_for("lo:host-a") == "127.0.0.1", "Address aliases were rejected");
        std::cout << "[PASS] fixed-capacity network ports, validation, selection and wire metadata\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
