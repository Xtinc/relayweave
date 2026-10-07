add_library(relayweave_routing_lib
    src/link_quality.cpp
    src/probe_set.cpp
    src/icmp.cpp
    src/route_graph.cpp
)
target_link_libraries(relayweave_routing_lib PUBLIC asio nlohmann_json::nlohmann_json PRIVATE relayweave_protocol)
target_include_directories(relayweave_routing_lib PUBLIC inc)
