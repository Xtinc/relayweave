foreach(name IN ITEMS node_config node_status)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_node_lib LABELS node unit)
endforeach()
relayweave_test(node_lifecycle node_lifecycle_test.cpp
    LIBRARIES relayweave_node_lib LABELS node integration)

relayweave_test(cluster_integration cluster_integration_test.cpp TIMEOUT 35
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(node_links node_links_test.cpp TIMEOUT 90
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(node_flows node_flows_test.cpp TIMEOUT 100
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(udp_session_routing udp_session_routing_test.cpp TIMEOUT 15
    LIBRARIES relayweave_node_lib LABELS node integration)
relayweave_test(topology topology_test.cpp
    LIBRARIES relayweave_node_lib LABELS node routing integration)
