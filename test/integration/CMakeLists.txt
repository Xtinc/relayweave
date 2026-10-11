relayweave_test(relay_integration relay_integration_test.cpp TIMEOUT 15
    LIBRARIES relayweave_agent_lib relayweave_node_lib LABELS end_to_end integration)
relayweave_test(udp_relay_integration udp_relay_integration_test.cpp TIMEOUT 20
    LIBRARIES relayweave_agent_lib relayweave_node_lib LABELS end_to_end integration)
relayweave_test(relay_paths relay_paths_test.cpp TIMEOUT 60
    LIBRARIES relayweave_agent_lib relayweave_node_lib LABELS end_to_end cluster integration)
relayweave_test(agent_reconnect agent_reconnect_test.cpp TIMEOUT 15
    LIBRARIES relayweave_agent_lib relayweave_node_lib LABELS end_to_end agent integration)
relayweave_test(agent_cluster agent_cluster_test.cpp TIMEOUT 25
    LIBRARIES relayweave_agent_lib relayweave_node_lib LABELS end_to_end agent cluster integration)
relayweave_test(proxy_relay_integration proxy_relay_integration_test.cpp TIMEOUT 25
    LIBRARIES relayweave_proxy_lib relayweave_agent_lib relayweave_node_lib
    LABELS end_to_end proxy integration)
