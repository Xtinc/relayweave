foreach(name IN ITEMS agent_lifecycle agent_session forwarder)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_agent_lib LABELS agent integration)
endforeach()
relayweave_test(agent_routing agent_routing_test.cpp
    LIBRARIES relayweave_agent_lib LABELS agent routing unit)
relayweave_test(agent_path_cache agent_path_cache_test.cpp TIMEOUT 30
    LIBRARIES relayweave_agent_lib LABELS agent routing integration)
