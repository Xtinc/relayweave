foreach(name IN ITEMS node_config node_status)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_node_lib LABELS node unit)
endforeach()
relayweave_test(node_lifecycle node_lifecycle_test.cpp
    LIBRARIES relayweave_node_lib LABELS node integration)
relayweave_test(node_shutdown node_shutdown_test.cpp
    LIBRARIES relayweave_node_lib LABELS node integration)
relayweave_test(node_metrics node_metrics_test.cpp
    LIBRARIES relayweave_node_lib LABELS node integration)
relayweave_test(node_relay_capacity node_relay_capacity_test.cpp
    LIBRARIES relayweave_node_lib LABELS node integration)

relayweave_test(cluster_integration cluster_integration_test.cpp TIMEOUT 90
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(node_links node_links_test.cpp TIMEOUT 90
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(node_flows node_flows_test.cpp TIMEOUT 100
    LIBRARIES relayweave_node_lib LABELS node cluster integration)
relayweave_test(udp_session_routing udp_session_routing_test.cpp TIMEOUT 15
    LIBRARIES relayweave_node_lib LABELS node integration)
relayweave_test(topology topology_test.cpp
    LIBRARIES relayweave_node_lib LABELS node routing integration)

find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND AND UNIX)
    add_test(NAME node_process COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/node_process_test.py"
        "$<TARGET_FILE:relayweave_node>" "${PROJECT_SOURCE_DIR}/test/data")
    set_tests_properties(node_process PROPERTIES TIMEOUT 15 LABELS "node;integration")
endif()
