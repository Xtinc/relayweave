# Pure statistics and graph tests need neither network nor raw socket privileges.
relayweave_test(link_quality link_quality_test.cpp
    LIBRARIES relayweave_routing_lib LABELS routing unit)
relayweave_test(routing routing_test.cpp
    LIBRARIES relayweave_routing_lib LABELS routing unit
    ARGS "${CMAKE_CURRENT_BINARY_DIR}/routing_test_report.html")

relayweave_test(probe_set probe_set_test.cpp
    LIBRARIES relayweave_routing_lib LABELS routing integration)
relayweave_test(probe_integration probe_integration_test.cpp TIMEOUT 20
    LIBRARIES relayweave_agent_lib LABELS routing integration icmp)
set_tests_properties(probe_integration PROPERTIES SKIP_RETURN_CODE 77)
