# Pure statistics and graph tests need neither network access nor raw socket privileges.
add_executable(test_link_quality link_quality_test.cpp)
target_link_libraries(test_link_quality PRIVATE relayweave_routing_lib)
add_test(NAME link_quality COMMAND test_link_quality)

add_executable(test_routing routing_test.cpp)
target_link_libraries(test_routing PRIVATE relayweave_routing_lib)
add_test(NAME routing COMMAND test_routing "${CMAKE_CURRENT_BINARY_DIR}/routing_test_report.html")
set_tests_properties(link_quality routing PROPERTIES LABELS "routing;unit")

# DNS and shutdown checks run without CAP_NET_RAW.
add_executable(test_probe_set probe_set_test.cpp)
target_link_libraries(test_probe_set PRIVATE relayweave_routing_lib)
add_test(NAME probe_set COMMAND test_probe_set)
set_tests_properties(probe_set PROPERTIES TIMEOUT 10 LABELS "routing;integration")

# Real ICMP also exercises Agent composition; only permission errors skip the test.
add_executable(test_probe_integration probe_integration_test.cpp)
target_link_libraries(test_probe_integration PRIVATE relayweave_agent_lib)
add_test(NAME probe_integration COMMAND test_probe_integration)
set_tests_properties(probe_integration PROPERTIES TIMEOUT 20 SKIP_RETURN_CODE 77
    LABELS "routing;integration;icmp")

# Manual diagnostic, not a CTest case. Optional count defaults to four probes.
add_executable(test_icmp icmp_test.cpp)
target_link_libraries(test_icmp PRIVATE relayweave_routing_lib)
