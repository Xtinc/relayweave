# Interactive diagnostics are not automatic tests.
add_executable(test_tls_channel_peer tls_channel_peer.cpp)
target_link_libraries(test_tls_channel_peer PRIVATE relayweave_protocol)
target_include_directories(test_tls_channel_peer PRIVATE "${PROJECT_SOURCE_DIR}/test/support")

add_executable(test_icmp icmp_test.cpp)
target_link_libraries(test_icmp PRIVATE relayweave_routing_lib)
