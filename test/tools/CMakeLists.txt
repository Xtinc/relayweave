# Interactive diagnostics are not automatic tests.
add_executable(test_tls_channel_peer tls_channel_peer.cpp)
target_link_libraries(test_tls_channel_peer PRIVATE relayweave_protocol)
target_include_directories(test_tls_channel_peer PRIVATE "${PROJECT_SOURCE_DIR}/test/support")

add_executable(test_icmp icmp_test.cpp)
target_link_libraries(test_icmp PRIVATE relayweave_routing_lib)

# Manual container comparison; separate from the five production channel benchmarks.
foreach(name IN ITEMS fair_queue_compare fair_queue_memory)
    add_executable(${name} fair_queue_compare.cpp)
    target_link_libraries(${name} PRIVATE relayweave_protocol)
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}/test/support")
endforeach()
target_compile_definitions(fair_queue_memory PRIVATE FAIR_QUEUE_COUNT_ALLOCATIONS)
