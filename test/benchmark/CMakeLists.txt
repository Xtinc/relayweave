# Manual performance workloads, never mixed into functional CTest runs.
add_executable(benchmark_udp_node udp_node_benchmark.cpp)
target_link_libraries(benchmark_udp_node PRIVATE relayweave_node_lib)
target_include_directories(benchmark_udp_node PRIVATE "${PROJECT_SOURCE_DIR}/test/support")

add_executable(benchmark_dual_index_map dual_index_map_benchmark.cpp)
target_link_libraries(benchmark_dual_index_map PRIVATE relayweave_protocol)
target_include_directories(benchmark_dual_index_map PRIVATE "${PROJECT_SOURCE_DIR}/test/support")
