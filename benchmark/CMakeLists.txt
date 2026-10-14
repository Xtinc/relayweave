# Manual performance workloads, never mixed into functional CTest runs.
function(relayweave_benchmark name source)
    add_executable(benchmark_${name} ${source})
    target_link_libraries(benchmark_${name} PRIVATE ${ARGN})
    target_include_directories(benchmark_${name} PRIVATE "${PROJECT_SOURCE_DIR}/test/support")
    target_compile_definitions(benchmark_${name} PRIVATE
        BENCHMARK_NAME="${name}"
        BENCHMARK_BUILD="$<CONFIG>"
        BENCHMARK_COMPILER="${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}"
        BENCHMARK_RESULTS_DIR="${CMAKE_CURRENT_BINARY_DIR}/results")
endfunction()

relayweave_benchmark(common common_benchmark.cpp relayweave_protocol)
relayweave_benchmark(routing routing_benchmark.cpp relayweave_agent_lib relayweave_node_lib)
relayweave_benchmark(node node_benchmark.cpp relayweave_node_lib)
relayweave_benchmark(relay relay_benchmark.cpp relayweave_proxy_lib)
relayweave_benchmark(udp_node udp_node_benchmark.cpp relayweave_node_lib)
relayweave_benchmark(dual_index_map dual_index_map_benchmark.cpp relayweave_protocol)

relayweave_benchmark(flow flow_benchmark.cpp relayweave_protocol)
target_compile_definitions(benchmark_flow PRIVATE
    FLOW_BENCHMARK_BUILD="$<CONFIG>"
    FLOW_BENCHMARK_RESULTS_DIR="${CMAKE_CURRENT_BINARY_DIR}/results/flow")

add_custom_target(benchmarks DEPENDS benchmark_common benchmark_routing benchmark_node benchmark_relay
    benchmark_udp_node benchmark_dual_index_map benchmark_flow)
