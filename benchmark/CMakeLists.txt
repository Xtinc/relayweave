# Five manual workloads for the data path; never part of functional CTest runs.
function(relayweave_benchmark name)
    add_executable(benchmark_${name} ${name}_benchmark.cpp)
    target_link_libraries(benchmark_${name} PRIVATE relayweave_protocol)
    target_include_directories(benchmark_${name} PRIVATE "${PROJECT_SOURCE_DIR}/test/support")
    target_compile_definitions(benchmark_${name} PRIVATE
        BENCHMARK_NAME="${name}"
        BENCHMARK_BUILD="$<CONFIG>"
        BENCHMARK_COMPILER="${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}"
        BENCHMARK_RESULTS_DIR="${CMAKE_CURRENT_BINARY_DIR}/results")
endfunction()

relayweave_benchmark(tcp_relay)
relayweave_benchmark(tls_relay)
relayweave_benchmark(udp_relay)
relayweave_benchmark(flow)
relayweave_benchmark(congestion)

add_custom_target(benchmarks DEPENDS benchmark_tcp_relay benchmark_tls_relay benchmark_udp_relay
    benchmark_flow benchmark_congestion)
