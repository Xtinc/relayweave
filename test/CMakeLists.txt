add_executable(test_tls_channel_peer tls_channel_peer.cpp)
target_link_libraries(test_tls_channel_peer PRIVATE relayweave_protocol)

add_executable(test_tls_channel tls_channel_test.cpp)
target_link_libraries(test_tls_channel PRIVATE relayweave_protocol)
add_test(NAME tls_channel_mtls COMMAND test_tls_channel)

add_executable(test_cluster_integration cluster_integration_test.cpp)
target_link_libraries(test_cluster_integration PRIVATE relayweave_node_lib)
add_test(NAME cluster_integration COMMAND test_cluster_integration)
set_tests_properties(cluster_integration PROPERTIES TIMEOUT 35)

add_executable(test_node_links node_links_test.cpp)
target_link_libraries(test_node_links PRIVATE relayweave_node_lib)
add_test(NAME node_links COMMAND test_node_links)
set_tests_properties(node_links PROPERTIES TIMEOUT 90 LABELS "cluster;integration")

add_executable(test_lnk_frame lnk_frame_test.cpp)
target_link_libraries(test_lnk_frame PRIVATE relayweave_protocol)
add_test(NAME lnk_frame COMMAND test_lnk_frame)
set_tests_properties(lnk_frame PROPERTIES TIMEOUT 10 LABELS "protocol;unit")

add_executable(test_pooled_buffer pooled_buffer_test.cpp)
target_link_libraries(test_pooled_buffer PRIVATE relayweave_protocol)
add_test(NAME pooled_buffer COMMAND test_pooled_buffer)
set_tests_properties(pooled_buffer PROPERTIES TIMEOUT 10 LABELS "protocol;unit")

add_executable(test_node_flows node_flows_test.cpp)
target_link_libraries(test_node_flows PRIVATE relayweave_node_lib)
add_test(NAME node_flows COMMAND test_node_flows)
set_tests_properties(node_flows PROPERTIES TIMEOUT 100 LABELS "cluster;integration")

add_executable(test_lnk_channel_flows lnk_channel_flows_test.cpp)
target_link_libraries(test_lnk_channel_flows PRIVATE relayweave_protocol)
add_test(NAME lnk_channel_flows COMMAND test_lnk_channel_flows)
set_tests_properties(lnk_channel_flows PROPERTIES TIMEOUT 45 LABELS "protocol;unit")

add_subdirectory(route)

add_executable(test_agent_routing agent_routing_test.cpp)
target_link_libraries(test_agent_routing PRIVATE relayweave_agent_lib)
add_test(NAME agent_routing COMMAND test_agent_routing)
set_tests_properties(agent_routing PROPERTIES LABELS "routing;unit")

add_executable(test_topology topology_test.cpp)
target_link_libraries(test_topology PRIVATE relayweave_node_lib)
add_test(NAME topology COMMAND test_topology)
set_tests_properties(topology PROPERTIES LABELS "routing;unit")

add_executable(test_relay_integration relay_integration_test.cpp)
target_link_libraries(test_relay_integration PRIVATE relayweave_agent_lib relayweave_node_lib)
add_test(NAME relay_integration COMMAND test_relay_integration)
set_tests_properties(relay_integration PROPERTIES
    TIMEOUT 15
)

add_executable(test_udp_relay_integration udp_relay_integration_test.cpp)
target_link_libraries(test_udp_relay_integration PRIVATE relayweave_agent_lib relayweave_node_lib)
add_test(NAME udp_relay_integration COMMAND test_udp_relay_integration)
set_tests_properties(udp_relay_integration PROPERTIES TIMEOUT 20)

add_executable(test_udp_session_routing udp_session_routing_test.cpp)
target_link_libraries(test_udp_session_routing PRIVATE relayweave_node_lib)
add_test(NAME udp_session_routing COMMAND test_udp_session_routing)
set_tests_properties(udp_session_routing PROPERTIES TIMEOUT 15)

add_executable(benchmark_udp_node udp_node_benchmark.cpp)
target_link_libraries(benchmark_udp_node PRIVATE relayweave_node_lib)

add_executable(test_relay_protocol relay_protocol_test.cpp)
target_link_libraries(test_relay_protocol PRIVATE relayweave_protocol)
add_test(NAME relay_protocol COMMAND test_relay_protocol)

add_executable(test_dual_index_map dual_index_map_test.cpp)
target_link_libraries(test_dual_index_map PRIVATE relayweave_protocol)
add_test(NAME dual_index_map COMMAND test_dual_index_map)

add_executable(test_async_event async_event_test.cpp)
target_link_libraries(test_async_event PRIVATE relayweave_protocol)
add_test(NAME async_event COMMAND test_async_event)
set_tests_properties(async_event PROPERTIES TIMEOUT 10 LABELS "protocol;unit")

add_executable(test_lru_cache lru_cache_test.cpp)
target_link_libraries(test_lru_cache PRIVATE relayweave_protocol)
add_test(NAME lru_cache COMMAND test_lru_cache)
set_tests_properties(lru_cache PROPERTIES LABELS "unit")

add_executable(test_agent_path_cache agent_path_cache_test.cpp)
target_link_libraries(test_agent_path_cache PRIVATE relayweave_agent_lib)
add_test(NAME agent_path_cache COMMAND test_agent_path_cache)
set_tests_properties(agent_path_cache PROPERTIES TIMEOUT 30 LABELS "routing;integration")

add_executable(benchmark_dual_index_map dual_index_map_benchmark.cpp)
target_link_libraries(benchmark_dual_index_map PRIVATE relayweave_protocol)

add_executable(test_agent_lifecycle agent_lifecycle_test.cpp)
target_link_libraries(test_agent_lifecycle PRIVATE relayweave_agent_lib)
add_test(NAME agent_lifecycle COMMAND test_agent_lifecycle)
set_tests_properties(agent_lifecycle PROPERTIES TIMEOUT 10)

add_executable(test_agent_reconnect agent_reconnect_test.cpp)
target_link_libraries(test_agent_reconnect PRIVATE relayweave_agent_lib relayweave_node_lib)
add_test(NAME agent_reconnect COMMAND test_agent_reconnect)
set_tests_properties(agent_reconnect PROPERTIES TIMEOUT 15)

add_executable(test_agent_cluster agent_cluster_test.cpp)
target_link_libraries(test_agent_cluster PRIVATE relayweave_agent_lib relayweave_node_lib)
add_test(NAME agent_cluster COMMAND test_agent_cluster)
set_tests_properties(agent_cluster PROPERTIES TIMEOUT 25)

add_executable(test_proxy proxy_test.cpp)
target_link_libraries(test_proxy PRIVATE relayweave_proxy_lib)
add_test(NAME proxy COMMAND test_proxy)
set_tests_properties(proxy PROPERTIES TIMEOUT 15)

add_executable(test_proxy_relay_integration proxy_relay_integration_test.cpp)
target_link_libraries(test_proxy_relay_integration PRIVATE
    relayweave_proxy_lib
    relayweave_agent_lib
    relayweave_node_lib
)
add_test(NAME proxy_relay_integration COMMAND test_proxy_relay_integration)
set_tests_properties(proxy_relay_integration PROPERTIES TIMEOUT 25)

set(TLS_CHANNEL_PEER_DATA_FILES
    data/tls_channel_debug.json
    data/tls_channel_test_ca.pem
    data/tls_channel_test_server.pem
    data/tls_channel_test_server.key
    data/tls_channel_test_client_ca.pem
    data/tls_channel_test_client.pem
    data/tls_channel_test_client.key
    data/node_config_modern.json
    data/node_config_legacy.json
)

foreach(test_target IN ITEMS test_tls_channel_peer test_tls_channel test_relay_integration test_udp_relay_integration
                             test_udp_session_routing test_agent_reconnect test_agent_cluster benchmark_udp_node
                             test_proxy_relay_integration test_agent_path_cache)
    foreach(data_file IN LISTS TLS_CHANNEL_PEER_DATA_FILES)
        add_custom_command(TARGET ${test_target} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory
            "$<TARGET_FILE_DIR:${test_target}>/data"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${CMAKE_CURRENT_SOURCE_DIR}/${data_file}"
            "$<TARGET_FILE_DIR:${test_target}>/${data_file}"
            VERBATIM
        )
    endforeach()
endforeach()
