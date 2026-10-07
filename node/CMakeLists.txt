add_library(relayweave_node_lib
    src/app_common.cpp
    src/pipeline_mgr.cpp
    src/datagram_mgr.cpp
    src/registry_mgr.cpp
    src/control_router.cpp
    src/node_config.cpp
    src/relay_node.cpp
    src/cluster_mgr.cpp
    src/topology.cpp
    src/nodelink_mgr.cpp
    src/lnk_channel.cpp
    src/lnk_channel_flows.cpp
)
target_link_libraries(relayweave_node_lib PUBLIC
    relayweave_protocol
    relayweave_routing_lib
)
target_include_directories(relayweave_node_lib PUBLIC inc)

add_executable(relayweave_node src/main.cpp)
target_link_libraries(relayweave_node PRIVATE relayweave_node_lib)
target_include_directories(relayweave_node PRIVATE "${PROJECT_BINARY_DIR}/generated")
set_target_properties(relayweave_node PROPERTIES OUTPUT_NAME relayweave-node)

install(TARGETS relayweave_node
    RUNTIME DESTINATION usr/bin
    COMPONENT node
)
install(FILES "${PROJECT_SOURCE_DIR}/packaging/node/relayweave-node.service"
    DESTINATION lib/systemd/system
    COMPONENT node
)
install(FILES node.example.json
    DESTINATION etc/relayweave
    RENAME node.json
    COMPONENT node
)
install(DIRECTORY DESTINATION etc/relayweave/certs COMPONENT node)
install(PROGRAMS "${PROJECT_SOURCE_DIR}/scripts/provision_relayweave.sh"
    DESTINATION usr/sbin
    RENAME relayweave-provision-node
    COMPONENT node
)
