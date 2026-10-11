add_library(relayweave_agent_lib
    src/agent_routing.cpp
    src/relay_agent.cpp
    src/forwarder.cpp
    src/agent_session.cpp
)
target_link_libraries(relayweave_agent_lib PUBLIC relayweave_protocol relayweave_routing_lib)
target_include_directories(relayweave_agent_lib PUBLIC inc)

add_executable(relayweave_agent src/main.cpp)
target_link_libraries(relayweave_agent PRIVATE relayweave_agent_lib)
target_include_directories(relayweave_agent PRIVATE "${PROJECT_BINARY_DIR}/generated")
set_target_properties(relayweave_agent PROPERTIES OUTPUT_NAME relayweave-agent)

install(TARGETS relayweave_agent
    RUNTIME DESTINATION usr/bin
    COMPONENT agent
)
install(FILES "${PROJECT_SOURCE_DIR}/packaging/agent/relayweave-agent.service"
    DESTINATION lib/systemd/system
    COMPONENT agent
)
install(FILES agent.example.json
    DESTINATION etc/relayweave
    RENAME agent.json
    COMPONENT agent
)
install(DIRECTORY DESTINATION etc/relayweave/certs COMPONENT agent)
install(PROGRAMS "${PROJECT_SOURCE_DIR}/scripts/provision_relayweave.sh"
    DESTINATION usr/sbin
    RENAME relayweave-provision-agent
    COMPONENT agent
)
