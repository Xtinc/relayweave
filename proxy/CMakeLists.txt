add_library(relayweave_proxy_lib
    src/proxy_server.cpp
)
target_link_libraries(relayweave_proxy_lib PUBLIC relayweave_protocol)
target_include_directories(relayweave_proxy_lib PUBLIC inc)

add_executable(relayweave_proxy src/main.cpp)
target_link_libraries(relayweave_proxy PRIVATE relayweave_proxy_lib)
target_include_directories(relayweave_proxy PRIVATE "${PROJECT_BINARY_DIR}/generated")
set_target_properties(relayweave_proxy PROPERTIES OUTPUT_NAME relayweave-proxy)

install(TARGETS relayweave_proxy
    RUNTIME DESTINATION usr/bin
    COMPONENT proxy
)
install(FILES "${PROJECT_SOURCE_DIR}/packaging/proxy/relayweave-proxy.service"
    DESTINATION lib/systemd/system
    COMPONENT proxy
)
install(FILES proxy.example.json
    DESTINATION etc/relayweave
    RENAME proxy.json
    COMPONENT proxy
)
install(PROGRAMS "${PROJECT_SOURCE_DIR}/scripts/provision_relayweave.sh"
    DESTINATION usr/sbin
    RENAME relayweave-provision-proxy
    COMPONENT proxy
)
