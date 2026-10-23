foreach(name IN ITEMS agent_lifecycle agent_session forwarder)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_agent_lib LABELS agent integration)
endforeach()
relayweave_test(agent_routing agent_routing_test.cpp
    LIBRARIES relayweave_agent_lib LABELS agent routing unit)
relayweave_test(agent_path_cache agent_path_cache_test.cpp TIMEOUT 30
    LIBRARIES relayweave_agent_lib LABELS agent routing integration)
relayweave_test(node_connection node_connection_test.cpp
    LIBRARIES relayweave_agent_lib LABELS agent integration)

find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND AND UNIX)
    add_test(NAME agent_process COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/agent_process_test.py"
        "$<TARGET_FILE:relayweave_agent>" "${PROJECT_SOURCE_DIR}/test/data")
    set_tests_properties(agent_process PROPERTIES TIMEOUT 20 LABELS "agent;integration")
endif()
