# Certificate generation and isolated file installation, temporary packages,
# and mocked systemctl; no host configuration or services are changed.
find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_test(NAME deployment COMMAND "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}" -p "*_test.py")
    set_tests_properties(deployment PROPERTIES TIMEOUT 180 LABELS "deployment;integration")
endif()
