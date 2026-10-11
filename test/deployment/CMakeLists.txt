# Standard library, temporary directories, mocked systemctl and provisioning
# dry-run only; these tests need no Dashboard dependencies.
find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_test(NAME deployment COMMAND "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}" -p "*_test.py")
    set_tests_properties(deployment PROPERTIES TIMEOUT 30 LABELS "deployment;integration")
endif()
