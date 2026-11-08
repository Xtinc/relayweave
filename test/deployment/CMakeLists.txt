# Certificate generation and isolated file installation, temporary packages,
# and mocked systemctl; no host configuration or services are changed.
find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_test(NAME deployment COMMAND "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}" -p "*_test.py")
    set_tests_properties(deployment PROPERTIES TIMEOUT 180 LABELS "deployment;integration")
endif()

# OpenSSL selection/configuration also applies to native Windows builds.
if(Python3_Interpreter_FOUND AND WIN32 AND MSVC)
    add_test(NAME openssl_build COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/openssl_build_test.py")
    set_tests_properties(openssl_build PROPERTIES TIMEOUT 300 LABELS "build;unit"
        ENVIRONMENT "CMAKE_GENERATOR=${CMAKE_GENERATOR}")
endif()
