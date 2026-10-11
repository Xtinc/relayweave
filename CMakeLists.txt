cmake_minimum_required(VERSION 3.21)
project(relayweave VERSION 1.0.4 LANGUAGES CXX)

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/asio-1.38.2/include/asio.hpp"
    OR NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/json-3.12.0/CMakeLists.txt"
    OR NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/json-3.12.0/include/nlohmann/json.hpp")
    find_package(Git REQUIRED)
    file(MAKE_DIRECTORY "${PROJECT_SOURCE_DIR}/vendor")
endif()

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/asio-1.38.2/include/asio.hpp")
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" clone --depth 1 --branch asio-1-38-2
            https://github.com/chriskohlhoff/asio.git
            "${PROJECT_SOURCE_DIR}/vendor/asio-1.38.2"
        COMMAND_ERROR_IS_FATAL ANY
    )
endif()

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/json-3.12.0/CMakeLists.txt"
    OR NOT EXISTS "${PROJECT_SOURCE_DIR}/vendor/json-3.12.0/include/nlohmann/json.hpp")
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" clone --depth 1 --branch v3.12.0
            https://github.com/nlohmann/json.git
            "${PROJECT_SOURCE_DIR}/vendor/json-3.12.0"
        COMMAND_ERROR_IS_FATAL ANY
    )
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(GCC_LOW_MEMORY "Trade GCC compile time for lower memory usage" ON)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND GCC_LOW_MEMORY)
    add_compile_options(--param=ggc-min-expand=20 --param=ggc-min-heapsize=65535)
endif()

set(CMAKE_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
)

configure_file(
    cmake/relayweave_version.h.in
    "${PROJECT_BINARY_DIR}/generated/relayweave_version.h"
    @ONLY
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_link_options(-static-libstdc++ -static-libgcc)
endif()

if(WIN32 AND NOT DEFINED OPENSSL_ROOT_DIR)
    set(OPENSSL_ROOT_DIR "D:/Program Files/OpenSSL-Win64")
endif()
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

add_library(asio INTERFACE)
target_include_directories(asio INTERFACE "${CMAKE_SOURCE_DIR}/vendor/asio-1.38.2/include")
target_compile_definitions(asio INTERFACE $<$<CXX_COMPILER_ID:MSVC>:_WIN32_WINNT=0x0601>)
target_link_libraries(asio INTERFACE Threads::Threads)

option(ENABLE_IO_URING "Use Asio's io_uring backend on Linux" OFF)

# Keep epoll as the default because io_uring can cause high iowait on some VMs.
if(ENABLE_IO_URING)
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(FATAL_ERROR "ENABLE_IO_URING is only supported on Linux")
    endif()
    if(CMAKE_SYSTEM_VERSION VERSION_LESS "5.10.0")
        message(FATAL_ERROR
            "ENABLE_IO_URING requires Linux 5.10 or newer (found ${CMAKE_SYSTEM_VERSION})")
    endif()

    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)

    target_compile_definitions(asio INTERFACE
        ASIO_HAS_IO_URING
        ASIO_DISABLE_EPOLL
    )
    target_link_libraries(asio INTERFACE PkgConfig::LIBURING)
    message(STATUS
        "Asio backend: io_uring (Linux ${CMAKE_SYSTEM_VERSION}, liburing ${LIBURING_VERSION})")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(STATUS "Asio backend: epoll (ENABLE_IO_URING=OFF)")
endif()

set(JSON_BuildTests OFF CACHE INTERNAL "")
add_subdirectory(vendor/json-3.12.0)

add_subdirectory(protocol)
add_subdirectory(route)
add_subdirectory(node)
add_subdirectory(agent)
add_subdirectory(proxy)
add_subdirectory(dashboard)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(test)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    include(cmake/Packaging.cmake)
endif()
