cmake_minimum_required(VERSION 3.24)
project(relayweave VERSION 1.0.7 LANGUAGES CXX)

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

find_package(Threads REQUIRED)
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL 3.2 REQUIRED COMPONENTS SSL Crypto)

include(FetchContent)
FetchContent_Declare(asio
    URL https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-38-2.tar.gz
    URL_HASH SHA256=9f2648fa483e58a6bf848d970ee0ea650ca19ed7769dfa520ed4f7b8d27af1db
)
FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/archive/refs/tags/v3.12.0.tar.gz
    URL_HASH SHA256=4b92eb0c06d10683f7447ce9406cb97cd4b453be18d7279320f7b2f025c10187
)
set(JSON_BuildTests OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(asio nlohmann_json)

add_library(asio INTERFACE)
target_include_directories(asio INTERFACE "${asio_SOURCE_DIR}/include")
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

option(BUILD_BENCHMARKS "Build manual performance benchmarks" OFF)
if(BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    include(cmake/Packaging.cmake)
endif()
