include_guard(GLOBAL)

set(RELAYWEAVE_OPENSSL_BUILD_JOBS "1" CACHE STRING "Parallel jobs when building vendored OpenSSL")

if(OPENSSL_USE_STATIC_LIBS)
    enable_language(C)
endif()

function(relayweave_setup_openssl)
    if(NOT OPENSSL_USE_STATIC_LIBS)
        unset(OPENSSL_INCLUDE_DIR CACHE)
        unset(OPENSSL_SSL_LIBRARY CACHE)
        unset(OPENSSL_CRYPTO_LIBRARY CACHE)
        if(UNIX)
            set(CMAKE_FIND_LIBRARY_SUFFIXES "${CMAKE_SHARED_LIBRARY_SUFFIX}")
        endif()
        find_package(OpenSSL 3.2 REQUIRED COMPONENTS SSL Crypto)
        message(STATUS "Using system OpenSSL ${OPENSSL_VERSION} (shared)")
        return()
    endif()

    set(vendor_source "")
    set(vendor_version "0")
    file(GLOB version_files CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/vendor/openssl-*/VERSION.dat")
    foreach(version_file IN LISTS version_files)
        get_filename_component(candidate "${version_file}" DIRECTORY)
        if(NOT EXISTS "${candidate}/Configure")
            continue()
        endif()
        file(STRINGS "${version_file}" release_tag REGEX "^PRE_RELEASE_TAG=")
        if(NOT release_tag STREQUAL "PRE_RELEASE_TAG=")
            continue()
        endif()
        foreach(component IN ITEMS MAJOR MINOR PATCH)
            file(STRINGS "${version_file}" field REGEX "^${component}=[0-9]+$")
            string(REPLACE "${component}=" "" openssl_${component} "${field}")
        endforeach()
        set(version "${openssl_MAJOR}.${openssl_MINOR}.${openssl_PATCH}")
        if(NOT version MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$" OR
           version VERSION_LESS "3.2" OR NOT version VERSION_GREATER vendor_version)
            continue()
        endif()
        set(vendor_source "${candidate}")
        set(vendor_version "${version}")
    endforeach()

    if(NOT vendor_source)
        message(FATAL_ERROR
            "OpenSSL >= 3.2 source directory is required in vendor/openssl-<version>; "
            "system OpenSSL is not used")
    endif()

    if(NOT UNIX OR CMAKE_CROSSCOMPILING)
        message(FATAL_ERROR
            "Vendored OpenSSL currently supports native Unix builds only.")
    endif()
    if(NOT RELAYWEAVE_OPENSSL_BUILD_JOBS MATCHES "^[1-9][0-9]*$")
        message(FATAL_ERROR "RELAYWEAVE_OPENSSL_BUILD_JOBS must be a positive integer")
    endif()

    find_package(Perl REQUIRED)
    find_program(OPENSSL_MAKE_EXECUTABLE NAMES gmake make REQUIRED)
    include(ExternalProject)

    set(prefix "${PROJECT_BINARY_DIR}/_deps/openssl-${vendor_version}-static")
    set(install_dir "${prefix}/install")
    set(ssl_library "${install_dir}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(crypto_library "${install_dir}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    ExternalProject_Add(relayweave_openssl
        PREFIX "${prefix}"
        SOURCE_DIR "${vendor_source}"
        INSTALL_DIR "${install_dir}"
        CONFIGURE_COMMAND "${CMAKE_COMMAND}" -E env "CC=${CMAKE_C_COMPILER}"
            "${PERL_EXECUTABLE}" <SOURCE_DIR>/Configure
            "--prefix=<INSTALL_DIR>" "--openssldir=<INSTALL_DIR>/ssl" --libdir=lib
            no-shared no-apps no-docs no-module no-zlib no-zstd no-brotli
            -fPIC
        BUILD_COMMAND "${OPENSSL_MAKE_EXECUTABLE}" "-j${RELAYWEAVE_OPENSSL_BUILD_JOBS}"
        INSTALL_COMMAND "${OPENSSL_MAKE_EXECUTABLE}" install_sw
        BUILD_BYPRODUCTS "${ssl_library}" "${crypto_library}"
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
        LOG_OUTPUT_ON_FAILURE ON
    )

    file(MAKE_DIRECTORY "${install_dir}/include")
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${crypto_library}"
        INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
        INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS}"
    )
    add_dependencies(OpenSSL::Crypto relayweave_openssl)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${ssl_library}"
        INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
        INTERFACE_LINK_LIBRARIES OpenSSL::Crypto
    )
    add_dependencies(OpenSSL::SSL relayweave_openssl)
    message(STATUS "Using vendored OpenSSL ${vendor_version} (static): ${vendor_source}")
endfunction()
