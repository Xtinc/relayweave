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

    if(CMAKE_CROSSCOMPILING OR (NOT UNIX AND NOT (WIN32 AND MSVC)))
        message(FATAL_ERROR
            "Vendored OpenSSL supports native Unix and Windows MSVC builds only.")
    endif()
    if(NOT RELAYWEAVE_OPENSSL_BUILD_JOBS MATCHES "^[1-9][0-9]*$")
        message(FATAL_ERROR "RELAYWEAVE_OPENSSL_BUILD_JOBS must be a positive integer")
    endif()

    find_package(Perl REQUIRED)
    include(ExternalProject)

    set(prefix "${PROJECT_BINARY_DIR}/_deps/openssl-${vendor_version}-static")
    set(install_dir "${prefix}/install")
    set(binary_dir "${prefix}/src/relayweave_openssl-build")
    set(command_prefix "")
    set(configure_options "")
    set(crypto_dependencies "Threads::Threads;${CMAKE_DL_LIBS}")
    if(WIN32 AND MSVC)
        # Configure and nmake need the matching Visual Studio developer environment.
        find_program(OPENSSL_MAKE_EXECUTABLE NAMES nmake REQUIRED)
        execute_process(COMMAND "${PERL_EXECUTABLE}" -e "print $^O"
            OUTPUT_VARIABLE perl_platform RESULT_VARIABLE perl_result)
        if(NOT perl_result EQUAL 0 OR NOT perl_platform STREQUAL "MSWin32")
            message(FATAL_ERROR "Windows OpenSSL requires native Windows Perl (e.g. Strawberry Perl)")
        endif()
        if(CMAKE_C_COMPILER_ARCHITECTURE_ID STREQUAL "x64")
            set(openssl_target VC-WIN64A)
        elseif(CMAKE_C_COMPILER_ARCHITECTURE_ID STREQUAL "X86")
            set(openssl_target VC-WIN32)
        elseif(CMAKE_C_COMPILER_ARCHITECTURE_ID STREQUAL "ARM64")
            set(openssl_target VC-WIN64-ARM)
        else()
            message(FATAL_ERROR "Unsupported OpenSSL MSVC architecture: ${CMAKE_C_COMPILER_ARCHITECTURE_ID}")
        endif()

        # A Visual Studio build tree can switch configurations without mixing CRTs.
        get_property(multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
        if(multi_config)
            set(openssl_configs "${CMAKE_CONFIGURATION_TYPES}")
            set(openssl_config "$<CONFIG>")
        else()
            set(openssl_config "${CMAKE_BUILD_TYPE}")
            if(NOT openssl_config)
                set(openssl_config Release)
            endif()
            set(openssl_configs "${openssl_config}")
        endif()
        set(binary_dir "${prefix}/build")
        foreach(config IN LISTS openssl_configs)
            file(MAKE_DIRECTORY "${binary_dir}/${config}" "${install_dir}/${config}/include")
        endforeach()
        set(command_prefix "${CMAKE_COMMAND}" -E chdir "${binary_dir}/${openssl_config}")
        string(APPEND install_dir "/${openssl_config}")
        set(runtime_library "${CMAKE_MSVC_RUNTIME_LIBRARY}")
        if(NOT DEFINED CMAKE_MSVC_RUNTIME_LIBRARY)
            set(runtime_library "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
        endif()
        set(runtime_flag "")
        set(runtime_libraries MultiThreaded MultiThreadedDebug MultiThreadedDLL MultiThreadedDebugDLL)
        set(runtime_flags /MT /MTd /MD /MDd)
        foreach(runtime flag IN ZIP_LISTS runtime_libraries runtime_flags)
            string(APPEND runtime_flag "$<$<STREQUAL:${runtime_library},${runtime}>:${flag}>")
        endforeach()
        list(APPEND configure_options "${openssl_target}"
            "--$<IF:$<CONFIG:Debug>,debug,release>" "${runtime_flag}")
        set(build_command "${OPENSSL_MAKE_EXECUTABLE}" /NOLOGO)
        set(install_command "${OPENSSL_MAKE_EXECUTABLE}" /NOLOGO install_sw)
        list(APPEND crypto_dependencies ws2_32 gdi32 advapi32 crypt32 user32)
    else()
        find_program(OPENSSL_MAKE_EXECUTABLE NAMES gmake make REQUIRED)
        list(APPEND configure_options -fPIC)
        set(build_command "${OPENSSL_MAKE_EXECUTABLE}" "-j${RELAYWEAVE_OPENSSL_BUILD_JOBS}")
        set(install_command "${OPENSSL_MAKE_EXECUTABLE}" install_sw)
    endif()
    ExternalProject_Add(relayweave_openssl
        PREFIX "${prefix}"
        SOURCE_DIR "${vendor_source}"
        BINARY_DIR "${binary_dir}"
        INSTALL_DIR "${prefix}/install"
        CONFIGURE_COMMAND ${command_prefix}
        "${CMAKE_COMMAND}" -E env "CC=${CMAKE_C_COMPILER}"
        "${PERL_EXECUTABLE}" <SOURCE_DIR>/Configure
        "--prefix=${install_dir}" "--openssldir=${install_dir}/ssl" --libdir=lib
        no-shared no-apps no-docs no-module no-zlib no-zstd no-brotli
        ${configure_options}
        BUILD_COMMAND ${command_prefix} ${build_command}
        INSTALL_COMMAND ${command_prefix} ${install_command}
        BUILD_BYPRODUCTS "${install_dir}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${install_dir}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}"
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
        LOG_OUTPUT_ON_FAILURE ON
    )

    if(NOT (WIN32 AND MSVC))
        file(MAKE_DIRECTORY "${install_dir}/include")
    endif()
    foreach(component IN ITEMS Crypto SSL)
        string(TOLOWER "${component}" library_name)
        add_library(OpenSSL::${component} STATIC IMPORTED GLOBAL)
        set_target_properties(OpenSSL::${component} PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include")
        if(WIN32 AND MSVC)
            # IMPORTED_LOCATION does not evaluate generator expressions.
            set_property(TARGET OpenSSL::${component} PROPERTY IMPORTED_CONFIGURATIONS "${openssl_configs}")
            foreach(config IN LISTS openssl_configs)
                string(TOUPPER "${config}" config_upper)
                set_target_properties(OpenSSL::${component} PROPERTIES
                    "IMPORTED_LOCATION_${config_upper}" "${prefix}/install/${config}/lib/lib${library_name}.lib")
            endforeach()
        else()
            set_target_properties(OpenSSL::${component} PROPERTIES
                IMPORTED_LOCATION "${install_dir}/lib/lib${library_name}${CMAKE_STATIC_LIBRARY_SUFFIX}")
        endif()
        add_dependencies(OpenSSL::${component} relayweave_openssl)
    endforeach()
    set_property(TARGET OpenSSL::Crypto PROPERTY INTERFACE_LINK_LIBRARIES "${crypto_dependencies}")
    set_property(TARGET OpenSSL::SSL PROPERTY INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)
    message(STATUS "Using vendored OpenSSL ${vendor_version} (static): ${vendor_source}")
endfunction()
