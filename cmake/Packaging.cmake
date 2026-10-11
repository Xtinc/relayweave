set(RELAYWEAVE_PACKAGE_RELEASE "2" CACHE STRING "Debian package revision")
set(RELAYWEAVE_PACKAGE_MAINTAINER "xtc <784092840@qq.com>" CACHE STRING "Debian package maintainer")

set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_NAME "relayweave")
set(CPACK_PACKAGE_VENDOR "RelayWeave")
set(CPACK_PACKAGE_CONTACT "${RELAYWEAVE_PACKAGE_MAINTAINER}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Distributed cluster relay proxy")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DIRECTORY "${PROJECT_BINARY_DIR}/packages")
set(CPACK_PACKAGING_INSTALL_PREFIX "/")
set(CPACK_VERBATIM_VARIABLES YES)

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_COMPONENTS_GROUPING IGNORE)
set(CPACK_COMPONENTS_ALL agent node proxy dashboard)

set(CPACK_COMPONENT_AGENT_DISPLAY_NAME "RelayWeave Agent")
set(CPACK_COMPONENT_AGENT_DESCRIPTION "RelayWeave service publishing and local forwarding agent")
set(CPACK_COMPONENT_NODE_DISPLAY_NAME "RelayWeave Node")
set(CPACK_COMPONENT_NODE_DESCRIPTION "RelayWeave clustered relay node")
set(CPACK_COMPONENT_PROXY_DISPLAY_NAME "RelayWeave Application Proxy")
set(CPACK_COMPONENT_PROXY_DESCRIPTION "RelayWeave HTTP, HTTPS CONNECT, and SOCKS5 application proxy")

set(CPACK_DEBIAN_PACKAGE_RELEASE "${RELAYWEAVE_PACKAGE_RELEASE}")
set(CPACK_DEBIAN_PACKAGE_SECTION "net")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)

set(CPACK_DEBIAN_AGENT_PACKAGE_NAME "relayweave-agent")
set(CPACK_DEBIAN_AGENT_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_AGENT_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_AGENT_PACKAGE_DEPENDS "systemd, openssl")
set(CPACK_DEBIAN_AGENT_PACKAGE_DESCRIPTION
    "RelayWeave agent for publishing services and forwarding local TCP, TLS, and UDP traffic.")

set(CPACK_DEBIAN_NODE_PACKAGE_NAME "relayweave-node")
set(CPACK_DEBIAN_NODE_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_NODE_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_NODE_PACKAGE_DEPENDS "systemd, openssl")
set(CPACK_DEBIAN_NODE_PACKAGE_DESCRIPTION
    "RelayWeave node for clustered service discovery and TCP, TLS, and UDP traffic relay.")

set(CPACK_DEBIAN_PROXY_PACKAGE_NAME "relayweave-proxy")
set(CPACK_DEBIAN_PROXY_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PROXY_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PROXY_PACKAGE_DEPENDS "systemd")
set(CPACK_DEBIAN_PROXY_PACKAGE_DESCRIPTION
    "RelayWeave application proxy providing HTTP forwarding, HTTPS CONNECT, and SOCKS5 TCP CONNECT.")

set(CPACK_COMPONENT_DASHBOARD_DISPLAY_NAME "RelayWeave Dashboard")
set(CPACK_COMPONENT_DASHBOARD_DESCRIPTION "Independent cluster dashboard published through RelayWeave Agent")
set(CPACK_DEBIAN_DASHBOARD_PACKAGE_NAME "relayweave-dashboard")
set(CPACK_DEBIAN_DASHBOARD_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_DASHBOARD_PACKAGE_ARCHITECTURE "all")
set(CPACK_DEBIAN_DASHBOARD_PACKAGE_DEPENDS "systemd, adduser, openssl, python3 (>= 3.10), python3-flask, python3-cbor2, python3-waitress")
set(CPACK_DEBIAN_DASHBOARD_PACKAGE_DESCRIPTION
    "Single-process cluster dashboard with persistent history and a loopback HTTP listener, published by a separate RelayWeave Agent.")

foreach(component IN ITEMS agent node proxy dashboard)
    # A CRLF checkout makes the kernel look for /bin/sh\r in maintainer
    # scripts. Normalize the packaged copies even if Git attributes were bypassed.
    string(TOUPPER "${component}" component_upper)
    set(control_files "")
    foreach(control_file IN ITEMS conffiles postinst prerm postrm)
        set(control_output "${PROJECT_BINARY_DIR}/packaging/${component}/${control_file}")
        configure_file(
            "${PROJECT_SOURCE_DIR}/packaging/${component}/${control_file}"
            "${control_output}"
            @ONLY NEWLINE_STYLE UNIX
        )
        list(APPEND control_files "${control_output}")
    endforeach()
    set(CPACK_DEBIAN_${component_upper}_PACKAGE_CONTROL_EXTRA "${control_files}")

    install(FILES
        "${PROJECT_SOURCE_DIR}/docs/RelayWeave设计.md"
        "${PROJECT_SOURCE_DIR}/docs/RelayWeave打包与安装.md"
        "${PROJECT_SOURCE_DIR}/docs/证书制作与部署.md"
        DESTINATION "usr/share/doc/relayweave-${component}"
        COMPONENT ${component}
    )
endforeach()

include(CPack)
