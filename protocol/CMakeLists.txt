add_library(relayweave_protocol
    src/message.cpp
    src/xfr_channel.cpp
    src/tls_channel.cpp
    src/tls_rpk.cpp
    src/lnk_channel.cpp
    src/flow_protocol.cpp
)
target_link_libraries(relayweave_protocol PUBLIC
    nlohmann_json::nlohmann_json
    asio
    OpenSSL::SSL
    OpenSSL::Crypto
)
if(OPENSSL_USE_STATIC_LIBS)
    target_link_libraries(relayweave_protocol PUBLIC ZLIB::ZLIB zstd::libzstd_static)
endif()
target_include_directories(relayweave_protocol PUBLIC inc)
