add_library(relayweave_protocol
    src/message.cpp
    src/xfr_channel.cpp
    src/tls_channel.cpp
)
target_link_libraries(relayweave_protocol PUBLIC
    nlohmann_json::nlohmann_json
    asio
    OpenSSL::SSL
    OpenSSL::Crypto
)
target_include_directories(relayweave_protocol PUBLIC inc)
