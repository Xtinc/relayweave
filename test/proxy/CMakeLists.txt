relayweave_test(proxy proxy_test.cpp TIMEOUT 15
    LIBRARIES relayweave_proxy_lib LABELS proxy integration)
relayweave_test(socks5_udp socks5_udp_test.cpp TIMEOUT 30
    LIBRARIES relayweave_proxy_lib LABELS proxy integration)
