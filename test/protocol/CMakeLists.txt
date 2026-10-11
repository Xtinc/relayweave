foreach(name IN ITEMS relay_protocol lnk_frame)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_protocol LABELS protocol unit)
endforeach()

# These use real loopback sockets or cross-executor channel delivery.
foreach(name IN ITEMS tcp_relay udp_relay lnk_flow_receive)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_protocol LABELS protocol integration)
endforeach()
relayweave_test(tls_channel tls_channel_test.cpp NAME tls_channel_mtls TIMEOUT 15
    LIBRARIES relayweave_protocol LABELS protocol integration)
relayweave_test(lnk_channel_flows lnk_channel_flows_test.cpp TIMEOUT 45
    LIBRARIES relayweave_protocol LABELS protocol integration)
