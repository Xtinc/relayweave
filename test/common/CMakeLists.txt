foreach(name IN ITEMS async_event dual_index_map lru_cache pooled_buffer token_bucket traffic_counter)
    relayweave_test(${name} ${name}_test.cpp
        LIBRARIES relayweave_protocol LABELS common unit)
endforeach()
