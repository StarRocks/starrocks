# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

set(EXEC_FILES
        ./test_main.cpp
        ./column/array_column_test.cpp
        ./column/binary_column_test.cpp
        ./column/chunk_test.cpp
        ./column/column_helper_test.cpp
        ./column/column_pool_test.cpp
        ./column/const_column_test.cpp
        ./column/date_value_test.cpp
        ./column/field_test.cpp
        ./column/fixed_length_column_test.cpp
        ./column/decimalv3_column_test.cpp
        ./column/nullable_column_test.cpp
        ./column/object_column_test.cpp
        ./column/timestamp_value_test.cpp
        ./column/vectorized_schema_test.cpp
        ./common/config_test.cpp
        ./common/status_test.cpp
        ./env/compressed_file_test.cpp
        ./env/env_broker_test.cpp
        ./env/env_posix_test.cpp
        ./env/env_memory_test.cpp
        ./env/output_stream_wrapper_test.cpp
        ./exec/buffered_reader_test.cpp
        ./exec/es_query_builder_test.cpp
        ./exec/column_value_range_test.cpp
        ./exec/plain_text_line_reader_bzip_test.cpp
        ./exec/plain_text_line_reader_gzip_test.cpp
        ./exec/plain_text_line_reader_lz4frame_test.cpp
        ./exec/plain_text_line_reader_uncompressed_test.cpp
        ./exec/vectorized/agg_hash_map_test.cpp
        #./exec/vectorized/csv_scanner_test.cpp
        ./exec/vectorized/chunks_sorter_test.cpp
        ./exec/vectorized/chunks_sorter_heapsorter_test.cpp
        ./exec/vectorized/join_hash_map_test.cpp
        ./exec/vectorized/json_scanner_test.cpp
        ./exec/vectorized/json_parser_test.cpp
        ./exec/vectorized/hdfs_scanner_test.cpp
        ./exec/vectorized/orc_scanner_adapter_test.cpp
        ./exec/vectorized/table_function_node_test.cpp
        ./exec/vectorized/analytor_test.cpp
        ./exec/pipeline/pipeline_test_base.cpp
        ./exec/pipeline/table_function_operator_test.cpp
        ./exec/parquet/parquet_schema_test.cpp
        ./exec/parquet/encoding_test.cpp
        ./exec/parquet/page_reader_test.cpp
        ./exec/parquet/metadata_test.cpp
        ./exec/parquet/group_reader_test.cpp
        ./exec/parquet/file_reader_test.cpp
        ./exprs/agg/aggregate_test.cpp
        ./exprs/vectorized/arithmetic_expr_test.cpp
        ./exprs/vectorized/arithmetic_operation_test.cpp
        ./exprs/vectorized/array_element_expr_test.cpp
        ./exprs/vectorized/decimal_binary_function_test.cpp
        ./exprs/vectorized/array_expr_test.cpp
        ./exprs/vectorized/array_functions_test.cpp
        ./exprs/vectorized/binary_functions_test.cpp
        ./exprs/vectorized/binary_predicate_test.cpp
        ./exprs/vectorized/bitmap_functions_test.cpp
        ./exprs/vectorized/case_expr_test.cpp
        ./exprs/vectorized/cast_expr_test.cpp
        ./exprs/vectorized/decimal_cast_expr_decimal_test.cpp
        ./exprs/vectorized/decimal_cast_expr_integer_test.cpp
        ./exprs/vectorized/decimal_cast_expr_float_test.cpp
        ./exprs/vectorized/decimal_cast_expr_time_test.cpp
        ./exprs/vectorized/decimal_cast_expr_decimalv2_test.cpp
        ./exprs/vectorized/coalesce_expr_test.cpp
        ./exprs/vectorized/compound_predicate_test.cpp
        ./exprs/vectorized/condition_expr_test.cpp
        ./exprs/vectorized/encryption_functions_test.cpp
        ./exprs/vectorized/function_call_expr_test.cpp
        ./exprs/vectorized/geography_functions_test.cpp
        ./exprs/vectorized/hash_functions_test.cpp
        ./exprs/vectorized/hyperloglog_functions_test.cpp
        ./exprs/vectorized/if_expr_test.cpp
        ./exprs/vectorized/function_helper_test.cpp
        ./exprs/vectorized/in_iterator_predicate_test.cpp
        ./exprs/vectorized/in_predicate_test.cpp
        ./exprs/vectorized/is_null_predicate_test.cpp
        ./exprs/vectorized/json_functions_test.cpp
        ./exprs/vectorized/like_test.cpp
        ./exprs/vectorized/math_functions_test.cpp
        ./exprs/vectorized/null_if_expr_test.cpp
        ./exprs/vectorized/percentile_functions_test.cpp
        ./exprs/vectorized/string_fn_concat_test.cpp
        ./exprs/vectorized/string_fn_locate_test.cpp
        ./exprs/vectorized/string_fn_pad_test.cpp
        ./exprs/vectorized/string_fn_repeat_test.cpp
        ./exprs/vectorized/string_fn_reverse_test.cpp
        ./exprs/vectorized/string_fn_sm3_test.cpp
        ./exprs/vectorized/string_fn_space_test.cpp
        ./exprs/vectorized/string_fn_substr_test.cpp
        ./exprs/vectorized/string_fn_test.cpp
        ./exprs/vectorized/string_fn_trim_test.cpp
        ./exprs/vectorized/string_fn_money_format_decimal_test.cpp
        ./exprs/vectorized/time_functions_test.cpp
        ./exprs/vectorized/es_functions_test.cpp
        ./exprs/vectorized/utility_functions_test.cpp
        ./exprs/vectorized/runtime_filter_test.cpp
        ./formats/csv/array_converter_test.cpp
        ./formats/csv/binary_converter_test.cpp
        ./formats/csv/boolean_converter_test.cpp
        ./formats/csv/date_converter_test.cpp
        ./formats/csv/datetime_converter_test.cpp
        ./formats/csv/decimalv2_converter_test.cpp
        ./formats/csv/float_converter_test.cpp
        ./formats/csv/nullable_converter_test.cpp
        ./formats/csv/numeric_converter_test.cpp
        ./formats/json/binary_column_test.cpp
        ./formats/json/numeric_column_test.cpp
        ./formats/json/nullable_column_test.cpp
        ./geo/geo_types_test.cpp
        ./geo/wkt_parse_test.cpp
        ./http/http_utils_test.cpp
        ./http/message_body_sink_test.cpp
        ./http/stream_load_test.cpp
        ./storage/decimal12_test.cpp
        ./storage/disjunctive_predicates_test.cpp
        ./storage/utils_test.cpp
        ./storage/del_vector_test.cpp
        ./storage/file_utils_test.cpp
        ./storage/fs/file_block_manager_test.cpp
        ./storage/tablet_schema_map_test.cpp
        ./storage/hll_test.cpp
        ./storage/key_coder_test.cpp
        ./storage/lru_cache_test.cpp
        ./storage/kv_store_test.cpp
        ./storage/protobuf_file_test.cpp
        ./storage/page_cache_test.cpp
        ./storage/primary_index_test.cpp
        ./storage/primary_key_encoder_test.cpp
        ./storage/rowset_update_state_test.cpp
        ./storage/rowset/beta_rowset_test.cpp
        ./storage/rowset/binary_dict_page_test.cpp
        ./storage/rowset/binary_plain_page_test.cpp
        ./storage/rowset/binary_prefix_page_test.cpp
        ./storage/rowset/bitmap_index_test.cpp
        ./storage/rowset/bitshuffle_page_test.cpp
        ./storage/rowset/block_bloom_filter_test.cpp
        ./storage/rowset/bloom_filter_index_reader_writer_test.cpp
        ./storage/rowset/column_reader_writer_test.cpp
        ./storage/rowset/encoding_info_test.cpp
        ./storage/rowset/frame_of_reference_page_test.cpp
        ./storage/rowset/ordinal_page_index_test.cpp
        ./storage/rowset/plain_page_test.cpp
        ./storage/rowset/rle_page_test.cpp
        ./storage/rowset/segment_rewriter_test.cpp
        ./storage/rowset/segment_test.cpp
        ./storage/rowset/segment_iterator_test.cpp
        ./storage/rowset/zone_map_index_test.cpp
        ./storage/rowset/unique_rowset_id_generator_test.cpp
        ./storage/rowset/default_value_column_iterator_test.cpp
        ./storage/rowset/index_page_test.cpp
        ./storage/async_delta_writer_executor_test.cpp
        ./storage/selection_vector_test.cpp
        ./storage/snapshot_meta_test.cpp
        ./storage/short_key_index_test.cpp
        ./storage/storage_types_test.cpp
        ./storage/tablet_meta_test.cpp
        ./storage/tablet_meta_manager_test.cpp
        ./storage/table_schema_test.cpp
        ./storage/tablet_updates_test.cpp
        ./storage/update_manager_test.cpp
        ./storage/vectorized/aggregate_iterator_test.cpp
        ./storage/vectorized/chunk_aggregator_test.cpp
        ./storage/vectorized/chunk_helper_test.cpp
        ./storage/vectorized/column_aggregator_test.cpp
        ./storage/vectorized/column_predicate_test.cpp
        ./storage/vectorized/conjunctive_predicates_test.cpp
        ./storage/vectorized/convert_helper_test.cpp
        ./storage/vectorized/merge_iterator_test.cpp
        ./storage/vectorized/memtable_test.cpp
        ./storage/vectorized/projection_iterator_test.cpp
        ./storage/vectorized/push_handler_test.cpp
        ./storage/vectorized/range_test.cpp
        ./storage/vectorized/row_source_mask_test.cpp
        ./storage/vectorized/union_iterator_test.cpp
        ./storage/vectorized/unique_iterator_test.cpp
        ./storage/vectorized/cumulative_compaction_test.cpp
        ./storage/vectorized/base_compaction_test.cpp
        ./storage/vectorized/rowset_merger_test.cpp
        ./storage/vectorized/schema_change_test.cpp
        ./plugin/plugin_mgr_test.cpp
        ./runtime/buffer_control_block_test.cpp
        ./runtime/datetime_value_test.cpp
        ./runtime/decimalv2_value_test.cpp
        ./runtime/decimalv3_test.cpp
        ./runtime/decimal_value_test.cpp
        ./runtime/external_scan_context_mgr_test.cpp
        ./runtime/fragment_mgr_test.cpp
        ./runtime/free_list_test.cpp
        ./runtime/int128_arithmetic_ops_test.cpp
        ./runtime/kafka_consumer_pipe_test.cpp
        ./runtime/large_int_value_test.cpp
        ./runtime/memory/chunk_allocator_test.cpp
        ./runtime/memory/system_allocator_test.cpp
        ./runtime/mem_pool_test.cpp
        ./runtime/raw_value_test.cpp
        ./runtime/result_queue_mgr_test.cpp
        ./runtime/snapshot_loader_test.cpp
        ./runtime/stream_load_pipe_test.cpp
        ./runtime/string_buffer_test.cpp
        ./runtime/string_value_test.cpp
        ./runtime/type_descriptor_test.cpp
        ./runtime/thread_resource_mgr_test.cpp
        ./runtime/type_descriptor_test.cpp
        ./runtime/vectorized/sorted_chunks_merger_test.cpp
        ./serde/column_array_serde_test.cpp
        ./serde/protobuf_serde_test.cpp
        ./simd/simd_test.cpp
        ./simd/simd_selector_test.cpp
        ./simd/simd_mulselector_test.cpp
        ./util/aes_util_test.cpp
        ./util/bitmap_test.cpp
        ./util/bitmap_value_test.cpp
        ./util/bit_stream_utils_test.cpp
        ./util/bit_util_test.cpp
        ./util/block_compression_test.cpp
        ./util/blocking_queue_test.cpp
        ./util/brpc_stub_cache_test.cpp
        ./util/c_string_test.cpp
        ./util/cidr_test.cpp
        ./util/coding_test.cpp
        ./util/core_local_test.cpp
        ./util/countdown_latch_test.cpp
        ./util/crc32c_test.cpp
        ./util/dynamic_cache_test.cpp
        ./util/faststring_test.cpp
        ./util/file_cache_test.cpp
        ./util/filesystem_util_test.cpp
        ./util/frame_of_reference_coding_test.cpp
        ./util/internal_queue_test.cpp
        ./util/json_util_test.cpp
        ./util/lru_cache_util_test.cpp
        ./util/md5_test.cpp
        ./util/monotime_test.cpp
        ./util/mysql_row_buffer_test.cpp
        ./util/new_metrics_test.cpp
        ./util/parse_util_test.cpp
        ./util/path_trie_test.cpp
        ./util/path_util_test.cpp
        ./util/radix_sort_test.cpp
        ./util/rle_encoding_test.cpp
        ./util/scoped_cleanup_test.cpp
        ./util/string_parser_test.cpp
        ./util/string_util_test.cpp
        ./util/tdigest_test.cpp
        ./util/thread_test.cpp
        ./util/trace_test.cpp
        ./util/types_test.cpp
        ./util/uid_util_test.cpp
        ./util/utf8_check_test.cpp
        ./util/buffered_stream_test.cpp
        ./util/int96_test.cpp
        ./util/bit_packing_test.cpp
        ./util/gc_helper_test.cpp
        )

# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx)
        set_source_files_properties(./exprs/vectorized/json_functions_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/binary_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/numeric_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
        set_source_files_properties(./formats/json/nullable_column_test.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
endif()

if (USE_AVX2)
    set(EXEC_FILES ${EXEC_FILES} ./column/avx_numeric_column_test.cpp)
endif()

if (WITH_HDFS)
    set(EXEC_FILES ${EXEC_FILES} ./exec/vectorized/hdfs_scan_node_test.cpp)
endif ()

add_executable(starrocks_test ${EXEC_FILES})

TARGET_LINK_LIBRARIES(starrocks_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(starrocks_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
SET_TARGET_PROPERTIES(${PLUGIN_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/test")

# bytes_test requires tcmalloc which conflicts with sanitizer.
# Thus we need a standalone binary without sanitizer to test it.
add_executable(bytes_test
        ./column/bytes_test.cpp
        ../src/service/mem_hook.cpp
        )

set_target_properties(bytes_test PROPERTIES
        COMPILE_OPTIONS
        "-lpthread;-std=gnu++17;-g;-fno-sanitize=address")

set_target_properties(bytes_test PROPERTIES
        LINK_OPTIONS
        "-static-libstdc++;-static-libgcc;-fno-sanitize=address")

TARGET_LINK_LIBRARIES(bytes_test
        pthread
        gtest
        tcmalloc
        )

add_executable(starrocks_column_to_arrow_test ./util/arrow/starrocks_column_to_arrow_test.cpp)
TARGET_LINK_LIBRARIES(starrocks_column_to_arrow_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(starrocks_column_to_arrow_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_executable(engine_storage_migration_task_test ./storage/task/engine_storage_migration_task_test.cpp)
TARGET_LINK_LIBRARIES(engine_storage_migration_task_test ${TEST_LINK_LIBS})
SET_TARGET_PROPERTIES(engine_storage_migration_task_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
