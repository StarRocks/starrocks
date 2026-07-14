# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16.12)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec")

set(EXEC_SCHEMA_SCANNER_CORE_FILES
    schema_scanner.cpp
    schema_scanner_factory.cpp
)

ADD_BE_LIB(ExecSchemaScannerCore
    ${EXEC_SCHEMA_SCANNER_CORE_FILES}
)

target_link_libraries(ExecSchemaScannerCore PUBLIC
    Expr
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

set(EXEC_JOIN_CORE_FILES
    join/join_hash_table_descriptor.cpp
    join/join_key_constructor.cpp
    join/join_hash_table.cpp
    join/join_hash_map_factory.cpp
    join/join_hash_map_mapping_inst.cpp
    join/join_hash_map_linear_chained_inst.cpp
    join/join_hash_map_linear_chained_set_inst.cpp
    join/join_hash_map_bucket_chained_inst.cpp
    join/join_hash_map_asof_inst.cpp
)

ADD_BE_LIB(ExecJoinCore
    ${EXEC_JOIN_CORE_FILES}
)

target_link_libraries(ExecJoinCore PUBLIC
    ComputeEnv
    Expr
    Runtime
    ColumnSortCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

if (WITH_PCH)
    target_precompile_headers(ExecJoinCore
        PRIVATE
        join/join_hash_map.h
        join/join_hash_map.hpp
        join/join_hash_map_method.hpp
    )
endif()

# Staged pipeline compatibility groups. These source lists are not final clean
# module boundaries yet; keep them named by intended ownership so later
# cycle-cut leaves can promote them into real targets incrementally.
set(PIPELINE_GRAPH_SRCS
)

set(PIPELINE_RUNTIME_SRCS
    pipeline/driver_executor_factory.cpp
    pipeline/driver_queue_factory.cpp
    pipeline/pipeline_driver_executor.cpp
    pipeline/pipeline_driver_queue.cpp
    pipeline/pipeline_driver_poller.cpp
    pipeline/audit_statistics_reporter.cpp
    pipeline/exec_state_reporter.cpp
    pipeline/fragment_context_cancel.cpp
    pipeline/schedule/timeout_tasks.cpp
    pipeline/runtime_filter_types.cpp
)

set(PIPELINE_ADAPTERS_SRCS
    pipeline/exec_node_pipeline_adapter.cpp
    pipeline/fragment_execution_params.cpp
    pipeline/pipeline_builder_operators.cpp
    pipeline/pipeline_driver_instantiator.cpp
)

set(PIPELINE_OPERATORS_BATCH_SRCS
    pipeline/capture_version_operator.cpp
    pipeline/exchange/exchange_merge_sort_source_operator.cpp
    pipeline/exchange/exchange_parallel_merge_source_operator.cpp
    pipeline/exchange/exchange_compression_strategy.cpp
    pipeline/exchange/exchange_sink_operator.cpp
    pipeline/exchange/exchange_source_operator.cpp
    pipeline/exchange/local_exchange.cpp
    pipeline/exchange/local_exchange_sink_operator.cpp
    pipeline/exchange/local_exchange_source_operator.cpp
    pipeline/exchange/multi_cast_local_exchange.cpp
    pipeline/exchange/mem_limited_chunk_queue.cpp
    pipeline/exchange/spillable_multi_cast_local_exchange.cpp
    pipeline/exchange/multi_cast_local_exchange_sink_operator.cpp
    pipeline/exchange/multi_cast_local_exchange_source_operator.cpp
    pipeline/exchange/split_local_exchange.cpp
    pipeline/exchange/sink_buffer.cpp
    pipeline/limit_operator.cpp
    pipeline/offset_operator.cpp
    pipeline/project_operator.cpp
    pipeline/dict_decode_operator.cpp
    pipeline/scan/glm_manager.cpp
    pipeline/scan/balanced_chunk_buffer.cpp
    pipeline/scan/chunk_source.cpp
    pipeline/scan/morsel.cpp
    pipeline/scan/morsel_queue_factory.cpp
    pipeline/scan/scan_operator.cpp
    pipeline/scan/olap_chunk_source.cpp
    pipeline/scan/olap_scan_operator.cpp
    pipeline/scan/olap_scan_prepare_operator.cpp
    pipeline/scan/olap_scan_context.cpp
    pipeline/scan/connector_scan_operator.cpp
    pipeline/scan/meta_chunk_source.cpp
    pipeline/scan/meta_scan_operator.cpp
    pipeline/scan/meta_scan_prepare_operator.cpp
    pipeline/scan/olap_meta_scan_prepare_operator.cpp
    pipeline/scan/lake_meta_scan_prepare_operator.cpp
    pipeline/scan/schema_chunk_source.cpp
    pipeline/scan/schema_scan_operator.cpp
    pipeline/scan/schema_scan_context.cpp
    pipeline/sink/connector_sink_operator.cpp
    pipeline/sink/olap_table_sink_operator.cpp
    pipeline/sink/result_sink_operator.cpp
    pipeline/sink/sink_io_buffer.cpp
    pipeline/sink/blackhole_table_sink_operator.cpp
    pipeline/scan/chunk_buffer_limiter.cpp
    pipeline/sink/file_sink_operator.cpp
    pipeline/sink/export_sink_operator.cpp
    pipeline/sink/dictionary_cache_sink_operator.cpp
    pipeline/sink/mysql_table_sink_operator.cpp
    pipeline/sink/memory_scratch_sink_operator.cpp
    pipeline/select_operator.cpp
    pipeline/nljoin/nljoin_context.cpp
    pipeline/nljoin/nljoin_build_operator.cpp
    pipeline/nljoin/nljoin_probe_operator.cpp
    pipeline/sort/partition_sort_sink_operator.cpp
    pipeline/sort/local_partition_topn_sink.cpp
    pipeline/sort/local_partition_topn_source.cpp
    pipeline/sort/local_partition_topn_context.cpp
    pipeline/sort/local_merge_sort_source_operator.cpp
    pipeline/sort/local_parallel_merge_sort_source_operator.cpp
    pipeline/sort/sort_context.cpp
    pipeline/aggregate/aggregate_operators.cpp
    pipeline/aggregate/repeat/repeat_operator.cpp
    pipeline/analysis/analytic_sink_operator.cpp
    pipeline/analysis/analytic_source_operator.cpp
    pipeline/bucket_process_operator.cpp
    pipeline/table_function_operator.cpp
    pipeline/assert_num_rows_operator.cpp
    pipeline/enforce_unique_row_locator_operator.cpp
    pipeline/set/union_passthrough_operator.cpp
    pipeline/set/union_const_source_operator.cpp
    pipeline/set/raw_values_source_operator.cpp
    pipeline/hashjoin/hash_join_build_operator.cpp
    pipeline/hashjoin/hash_join_probe_operator.cpp
    pipeline/hashjoin/hash_joiner_factory.cpp
    pipeline/set/except_context.cpp
    pipeline/set/except_build_sink_operator.cpp
    pipeline/set/except_probe_sink_operator.cpp
    pipeline/set/except_output_source_operator.cpp
    pipeline/set/intersect_context.cpp
    pipeline/set/intersect_build_sink_operator.cpp
    pipeline/set/intersect_probe_sink_operator.cpp
    pipeline/set/intersect_output_source_operator.cpp
    pipeline/hash_partition_context.cpp
    pipeline/hash_partition_sink_operator.cpp
    pipeline/hash_partition_source_operator.cpp
    pipeline/chunk_accumulate_operator.cpp
    pipeline/group_execution/group_operator.cpp
    pipeline/wait_operator.cpp
    pipeline/fetch_task.cpp
    pipeline/fetch_processor.cpp
    pipeline/fetch_sink_operator.cpp
    pipeline/fetch_source_operator.cpp
    pipeline/lookup/tablet_adaptor.cpp
    pipeline/lookup_request.cpp
    pipeline/lookup_operator.cpp
)

set(PIPELINE_EXTENSIONS_SRCS
    pipeline/nljoin/spillable_nljoin_build_operator.cpp
    pipeline/nljoin/spillable_nljoin_probe_operator.cpp
    pipeline/sort/spillable_partition_sort_sink_operator.cpp
    pipeline/hashjoin/spillable_hash_join_build_operator.cpp
    pipeline/hashjoin/spillable_hash_join_probe_operator.cpp
    pipeline/adaptive/collect_stats_sink_operator.cpp
    pipeline/adaptive/collect_stats_source_operator.cpp
    pipeline/adaptive/collect_stats_context.cpp
    pipeline/adaptive/collect_stats_event.cpp
    pipeline/adaptive/utils.cpp
    pipeline/spill_process_operator.cpp
    pipeline/spill_process_channel.cpp
    query_cache/multilane_operator.cpp
    query_cache/cache_operator.cpp
    query_cache/conjugate_operator.cpp
)

set(EXEC_NON_PIPELINE_FILES
    catalog_scan_metrics.cpp
    fetch_node.cpp
    lookup_node.cpp
    capture_version_node.cpp
    empty_set_node.cpp
    exec_factory.cpp
    pipeline_node.cpp
    exchange_node.cpp
    scan_node.cpp
    select_node.cpp
    short_circuit.cpp
    short_circuit_hybrid.cpp
    agg_runtime_filter_builder.cpp
    aggregator.cpp
    sorted_streaming_aggregator.cpp
    aggregate/agg_hash_variant.cpp
    aggregate/compress_serializer.cpp
    aggregate/aggregate_base_node.cpp
    aggregate/aggregate_blocking_node.cpp
    aggregate/distinct_blocking_node.cpp
    aggregate/aggregate_streaming_node.cpp
    aggregate/distinct_streaming_node.cpp
    partition/chunks_partitioner.cpp
    partition/partition_hash_variant.cpp
    analytic_node.cpp
    analytor.cpp
    tablet_scanner.cpp
    olap_scan_node.cpp
    meta_scan_node.cpp
    olap_meta_scanner.cpp
    olap_meta_scan_node.cpp
    lake_meta_scanner.cpp
    lake_meta_scan_node.cpp
    hash_joiner.cpp
    hash_join_node.cpp
    hash_join_components.cpp
    topn_node.cpp
    chunks_sorter.cpp
    chunks_sorter_heap_sort.cpp
    chunks_sorter_topn.cpp
    chunks_sorter_full_sort.cpp
    spillable_chunks_sorter.cpp
    cross_join_node.cpp
    union_node.cpp
    raw_values_node.cpp
    except_hash_set.cpp
    except_node.cpp
    parquet_writer.cpp
    file_scan_node.cpp
    assert_num_rows_node.cpp
    enforce_unique_row_locator_node.cpp
    intersect_hash_set.cpp
    intersect_node.cpp
    project_node.cpp
    dict_decode_node.cpp
    repeat_node.cpp
    table_function_node.cpp
    schema_scan_node.cpp
    dictionary_cache_writer.cpp
    arrow_flight_batch_reader.cpp
    connector_scan_node.cpp
)

set(EXEC_DATA_SINK_COMPAT_FILES
    data_sinks/data_sink_factory.cpp
    data_sinks/data_sink_pipeline.cpp
    data_sinks/pipeline_sink_builder.cpp
    data_sinks/dictionary_cache_sink.cpp
    data_sinks/hive_table_sink.cpp
    data_sinks/iceberg_table_sink.cpp
    data_sinks/table_function_table_sink.cpp
)

set(EXEC_RUNTIME_COMPAT_FILES
    lookup_stream_mgr.cpp
    exec_env.cpp
    pipeline/pipeline_fragment_reporter.cpp
    runtime_compat/runtime_state_helper.cpp
    stream_load/transaction_mgr.cpp
    stream_load/stream_load_executor.cpp
    batch_write/isomorphic_batch_write.cpp
    batch_write/batch_write_mgr.cpp
    batch_write/batch_write_util.cpp
    batch_write/txn_state_cache.cpp
    runtime_filter_compat/runtime_filter_merger.cpp
    runtime_filter_compat/runtime_filter_port.cpp
    runtime_filter_compat/runtime_filter_rpc.cpp
    runtime_filter_compat/runtime_filter_worker_context.cpp
    runtime_filter_compat/runtime_filter_serde.cpp
)

# `pipeline/aggregate/aggregate_operators.cpp` intentionally includes the
# aggregate operator implementation .cpp files to control template
# instantiation. Do not add those implementation files separately here.
#
# Keep graph and adapter sources named separately, but compile them through the
# runtime compatibility staging target until later cycle-cut leaves land.
add_library(pipeline_runtime_compat_exec_objs OBJECT
    ${PIPELINE_RUNTIME_SRCS}
    ${PIPELINE_GRAPH_SRCS}
    ${PIPELINE_ADAPTERS_SRCS}
)

add_library(pipeline_operators_batch_exec_objs OBJECT
    ${PIPELINE_OPERATORS_BATCH_SRCS}
)

add_library(pipeline_extensions_exec_objs OBJECT
    ${PIPELINE_EXTENSIONS_SRCS}
)

IF(ENABLE_MULTI_DYNAMIC_LIBS)
    set_property(TARGET pipeline_runtime_compat_exec_objs PROPERTY POSITION_INDEPENDENT_CODE ON)
    set_property(TARGET pipeline_operators_batch_exec_objs PROPERTY POSITION_INDEPENDENT_CODE ON)
    set_property(TARGET pipeline_extensions_exec_objs PROPERTY POSITION_INDEPENDENT_CODE ON)
ENDIF()

ADD_BE_LIB(ExecRuntime
    runtime/group_execution/execution_group.cpp
    runtime/group_execution/execution_group_builder.cpp
    runtime/pipeline.cpp
    runtime/pipeline_builder_context.cpp
    runtime/pipeline_driver.cpp
    runtime/fragment_context.cpp
    runtime/fragment_context_manager.cpp
    runtime/query_context.cpp
    runtime/query_context_manager.cpp
    runtime/schedule/event_scheduler.cpp
    runtime/schedule/pipeline_driver_observer.cpp
)

target_link_libraries(ExecRuntime PUBLIC
    ComputeEnv
    ExecPrimitive
    StoragePrimitive
    Expr
    Runtime
    Platform
    FileSystem
    IO
    ColumnSortCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

ADD_BE_LIB(Exec
    ${EXEC_NON_PIPELINE_FILES}
    ${EXEC_DATA_SINK_COMPAT_FILES}
    ${EXEC_RUNTIME_COMPAT_FILES}
    $<TARGET_OBJECTS:pipeline_runtime_compat_exec_objs>
    $<TARGET_OBJECTS:pipeline_operators_batch_exec_objs>
    $<TARGET_OBJECTS:pipeline_extensions_exec_objs>
)

target_link_libraries(Exec PUBLIC ExecRuntime ExecPrimitive ExecSchemaScannerCore ExecJoinCore Runtime ComputeEnv Cache)
target_link_libraries(Exec PRIVATE
    DataSinkFile
    DataSinkResult
    DataSinkExchange
    DataSinkExternal
    DataSinkTablet
    Formats
    Platform
    Connector
    ConnectorLake
    ConnectorFile
    ConnectorHive
    ConnectorIceberg
)
