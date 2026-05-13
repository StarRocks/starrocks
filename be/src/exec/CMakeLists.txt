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

set(EXEC_CORE_FILES
    exec_node.cpp
    runtime_filter/runtime_filter_descriptor.cpp
    runtime_filter/runtime_filter_probe.cpp
    runtime_filter/runtime_filter_probe_wait.cpp
    runtime_filter/runtime_filter_registry.cpp
    runtime_filter/runtime_filter_helper.cpp
)

ADD_BE_LIB(ExecCore
    ${EXEC_CORE_FILES}
)

target_link_libraries(ExecCore PUBLIC ExprCore RuntimeCore)

set(EXEC_SINK_CORE_FILES
    data_sink.cpp
)

ADD_BE_LIB(ExecSinkCore
    ${EXEC_SINK_CORE_FILES}
)

target_link_libraries(ExecSinkCore PUBLIC Common Base Gutil StarRocksGen)

set(SPILL_CORE_FILES
    spill/dir_manager.cpp
    spill/block_reader.cpp
    spill/log_block_manager.cpp
    spill/file_block_manager.cpp
    spill/hybird_block_manager.cpp
    spill/query_spill_manager.cpp
    spill/operator_mem_resource_manager.cpp
    spill/spill_metrics.cpp
)

ADD_BE_LIB(SpillCore
    ${SPILL_CORE_FILES}
)

target_link_libraries(SpillCore PUBLIC RuntimeCore FSCore IOCore Common Base Gutil StarRocksGen)

set(EXEC_SCHEMA_SCANNER_CORE_FILES
    schema_scanner.cpp
)

ADD_BE_LIB(ExecSchemaScannerCore
    ${EXEC_SCHEMA_SCANNER_CORE_FILES}
)

target_link_libraries(ExecSchemaScannerCore PUBLIC
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

set(EXEC_SCHEMA_SCANNER_FILES
    schema_scanner/schema_be_bvars_scanner.cpp
    schema_scanner/schema_be_configs_scanner.cpp
    schema_scanner/schema_be_threads_scanner.cpp
    schema_scanner/schema_charsets_scanner.cpp
    schema_scanner/schema_collations_scanner.cpp
    schema_scanner/schema_dummy_scanner.cpp
    schema_scanner/schema_events_scanner.cpp
    schema_scanner/schema_statistics_scanner.cpp
    schema_scanner/schema_triggers_scanner.cpp
)

ADD_BE_LIB(ExecSchemaScanners
    ${EXEC_SCHEMA_SCANNER_FILES}
)

target_link_libraries(ExecSchemaScanners PUBLIC
    ExecSchemaScannerCore
    RuntimeCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

set(EXEC_SORTING_CORE_FILES
    sorting/compare_column.cpp
    sorting/merge.cpp
    sorting/merge_cascade.cpp
    sorting/merge_column.cpp
    sorting/sort_column.cpp
    sorting/sort_cursor.cpp
    sorting/sort_permute.cpp
)

ADD_BE_LIB(ExecSortingCore
    ${EXEC_SORTING_CORE_FILES}
)

target_link_libraries(ExecSortingCore PUBLIC
    ExprCore
    RuntimeCore
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
    ExecSortingCore
    ExprCore
    Serde
    RuntimeCore
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
set(PIPELINE_PRIMITIVES_SRCS
    pipeline/operator.cpp
)

set(PIPELINE_GRAPH_SRCS
    pipeline/pipeline.cpp
    pipeline/group_execution/execution_group.cpp
    pipeline/group_execution/execution_group_builder.cpp
)

set(PIPELINE_RUNTIME_SRCS
    pipeline/pipeline_metrics.cpp
    pipeline/pipeline_driver_executor.cpp
    pipeline/pipeline_driver_queue.cpp
    pipeline/pipeline_driver_poller.cpp
    pipeline/pipeline_driver.cpp
    pipeline/audit_statistics_reporter.cpp
    pipeline/exec_state_reporter.cpp
    pipeline/driver_limiter.cpp
    pipeline/fragment_context.cpp
    pipeline/query_context.cpp
    pipeline/operator_factory.cpp
    pipeline/source_operator.cpp
    pipeline/schedule/timeout_tasks.cpp
    pipeline/schedule/event_scheduler.cpp
    pipeline/schedule/observer.cpp
    pipeline/runtime_filter_types.cpp
    workgroup/pipeline_executor_set.cpp
    workgroup/pipeline_executor_set_manager.cpp
)

set(PIPELINE_ADAPTERS_SRCS
    pipeline/exec_node_pipeline_adapter.cpp
    pipeline/fragment_executor.cpp
    pipeline/pipeline_builder.cpp
)

set(PIPELINE_OPERATORS_BATCH_SRCS
    pipeline/capture_version_operator.cpp
    pipeline/exchange/exchange_merge_sort_source_operator.cpp
    pipeline/exchange/exchange_parallel_merge_source_operator.cpp
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
    pipeline/adaptive/utils.cpp
    pipeline/adaptive/event.cpp
    pipeline/spill_process_operator.cpp
    pipeline/spill_process_channel.cpp
    query_cache/multilane_operator.cpp
    query_cache/cache_operator.cpp
    query_cache/cache_manager.cpp
    query_cache/lane_arbiter.cpp
    query_cache/conjugate_operator.cpp
    query_cache/ticket_checker.cpp
    spill/spill_components.cpp
    spill/executor.cpp
    spill/spiller.cpp
    spill/spiller_factory.cpp
    spill/mem_table.cpp
    spill/serde.cpp
    spill/input_stream.cpp
    spill/data_stream.cpp
)

set(EXEC_NON_PIPELINE_FILES
    spill/spill_dir_manager_bootstrap.cpp
    catalog_scan_metrics.cpp
    fetch_node.cpp
    lookup_node.cpp
    query_scan_metrics.cpp
    capture_version_node.cpp
    empty_set_node.cpp
    exec_factory.cpp
    pipeline_node.cpp
    exchange_node.cpp
    scan_node.cpp
    select_node.cpp
    sort_exec_exprs.cpp
    short_circuit.cpp
    short_circuit_hybrid.cpp
    olap_common.cpp
    tablet_info.cpp
    write_combined_txn_log.cpp
    mysql_scanner.cpp
    benchmark_scanner.cpp
    cache_stats_scanner.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_query_builder.cpp
    es/es_scroll_parser.cpp
    local_file_writer.cpp
    plain_text_builder.cpp
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
    olap_scan_prepare.cpp
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
    arrow_to_starrocks_converter.cpp
    arrow_to_json_converter.cpp
    parquet_schema_builder.cpp
    parquet_reader.cpp
    parquet_writer.cpp
    parquet_builder.cpp
    file_scan_node.cpp
    assert_num_rows_node.cpp
    intersect_hash_set.cpp
    intersect_node.cpp
    json_parser.cpp
    project_node.cpp
    dict_decode_node.cpp
    repeat_node.cpp
    table_function_node.cpp
    schema_scanner_factory.cpp
    schema_scan_node.cpp
    dictionary_cache_writer.cpp
    arrow_flight_batch_reader.cpp
    iceberg/iceberg_delete_builder.cpp    
    paimon/paimon_delete_file_builder.cpp
    hdfs_scanner/cache_select_scanner.cpp
    hdfs_scanner/hdfs_scanner_text.cpp
    hdfs_scanner/hdfs_scanner_json.cpp
    hdfs_scanner/hdfs_scanner_partition.cpp
    hdfs_scanner/hdfs_scanner_parquet.cpp
    hdfs_scanner/hdfs_scanner_orc.cpp
    hdfs_scanner/hdfs_scanner.cpp
    hdfs_scanner/jni_scanner.cpp
    file_scanner/avro_cpp_scanner.cpp
    file_scanner/avro_scanner.cpp
    file_scanner/csv_scanner.cpp
    file_scanner/file_scanner.cpp
    file_scanner/json_scanner.cpp
    file_scanner/orc_scanner.cpp
    file_scanner/parquet_scanner.cpp
    schema_scanner/schema_analyze_status.cpp
    schema_scanner/schema_tables_scanner.cpp
    schema_scanner/schema_schemata_scanner.cpp
    schema_scanner/schema_variables_scanner.cpp
    schema_scanner/schema_columns_scanner.cpp
    schema_scanner/schema_column_stats_usage_scanner.cpp
    schema_scanner/schema_views_scanner.cpp
    schema_scanner/schema_materialized_views_scanner.cpp
    schema_scanner/schema_tasks_scanner.cpp
    schema_scanner/schema_task_runs_scanner.cpp
    schema_scanner/schema_loads_scanner.cpp
    schema_scanner/schema_load_tracking_logs_scanner.cpp
    schema_scanner/schema_partitions_meta_scanner.cpp
    schema_scanner/schema_user_privileges_scanner.cpp
    schema_scanner/schema_schema_privileges_scanner.cpp
    schema_scanner/schema_table_privileges_scanner.cpp
    schema_scanner/schema_tables_config_scanner.cpp
    schema_scanner/schema_be_tablets_scanner.cpp
    schema_scanner/schema_be_txns_scanner.cpp
    schema_scanner/schema_be_logs_scanner.cpp
    schema_scanner/schema_be_metrics_scanner.cpp
    schema_scanner/schema_fe_metrics_scanner.cpp
    schema_scanner/schema_fe_threads_scanner.cpp
    schema_scanner/schema_fe_tablet_schedules_scanner.cpp
    schema_scanner/schema_be_compactions_scanner.cpp
    schema_scanner/schema_be_cloud_native_compactions_scanner.cpp
    schema_scanner/schema_be_tablet_write_log_scanner.cpp
    schema_scanner/schema_pipe_files.cpp
    schema_scanner/schema_pipes.cpp
    schema_scanner/schema_recyclebin_catalogs.cpp
    schema_scanner/starrocks_role_edges_scanner.cpp
    schema_scanner/starrocks_grants_to_scanner.cpp
    schema_scanner/schema_helper.cpp
    schema_scanner/schema_routine_load_jobs_scanner.cpp
    schema_scanner/schema_stream_loads_scanner.cpp
    schema_scanner/schema_be_datacache_metrics_scanner.cpp
    schema_scanner/sys_object_dependencies.cpp
    schema_scanner/sys_fe_locks.cpp
    schema_scanner/sys_fe_memory_usage.cpp
    schema_scanner/schema_temp_tables_scanner.cpp
    schema_scanner/schema_warehouse_metrics.cpp
    schema_scanner/schema_warehouse_queries.cpp
    schema_scanner/schema_cluster_snapshots_scanner.cpp
    schema_scanner/schema_cluster_snapshot_jobs_scanner.cpp
    schema_scanner/schema_applicable_roles_scanner.cpp
    schema_scanner/schema_keywords_scanner.cpp
    schema_scanner/schema_tablet_reshard_jobs_scanner.cpp
    jdbc_scanner.cpp
    jdbc/type_checker.cpp
    jdbc/type_checker_manager.cpp
    jdbc/type_checker_xml_loader.cpp
    sorting/merge_path.cpp
    connector_scan_node.cpp
    workgroup/work_group.cpp
    workgroup/mem_tracker_manager.cpp
    workgroup/scan_executor.cpp
    workgroup/scan_task_queue.cpp
    workgroup/lock_free_scan_task_queue.cpp
    workgroup/lock_free_work_group_scan_task_queue.cpp
)

set(EXEC_DATA_SINK_FILES
    data_sinks/arrow_result_writer.cpp
    data_sinks/blackhole_table_sink.cpp
    data_sinks/buffer_control_result_writer.cpp
    data_sinks/customized_result_writer.cpp
    data_sinks/data_sink_factory.cpp
    data_sinks/data_sink_pipeline.cpp
    data_sinks/data_stream_sender.cpp
    data_sinks/dictionary_cache_sink.cpp
    data_sinks/export_sink.cpp
    data_sinks/file_result_writer.cpp
    data_sinks/hive_table_sink.cpp
    data_sinks/http_result_writer.cpp
    data_sinks/iceberg_table_sink.cpp
    data_sinks/memory_scratch_sink.cpp
    data_sinks/metadata_result_writer.cpp
    data_sinks/multi_cast_data_stream_sink.cpp
    data_sinks/multi_olap_table_sink.cpp
    data_sinks/mysql_result_writer.cpp
    data_sinks/mysql_table_sink.cpp
    data_sinks/mysql_table_writer.cpp
    data_sinks/range_router.cpp
    data_sinks/range_tablet_sink_sender.cpp
    data_sinks/result_sink.cpp
    data_sinks/schema_table_sink.cpp
    data_sinks/statistic_result_writer.cpp
    data_sinks/table_function_table_sink.cpp
    data_sinks/tablet_sink.cpp
    data_sinks/tablet_sink_colocate_sender.cpp
    data_sinks/tablet_sink_index_channel.cpp
    data_sinks/tablet_sink_sender.cpp
    data_sinks/variable_result_writer.cpp
)

ADD_BE_LIB(PipelinePrimitives
    ${PIPELINE_PRIMITIVES_SRCS}
)

target_link_libraries(PipelinePrimitives PUBLIC
    ExecCore
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
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

ADD_BE_LIB(Exec
    ${EXEC_NON_PIPELINE_FILES}
    ${EXEC_DATA_SINK_FILES}
    $<TARGET_OBJECTS:pipeline_runtime_compat_exec_objs>
    $<TARGET_OBJECTS:pipeline_operators_batch_exec_objs>
    $<TARGET_OBJECTS:pipeline_extensions_exec_objs>
)

target_link_libraries(Exec PUBLIC PipelinePrimitives ExecSinkCore ExecCore SpillCore ExecSchemaScannerCore ExecSchemaScanners ExecSortingCore ExecJoinCore)
