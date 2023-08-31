# This file is made available under Elastic License 2.0.
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/be/src/exec/CMakeLists.txt

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 2.8.12)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec")

set(EXEC_FILES
    data_sink.cpp
    empty_set_node.cpp
    exec_node.cpp
    exchange_node.cpp
    scan_node.cpp
    select_node.cpp
    sort_exec_exprs.cpp
    olap_common.cpp
    tablet_info.cpp
    tablet_sink.cpp
    mysql_scanner.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_query_builder.cpp
    es/es_scroll_parser.cpp
    local_file_writer.cpp
    broker_writer.cpp
    plain_text_builder.cpp
    vectorized/aggregator.cpp
    vectorized/sorted_streaming_aggregator.cpp
    vectorized/aggregate/agg_hash_variant.cpp
    vectorized/aggregate/aggregate_base_node.cpp
    vectorized/aggregate/aggregate_blocking_node.cpp
    vectorized/aggregate/distinct_blocking_node.cpp
    vectorized/aggregate/aggregate_streaming_node.cpp
    vectorized/aggregate/distinct_streaming_node.cpp
    vectorized/partition/chunks_partitioner.cpp
    vectorized/partition/partition_hash_variant.cpp
    vectorized/analytic_node.cpp
    vectorized/analytor.cpp
    vectorized/csv_scanner.cpp
    vectorized/tablet_scanner.cpp
    vectorized/olap_scan_node.cpp
    vectorized/olap_scan_prepare.cpp
    vectorized/olap_meta_scanner.cpp
    vectorized/olap_meta_scan_node.cpp
    vectorized/hash_joiner.cpp
    vectorized/hash_join_node.cpp
    vectorized/join_hash_map.cpp
    vectorized/topn_node.cpp
    vectorized/chunks_sorter.cpp
    vectorized/chunks_sorter_heap_sort.cpp
    vectorized/chunks_sorter_topn.cpp
    vectorized/chunks_sorter_full_sort.cpp
    vectorized/cross_join_node.cpp
    vectorized/union_node.cpp
    vectorized/tablet_info.cpp
    vectorized/except_hash_set.cpp
    vectorized/except_node.cpp
    vectorized/file_scanner.cpp
    vectorized/orc_scanner.cpp
    vectorized/jni_scanner.cpp
    vectorized/arrow_to_starrocks_converter.cpp
    vectorized/arrow_to_json_converter.cpp
    vectorized/parquet_scanner.cpp
    vectorized/parquet_reader.cpp
    vectorized/file_scan_node.cpp
    vectorized/assert_num_rows_node.cpp
    vectorized/intersect_hash_set.cpp
    vectorized/intersect_node.cpp
    vectorized/hdfs_scanner.cpp
    vectorized/hdfs_scanner_orc.cpp
    vectorized/hdfs_scanner_parquet.cpp
    vectorized/hdfs_scanner_text.cpp
    vectorized/json_scanner.cpp
    vectorized/json_parser.cpp
    vectorized/project_node.cpp
    vectorized/dict_decode_node.cpp
    vectorized/repeat_node.cpp
    vectorized/table_function_node.cpp
    vectorized/schema_scanner.cpp
    vectorized/schema_scan_node.cpp
    vectorized/schema_scanner/schema_be_metrics_scanner.cpp
    vectorized/schema_scanner/schema_be_tablets_scanner.cpp
    vectorized/schema_scanner/schema_be_txns_scanner.cpp
    vectorized/schema_scanner/schema_be_configs_scanner.cpp
    vectorized/schema_scanner/schema_be_threads_scanner.cpp
    vectorized/schema_scanner/schema_be_logs_scanner.cpp
    vectorized/schema_scanner/schema_fe_tablet_schedules_scanner.cpp
    vectorized/schema_scanner/schema_tables_scanner.cpp
    vectorized/schema_scanner/schema_dummy_scanner.cpp
    vectorized/schema_scanner/schema_schemata_scanner.cpp
    vectorized/schema_scanner/schema_variables_scanner.cpp
    vectorized/schema_scanner/schema_columns_scanner.cpp
    vectorized/schema_scanner/schema_charsets_scanner.cpp
    vectorized/schema_scanner/schema_collations_scanner.cpp
    vectorized/schema_scanner/schema_statistics_scanner.cpp
    vectorized/schema_scanner/schema_triggers_scanner.cpp
    vectorized/schema_scanner/schema_events_scanner.cpp
    vectorized/schema_scanner/schema_views_scanner.cpp
    vectorized/schema_scanner/schema_materialized_views_scanner.cpp
    vectorized/schema_scanner/schema_tasks_scanner.cpp
    vectorized/schema_scanner/schema_task_runs_scanner.cpp
    vectorized/schema_scanner/schema_user_privileges_scanner.cpp
    vectorized/schema_scanner/schema_schema_privileges_scanner.cpp
    vectorized/schema_scanner/schema_table_privileges_scanner.cpp
    vectorized/schema_scanner/schema_tables_config_scanner.cpp
    vectorized/schema_scanner/schema_helper.cpp
    vectorized/jdbc_scanner.cpp
    vectorized/sorting/compare_column.cpp
    vectorized/sorting/merge_column.cpp
    vectorized/sorting/merge_cascade.cpp
    vectorized/sorting/sort_column.cpp
    vectorized/sorting/sort_permute.cpp
    vectorized/connector_scan_node.cpp
    pipeline/exchange/exchange_merge_sort_source_operator.cpp
    pipeline/exchange/exchange_sink_operator.cpp
    pipeline/exchange/exchange_source_operator.cpp
    pipeline/exchange/local_exchange.cpp
    pipeline/exchange/local_exchange_sink_operator.cpp
    pipeline/exchange/local_exchange_source_operator.cpp
    pipeline/exchange/multi_cast_local_exchange.cpp
    pipeline/exchange/sink_buffer.cpp
    pipeline/fragment_executor.cpp
    pipeline/operator.cpp
    pipeline/limit_operator.cpp
    pipeline/pipeline_builder.cpp
    pipeline/project_operator.cpp
    pipeline/dict_decode_operator.cpp
    pipeline/result_sink_operator.cpp
    pipeline/olap_table_sink_operator.cpp
    pipeline/scan/balanced_chunk_buffer.cpp
    pipeline/scan/chunk_source.cpp
    pipeline/scan/morsel.cpp
    pipeline/scan/scan_operator.cpp
    pipeline/scan/olap_chunk_source.cpp
    pipeline/scan/olap_scan_operator.cpp
    pipeline/scan/olap_scan_prepare_operator.cpp
    pipeline/scan/olap_scan_context.cpp
    pipeline/scan/connector_scan_operator.cpp
    pipeline/scan/olap_meta_chunk_source.cpp
    pipeline/scan/olap_meta_scan_operator.cpp
    pipeline/scan/olap_meta_scan_prepare_operator.cpp
    pipeline/scan/olap_schema_chunk_source.cpp
    pipeline/scan/olap_schema_scan_operator.cpp
    pipeline/scan/olap_schema_scan_context.cpp
    pipeline/scan/morsel.cpp
    pipeline/scan/chunk_buffer_limiter.cpp
    pipeline/sink/file_sink_operator.cpp
    pipeline/sink/export_sink_operator.cpp
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
    pipeline/sort/sort_context.cpp
    pipeline/pipeline_driver_executor.cpp
    pipeline/pipeline_driver_queue.cpp
    pipeline/pipeline_driver_poller.cpp
    pipeline/pipeline_driver.cpp
    pipeline/audit_statistics_reporter.cpp
    pipeline/exec_state_reporter.cpp
    pipeline/driver_limiter.cpp
    pipeline/fragment_context.cpp
    pipeline/query_context.cpp
    pipeline/aggregate/aggregate_blocking_sink_operator.cpp
    pipeline/aggregate/aggregate_blocking_source_operator.cpp
    pipeline/aggregate/aggregate_streaming_sink_operator.cpp
    pipeline/aggregate/aggregate_streaming_source_operator.cpp
    pipeline/aggregate/aggregate_distinct_blocking_sink_operator.cpp
    pipeline/aggregate/aggregate_distinct_blocking_source_operator.cpp
    pipeline/aggregate/aggregate_distinct_streaming_sink_operator.cpp
    pipeline/aggregate/aggregate_distinct_streaming_source_operator.cpp
    pipeline/aggregate/sorted_aggregate_streaming_sink_operator.cpp
    pipeline/aggregate/sorted_aggregate_streaming_source_operator.cpp
    pipeline/aggregate/repeat/repeat_operator.cpp
    pipeline/analysis/analytic_sink_operator.cpp
    pipeline/analysis/analytic_source_operator.cpp
    pipeline/table_function_operator.cpp
    pipeline/assert_num_rows_operator.cpp
    pipeline/set/union_passthrough_operator.cpp
    pipeline/set/union_const_source_operator.cpp
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
    workgroup/work_group.cpp
    workgroup/scan_executor.cpp
    workgroup/scan_task_queue.cpp
    query_cache/multilane_operator.cpp
    query_cache/cache_operator.cpp
    query_cache/cache_manager.cpp
    query_cache/lane_arbiter.cpp
    query_cache/conjugate_operator.cpp
    query_cache/ticket_checker.cpp)

# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set_source_files_properties(vectorized/json_scanner.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
    set_source_files_properties(vectorized/json_parser.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
endif()

add_library(Exec STATIC
    ${EXEC_FILES}
)
