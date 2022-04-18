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
    broker_reader.cpp
    buffered_reader.cpp
    data_sink.cpp
    decompressor.cpp
    empty_set_node.cpp
    exec_node.cpp
    exchange_node.cpp
    local_file_reader.cpp
    scan_node.cpp
    select_node.cpp
    sort_exec_exprs.cpp
    olap_common.cpp
    tablet_info.cpp
    tablet_sink.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_query_builder.cpp
    local_file_writer.cpp
    broker_writer.cpp
    parquet_reader.cpp
    parquet_builder.cpp
    plain_text_builder.cpp
    vectorized/aggregator.cpp
    vectorized/aggregate/aggregate_base_node.cpp
    vectorized/aggregate/aggregate_blocking_node.cpp
    vectorized/aggregate/distinct_blocking_node.cpp
    vectorized/aggregate/aggregate_streaming_node.cpp
    vectorized/aggregate/distinct_streaming_node.cpp
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
    vectorized/chunk_sorter_heapsorter.cpp
    vectorized/chunks_sorter_topn.cpp
    vectorized/chunks_sorter_full_sort.cpp
    vectorized/cross_join_node.cpp
    vectorized/union_node.cpp
    vectorized/tablet_info.cpp
    vectorized/except_hash_set.cpp
    vectorized/except_node.cpp
    vectorized/file_scanner.cpp
    vectorized/orc_scanner.cpp
    vectorized/orc_scanner_adapter.cpp
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
    vectorized/es_http_scan_node.cpp
    vectorized/es_http_scanner.cpp
    vectorized/es_http_components.cpp
    vectorized/schema_scanner.cpp
    vectorized/schema_scan_node.cpp
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
    vectorized/schema_scanner/schema_user_privileges_scanner.cpp
    vectorized/schema_scanner/schema_schema_privileges_scanner.cpp
    vectorized/schema_scanner/schema_table_privileges_scanner.cpp
    vectorized/schema_scanner/schema_helper.cpp
    vectorized/jdbc_scan_node.cpp
    vectorized/jdbc_scanner.cpp
    vectorized/sorting/compare_column.cpp
    vectorized/sorting/merge_column.cpp
    vectorized/sorting/sort_column.cpp
    vectorized/sorting/sort_permute.cpp
    parquet/column_chunk_reader.cpp
    parquet/column_reader.cpp
    parquet/encoding.cpp
    parquet/level_codec.cpp
    parquet/page_reader.cpp
    parquet/schema.cpp
    parquet/stored_column_reader.cpp
    parquet/utils.cpp
    parquet/metadata.cpp
    parquet/group_reader.cpp
    parquet/file_reader.cpp
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
    pipeline/operator_with_dependency.cpp
    pipeline/limit_operator.cpp
    pipeline/olap_chunk_source.cpp
    pipeline/pipeline_builder.cpp
    pipeline/project_operator.cpp
    pipeline/dict_decode_operator.cpp
    pipeline/result_sink_operator.cpp
    pipeline/scan_operator.cpp
    pipeline/olap_scan_operator.cpp
    pipeline/hdfs_scan_operator.cpp
    pipeline/hdfs_chunk_source.cpp
    pipeline/jdbc_scan_operator.cpp
    pipeline/select_operator.cpp
    pipeline/crossjoin/cross_join_right_sink_operator.cpp
    pipeline/crossjoin/cross_join_left_operator.cpp
    pipeline/sort/partition_sort_sink_operator.cpp
    pipeline/sort/local_merge_sort_source_operator.cpp
    pipeline/sort/sort_context.cpp
    pipeline/pipeline_driver_executor.cpp
    pipeline/pipeline_driver_queue.cpp
    pipeline/pipeline_driver_poller.cpp
    pipeline/pipeline_driver.cpp
    pipeline/exec_state_reporter.cpp
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
    workgroup/work_group.cpp
    workgroup/scan_executor.cpp
    workgroup/scan_task_queue.cpp
)

# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set_source_files_properties(vectorized/json_scanner.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
    set_source_files_properties(vectorized/json_parser.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
endif()

set(EXEC_FILES
    ${EXEC_FILES}
    vectorized/mysql_scan_node.cpp
    mysql_scanner.cpp
    vectorized/hdfs_scan_node.cpp)

add_library(Exec STATIC
    ${EXEC_FILES}
)
