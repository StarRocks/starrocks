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

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec")

set(EXEC_FILES
    aggregation_node.cpp
    aggregation_node_ir.cpp
    analytic_eval_node.cpp
    blocking_join_node.cpp
    file_scan_node.cpp
    broker_reader.cpp
    buffered_reader.cpp
    file_scanner.cpp
    broker_scanner.cpp
    convert_scan_node.cpp
    cross_join_node.cpp
    data_sink.cpp
    decompressor.cpp
    empty_set_node.cpp
    exec_node.cpp
    exchange_node.cpp
    hash_join_node.cpp
    hash_join_node_ir.cpp
    hash_table.cpp
    local_file_reader.cpp
    merge_node.cpp
    merge_join_node.cpp
    scan_node.cpp
    select_node.cpp
    text_converter.cpp
    topn_node.cpp
    sort_exec_exprs.cpp
    olap_scan_node.cpp
    olap_scanner.cpp
    olap_common.cpp
    tablet_info.cpp
    tablet_sink.cpp
    plain_text_line_reader.cpp
    es_http_scan_node.cpp
    es_http_scanner.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_scroll_parser.cpp
    es/es_query_builder.cpp
    spill_sort_node.cc
    union_node.cpp
    union_node_ir.cpp
    set_operation_node.cpp
    intersect_node.cpp
    except_node.cpp
    repeat_node.cpp
    schema_scanner.cpp
    schema_scan_node.cpp
    schema_scanner/schema_tables_scanner.cpp
    schema_scanner/schema_dummy_scanner.cpp
    schema_scanner/schema_schemata_scanner.cpp
    schema_scanner/schema_variables_scanner.cpp
    schema_scanner/schema_columns_scanner.cpp
    schema_scanner/schema_charsets_scanner.cpp
    schema_scanner/schema_collations_scanner.cpp
    schema_scanner/schema_statistics_scanner.cpp
    schema_scanner/schema_triggers_scanner.cpp
    schema_scanner/schema_events_scanner.cpp
    schema_scanner/schema_views_scanner.cpp
    schema_scanner/schema_user_privileges_scanner.cpp
    schema_scanner/schema_schema_privileges_scanner.cpp
    schema_scanner/schema_table_privileges_scanner.cpp
    schema_scanner/schema_helper.cpp
    partitioned_hash_table.cc
    partitioned_hash_table_ir.cc
    partitioned_aggregation_node.cc
    partitioned_aggregation_node_ir.cc
    local_file_writer.cpp
    broker_writer.cpp
    parquet_scanner.cpp
    parquet_reader.cpp
    parquet_writer.cpp
    orc_scanner.cpp
    json_scanner.cpp
    assert_num_rows_node.cpp
    vectorized/adapter_node.cpp
    vectorized/aggregator.cpp
    vectorized/aggregate/aggregate_base_node.cpp
    vectorized/aggregate/aggregate_blocking_node.cpp
    vectorized/aggregate/distinct_blocking_node.cpp
    vectorized/aggregate/aggregate_streaming_node.cpp
    vectorized/aggregate/distinct_streaming_node.cpp
    vectorized/analytic_node.cpp
    vectorized/csv_scanner.cpp
    vectorized/olap_scanner.cpp
    vectorized/olap_scan_node.cpp
    vectorized/hash_join_node.cpp
    vectorized/join_hash_map.cpp
    vectorized/topn_node.cpp
    vectorized/chunks_sorter.cpp
    vectorized/chunks_sorter_topn.cpp
    vectorized/chunks_sorter_full_sort.cpp
    vectorized/cross_join_node.cpp
    vectorized/union_node.cpp
    vectorized/tablet_info.cpp
    vectorized/except_node.cpp
    vectorized/file_scanner.cpp
    vectorized/orc_scanner.cpp
    vectorized/orc_scanner_adapter.cpp
    vectorized/arrow_to_starrocks_converter.cpp
    vectorized/parquet_scanner.cpp
    vectorized/parquet_reader.cpp
    vectorized/file_scan_node.cpp
    vectorized/assert_num_rows_node.cpp
    vectorized/intersect_node.cpp
    vectorized/hdfs_scanner.cpp
    vectorized/hdfs_scanner_orc.cpp
    vectorized/json_scanner.cpp
    vectorized/project_node.cpp
    vectorized/repeat_node.cpp
    vectorized/table_function_node.cpp
    vectorized/mysql_scan_node.cpp
    vectorized/es_http_scan_node.cpp
    vectorized/es_http_scanner.cpp
    vectorized/es_http_components.cpp
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
    pipeline/exchange/exchange_sink_operator.cpp
    pipeline/exchange/exchange_source_operator.cpp
    pipeline/exchange/local_exchange.cpp
    pipeline/exchange/local_exchange_sink_operator.cpp
    pipeline/exchange/local_exchange_source_operator.cpp
    pipeline/fragment_executor.cpp
    pipeline/operator.cpp
    pipeline/limit_operator.cpp
    pipeline/olap_chunk_source.cpp
    pipeline/pipeline_builder.cpp
    pipeline/project_operator.cpp
    pipeline/result_sink_operator.cpp
    pipeline/scan_operator.cpp
    pipeline/sort/sort_sink_operator.cpp
    pipeline/sort/sort_source_operator.cpp
    pipeline/pipeline_driver_dispatcher.cpp
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
)

if (WITH_MYSQL)
    set(EXEC_FILES
        ${EXEC_FILES}
        mysql_scan_node.cpp
        mysql_scanner.cpp
        )
endif()

IF (WITH_HDFS)
    set(EXEC_FILES ${EXEC_FILES}
        vectorized/hdfs_scan_node.cpp
    )
endif()

add_library(Exec STATIC
    ${EXEC_FILES}
)
