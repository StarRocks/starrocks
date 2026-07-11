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

set(BE_TEST_MACOS_LINK_STUBS)
if (APPLE)
    list(APPEND BE_TEST_MACOS_LINK_STUBS ../base/macos_link_stubs.cpp)
endif()

set(CONNECTOR_COMMON_TEST_DEPENDENCIES ${STARROCKS_DEPENDENCIES})
list(REMOVE_ITEM CONNECTOR_COMMON_TEST_DEPENDENCIES arrow_testing)

set(CONNECTOR_COMMON_TEST_LINK_LIBS
    ConnectorCommon
    ${CONNECTOR_COMMON_TEST_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_common_test
        ${BE_TEST_MACOS_LINK_STUBS}
        ${BE_TEST_JNI_ENV_STUB}
        common/connector_common_test.cpp
        common/partition_chunk_writer_test.cpp
        common/partition_chunk_writer_memory_manager_test.cpp
)
target_link_libraries(connector_common_test ${CONNECTOR_COMMON_TEST_LINK_LIBS} gtest_main)
set_target_properties(connector_common_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

set(CONNECTOR_FILE_TEST_LINK_LIBS
    ConnectorFile
    ${CONNECTOR_COMMON_TEST_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_file_test
        ${BE_TEST_MACOS_LINK_STUBS}
        ${BE_TEST_JNI_ENV_STUB}
        file/file_test_main.cpp
        file/file_chunk_sink_test.cpp
        file/file_connector_rejected_record_test.cpp
        file/file_scan_metrics_test.cpp
        file/file_scan_utils_test.cpp
        file/scanner/arrow_scanner_test.cpp
        file/scanner/avro_cpp_scanner_test.cpp
        file/scanner/avro_scanner_test.cpp
        file/scanner/csv_scanner_test.cpp
        file/scanner/file_scanner_test.cpp
        file/scanner/json_scanner_test.cpp
        file/scanner/orc_scanner_test.cpp
        file/scanner/parquet_schema_builder_test.cpp
        file/scanner/parquet_scanner_test.cpp
        file/scanner/stream_source_meta_test.cpp
)
target_link_libraries(connector_file_test ${CONNECTOR_FILE_TEST_LINK_LIBS})
set_target_properties(connector_file_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

set(CONNECTOR_HIVE_TEST_LINK_LIBS
    ConnectorHive
    ${CONNECTOR_COMMON_TEST_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_hive_test
        ${BE_TEST_MACOS_LINK_STUBS}
        ${BE_TEST_JNI_ENV_STUB}
        hive/hive_test_main.cpp
        hive/hive_connector_test.cpp
        hive/hive_chunk_sink_test.cpp
        hive/scanner/cache_select_scanner_test.cpp
        hive/scanner/hdfs_scanner_json_test.cpp
        hive/scanner/hdfs_scanner_test.cpp
        hive/scanner/jni_scanner_test.cpp
)
target_link_libraries(connector_hive_test ${CONNECTOR_HIVE_TEST_LINK_LIBS} hdfs jvm)
set_target_properties(connector_hive_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

set(CONNECTOR_ICEBERG_TEST_LINK_LIBS
    ConnectorIceberg
    ${CONNECTOR_COMMON_TEST_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_iceberg_test
        ${BE_TEST_MACOS_LINK_STUBS}
        ${BE_TEST_JNI_ENV_STUB}
        iceberg/iceberg_test_main.cpp
        iceberg/iceberg_chunk_sink_test.cpp
        iceberg/iceberg_delete_sink_test.cpp
        iceberg/iceberg_row_delta_sink_test.cpp
)
target_link_libraries(connector_iceberg_test ${CONNECTOR_ICEBERG_TEST_LINK_LIBS})
set_target_properties(connector_iceberg_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

set(CONNECTOR_CACHE_STATS_TEST_LINK_LIBS
    ConnectorCacheStats
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_cache_stats_test
        ${BE_TEST_MACOS_LINK_STUBS}
        ${BE_TEST_JNI_ENV_STUB}
        cache_stats/cache_stats_connector_test.cpp
)
target_link_libraries(connector_cache_stats_test ${CONNECTOR_CACHE_STATS_TEST_LINK_LIBS} gtest_main)
set_target_properties(connector_cache_stats_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

if (WITH_CONNECTOR_MYSQL)
    set(CONNECTOR_MYSQL_TEST_LINK_LIBS
        ConnectorMySQL
        ${STARROCKS_DEPENDENCIES}
        ${STARROCKS_STATIC_LINK_LIBS}
        ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
        ${WL_START_GROUP}
        gmock
        gtest
        ${WL_END_GROUP}
    )

    add_executable(connector_mysql_test
            ${BE_TEST_MACOS_LINK_STUBS}
            ${BE_TEST_JNI_ENV_STUB}
            mysql/mysql_connector_test.cpp
    )
    target_link_libraries(connector_mysql_test ${CONNECTOR_MYSQL_TEST_LINK_LIBS} gtest_main)
    set_target_properties(connector_mysql_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
endif()

if (WITH_CONNECTOR_BENCHMARK)
    set(CONNECTOR_BENCHMARK_TEST_LINK_LIBS
        ConnectorBenchmark
        ${STARROCKS_DEPENDENCIES}
        ${STARROCKS_STATIC_LINK_LIBS}
        ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
        ${WL_START_GROUP}
        gmock
        gtest
        ${WL_END_GROUP}
    )

    add_executable(connector_benchmark_test
            ${BE_TEST_MACOS_LINK_STUBS}
            ${BE_TEST_JNI_ENV_STUB}
            benchmark/benchmark_connector_test.cpp
            benchmark/benchmark_scanner_test.cpp
    )
    target_link_libraries(connector_benchmark_test ${CONNECTOR_BENCHMARK_TEST_LINK_LIBS} gtest_main)
    set_target_properties(connector_benchmark_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
endif()

if (WITH_CONNECTOR_JDBC)
    set(CONNECTOR_JDBC_TEST_LINK_LIBS
        ConnectorJDBC
        hdfs
        jvm
        ${STARROCKS_DEPENDENCIES}
        ${STARROCKS_STATIC_LINK_LIBS}
        ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
        ${WL_START_GROUP}
        gmock
        gtest
        ${WL_END_GROUP}
    )

    add_executable(connector_jdbc_test
            ${BE_TEST_MACOS_LINK_STUBS}
            jdbc/jdbc_connector_test.cpp
            jdbc/type_checker_test.cpp
            jdbc/type_checker_xml_loader_test.cpp
    )
    target_link_libraries(connector_jdbc_test ${CONNECTOR_JDBC_TEST_LINK_LIBS} gtest_main)
    set_target_properties(connector_jdbc_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
endif()

if (WITH_CONNECTOR_ELASTICSEARCH)
    set(CONNECTOR_ELASTICSEARCH_TEST_LINK_LIBS
        ConnectorElasticsearch
        ${STARROCKS_DEPENDENCIES}
        ${STARROCKS_STATIC_LINK_LIBS}
        ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
        ${WL_START_GROUP}
        gmock
        gtest
        ${WL_END_GROUP}
    )

    add_executable(connector_elasticsearch_test
            ${BE_TEST_MACOS_LINK_STUBS}
            ${BE_TEST_JNI_ENV_STUB}
            ${SRC_DIR}/runtime/descriptors_ext.cpp
            elasticsearch/es_query_builder_test.cpp
            elasticsearch/es_scan_reader_test.cpp
            elasticsearch/es_scroll_parser_test.cpp
    )
    target_link_libraries(connector_elasticsearch_test ${CONNECTOR_ELASTICSEARCH_TEST_LINK_LIBS} gtest_main)
    set_target_properties(connector_elasticsearch_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
endif()
