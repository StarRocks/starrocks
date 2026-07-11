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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/connector")

ADD_BE_LIB(ConnectorCommon
        common/connector_sink_commit.cpp
        common/connector_sink_executor.cpp
        common/hive_partition_utils.cpp
        common/partition_chunk_writer.cpp
        common/partition_chunk_writer_memory_manager.cpp
        common/partitioned_connector_chunk_sink.cpp
        common/spill_partition_chunk_writer.cpp
        common/utils.cpp
)

target_link_libraries(ConnectorCommon PUBLIC
        ComputeEnv
        ConnectorPrimitive
        Runtime
        Platform
        FormatCore
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

ADD_BE_LIB(ConnectorHive
        hive/hive_connector.cpp
        hive/hive_chunk_sink.cpp
        hive/scanner/cache_select_scanner.cpp
        hive/scanner/hdfs_scanner.cpp
        hive/scanner/hdfs_scanner_avro.cpp
        hive/scanner/hdfs_scanner_json.cpp
        hive/scanner/hdfs_scanner_orc.cpp
        hive/scanner/hdfs_scanner_parquet.cpp
        hive/scanner/hdfs_scanner_partition.cpp
        hive/scanner/hdfs_scanner_text.cpp
        hive/scanner/jni_scanner.cpp
)

target_link_libraries(ConnectorHive PUBLIC
        ConnectorCommon
        ConnectorPrimitive
        ComputeEnv
        Cache
        StoragePrimitive
        ExecPrimitive
        Expr
        Runtime
        Platform
        FormatCore
        FormatCsv
        FormatJson
        FormatAvro
        FormatOrc
        FormatParquet
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

target_link_libraries(ConnectorHive PRIVATE Formats)

ADD_BE_LIB(ConnectorIceberg
        iceberg/iceberg_connector.cpp
        iceberg/iceberg_chunk_sink.cpp
        iceberg/iceberg_utils.cpp
        iceberg/iceberg_delete_sink.cpp
        iceberg/iceberg_row_delta_sink.cpp
)

target_link_libraries(ConnectorIceberg PUBLIC
        ConnectorCommon
        ConnectorPrimitive
        FormatCore
        FormatParquet
        ChunkCore
        Types
        Common
        StarRocksGen
)

target_link_libraries(ConnectorIceberg PRIVATE
        Expr
        Runtime
        FileSystem
        ColumnSortCore
        ColumnCore
        Base
        Gutil
)

ADD_BE_LIB(Connector
        connector_registry.cpp
        lake_connector.cpp
)

target_link_libraries(Connector PUBLIC ConnectorCommon ConnectorPrimitive ComputeEnv Cache)
target_link_libraries(Connector PRIVATE Platform)

ADD_BE_LIB(ConnectorFile
        file/file_connector.cpp
        file/file_scan_metrics.cpp
        file/file_scan_utils.cpp
        file/file_chunk_sink.cpp
        file/scanner/avro_cpp_scanner.cpp
        file/scanner/avro_scanner.cpp
        file/scanner/csv_scanner.cpp
        file/scanner/file_scanner.cpp
        file/scanner/json_scanner.cpp
        file/scanner/orc_scanner.cpp
        file/scanner/parquet_scanner.cpp
        file/scanner/arrow_scanner.cpp
        file/scanner/stream_source_meta.cpp
        file/scanner/parquet_reader.cpp
        file/scanner/parquet_schema_builder.cpp
)

target_link_libraries(ConnectorFile PUBLIC
        ConnectorCommon
        ConnectorPrimitive
        ComputeEnv
        Cache
        Expr
        Runtime
        Platform
        FormatCore
        FormatCsv
        FormatJson
        FormatAvro
        FormatOrc
        FormatParquet
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

ADD_BE_LIB(ConnectorCacheStats
        cache_stats/cache_stats_connector.cpp
        cache_stats/cache_stats_scanner.cpp
)

target_link_libraries(ConnectorCacheStats PUBLIC
        ConnectorPrimitive
        Storage
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

if (WITH_CONNECTOR_ELASTICSEARCH)
    ADD_BE_LIB(ConnectorElasticsearch
            elasticsearch/es_connector.cpp
            elasticsearch/es_predicate.cpp
            elasticsearch/es_query_builder.cpp
            elasticsearch/es_scan_reader.cpp
            elasticsearch/es_scroll_parser.cpp
            elasticsearch/es_scroll_query.cpp
    )

    target_link_libraries(ConnectorElasticsearch PUBLIC
            ConnectorPrimitive
            Platform
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
endif()

if (WITH_CONNECTOR_MYSQL)
    ADD_BE_LIB(ConnectorMySQL
            mysql/mysql_connector.cpp
            mysql/mysql_scanner.cpp
    )

    target_link_libraries(ConnectorMySQL PUBLIC
            ConnectorPrimitive
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

    target_link_libraries(ConnectorMySQL PRIVATE
            mysql
    )
endif()

if (WITH_CONNECTOR_BENCHMARK)
    ADD_BE_LIB(ConnectorBenchmark
            benchmark/benchmark_connector.cpp
            benchmark/benchmark_scanner.cpp
    )

    target_link_libraries(ConnectorBenchmark PUBLIC
            ConnectorPrimitive
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

    target_link_libraries(ConnectorBenchmark PRIVATE
            benchgen
            arrow
    )
endif()

if (WITH_CONNECTOR_JDBC)
    ADD_BE_LIB(ConnectorJDBC
            jdbc/jdbc_connector.cpp
            jdbc/jdbc_scanner.cpp
            jdbc/jdbc_driver_manager.cpp
            jdbc/type_checker.cpp
            jdbc/type_checker_manager.cpp
            jdbc/type_checker_xml_loader.cpp
    )

    target_link_libraries(ConnectorJDBC PUBLIC
            ConnectorPrimitive
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

    target_link_libraries(ConnectorJDBC PRIVATE
            Platform
            FileSystem
            libxml2
    )
endif()

ADD_BE_LIB(ConnectorBuiltinRegistry
        connector_registry_init.cpp
)

target_link_libraries(ConnectorBuiltinRegistry PUBLIC
        Connector
        ConnectorFile
        Common
        Base
        Gutil
        StarRocksGen
)
