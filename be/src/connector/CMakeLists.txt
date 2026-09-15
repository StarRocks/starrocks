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
        hive/paimon/paimon_cpp_loader.cpp
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

# The paimon-cpp shim: the only binary linked against libpaimon.so. It is
# loaded lazily by paimon_cpp_loader.cpp (compiled into ConnectorHive above)
# through dlopen on the first paimon-native scan, so starrocks_be carries no
# paimon DT_NEEDED and starts without the paimon libraries. BE symbols
# (HdfsScanner, Status, Chunk, ...) stay undefined here and resolve against
# the executable at dlopen time: starrocks_be links with -rdynamic (for
# backtraces), which also exports its symbols to dlopen()ed libraries. The
# sources are compiled again in this target (not reused from ConnectorHive)
# because regular BE objects are not built with -fPIC, and linking BE static
# libs would duplicate global state.
# Skipped for UT builds (the test binary compiles the sources directly) and
# for format-lib builds (no starrocks_be to load it, and its objects would
# carry -DBUILD_FORMAT_LIB).
if ("${WITH_PAIMON_CPP}" STREQUAL "ON" AND NOT "${MAKE_TEST}" STREQUAL "ON" AND NOT BUILD_FORMAT_LIB)
    add_library(starrocks_paimon SHARED
            hive/paimon/paimon_cpp_entry.cpp
            hive/paimon/paimon_predicate_converter.cpp
            hive/paimon/paimon_file_system.cpp
            hive/paimon/paimon_scanner.cpp
    )
    set_target_properties(starrocks_paimon PROPERTIES POSITION_INDEPENDENT_CODE ON)
    # WRAP_LINKER_FLAGS keeps __cxa_throw/__floattidf wrapping consistent with
    # the executable; the __wrap_* definitions resolve from it at dlopen.
    # fmt is linked in directly: libfmt.a is built with hidden visibility, so
    # the executable's copy is absent from its .dynsym and cannot satisfy the
    # shim at dlopen time. fmt is stateless, so a second copy is harmless.
    target_link_libraries(starrocks_paimon PRIVATE ${WRAP_LINKER_FLAGS} ${DEP_SANITIZE_LIBS} paimon fmt)
    # The paimon plugin libraries register themselves through load-time
    # constructors and export nothing the shim references, so an as-needed
    # link silently drops their DT_NEEDED entries and paimon ends up with no
    # parquet/orc/avro format at runtime. Force every listed library to stay.
    target_link_options(starrocks_paimon PRIVATE "LINKER:--no-as-needed")
    # $ORIGIN: resolve libpaimon.so and its plugins from the shim's own
    # directory; CMAKE_SKIP_RPATH is ON, so set the rpath by hand.
    target_link_options(starrocks_paimon PRIVATE "LINKER:-rpath,$ORIGIN")
    # build.sh copies this directory into the package as be/lib/paimon-cpp-lib,
    # together with the libpaimon*.so files from thirdparty.
    install(TARGETS starrocks_paimon LIBRARY DESTINATION ${OUTPUT_DIR}/lib/paimon-cpp-lib)
endif ()

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
)

target_link_libraries(Connector PUBLIC
        ConnectorPrimitive
        Common
        Base
        Gutil
        StarRocksGen
)

ADD_BE_LIB(ConnectorADBC
        adbc_connector.cpp
        ../exec/adbc_scanner.cpp
)

target_link_libraries(ConnectorADBC PUBLIC
        ConnectorPrimitive
        Runtime
        ChunkCore
        ColumnCore
        Types
        Common
        Base
        Gutil
        StarRocksGen
)

target_link_libraries(ConnectorADBC PRIVATE
        Platform
        arrow
        adbc_driver_manager
)

ADD_BE_LIB(ConnectorLake
        lake/lake_connector.cpp
        lake/lake_global_late_materialization_context.cpp
)

target_link_libraries(ConnectorLake PUBLIC
        ConnectorPrimitive
        Storage
        ComputeEnv
        StoragePrimitive
        ExecPrimitive
        Expr
        Runtime
        Platform
        FileSystem
        IO
        ChunkCore
        ColumnCore
        Types
        Common
        Base
        Gutil
        StarRocksGen
)

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
