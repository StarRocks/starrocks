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

ADD_BE_LIB(ConnectorPrimitive
        connector.cpp
        data_source.cpp
        data_source_provider.cpp
)

target_link_libraries(ConnectorPrimitive PUBLIC
        ExecPrimitive
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

ADD_BE_LIB(Connector
        connector_registry.cpp
        hive_connector.cpp
        es_connector.cpp
        jdbc_connector.cpp
        lake_connector.cpp
        mysql_connector.cpp
        cache_stats_connector.cpp
        file_connector.cpp
        file_scan_metrics.cpp
        iceberg_connector.cpp
        connector_chunk_sink.cpp
        file_chunk_sink.cpp
        hive_chunk_sink.cpp
        iceberg_chunk_sink.cpp
        utils.cpp
        async_flush_stream_poller.cpp
        sink_memory_manager.cpp
        partition_chunk_writer.cpp
        connector_sink_executor.cpp
        deletion_vector/deletion_vector.cpp
        deletion_vector/deletion_bitmap.cpp
        iceberg_delete_sink.cpp
        iceberg_row_delta_sink.cpp
)

target_link_libraries(Connector PUBLIC ConnectorPrimitive)

if (WITH_CONNECTOR_BENCHMARK)
    ADD_BE_LIB(ConnectorBenchmark
            benchmark/benchmark_connector.cpp
            benchmark/benchmark_scanner.cpp
    )

    target_link_libraries(ConnectorBenchmark PUBLIC
            ConnectorPrimitive
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

    target_link_libraries(ConnectorBenchmark PRIVATE
            benchgen
            arrow
    )
endif()

ADD_BE_LIB(ConnectorBootstrap
        connector_bootstrap.cpp
)

target_link_libraries(ConnectorBootstrap PUBLIC
        Common
)

target_link_libraries(ConnectorBootstrap PRIVATE
        Connector
        Base
        Gutil
        StarRocksGen
)

if (WITH_CONNECTOR_BENCHMARK)
    target_compile_definitions(ConnectorBootstrap PRIVATE STARROCKS_WITH_CONNECTOR_BENCHMARK)
    target_link_libraries(ConnectorBootstrap PRIVATE
            ConnectorBenchmark
    )
endif()

ADD_BE_LIB(ConnectorBuiltinRegistry
        connector_registry_init.cpp
)

target_link_libraries(ConnectorBuiltinRegistry PUBLIC
        Connector
        Common
        Base
        Gutil
        StarRocksGen
)
