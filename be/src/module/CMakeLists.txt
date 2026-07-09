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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/module")

ADD_BE_LIB(ModuleBootstrap
    connector_bootstrap.cpp
    fs_provider_bootstrap.cpp
)

target_link_libraries(ModuleBootstrap PUBLIC
        Common
)

target_link_libraries(ModuleBootstrap PRIVATE
        FileSystem
        HdfsFileSystem
        ComputeEnv
        Connector
        ConnectorCacheStats
        Base
        Gutil
        StarRocksGen
)

if (WITH_CONNECTOR_BENCHMARK)
    target_compile_definitions(ModuleBootstrap PRIVATE STARROCKS_WITH_CONNECTOR_BENCHMARK)
    target_link_libraries(ModuleBootstrap PRIVATE
            ConnectorBenchmark
    )
endif()

if (WITH_CONNECTOR_JDBC)
    target_compile_definitions(ModuleBootstrap PRIVATE STARROCKS_WITH_CONNECTOR_JDBC)
    target_link_libraries(ModuleBootstrap PRIVATE
            ConnectorJDBC
    )
endif()

if (WITH_CONNECTOR_ELASTICSEARCH)
    target_compile_definitions(ModuleBootstrap PRIVATE STARROCKS_WITH_CONNECTOR_ELASTICSEARCH)
    target_link_libraries(ModuleBootstrap PRIVATE
            ConnectorElasticsearch
    )
endif()

if (WITH_CONNECTOR_MYSQL)
    target_compile_definitions(ModuleBootstrap PRIVATE STARROCKS_WITH_CONNECTOR_MYSQL)
    target_link_libraries(ModuleBootstrap PRIVATE
            ConnectorMySQL
    )
endif()
