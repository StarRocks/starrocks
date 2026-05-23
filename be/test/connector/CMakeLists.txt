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

set(CONNECTOR_PRIMITIVE_TEST_LINK_LIBS
    ConnectorPrimitive
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
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(connector_primitive_test ${BE_TEST_MACOS_LINK_STUBS} connector_primitive_test.cpp)
target_link_libraries(connector_primitive_test ${CONNECTOR_PRIMITIVE_TEST_LINK_LIBS} gtest_main)
set_target_properties(connector_primitive_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

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
            benchmark/benchmark_connector_test.cpp
            benchmark/benchmark_scanner_test.cpp
    )
    target_link_libraries(connector_benchmark_test ${CONNECTOR_BENCHMARK_TEST_LINK_LIBS} gtest_main)
    set_target_properties(connector_benchmark_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
endif()
