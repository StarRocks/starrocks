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

set(EXEC_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    exec_node_exec_core_test.cpp
    runtime_filter_exec_core_test.cpp
)

set(EXEC_CORE_TEST_LINK_LIBS
    ExecCore
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
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

add_library(starrocks_exec_core_test_objs OBJECT ${EXEC_CORE_TEST_FILES})
target_link_libraries(starrocks_exec_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_exec_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_core_test $<TARGET_OBJECTS:starrocks_exec_core_test_objs>)
target_link_libraries(exec_core_test ${EXEC_CORE_TEST_LINK_LIBS} gtest_main)

set(DATA_SINK_CORE_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    data_sink/data_sink_core_test.cpp
)

set(DATA_SINK_CORE_TEST_LINK_LIBS
    ExecSinkCore
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

add_library(starrocks_data_sink_core_test_objs OBJECT ${DATA_SINK_CORE_TEST_FILES})
target_link_libraries(starrocks_data_sink_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_data_sink_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(data_sink_core_test $<TARGET_OBJECTS:starrocks_data_sink_core_test_objs>)
target_link_libraries(data_sink_core_test ${DATA_SINK_CORE_TEST_LINK_LIBS} gtest_main)

set(SCHEMA_SCANNER_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    schema_scanner/schema_scanner_core_test.cpp
)

set(SCHEMA_SCANNER_CORE_TEST_LINK_LIBS
    ExecSchemaScannerCore
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
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

add_library(starrocks_schema_scanner_core_test_objs OBJECT ${SCHEMA_SCANNER_CORE_TEST_FILES})
target_link_libraries(starrocks_schema_scanner_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_schema_scanner_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(schema_scanner_core_test $<TARGET_OBJECTS:starrocks_schema_scanner_core_test_objs>)
target_link_libraries(schema_scanner_core_test ${SCHEMA_SCANNER_CORE_TEST_LINK_LIBS} gtest_main)

set(EXEC_SCHEMA_SCANNERS_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    schema_scanner/exec_schema_scanners_test.cpp
)

set(EXEC_SCHEMA_SCANNERS_TEST_LINK_LIBS
    ExecSchemaScanners
    ExecSchemaScannerCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
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

add_library(starrocks_exec_schema_scanners_test_objs OBJECT ${EXEC_SCHEMA_SCANNERS_TEST_FILES})
target_link_libraries(starrocks_exec_schema_scanners_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_exec_schema_scanners_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_schema_scanners_test $<TARGET_OBJECTS:starrocks_exec_schema_scanners_test_objs>)
target_link_libraries(exec_schema_scanners_test ${EXEC_SCHEMA_SCANNERS_TEST_LINK_LIBS} gtest_main)

set(SORTING_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    sorting/sorting_core_test.cpp
)

set(SORTING_CORE_TEST_LINK_LIBS
    ExecSortingCore
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
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

add_library(starrocks_sorting_core_test_objs OBJECT ${SORTING_CORE_TEST_FILES})
target_link_libraries(starrocks_sorting_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_sorting_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_sorting_core_test $<TARGET_OBJECTS:starrocks_sorting_core_test_objs>)
target_link_libraries(exec_sorting_core_test ${SORTING_CORE_TEST_LINK_LIBS} gtest_main)

set(JOIN_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    join_hash_map_test.cpp
)

set(JOIN_CORE_TEST_LINK_LIBS
    ExecJoinCore
    ExecSortingCore
    ExprCore
    SerdeCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
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

add_library(starrocks_join_core_test_objs OBJECT ${JOIN_CORE_TEST_FILES})
target_link_libraries(starrocks_join_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_join_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_join_core_test $<TARGET_OBJECTS:starrocks_join_core_test_objs>)
target_link_libraries(exec_join_core_test ${JOIN_CORE_TEST_LINK_LIBS} gtest_main)
