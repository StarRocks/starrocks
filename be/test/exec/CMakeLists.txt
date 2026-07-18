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

set(EXEC_RUNTIME_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    ${BE_TEST_JNI_ENV_STUB}
    pipeline_driver_exec_runtime_test.cpp
    pipeline_event_scheduler_exec_runtime_test.cpp
    pipeline_execution_group_exec_runtime_test.cpp
    pipeline_builder_context_exec_runtime_test.cpp
    pipeline_fragment_context_exec_runtime_test.cpp
    pipeline/query_context_manger_test.cpp
)

set(EXEC_RUNTIME_TEST_LINK_LIBS
    ExecRuntime
    ComputeEnv
    ExecPrimitive
    StoragePrimitive
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
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_exec_runtime_test_objs OBJECT ${EXEC_RUNTIME_TEST_FILES})
target_link_libraries(starrocks_exec_runtime_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_exec_runtime_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_runtime_test $<TARGET_OBJECTS:starrocks_exec_runtime_test_objs>)
target_link_libraries(exec_runtime_test ${EXEC_RUNTIME_TEST_LINK_LIBS} gtest_main)

set(JOIN_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    ${BE_TEST_JNI_ENV_STUB}
    join_hash_map_test.cpp
)

set(JOIN_CORE_TEST_LINK_LIBS
    ExecJoinCore
    ComputeEnv
    Expr
    Runtime
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

add_library(starrocks_join_core_test_objs OBJECT ${JOIN_CORE_TEST_FILES})
target_link_libraries(starrocks_join_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_join_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_join_core_test $<TARGET_OBJECTS:starrocks_join_core_test_objs>)
target_link_libraries(exec_join_core_test ${JOIN_CORE_TEST_LINK_LIBS} gtest_main)
