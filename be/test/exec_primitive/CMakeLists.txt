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

set(EXEC_PRIMITIVE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    data_sink/data_sink_core_test.cpp
    exec_node_exec_primitive_test.cpp
    ${BE_TEST_JNI_ENV_STUB}
    footer_prefetch_state_exec_primitive_test.cpp
    morsel_primitive_test.cpp
    pipeline_event_exec_primitive_test.cpp
    pipeline_metrics_exec_primitive_test.cpp
    pipeline_observer_exec_primitive_test.cpp
    result_to_arrow_converter_exec_primitive_test.cpp
    runtime_filter_exec_primitive_test.cpp
    warm_slot_reservation_exec_primitive_test.cpp
)

set(EXEC_PRIMITIVE_TEST_LINK_LIBS
    ExecPrimitive
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

add_library(starrocks_exec_primitive_test_objs OBJECT ${EXEC_PRIMITIVE_TEST_FILES})
target_link_libraries(starrocks_exec_primitive_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_exec_primitive_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(exec_primitive_test $<TARGET_OBJECTS:starrocks_exec_primitive_test_objs>)
target_link_libraries(exec_primitive_test ${EXEC_PRIMITIVE_TEST_LINK_LIBS} gtest_main)
