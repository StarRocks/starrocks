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

set(RUNTIME_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    byte_buffer_test.cpp
    current_thread_core_test.cpp
    descriptors_test.cpp
    diagnose_daemon_test.cpp
    exception_stack_test.cpp
    runtime_chunk_helper_test.cpp
    mem_tracker_test.cpp
    mem_pool_test.cpp
    message_body_sink_test.cpp
    process_memory_metrics_test.cpp
    load_fail_point_test.cpp
    mysql_table_descriptor_core_test.cpp
    query_statistics_test.cpp
    runtime_metrics_test.cpp
    # RuntimeEnv::init owns the process-global HLL registers allocator. Keep
    # this before serde_core_test, whose HLL tests exercise that allocator.
    runtime_env_test.cpp
    java_env_test.cpp
    jvm_metrics_test.cpp
    jvm_helper_test.cpp
    native_method_helper_test.cpp
    runtime_state_core_test.cpp
    runtime_filter_core_test.cpp
    serde_core_test.cpp
    time_guard_test.cpp
    memory/mem_chunk_allocator_test.cpp
    memory/system_allocator_test.cpp
    memory/memory_resource_test.cpp
    memory/counting_allocator_test.cpp
    memory/memory_allocator_test.cpp
    memory/memory_lock_test.cpp
)

set(RUNTIME_TEST_LINK_LIBS
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
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

add_library(starrocks_runtime_test_objs OBJECT ${RUNTIME_TEST_FILES})
target_link_libraries(starrocks_runtime_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_runtime_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(runtime_test $<TARGET_OBJECTS:starrocks_runtime_test_objs>)
target_link_libraries(runtime_test ${RUNTIME_TEST_LINK_LIBS} gtest_main)
