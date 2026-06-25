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

set(COMPUTE_ENV_TEST_LINK_LIBS
    ComputeEnv
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

set(COMPUTE_ENV_WORKGROUP_TEST_FILES
    workgroup/lock_free_scan_task_queue_test.cpp
    workgroup/lock_free_work_group_scan_task_queue_test.cpp
    workgroup/mem_tracker_manager_test.cpp
    workgroup/pipeline_executor_set_manager_test.cpp
    workgroup/pipeline_executor_set_test.cpp
    workgroup/scan_task_queue_test.cpp
    workgroup/work_group_manager_test.cpp
    workgroup/work_group_test.cpp
)

set(COMPUTE_ENV_SPILL_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    spill/operator_mem_resource_manager_test.cpp
    spill/query_spill_manager_test.cpp
    spill/spill_block_manager_test.cpp
    spill/spill_metrics_test.cpp
    spill/spill_test.cpp
)

set(COMPUTE_ENV_STAROS_TEST_FILES
    staros/staros_worker_metrics_test.cpp
)

if (NOT APPLE)
    list(APPEND COMPUTE_ENV_STAROS_TEST_FILES ../base/cxa_throw_wrap.cpp)
endif()

if ("${USE_STAROS}" STREQUAL "ON")
    list(APPEND COMPUTE_ENV_STAROS_TEST_FILES staros/starlet_filesystem_test.cpp)
    list(APPEND COMPUTE_ENV_STAROS_TEST_FILES staros/staros_worker_test.cpp)
endif()

add_executable(compute_env_test
        buffer_control_block_test.cpp
        compute_env_test.cpp
        compute_env_test_main.cpp
        data_stream_mgr_test.cpp
        fragment_dict_state_test.cpp
        load/load_stream_mgr_test.cpp
        load/stream_load_context_test.cpp
        load/stream_load_pipe_test.cpp
        load_path_state_helper_test.cpp
        load_path_mgr_test.cpp
        profile_report_worker_test.cpp
        query/file_scan_split_context_test.cpp
        query/query_scan_helpers_test.cpp
        query_cache_test.cpp
        rejected_record_writer_test.cpp
        result_queue_mgr_test.cpp
        runtime_range_pruner_test.cpp
        ${COMPUTE_ENV_WORKGROUP_TEST_FILES}
        ${COMPUTE_ENV_SPILL_TEST_FILES}
        ${COMPUTE_ENV_STAROS_TEST_FILES})
set_target_properties(compute_env_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
target_link_libraries(compute_env_test ${COMPUTE_ENV_TEST_LINK_LIBS})

set(COMPUTE_ENV_SORTING_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    sorting/sorted_chunks_merger_test.cpp
    sorting/sorting_test.cpp
)

set(COMPUTE_ENV_SORTING_TEST_LINK_LIBS
    ComputeEnv
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

add_executable(compute_env_sorting_test ${COMPUTE_ENV_SORTING_TEST_FILES})
target_link_libraries(compute_env_sorting_test ${COMPUTE_ENV_SORTING_TEST_LINK_LIBS} gtest_main)
