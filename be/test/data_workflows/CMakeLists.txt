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

set(DATA_WORKFLOWS_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    load/batch_write/batch_write_mgr_test.cpp
    load/batch_write/batch_write_util_test.cpp
    load/batch_write/isomorphic_batch_write_test.cpp
    load/batch_write/txn_state_cache_test.cpp
    load/tablet_writer/lake_tablets_channel_test.cpp
    load/tablet_writer/load_channel_mgr_test.cpp
    load/tablet_writer/load_channel_test.cpp
    load/tablet_writer/local_tablets_channel_test.cpp
    push_handler_test.cpp
    rejected_record_sync_daemon_test.cpp
    snapshot/snapshot_loader_test.cpp
)

add_library(starrocks_data_workflows_test_objs OBJECT ${DATA_WORKFLOWS_TEST_FILES})
target_link_libraries(starrocks_data_workflows_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_data_workflows_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(data_workflows_test ${CMAKE_SOURCE_DIR}/test/test_main.cpp $<TARGET_OBJECTS:starrocks_data_workflows_test_objs>)
target_link_libraries(data_workflows_test ${TEST_LINK_LIBS})
STARROCKS_FORCE_LOAD_LIBS(data_workflows_test Exprs ExprCore)
