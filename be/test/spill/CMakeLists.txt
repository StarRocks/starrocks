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

set(SPILL_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    operator_mem_resource_manager_test.cpp
    query_spill_manager_test.cpp
    spill_block_manager_test.cpp
    spill_metrics_test.cpp
)

set(SPILL_CORE_TEST_LINK_LIBS
    SpillCore
    RuntimeCore
    FSCore
    IOCore
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

add_library(starrocks_spill_core_test_objs OBJECT ${SPILL_CORE_TEST_FILES})
target_link_libraries(starrocks_spill_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_spill_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(spill_core_test $<TARGET_OBJECTS:starrocks_spill_core_test_objs>)
target_link_libraries(spill_core_test ${SPILL_CORE_TEST_LINK_LIBS} gtest_main)
