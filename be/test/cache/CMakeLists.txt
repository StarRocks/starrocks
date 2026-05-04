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

set(CACHE_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    data_cache_hit_rate_counter_test.cpp
    datacache_metrics_test.cpp
    datacache_utils_test.cpp
    mem_cache/lrucache_engine_test.cpp
    mem_cache/page_cache_test.cpp
)

if ("${WITH_STARCACHE}" STREQUAL "ON")
    list(APPEND CACHE_TEST_FILES
        disk_cache/block_cache_test.cpp
        disk_cache/starcache_engine_test.cpp
        disk_space_monitor_test.cpp
        peer_cache_test.cpp
    )
endif()

set(CACHE_TEST_LINK_LIBS
    Cache
)

list(APPEND CACHE_TEST_LINK_LIBS
    RuntimeCore
    FSCore
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

add_library(starrocks_cache_test_objs OBJECT ${CACHE_TEST_FILES})
target_link_libraries(starrocks_cache_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_cache_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(cache_test $<TARGET_OBJECTS:starrocks_cache_test_objs>)
target_link_libraries(cache_test ${CACHE_TEST_LINK_LIBS} gtest_main)
