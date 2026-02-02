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

set(COMMON_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    config_test.cpp
    process_exit_test.cpp
    tracer_test.cpp
)

set(COMMON_TEST_LINK_LIBS
    Common
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} -lresolv -liberty -lc -lm -ldl -rdynamic -pthread ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_common_test_objs OBJECT ${COMMON_TEST_FILES})
target_link_libraries(starrocks_common_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_common_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(common_test $<TARGET_OBJECTS:starrocks_common_test_objs>)
target_link_libraries(common_test ${COMMON_TEST_LINK_LIBS} gtest_main)
