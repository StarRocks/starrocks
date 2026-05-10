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

set(GEO_TEST_FILES
    geo_types_test.cpp
    wkt_parse_test.cpp
)

set(GEO_TEST_LINK_LIBS
    Geo
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

add_library(starrocks_geo_test_objs OBJECT ${GEO_TEST_FILES})
target_link_libraries(starrocks_geo_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_geo_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(geo_test $<TARGET_OBJECTS:starrocks_geo_test_objs>)
target_link_be_test_cxa_throw_wrap(geo_test)
target_link_libraries(geo_test ${GEO_TEST_LINK_LIBS} gtest_main)
