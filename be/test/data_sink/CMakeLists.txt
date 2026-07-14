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

set(DATA_SINK_FILE_TEST_DEPENDENCIES ${STARROCKS_DEPENDENCIES})
list(REMOVE_ITEM DATA_SINK_FILE_TEST_DEPENDENCIES arrow_testing)

set(DATA_SINK_FILE_TEST_LINK_LIBS
    DataSinkFile
    FormatCore
    FormatCsv
    FormatParquet
    Expr
    Runtime
    FileSystem
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
    ${DATA_SINK_FILE_TEST_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_data_sink_file_test_objs OBJECT
    ${BE_TEST_MACOS_LINK_STUBS}
    ${BE_TEST_JNI_ENV_STUB}
    file/plain_text_builder_test.cpp
)
target_link_libraries(starrocks_data_sink_file_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_data_sink_file_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_executable(data_sink_file_test $<TARGET_OBJECTS:starrocks_data_sink_file_test_objs>)
target_link_libraries(data_sink_file_test ${DATA_SINK_FILE_TEST_LINK_LIBS} gtest_main)
