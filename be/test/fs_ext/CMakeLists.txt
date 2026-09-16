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

set(HDFS_FILESYSTEM_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    hdfs_filesystem_test.cpp
)

set(HDFS_FILESYSTEM_TEST_LINK_LIBS
    HdfsFileSystem
    FileSystem
    Runtime
    IO
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

add_library(starrocks_hdfs_filesystem_test_objs OBJECT ${HDFS_FILESYSTEM_TEST_FILES})
target_link_libraries(starrocks_hdfs_filesystem_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_hdfs_filesystem_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(hdfs_filesystem_test hdfs_filesystem_test_main.cpp $<TARGET_OBJECTS:starrocks_hdfs_filesystem_test_objs>)
target_link_libraries(hdfs_filesystem_test ${HDFS_FILESYSTEM_TEST_LINK_LIBS})
