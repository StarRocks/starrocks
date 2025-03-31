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

set(CMAKE_VERBOSE_MAKEFILE ON)

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/starrocks_format")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/starrocks_format")

# Set starrocks libraries
set(STARROCKS_LIBS
    ${WL_START_GROUP}
    Common
    Column
    Connector
    Exec
    Exprs
    FileSystem
    Formats
    Gutil
    IO
    Serde
    Storage
    Rowset
    Runtime
    Types
    Util
    Script
    StarRocksGen
    Webserver
    TestUtil
    Cache
    ${WL_END_GROUP}
)

add_library(hdfs_so SHARED IMPORTED GLOBAL)
set_target_properties(hdfs_so PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/hadoop/lib/native/libhdfs.so)

SET(STARROCKS_FORMAT_LIBS
    ${STARROCKS_LIBS}
    ${STARROCKS_DEPENDENCIES}
    -static-libstdc++ -static-libgcc -lbfd
    -lresolv -liberty -lc -lm -ldl -rdynamic -pthread -Wl,-wrap=__cxa_throw
    hdfs_so
)
message(STATUS "STARROCKS_FORMAT_LIBS is ${STARROCKS_FORMAT_LIBS}")

# only build starrocks_be when TEST is off
if (NOT ${MAKE_TEST} STREQUAL "ON")

    add_library(starrocks_format SHARED
        starrocks_lib.cpp
        )

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    target_link_libraries(starrocks_format ${STARROCKS_FORMAT_LIBS})

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/format-lib/)

    install(TARGETS starrocks_format
        DESTINATION ${OUTPUT_DIR}/format-lib/)

endif()
