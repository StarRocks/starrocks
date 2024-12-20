# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.11.1)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILECOMMANDS ON)

project(starrocks-format-sdk)

# set CMAKE_BUILD_TYPE
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif ()

if (NOT DEFINED TARGET_NAME)
    set(TARGET_NAME ${PROJECT_NAME})
endif ()
message(STATUS "TARGET_NAME is ${TARGET_NAME}")

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
message(STATUS "BASE_DIR is ${BASE_DIR}")

# set thirdparty dir
set(STARROCKS_THIRDPARTY "$ENV{STARROCKS_THIRDPARTY}")
set(THIRDPARTY_DIR "$ENV{STARROCKS_THIRDPARTY}/installed")
message(STATUS "THIRDPARTY_DIR is ${THIRDPARTY_DIR}")

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

# set starrocks dirs
set(STARROCKS_BASE_DIR "${BASE_DIR}/../../../../")
set(BE_SRC_DIR "${STARROCKS_BASE_DIR}/be/src/")
set(GENSRC_DIR "${STARROCKS_BASE_DIR}/gensrc/build/")

# set jni c module dirs
set(SRC_DIR "${BASE_DIR}/")

include_directories(
        ${SRC_DIR}/
        ${GENSRC_DIR}/
)

set(JAVA_HOME ${THIRDPARTY_DIR}/open_jdk/)
add_library(jvm SHARED IMPORTED)
FILE(GLOB_RECURSE LIB_JVM ${JAVA_HOME}/jre/lib/*/libjvm.so)
set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
include_directories(${JAVA_HOME}/include)
include_directories(${JAVA_HOME}/include/linux)

# The arrow include must before the be thirdparty includes.
# add_library(arrow STATIC IMPORTED)
# set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${BASE_DIR}/arrow/cpp/build/release/libarrow.a)
# include_directories(SYSTEM ${BASE_DIR}/arrow/cpp/src/)

add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libarrow.a)

include_directories(
        SYSTEM ${BE_SRC_DIR}
        SYSTEM ${THIRDPARTY_DIR}/include
        SYSTEM ${GPERFTOOLS_HOME}/include
)

if (NOT WITH_STARCACHE)
    set(WITH_STARCACHE ON)
endif ()
message(STATUS "WITH_STARCACHE is ${WITH_STARCACHE}")

if (${WITH_STARCACHE} STREQUAL "ON")
    add_library(starcache STATIC IMPORTED GLOBAL)
    set(STARCACHE_DIR "${THIRDPARTY_DIR}/starcache")
    set_target_properties(starcache PROPERTIES IMPORTED_LOCATION ${STARCACHE_DIR}/lib/libstarcache.a)
    include_directories(SYSTEM ${STARCACHE_DIR}/include)
    message(STATUS "Link the starcache in ${STARCACHE_DIR}")
endif ()

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#   -fno-strict-aliasing: disable optimizations that assume strict aliasing. This
#       is unsafe to do if the code uses casts (which we obviously do).
#  -Wno-unknown-pragmas: suppress warnings for unknown (compiler specific) pragmas
#  -Wno-deprecated: gutil contains deprecated headers
#  -Wno-vla: we use C99-style variable-length arrays
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
#  -fno-omit-frame-pointers: Keep frame pointer for functions in register
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread -Wno-register -Wno-deprecated -Wno-vla -Wno-comment")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-strict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++20 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fPIC")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DUSE_STAROS")

if (${WITH_STARCACHE} STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DWITH_STARCACHE")
endif ()

# For any gcc builds:
#   -g: Enable symbols for profiler tools
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -g -Wno-unused-local-typedefs")
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -fcoroutines")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DDEBUG")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DNDEBUG")
endif ()

# Add flags that are common across build types
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${CXX_GCC_FLAGS}")

FILE(GLOB SC_FILES "*.cpp" "*.h")

# internal static dependencies
add_subdirectory(${SRC_DIR}/format)
add_subdirectory(${SRC_DIR}/convert)

# external dynamic dependencies
add_library(starrocks_format SHARED IMPORTED)
set_target_properties(starrocks_format
        PROPERTIES
        IMPORTED_LOCATION ${STARROCKS_BASE_DIR}/output/format-lib/libstarrocks_format.so
)

# target
add_library(${TARGET_NAME} SHARED ${SC_FILES})

target_link_libraries(${TARGET_NAME}
        -Wl,-Bsymbolic
        format
        convert
        starrocks_format
        -Wl,--whole-archive
        arrow
        -Wl,--no-whole-archive
        -static-libstdc++
        -static-libgcc
)

# copy dependency
add_custom_command(TARGET ${TARGET_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${STARROCKS_BASE_DIR}/output/format-lib/libstarrocks_format.so ${CMAKE_CURRENT_BINARY_DIR}
)