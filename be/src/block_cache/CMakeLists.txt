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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/block_cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/block_cache")

set(CACHE_FILES
  block_cache.cpp
  fb_cachelib.cpp
)

add_library(BlockCache STATIC
    ${CACHE_FILES}
)

set(CACHELIB_DIR "$ENV{CACHELIB_DIR}")
message(STATUS "cachelib dir: ${CACHELIB_DIR}")

add_library(Glog STATIC IMPORTED)
set_target_properties(Glog PROPERTIES IMPORTED_LOCATION ${CACHELIB_DIR}/lib64/libglogd.a)
LINK_LIBRARIES(${CACHELIB_DIR}/lib64/libfmtd.so)

link_directories(${CACHELIB_DIR}/deps/lib64)

list(INSERT CMAKE_PREFIX_PATH 0 ${CACHELIB_DIR}/lib/cmake)
list(INSERT CMAKE_PREFIX_PATH 0 ${CACHELIB_DIR}/lib64/cmake)
list(INSERT CMAKE_PREFIX_PATH 0 ${CACHELIB_DIR}/cmake)
message(STATUS "cmake prefix path: ${CMAKE_PREFIX_PATH}")

set(CMAKE_MODULE_PATH ${CMAKE_PREFIX_PATH})
message(STATUS "cmake module path: ${CMAKE_MODULE_PATH}")

#find_package(gflags CONFIG nothreads_static)
#target_link_libraries(BlockCache gflags::gflags)

find_package(cachelib CONFIG REQUIRED PATHS /tmp/cachelib/deps)

set(CACHELIB_INCLUDE_DIR ${CACHELIB_DIR}/include)
include_directories(BEFORE ${CACHELIB_INCLUDE_DIR})
include_directories(BEFORE ${CACHELIB_DIR}/deps/include)
message(STATUS "cachelib include path: ${CACHELIB_INCLUDE_DIR}")

target_link_libraries(BlockCache cachelib)
#target_link_options(BlockCache PUBLIC "LINKER:--copy-dt-needed-entries")
