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

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/block_cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/block_cache")

set(CACHE_FILES
  block_cache.cpp
)

if (${WITH_CACHELIB} STREQUAL "ON")
    list(APPEND CACHE_FILES cachelib_wrapper.cpp)
endif()

if (${WITH_STARCACHE} STREQUAL "ON")
    list(APPEND CACHE_FILES starcache_wrapper.cpp)
endif()

add_library(BlockCache STATIC
    ${CACHE_FILES}
)

if (${WITH_CACHELIB} STREQUAL "ON")
    set(CACHELIB_DIR ${THIRDPARTY_DIR}/cachelib)
    link_directories(${CACHELIB_DIR}/deps/lib64)

    include_directories(AFTER ${CACHELIB_DIR}/include)
    include_directories(AFTER ${CACHELIB_DIR}/deps/include)
endif()

