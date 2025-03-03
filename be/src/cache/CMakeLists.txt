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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/cache")

set(CACHE_FILES
  block_cache/block_cache.cpp
  block_cache/io_buffer.cpp
  block_cache/cache_options.h
  block_cache/datacache_utils.cpp
  block_cache/disk_space_monitor.cpp
  block_cache/block_cache_hit_rate_counter.hpp
  object_cache/lrucache_module.cpp
  object_cache/object_cache.cpp
)

if (${WITH_STARCACHE} STREQUAL "ON")
    list(APPEND CACHE_FILES block_cache/starcache_wrapper.cpp)
    list(APPEND CACHE_FILES object_cache/starcache_module.cpp)
endif()

add_library(Cache STATIC
    ${CACHE_FILES}
)
