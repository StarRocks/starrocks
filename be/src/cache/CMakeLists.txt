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

set(CACHE_CORE_FILES
  data_cache_hit_rate_counter.hpp
  disk_cache/io_buffer.cpp
)

ADD_BE_LIB(CacheCore
    ${CACHE_CORE_FILES}
)

set(CACHE_DATA_PLANE_FILES
  disk_cache/block_cache.cpp
  mem_cache/lrucache_engine.cpp
  mem_cache/page_cache.cpp
)

ADD_BE_LIB(CacheDataPlane
    ${CACHE_DATA_PLANE_FILES}
)

target_link_libraries(CacheDataPlane
    PUBLIC CacheCore
    PRIVATE RuntimeCore
)

set(CACHE_FILES
  disk_space_monitor.cpp
  mem_space_monitor.cpp
  datacache.cpp
  datacache_runtime_compat.cpp
  datacache_metrics.cpp
  datacache_utils.cpp
)

if (${WITH_STARCACHE} STREQUAL "ON")
    list(APPEND CACHE_FILES disk_cache/starcache_engine.cpp)
    list(APPEND CACHE_FILES peer_cache_engine.cpp)
endif()

ADD_BE_LIB(Cache
    ${CACHE_FILES}
)

target_link_libraries(Cache PUBLIC CacheCore CacheDataPlane)
