# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
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

set(CACHELIB_DIR ${THIRDPARTY_DIR}/cachelib)
link_directories(${CACHELIB_DIR}/deps/lib64)

include_directories(AFTER ${CACHELIB_DIR}/include)
include_directories(AFTER ${CACHELIB_DIR}/deps/include)
