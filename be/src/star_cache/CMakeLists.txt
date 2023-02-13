# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/star_cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/star_cache")

set(CACHE_FILES
  block_file.cpp
  disk_cache.cpp
  disk_space_manager.cpp
  hashtable_access_index.cpp
  mem_cache.cpp
  mem_space_manager.cpp
  star_cache.cpp
  utils.cpp
  size_based_admission_policy.cpp
  capacity_based_promotion_policy.cpp
  common/config.cpp
  common/lru_container.cpp
)

add_library(StarCache STATIC
    ${CACHE_FILES}
)
