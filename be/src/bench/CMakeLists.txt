# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

ADD_BE_BENCH(${SRC_DIR}/bench/chunks_sorter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/runtime_filter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/block_cache_bench)

set(CACHELIB_DIR "$ENV{CACHELIB_DIR}")
set(CACHELIB_INCLUDE_DIR ${CACHELIB_DIR}/include)
include_directories(BEFORE ${CACHELIB_INCLUDE_DIR})
message(STATUS "bench cachelib include path: ${CACHELIB_INCLUDE_DIR}")
