# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ADD_BE_BENCH(${SRC_DIR}/bench/chunks_sorter_bench)
#ADD_BE_BENCH(${SRC_DIR}/bench/runtime_filter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/block_cache_bench)

set(CACHELIB_DIR "$ENV{CACHELIB_DIR}")
link_directories(${CACHELIB_DIR}/deps/lib64)
include_directories(AFTER ${CACHELIB_DIR}/include)
include_directories(AFTER ${CACHELIB_DIR}/deps/include)
