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

ADD_BE_BENCH(${SRC_DIR}/bench/chunks_sorter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/runtime_filter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/csv_reader_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/shuffle_chunk_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_writer_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_writer_array_bench)
#ADD_BE_BENCH(${SRC_DIR}/bench/block_cache_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/roaring_bitmap_mem_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_dict_decode_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/get_dict_codes_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/persistent_index_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/orc_column_reader_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/hash_functions_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/binary_column_copy_bench)
