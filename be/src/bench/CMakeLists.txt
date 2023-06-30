# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

ADD_BE_BENCH(${SRC_DIR}/bench/chunks_sorter_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/runtime_filter_bench)
<<<<<<< HEAD
=======
ADD_BE_BENCH(${SRC_DIR}/bench/csv_reader_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/shuffle_chunk_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_writer_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_writer_array_bench)
#ADD_BE_BENCH(${SRC_DIR}/bench/block_cache_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/roaring_bitmap_mem_bench)
ADD_BE_BENCH(${SRC_DIR}/bench/parquet_dict_decode_bench)
>>>>>>> a0a54ac43 ([BugFix] handle null value in dict colum in parquet file (#25800))
