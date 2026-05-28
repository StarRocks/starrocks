// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace starrocks {

enum ReaderType {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
    READER_BYPASS_QUERY = 5,
};

inline bool is_query(ReaderType reader_type) {
    return reader_type == READER_QUERY || reader_type == READER_BYPASS_QUERY;
}

inline bool is_compaction(ReaderType reader_type) {
    return reader_type == READER_BASE_COMPACTION || reader_type == READER_CUMULATIVE_COMPACTION;
}

// ReaderStatistics used to collect statistics when scan data from storage
struct OlapReaderStatistics {
    int64_t create_segment_iter_ns = 0;
    int64_t io_ns = 0;
    int64_t compressed_bytes_read = 0;

    int64_t decompress_ns = 0;
    int64_t uncompressed_bytes_read = 0;

    // total read bytes in memory
    int64_t bytes_read = 0;

    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    int64_t block_fetch_ns = 0; // time of rowset reader's `next_batch()` call
    int64_t block_seek_num = 0;
    int64_t block_seek_ns = 0;

    int64_t decode_dict_ns = 0;
    int64_t decode_dict_count = 0; // rows * columns
    int64_t late_materialize_ns = 0;
    int64_t late_materialize_rows = 0; // number of rows that are late materialized after the filter

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t vec_cond_evaluate_ns = 0;
    int64_t rf_cond_input_rows = 0;
    int64_t rf_cond_output_rows = 0;
    int64_t rf_cond_evaluate_ns = 0;
    int64_t vec_cond_chunk_copy_ns = 0;
    int64_t branchless_cond_evaluate_ns = 0;
    int64_t expr_cond_evaluate_ns = 0;

    int64_t get_rowsets_ns = 0;
    int64_t get_delvec_ns = 0;
    int64_t get_delta_column_group_ns = 0;
    int64_t segment_init_ns = 0;
    int64_t column_iterator_init_ns = 0;
    int64_t bitmap_index_iterator_init_ns = 0;
    int64_t zone_map_filter_ns = 0;
    int64_t rows_key_range_filter_ns = 0;
    int64_t bf_filter_ns = 0;

    int64_t segment_stats_filtered = 0;
    int64_t rows_key_range_filtered = 0;
    int64_t rows_after_key_range = 0;
    int64_t rows_key_range_num = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_vector_index_filtered = 0;
    int64_t rows_bf_filtered = 0;
    int64_t rows_del_filtered = 0;
    int64_t del_filter_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;
    int64_t get_row_ranges_by_vector_index_timer = 0;
    int64_t vector_search_timer = 0;
    int64_t process_vector_distance_and_id_timer = 0;

    int64_t rows_del_vec_filtered = 0;

    int64_t gin_index_filter_ns = 0;
    int64_t rows_gin_filtered = 0;
    int64_t gin_prefix_filter_ns = 0;
    int64_t gin_ngram_filter_dict_ns = 0;
    int64_t gin_predicate_filter_dict_ns = 0;
    int64_t gin_dict_count = 0;
    int64_t gin_ngram_dict_count = 0;
    int64_t gin_ngram_dict_filtered = 0;
    int64_t gin_predicate_dict_filtered = 0;

    int64_t rowsets_read_count = 0;
    int64_t segments_read_count = 0;
    int64_t total_columns_data_page_count = 0;

    int64_t runtime_stats_filtered = 0;

    int64_t read_pk_index_ns = 0;

    // ------ for lake tablet ------
    // Rows skipped by segment metadata filter (sort key range filtering).
    int64_t segment_metadata_filtered = 0;
    // Number of segments skipped by segment metadata filter.
    int64_t segments_metadata_filtered = 0;

    int64_t pages_from_local_disk = 0;

    int64_t compressed_bytes_read_local_disk = 0;
    int64_t compressed_bytes_write_local_disk = 0;
    int64_t compressed_bytes_read_remote = 0;
    // bytes read requested from be, same as compressed_bytes_read for local tablet
    int64_t compressed_bytes_read_request = 0;

    int64_t io_count = 0;
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    int64_t io_count_request = 0;

    int64_t io_ns_read_local_disk = 0;
    int64_t io_ns_write_local_disk = 0;
    int64_t io_ns_remote = 0;

    int64_t prefetch_hit_count = 0;
    int64_t prefetch_wait_finish_ns = 0;
    int64_t prefetch_pending_ns = 0;
    // ------ for lake tablet ------

    // ------ for json type, to count flat column ------
    // key: json absolute path, value: count
    int64_t json_flatten_ns = 0;
    int64_t json_cast_ns = 0;
    int64_t json_merge_ns = 0;
    int64_t json_init_ns = 0;
    std::unordered_map<std::string, int64_t> flat_json_hits;
    std::unordered_map<std::string, int64_t> merge_json_hits;
    std::unordered_map<std::string, int64_t> dynamic_json_hits;
    std::unordered_map<std::string, int64_t> extract_json_hits;

    // Counters for data sampling
    int64_t sample_time_ns = 0;               // Records the time to prepare sample, actual IO time is not included
    int64_t sample_size = 0;                  // Records the number of hits in the sample. Granularity can be BLOCK/PAGE
    int64_t sample_population_size = 0;       // Records the total number of samples. Granularity can be BLOCK/PAGE
    int64_t sample_build_histogram_count = 0; // Records the number of histogram built for sampling
    int64_t sample_build_histogram_time_ns = 0; // Records the time to build histogram
};

// OlapWriterStatistics used to collect statistics when write data to storage
struct OlapWriterStatistics {
    int64_t write_remote_ns = 0;    // how much time is spent on write
    int64_t bytes_write_remote = 0; // how many bytes are written
    int64_t segment_count = 0;      // how many files are written
};

const char* const kBytesReadLocalDisk = "bytes_read_local_disk";
const char* const kBytesWriteLocalDisk = "bytes_write_local_disk";
const char* const kBytesReadRemote = "bytes_read_remote";
const char* const kBytesWriteRemote = "bytes_write_remote";
const char* const kIOCountLocalDisk = "io_count_local_disk";
const char* const kIOCountRemote = "io_count_remote";
const char* const kIONsReadLocalDisk = "io_ns_read_local_disk";
const char* const kIONsWriteLocalDisk = "io_ns_write_local_disk";
const char* const kIONsReadRemote = "io_ns_read_remote";
const char* const kIONsWriteRemote = "io_ns_write_remote";
const char* const kPrefetchHitCount = "prefetch_hit_count";
const char* const kPrefetchWaitFinishNs = "prefetch_wait_finish_ns";
const char* const kPrefetchPendingNs = "prefetch_pending_ns";

} // namespace starrocks
