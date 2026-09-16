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
    // Rows a vector (ANN) query skipped from the row-level delete-predicate evaluation because their page's
    // zone map proved they match no delete predicate (see SegmentIterator::_apply_del_predicate).
    int64_t rows_del_predicate_zone_map_pruned = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;
    int64_t vector_index_load_ns = 0;
    int64_t get_row_ranges_by_vector_index_timer = 0;
    int64_t vector_index_cache_lookup_ns = 0;
    int64_t vector_index_file_open_ns = 0;
    int64_t vector_index_read_file_ns = 0;
    int64_t vector_index_init_index_ns = 0;
    int64_t vector_index_searcher_init_ns = 0;
    int64_t vector_index_cache_hit_count = 0;
    int64_t vector_index_cache_miss_count = 0;
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

    int64_t lake_prepared_rowsets = 0;
    int64_t lake_prepared_segments = 0;
    int64_t lake_prepared_scan_rows = 0;
    int64_t lake_prepared_scan_ranges = 0;
    int64_t lake_reusable_segment_iter_created = 0;
    int64_t lake_reusable_segment_iter_reused = 0;
    // Time (ns) spent in the seed morsel's prepared-scan-range preparation (zonemap/bloom page-filter folding
    // + seek-range resolution). Only the seed pays this; refined children reuse the published range.
    int64_t lake_prepared_seed_ns = 0;
    // Breakdown of the seed prepare above, accumulated from the otherwise-discarded prepare-only
    // OlapReaderStatistics: where the seed's one-time per-segment prune spends its time / IO.
    int64_t lake_prepared_seed_io_ns = 0;
    int64_t lake_prepared_seed_io_count = 0;
    int64_t lake_prepared_seed_segment_init_ns = 0;
    int64_t lake_prepared_seed_vector_index_load_ns = 0;
    int64_t lake_prepared_seed_get_row_ranges_by_vector_index_ns = 0;
    int64_t lake_prepared_seed_vector_index_cache_lookup_ns = 0;
    int64_t lake_prepared_seed_vector_index_file_open_ns = 0;
    int64_t lake_prepared_seed_vector_index_read_file_ns = 0;
    int64_t lake_prepared_seed_vector_index_init_index_ns = 0;
    int64_t lake_prepared_seed_vector_index_searcher_init_ns = 0;
    int64_t lake_prepared_seed_vector_index_cache_hit_count = 0;
    int64_t lake_prepared_seed_vector_index_cache_miss_count = 0;
    int64_t lake_prepared_seed_vector_search_ns = 0;
    int64_t lake_prepared_seed_process_vector_distance_and_id_ns = 0;
    int64_t lake_prepared_seed_rows_vector_index_filtered = 0;
    int64_t lake_prepared_seed_zonemap_ns = 0;
    int64_t lake_prepared_seed_zonemap_filtered_rows = 0;
    int64_t lake_prepared_seed_bf_ns = 0;
    int64_t lake_prepared_seed_bf_filtered_rows = 0;
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

    // Add only counters produced since the previous snapshot. Parallel merge children keep
    // cumulative private counters (some readers consult their own totals) and publish snapshots
    // after each read; the consuming thread alone calls this helper on the shared destination.
    void add_delta(const OlapReaderStatistics& current, const OlapReaderStatistics& previous) {
        create_segment_iter_ns += current.create_segment_iter_ns - previous.create_segment_iter_ns;
        io_ns += current.io_ns - previous.io_ns;
        compressed_bytes_read += current.compressed_bytes_read - previous.compressed_bytes_read;
        decompress_ns += current.decompress_ns - previous.decompress_ns;
        uncompressed_bytes_read += current.uncompressed_bytes_read - previous.uncompressed_bytes_read;
        bytes_read += current.bytes_read - previous.bytes_read;
        block_load_ns += current.block_load_ns - previous.block_load_ns;
        blocks_load += current.blocks_load - previous.blocks_load;
        block_fetch_ns += current.block_fetch_ns - previous.block_fetch_ns;
        block_seek_num += current.block_seek_num - previous.block_seek_num;
        block_seek_ns += current.block_seek_ns - previous.block_seek_ns;
        decode_dict_ns += current.decode_dict_ns - previous.decode_dict_ns;
        decode_dict_count += current.decode_dict_count - previous.decode_dict_count;
        late_materialize_ns += current.late_materialize_ns - previous.late_materialize_ns;
        late_materialize_rows += current.late_materialize_rows - previous.late_materialize_rows;
        raw_rows_read += current.raw_rows_read - previous.raw_rows_read;
        rows_vec_cond_filtered += current.rows_vec_cond_filtered - previous.rows_vec_cond_filtered;
        vec_cond_ns += current.vec_cond_ns - previous.vec_cond_ns;
        vec_cond_evaluate_ns += current.vec_cond_evaluate_ns - previous.vec_cond_evaluate_ns;
        rf_cond_input_rows += current.rf_cond_input_rows - previous.rf_cond_input_rows;
        rf_cond_output_rows += current.rf_cond_output_rows - previous.rf_cond_output_rows;
        rf_cond_evaluate_ns += current.rf_cond_evaluate_ns - previous.rf_cond_evaluate_ns;
        vec_cond_chunk_copy_ns += current.vec_cond_chunk_copy_ns - previous.vec_cond_chunk_copy_ns;
        branchless_cond_evaluate_ns += current.branchless_cond_evaluate_ns - previous.branchless_cond_evaluate_ns;
        expr_cond_evaluate_ns += current.expr_cond_evaluate_ns - previous.expr_cond_evaluate_ns;
        get_rowsets_ns += current.get_rowsets_ns - previous.get_rowsets_ns;
        get_delvec_ns += current.get_delvec_ns - previous.get_delvec_ns;
        get_delta_column_group_ns += current.get_delta_column_group_ns - previous.get_delta_column_group_ns;
        segment_init_ns += current.segment_init_ns - previous.segment_init_ns;
        column_iterator_init_ns += current.column_iterator_init_ns - previous.column_iterator_init_ns;
        bitmap_index_iterator_init_ns += current.bitmap_index_iterator_init_ns - previous.bitmap_index_iterator_init_ns;
        zone_map_filter_ns += current.zone_map_filter_ns - previous.zone_map_filter_ns;
        rows_key_range_filter_ns += current.rows_key_range_filter_ns - previous.rows_key_range_filter_ns;
        bf_filter_ns += current.bf_filter_ns - previous.bf_filter_ns;
        segment_stats_filtered += current.segment_stats_filtered - previous.segment_stats_filtered;
        rows_key_range_filtered += current.rows_key_range_filtered - previous.rows_key_range_filtered;
        rows_after_key_range += current.rows_after_key_range - previous.rows_after_key_range;
        rows_key_range_num += current.rows_key_range_num - previous.rows_key_range_num;
        rows_stats_filtered += current.rows_stats_filtered - previous.rows_stats_filtered;
        rows_vector_index_filtered += current.rows_vector_index_filtered - previous.rows_vector_index_filtered;
        rows_bf_filtered += current.rows_bf_filtered - previous.rows_bf_filtered;
        rows_del_filtered += current.rows_del_filtered - previous.rows_del_filtered;
        del_filter_ns += current.del_filter_ns - previous.del_filter_ns;
        rows_del_predicate_zone_map_pruned +=
                current.rows_del_predicate_zone_map_pruned - previous.rows_del_predicate_zone_map_pruned;
        total_pages_num += current.total_pages_num - previous.total_pages_num;
        cached_pages_num += current.cached_pages_num - previous.cached_pages_num;
        rows_bitmap_index_filtered += current.rows_bitmap_index_filtered - previous.rows_bitmap_index_filtered;
        bitmap_index_filter_timer += current.bitmap_index_filter_timer - previous.bitmap_index_filter_timer;
        vector_index_load_ns += current.vector_index_load_ns - previous.vector_index_load_ns;
        get_row_ranges_by_vector_index_timer +=
                current.get_row_ranges_by_vector_index_timer - previous.get_row_ranges_by_vector_index_timer;
        vector_index_cache_lookup_ns += current.vector_index_cache_lookup_ns - previous.vector_index_cache_lookup_ns;
        vector_index_file_open_ns += current.vector_index_file_open_ns - previous.vector_index_file_open_ns;
        vector_index_read_file_ns += current.vector_index_read_file_ns - previous.vector_index_read_file_ns;
        vector_index_init_index_ns += current.vector_index_init_index_ns - previous.vector_index_init_index_ns;
        vector_index_searcher_init_ns += current.vector_index_searcher_init_ns - previous.vector_index_searcher_init_ns;
        vector_index_cache_hit_count += current.vector_index_cache_hit_count - previous.vector_index_cache_hit_count;
        vector_index_cache_miss_count += current.vector_index_cache_miss_count - previous.vector_index_cache_miss_count;
        vector_search_timer += current.vector_search_timer - previous.vector_search_timer;
        process_vector_distance_and_id_timer +=
                current.process_vector_distance_and_id_timer - previous.process_vector_distance_and_id_timer;
        rows_del_vec_filtered += current.rows_del_vec_filtered - previous.rows_del_vec_filtered;
        gin_index_filter_ns += current.gin_index_filter_ns - previous.gin_index_filter_ns;
        rows_gin_filtered += current.rows_gin_filtered - previous.rows_gin_filtered;
        gin_prefix_filter_ns += current.gin_prefix_filter_ns - previous.gin_prefix_filter_ns;
        gin_ngram_filter_dict_ns += current.gin_ngram_filter_dict_ns - previous.gin_ngram_filter_dict_ns;
        gin_predicate_filter_dict_ns += current.gin_predicate_filter_dict_ns - previous.gin_predicate_filter_dict_ns;
        gin_dict_count += current.gin_dict_count - previous.gin_dict_count;
        gin_ngram_dict_count += current.gin_ngram_dict_count - previous.gin_ngram_dict_count;
        gin_ngram_dict_filtered += current.gin_ngram_dict_filtered - previous.gin_ngram_dict_filtered;
        gin_predicate_dict_filtered += current.gin_predicate_dict_filtered - previous.gin_predicate_dict_filtered;
        rowsets_read_count += current.rowsets_read_count - previous.rowsets_read_count;
        segments_read_count += current.segments_read_count - previous.segments_read_count;
        total_columns_data_page_count += current.total_columns_data_page_count - previous.total_columns_data_page_count;
        runtime_stats_filtered += current.runtime_stats_filtered - previous.runtime_stats_filtered;
        read_pk_index_ns += current.read_pk_index_ns - previous.read_pk_index_ns;
        segment_metadata_filtered += current.segment_metadata_filtered - previous.segment_metadata_filtered;
        segments_metadata_filtered += current.segments_metadata_filtered - previous.segments_metadata_filtered;
        pages_from_local_disk += current.pages_from_local_disk - previous.pages_from_local_disk;
        compressed_bytes_read_local_disk +=
                current.compressed_bytes_read_local_disk - previous.compressed_bytes_read_local_disk;
        compressed_bytes_write_local_disk +=
                current.compressed_bytes_write_local_disk - previous.compressed_bytes_write_local_disk;
        compressed_bytes_read_remote += current.compressed_bytes_read_remote - previous.compressed_bytes_read_remote;
        compressed_bytes_read_request += current.compressed_bytes_read_request - previous.compressed_bytes_read_request;
        io_count += current.io_count - previous.io_count;
        io_count_local_disk += current.io_count_local_disk - previous.io_count_local_disk;
        io_count_remote += current.io_count_remote - previous.io_count_remote;
        io_count_request += current.io_count_request - previous.io_count_request;
        io_ns_read_local_disk += current.io_ns_read_local_disk - previous.io_ns_read_local_disk;
        io_ns_write_local_disk += current.io_ns_write_local_disk - previous.io_ns_write_local_disk;
        io_ns_remote += current.io_ns_remote - previous.io_ns_remote;
        prefetch_hit_count += current.prefetch_hit_count - previous.prefetch_hit_count;
        prefetch_wait_finish_ns += current.prefetch_wait_finish_ns - previous.prefetch_wait_finish_ns;
        prefetch_pending_ns += current.prefetch_pending_ns - previous.prefetch_pending_ns;
        lake_prepared_rowsets += current.lake_prepared_rowsets - previous.lake_prepared_rowsets;
        lake_prepared_segments += current.lake_prepared_segments - previous.lake_prepared_segments;
        lake_prepared_scan_rows += current.lake_prepared_scan_rows - previous.lake_prepared_scan_rows;
        lake_prepared_scan_ranges += current.lake_prepared_scan_ranges - previous.lake_prepared_scan_ranges;
        lake_reusable_segment_iter_created +=
                current.lake_reusable_segment_iter_created - previous.lake_reusable_segment_iter_created;
        lake_reusable_segment_iter_reused +=
                current.lake_reusable_segment_iter_reused - previous.lake_reusable_segment_iter_reused;
        lake_prepared_seed_ns += current.lake_prepared_seed_ns - previous.lake_prepared_seed_ns;
        lake_prepared_seed_io_ns += current.lake_prepared_seed_io_ns - previous.lake_prepared_seed_io_ns;
        lake_prepared_seed_io_count += current.lake_prepared_seed_io_count - previous.lake_prepared_seed_io_count;
        lake_prepared_seed_segment_init_ns +=
                current.lake_prepared_seed_segment_init_ns - previous.lake_prepared_seed_segment_init_ns;
        lake_prepared_seed_vector_index_load_ns +=
                current.lake_prepared_seed_vector_index_load_ns - previous.lake_prepared_seed_vector_index_load_ns;
        lake_prepared_seed_get_row_ranges_by_vector_index_ns +=
                current.lake_prepared_seed_get_row_ranges_by_vector_index_ns -
                previous.lake_prepared_seed_get_row_ranges_by_vector_index_ns;
        lake_prepared_seed_vector_index_cache_lookup_ns += current.lake_prepared_seed_vector_index_cache_lookup_ns -
                                                           previous.lake_prepared_seed_vector_index_cache_lookup_ns;
        lake_prepared_seed_vector_index_file_open_ns += current.lake_prepared_seed_vector_index_file_open_ns -
                                                        previous.lake_prepared_seed_vector_index_file_open_ns;
        lake_prepared_seed_vector_index_read_file_ns += current.lake_prepared_seed_vector_index_read_file_ns -
                                                        previous.lake_prepared_seed_vector_index_read_file_ns;
        lake_prepared_seed_vector_index_init_index_ns += current.lake_prepared_seed_vector_index_init_index_ns -
                                                         previous.lake_prepared_seed_vector_index_init_index_ns;
        lake_prepared_seed_vector_index_searcher_init_ns += current.lake_prepared_seed_vector_index_searcher_init_ns -
                                                            previous.lake_prepared_seed_vector_index_searcher_init_ns;
        lake_prepared_seed_vector_index_cache_hit_count += current.lake_prepared_seed_vector_index_cache_hit_count -
                                                           previous.lake_prepared_seed_vector_index_cache_hit_count;
        lake_prepared_seed_vector_index_cache_miss_count += current.lake_prepared_seed_vector_index_cache_miss_count -
                                                            previous.lake_prepared_seed_vector_index_cache_miss_count;
        lake_prepared_seed_vector_search_ns +=
                current.lake_prepared_seed_vector_search_ns - previous.lake_prepared_seed_vector_search_ns;
        lake_prepared_seed_process_vector_distance_and_id_ns +=
                current.lake_prepared_seed_process_vector_distance_and_id_ns -
                previous.lake_prepared_seed_process_vector_distance_and_id_ns;
        lake_prepared_seed_rows_vector_index_filtered += current.lake_prepared_seed_rows_vector_index_filtered -
                                                         previous.lake_prepared_seed_rows_vector_index_filtered;
        lake_prepared_seed_zonemap_ns += current.lake_prepared_seed_zonemap_ns - previous.lake_prepared_seed_zonemap_ns;
        lake_prepared_seed_zonemap_filtered_rows +=
                current.lake_prepared_seed_zonemap_filtered_rows - previous.lake_prepared_seed_zonemap_filtered_rows;
        lake_prepared_seed_bf_ns += current.lake_prepared_seed_bf_ns - previous.lake_prepared_seed_bf_ns;
        lake_prepared_seed_bf_filtered_rows +=
                current.lake_prepared_seed_bf_filtered_rows - previous.lake_prepared_seed_bf_filtered_rows;
        json_flatten_ns += current.json_flatten_ns - previous.json_flatten_ns;
        json_cast_ns += current.json_cast_ns - previous.json_cast_ns;
        json_merge_ns += current.json_merge_ns - previous.json_merge_ns;
        json_init_ns += current.json_init_ns - previous.json_init_ns;
        sample_time_ns += current.sample_time_ns - previous.sample_time_ns;
        sample_size += current.sample_size - previous.sample_size;
        sample_population_size += current.sample_population_size - previous.sample_population_size;
        sample_build_histogram_count += current.sample_build_histogram_count - previous.sample_build_histogram_count;
        sample_build_histogram_time_ns +=
                current.sample_build_histogram_time_ns - previous.sample_build_histogram_time_ns;
        const auto add_map_delta = [](auto& destination, const auto& now, const auto& before) {
            for (const auto& [key, value] : now) {
                const auto it = before.find(key);
                destination[key] += value - (it == before.end() ? 0 : it->second);
            }
        };
        add_map_delta(flat_json_hits, current.flat_json_hits, previous.flat_json_hits);
        add_map_delta(merge_json_hits, current.merge_json_hits, previous.merge_json_hits);
        add_map_delta(dynamic_json_hits, current.dynamic_json_hits, previous.dynamic_json_hits);
        add_map_delta(extract_json_hits, current.extract_json_hits, previous.extract_json_hits);
    }
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
