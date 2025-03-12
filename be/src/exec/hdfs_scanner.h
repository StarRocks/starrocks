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

#include <atomic>
#include <boost/algorithm/string.hpp>

#include "connector/deletion_vector/deletion_bitmap.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/scan/morsel.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "fs/fs.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeFilterProbeCollector;

struct HdfsSplitContext : public pipeline::ScanSplitContext {
    size_t split_start = 0;
    size_t split_end = 0;
    virtual std::unique_ptr<HdfsSplitContext> clone() = 0;
};
using HdfsSplitContextPtr = std::unique_ptr<HdfsSplitContext>;

struct SkipRowsContext {
    DeletionBitmapPtr deletion_bitmap;

    bool has_skip_rows() { return deletion_bitmap != nullptr && !deletion_bitmap->empty(); }
};
using SkipRowsContextPtr = std::shared_ptr<SkipRowsContext>;

struct OptimizationCounter {
    int bloom_filter_tried_counter = 0;
    int bloom_filter_success_counter = 0;
    int statistics_tried_counter = 0;
    int statistics_success_counter = 0;
    int page_index_tried_counter = 0;
    int page_index_filter_group_counter = 0;
    int page_index_success_counter = 0;
    OptimizationCounter& operator+=(const OptimizationCounter& counter) {
        bloom_filter_tried_counter += counter.bloom_filter_tried_counter;
        bloom_filter_success_counter += counter.bloom_filter_success_counter;
        statistics_tried_counter += counter.statistics_tried_counter;
        statistics_success_counter += counter.statistics_success_counter;
        page_index_tried_counter += counter.page_index_tried_counter;
        page_index_filter_group_counter += counter.page_index_filter_group_counter;
        page_index_success_counter += counter.page_index_success_counter;
        return *this;
    }
};
struct HdfsScanStats {
    int64_t raw_rows_read = 0;
    int64_t rows_read = 0;
    int64_t late_materialize_skip_rows = 0;

    int64_t io_ns = 0;
    int64_t io_count = 0;
    int64_t bytes_read = 0;

    int64_t expr_filter_ns = 0;
    int64_t column_read_ns = 0;
    int64_t column_convert_ns = 0;
    int64_t reader_init_ns = 0;

    // parquet only!
    // read & decode
    int64_t request_bytes_read = 0;
    int64_t request_bytes_read_uncompressed = 0;
    int64_t level_decode_ns = 0;
    int64_t value_decode_ns = 0;
    int64_t page_read_ns = 0;
    // reader init
    int64_t footer_read_ns = 0;
    int64_t footer_cache_read_ns = 0;
    int64_t footer_cache_read_count = 0;
    int64_t footer_cache_write_count = 0;
    int64_t footer_cache_write_bytes = 0;
    int64_t footer_cache_write_fail_count = 0;
    int64_t column_reader_init_ns = 0;
    // dict filter
    int64_t group_chunk_read_ns = 0;
    int64_t group_dict_filter_ns = 0;
    int64_t group_dict_decode_ns = 0;
    // io coalesce
    int64_t group_active_lazy_coalesce_together = 0;
    int64_t group_active_lazy_coalesce_seperately = 0;
    // page statistics
    bool has_page_statistics = false;
    // page skip
    int64_t page_skip = 0;
    // page index
    int64_t rows_before_page_index = 0;
    int64_t page_index_ns = 0;
    int64_t parquet_total_row_groups = 0;
    int64_t parquet_filtered_row_groups = 0;

    // late materialize round-by-round
    int64_t group_min_round_cost = 0;

    // orc stripe information
    std::vector<int64_t> orc_stripe_sizes{};
    int64_t orc_total_tiny_stripe_size = 0;

    // io coalesce
    int64_t orc_stripe_active_lazy_coalesce_together = 0;
    int64_t orc_stripe_active_lazy_coalesce_seperately = 0;

    // Iceberg v2 only!
    int64_t iceberg_delete_file_build_ns = 0;
    int64_t iceberg_delete_files_per_scan = 0;

    // deletion vector
    int64_t deletion_vector_build_ns = 0;
    int64_t deletion_vector_build_count = 0;
    int64_t build_rowid_filter_ns = 0;

    // reader filter info
    OptimizationCounter _optimzation_counter{};
};

class HdfsParquetProfile;

struct HdfsScanProfile {
    RuntimeProfile* runtime_profile = nullptr;
    RuntimeProfile::Counter* raw_rows_read_counter = nullptr;
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* late_materialize_skip_rows_counter = nullptr;
    RuntimeProfile::Counter* scan_ranges_counter = nullptr;
    RuntimeProfile::Counter* scan_ranges_size = nullptr;

    RuntimeProfile::Counter* reader_init_timer = nullptr;
    RuntimeProfile::Counter* open_file_timer = nullptr;
    RuntimeProfile::Counter* expr_filter_timer = nullptr;
    RuntimeProfile::Counter* column_read_timer = nullptr;
    RuntimeProfile::Counter* column_convert_timer = nullptr;

    RuntimeProfile::Counter* datacache_read_counter = nullptr;
    RuntimeProfile::Counter* datacache_read_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_mem_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_disk_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_timer = nullptr;
    RuntimeProfile::Counter* datacache_skip_write_counter = nullptr;
    RuntimeProfile::Counter* datacache_skip_write_bytes = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_counter = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_bytes = nullptr;
    RuntimeProfile::Counter* datacache_write_counter = nullptr;
    RuntimeProfile::Counter* datacache_write_bytes = nullptr;
    RuntimeProfile::Counter* datacache_write_timer = nullptr;
    RuntimeProfile::Counter* datacache_write_fail_counter = nullptr;
    RuntimeProfile::Counter* datacache_write_fail_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_block_buffer_counter = nullptr;
    RuntimeProfile::Counter* datacache_read_block_buffer_bytes = nullptr;

    RuntimeProfile::Counter* shared_buffered_shared_io_count = nullptr;
    RuntimeProfile::Counter* shared_buffered_shared_io_bytes = nullptr;
    RuntimeProfile::Counter* shared_buffered_shared_align_io_bytes = nullptr;
    RuntimeProfile::Counter* shared_buffered_shared_io_timer = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_count = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_bytes = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_timer = nullptr;

    RuntimeProfile::Counter* app_io_bytes_read_counter = nullptr;
    RuntimeProfile::Counter* app_io_timer = nullptr;
    RuntimeProfile::Counter* app_io_counter = nullptr;
    RuntimeProfile::Counter* fs_bytes_read_counter = nullptr;
    RuntimeProfile::Counter* fs_io_timer = nullptr;
    RuntimeProfile::Counter* fs_io_counter = nullptr;
};

struct HdfsScannerParams {
    // one file split (parition_id, file_path, file_length, offset, length, file_format)
    const THdfsScanRange* scan_range = nullptr;

    bool enable_split_tasks = false;
    const HdfsSplitContext* split_context = nullptr;

    // runtime bloom filter.
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;

    std::vector<ExprContext*> all_conjunct_ctxs;
    // all conjuncts except `conjunct_ctxs_by_slot`, like compound predicates
    std::vector<ExprContext*> scanner_conjunct_ctxs;
    std::unordered_set<SlotId> slots_in_conjunct;
    // slot used by conjunct_ctxs
    std::unordered_set<SlotId> slots_of_mutli_slot_conjunct;

    // conjunct ctxs grouped by slot.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // The FileSystem used to open the file to be scanned
    FileSystem* fs = nullptr;
    // The file to scan
    std::string path;
    // The file size. -1 means unknown.
    int64_t file_size = -1;
    // the table location
    std::string table_location;

    const TupleDescriptor* tuple_desc = nullptr;

    // columns read from file
    std::vector<SlotDescriptor*> materialize_slots;
    std::vector<int> materialize_index_in_chunk;

    // columns of partition info
    std::vector<SlotDescriptor*> partition_slots;
    std::vector<int> partition_index_in_chunk;
    // partition index in hdfs partition columns.
    std::vector<int> _partition_index_in_hdfs_partition_columns;

    // partition conjunct, used to generate partition columns
    std::vector<ExprContext*> partition_values;

    std::vector<SlotDescriptor*> extended_col_slots;
    std::vector<int> extended_col_index_in_chunk;
    std::vector<int> index_in_extended_columns;
    std::vector<ExprContext*> extended_col_values;

    // min max conjunct for filter row group or page
    // should clone in scanner
    std::vector<ExprContext*> min_max_conjunct_ctxs;

    const TupleDescriptor* min_max_tuple_desc = nullptr;

    std::vector<std::string>* hive_column_names = nullptr;

    bool case_sensitive = false;

    HdfsScanProfile* profile = nullptr;

    std::vector<const TIcebergDeleteFile*> deletes;

    std::shared_ptr<TDeletionVectorDescriptor> deletion_vector_descriptor = nullptr;

    const TIcebergSchema* iceberg_schema = nullptr;

    bool is_lazy_materialization_slot(SlotId slot_id) const;

    std::shared_ptr<TPaimonDeletionFile> paimon_deletion_file = nullptr;

    DataCacheOptions datacache_options{};
    bool use_file_metacache = false;

    std::atomic<int32_t>* lazy_column_coalesce_counter;
    bool can_use_any_column = false;
    bool can_use_min_max_count_opt = false;
    bool orc_use_column_names = false;
    bool parquet_page_index_enable = false;
    bool parquet_bloom_filter_enable = false;

    int64_t connector_max_split_size = 0;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
};

struct HdfsScannerContext {
    struct ColumnInfo {
        int idx_in_chunk;
        SlotDescriptor* slot_desc;
        bool decode_needed = true;

        std::string formatted_name(bool case_sensitive) const {
            return case_sensitive ? name() : boost::algorithm::to_lower_copy(name());
        }
        const std::string& name() const { return slot_desc->col_name(); }
        int32_t col_unique_id() const { return slot_desc->col_unique_id(); }
        const std::string& col_physical_name() const { return slot_desc->col_physical_name(); }
        const SlotId slot_id() const { return slot_desc->id(); }
        const TypeDescriptor& slot_type() const { return slot_desc->type(); }
    };

    std::string formatted_name(const std::string& name) {
        return case_sensitive ? name : boost::algorithm::to_lower_copy(name);
    }

    std::vector<SlotDescriptor*> slot_descs;
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // materialized column read from parquet file
    std::vector<ColumnInfo> materialized_columns;

    // partition column
    std::vector<ColumnInfo> partition_columns;

    // partition column value which read from hdfs file path
    Columns partition_values;

    // extended column
    std::vector<ColumnInfo> extended_columns;

    Columns extended_values;

    // scan range
    const THdfsScanRange* scan_range = nullptr;
    bool enable_split_tasks = false;
    const HdfsSplitContext* split_context = nullptr;
    std::vector<HdfsSplitContextPtr> split_tasks;
    bool has_split_tasks = false;
    size_t estimated_mem_usage_per_split_task = 0;

    // min max slots
    const TupleDescriptor* min_max_tuple_desc = nullptr;

    // min max conjunct
    std::vector<ExprContext*> min_max_conjunct_ctxs;

    // runtime filters.
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;

    std::vector<std::string>* hive_column_names = nullptr;

    bool case_sensitive = false;

    bool orc_use_column_names = false;

    bool can_use_any_column = false;

    bool can_use_min_max_count_opt = false;

    bool return_count_column = false;

    bool use_file_metacache = false;

    bool parquet_page_index_enable = false;

    bool parquet_bloom_filter_enable = false;

    std::string timezone;

    const TIcebergSchema* iceberg_schema = nullptr;

    HdfsScanStats* stats = nullptr;

    std::atomic<int32_t>* lazy_column_coalesce_counter;

    int64_t connector_max_split_size = 0;

    RuntimeScanRangePruner* rf_scan_range_pruner = nullptr;

    // update none_existed_slot
    // update conjunct
    void update_with_none_existed_slot(SlotDescriptor* slot);

    // update materialized column against data file.
    // and to update not_existed slots and conjuncts.
    // and to update `conjunct_ctxs_by_slot` field.
    Status update_materialized_columns(const std::unordered_set<std::string>& names);
    // "not existed columns" are materialized columns not found in file
    // this usually happens when use changes schema. for example
    // user create table with 3 fields A, B, C, and there is one file F1
    // but user change schema and add one field like D.
    // when user select(A, B, C, D), then D is the non-existed column in file F1.
    Status append_or_update_not_existed_columns_to_chunk(ChunkPtr* chunk, size_t row_count);

    // If there is no partition column in the chunk，append partition column to chunk，
    // otherwise update partition column in chunk
    void append_or_update_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count);
    void append_or_update_count_column_to_chunk(ChunkPtr* chunk, size_t row_count);

    void append_or_update_extended_column_to_chunk(ChunkPtr* chunk, size_t row_count);
    void append_or_update_column_to_chunk(ChunkPtr* chunk, size_t row_count, const std::vector<ColumnInfo>& columns,
                                          const Columns& values);

    // if we can skip this file by evaluating conjuncts of non-existed columns with default value.
    StatusOr<bool> should_skip_by_evaluating_not_existed_slots();
    std::vector<SlotDescriptor*> not_existed_slots;
    std::vector<ExprContext*> conjunct_ctxs_of_non_existed_slots;

    // other helper functions.
    bool can_use_dict_filter_on_slot(SlotDescriptor* slot) const;
    Status evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter);

    void merge_split_tasks();

    // used for parquet zone map filter only
    std::unique_ptr<ScanConjunctsManager> conjuncts_manager = nullptr;
    std::vector<std::unique_ptr<ColumnPredicate>> predicate_free_pool;
    PredicateTree predicate_tree;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
};

struct OpenFileOptions {
    FileSystem* fs = nullptr;
    std::string path;
    int64_t file_size = -1;
    HdfsScanStats* fs_stats = nullptr;
    HdfsScanStats* app_stats = nullptr;

    // for datacache
    DataCacheOptions datacache_options;

    // for compressed text file
    CompressionTypePB compression_type = CompressionTypePB::NO_COMPRESSION;
};

class HdfsScanner {
public:
    HdfsScanner() = default;
    virtual ~HdfsScanner() = default;

    Status init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params);
    Status open(RuntimeState* runtime_state);
    Status get_next(RuntimeState* runtime_state, ChunkPtr* chunk);
    void close() noexcept;

    int64_t num_bytes_read() const { return _app_stats.bytes_read; }
    int64_t raw_rows_read() const { return _app_stats.raw_rows_read; }
    int64_t num_rows_read() const { return _app_stats.rows_read; }
    int64_t cpu_time_spent() const { return _total_running_time - _app_stats.io_ns; }
    int64_t io_time_spent() const { return _app_stats.io_ns; }
    virtual int64_t estimated_mem_usage() const;
    void update_counter();

    RuntimeState* runtime_state() { return _runtime_state; }

    virtual Status do_open(RuntimeState* runtime_state) = 0;
    virtual void do_close(RuntimeState* runtime_state) noexcept = 0;
    virtual Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) = 0;
    virtual Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) = 0;
    virtual void do_update_counter(HdfsScanProfile* profile);
    virtual Status reinterpret_status(const Status& st);
    void move_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks);
    bool has_split_tasks() const { return _scanner_ctx.has_split_tasks; }

    static StatusOr<std::unique_ptr<RandomAccessFile>> create_random_access_file(
            std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<io::CacheInputStream>& cache_input_stream, const OpenFileOptions& options);

protected:
    Status open_random_access_file();
    static CompressionTypePB get_compression_type_from_path(const std::string& filename);

    void do_update_iceberg_v2_counter(RuntimeProfile* parquet_profile, const std::string& parent_name);
    void do_update_deletion_vector_build_counter(RuntimeProfile* parent_profile);
    void do_update_deletion_vector_filter_counter(RuntimeProfile* parent_profile);

private:
    bool _opened = false;
    std::atomic<bool> _closed = false;
    Status _build_scanner_context();
    void update_hdfs_counter(HdfsScanProfile* profile);

protected:
    HdfsScannerContext _scanner_ctx;
    HdfsScannerParams _scanner_params;
    RuntimeState* _runtime_state = nullptr;
    HdfsScanStats _app_stats;
    HdfsScanStats _fs_stats;
    std::unique_ptr<RandomAccessFile> _file;
    // by default it's no compression.
    CompressionTypePB _compression_type = CompressionTypePB::NO_COMPRESSION;
    std::shared_ptr<io::CacheInputStream> _cache_input_stream = nullptr;
    std::shared_ptr<io::SharedBufferedInputStream> _shared_buffered_input_stream = nullptr;
    int64_t _total_running_time = 0;
};

} // namespace starrocks
