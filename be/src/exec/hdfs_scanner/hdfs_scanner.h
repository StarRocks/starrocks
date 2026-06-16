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

#include "cache/cache_options.h"
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/column_access_path.h"
#include "common/runtime_profile.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_morsel.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/deletion_bitmap.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state_fwd.h"
#include "storage/column_predicate.h"
#include "storage/predicate_parser.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {

class HiveTableDescriptor;
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

struct HdfsScannerStats {
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
    int64_t page_read_counter = 0;
    int64_t page_cache_read_counter = 0;
    int64_t page_cache_write_counter = 0;
    int64_t page_cache_read_decompressed_counter = 0;
    int64_t page_cache_read_compressed_counter = 0;
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
    int64_t active_lazy_coalesce_together = 0;
    int64_t active_lazy_coalesce_seperately = 0;
    // page statistics
    bool has_page_statistics = false;
    // page skip
    int64_t page_skip = 0;
    // page index
    int64_t rows_before_page_index = 0;
    int64_t page_index_ns = 0;
    int64_t total_row_groups = 0;
    int64_t filtered_row_groups = 0;

    // late materialize round-by-round
    int64_t group_min_round_cost = 0;

    // Parquet global-dict opt application — populated by parquet::GroupReader as
    // it wraps column readers with dict-aware adapters.  These counters answer
    // "did the FE global-dict optimization actually reach Parquet column readers
    // on this scan instance, and through which wrapper path?" for Hive/Iceberg
    // workloads where the connector-level dict map can otherwise lie about
    // schema-evolved files.
    int64_t global_dict_total_row_groups = 0;       // selected row groups this scanner read
    int64_t global_dict_applied_row_groups = 0;     // row groups with >=1 dict-wrapped slot
    int64_t global_dict_applied_slots = 0;          // sum of wrapped slots across row groups
    int64_t global_dict_dict_code_reader_slots = 0; // wrapped via LowCardColumnReader
    int64_t global_dict_encode_reader_slots = 0;    // wrapped via LowRowsColumnReader

    // orc stripe information
    std::vector<int64_t> orc_stripe_sizes{};
    int64_t orc_total_tiny_stripe_size = 0;

    // Iceberg v2 only!
    int64_t iceberg_delete_file_build_ns = 0;
    int64_t iceberg_delete_files_per_scan = 0;

    // deletion vector
    int64_t deletion_vector_build_ns = 0;
    int64_t deletion_vector_build_count = 0;
    int64_t build_rowid_filter_ns = 0;

    // parquet row-group statistics hit tracking
    int bloom_filter_tried_counter = 0;
    int bloom_filter_success_counter = 0;
    int statistics_tried_counter = 0;
    int statistics_success_counter = 0;
    int page_index_tried_counter = 0;
    int page_index_filter_group_counter = 0;
    int page_index_success_counter = 0;
};

struct HdfsScannerProfile {
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
    RuntimeProfile::Counter* datacache_read_peer_counter = nullptr;
    RuntimeProfile::Counter* datacache_read_peer_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_peer_timer = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_peer_counter = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_peer_bytes = nullptr;
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

// Immutable scan options derived from the query plan node, embedded by value
// inside HdfsScannerContext so they are copied together with the rest of the
// per-scanner params.
struct HdfsScannerOptions {
    bool case_sensitive = false;
    bool use_min_max_opt = false;
    // Set by PruneHDFSScanColumnRule when all queried columns are partition columns
    // and a placeholder materialized column was injected.  Combined with
    // use_min_max_opt to skip reading the placeholder from the data file.
    bool can_use_any_column = false;
    bool use_count_opt = false;
    bool orc_use_column_names = false;
    bool parquet_page_index_enable = false;
    bool parquet_bloom_filter_enable = false;
    bool use_file_metacache = false;
    bool use_file_pagecache = false;
    bool enable_split_tasks = false;
    bool enable_dynamic_prune_scan_range = true;
    bool use_partition_column_value_only = false;
    int64_t connector_max_split_size = 0;
};

// All conjunct contexts and slot metadata derived from the scan plan node,
// embedded by value inside HdfsScannerContext.  HiveDataSource populates it
// once; HdfsScannerContext copies it (shallow — ExprContext* pointers are
// owned by HiveDataSource's ObjectPool) into the scanner.
//
// HdfsScannerContext::conjunct_ctxs_by_slot starts as a shallow copy of
// by_slot because update_with_none_existed_slot() erases entries from it
// when a column is absent from the data file.
struct HdfsScannerConjuncts {
    // Clone of (min_max_ctxs ∪ scan conjuncts), used to build the
    // ScanConjunctsManager predicate tree (Parquet row-group statistics, etc.).
    // Lifetime is managed by HiveDataSource's ObjectPool.
    std::vector<ExprContext*> all_ctxs;

    // Multi-slot predicates (e.g. "a + b > 5") that cannot be pushed into a
    // single-column reader.  Evaluated in HdfsScanner::get_next() after every
    // column is materialised.  ORC also folds them into search arguments for
    // stripe-level skipping, but the post-read pass is still required for
    // correctness.
    std::vector<ExprContext*> scanner_ctxs;

    // Single-slot predicates pushed into ORC stripe / Parquet row-group
    // statistics filtering via min_max_tuple_desc.
    std::vector<ExprContext*> min_max_ctxs;

    // Single-slot predicates keyed by SlotId.  HdfsScannerContext copies this
    // map because update_with_none_existed_slot() erases entries when a column
    // is absent from the data file.
    std::unordered_map<SlotId, std::vector<ExprContext*>> by_slot;

    // All SlotIds appearing in any conjunct.  Used by
    // is_lazy_materialization_slot() to decide which columns must be decoded
    // eagerly.
    std::unordered_set<SlotId> slots_in_conjunct;

    // Slots appearing in multi-field conjuncts; those slots must be fully
    // decoded even when they are not output columns.
    std::unordered_set<SlotId> slots_of_multi_field;
};

// Table-format-specific state carried through the scan pipeline.
struct TableSpecificData {
    std::vector<const TIcebergDeleteFile*> iceberg_delete_files;
    std::shared_ptr<TDeletionVectorDescriptor> deletion_vector_descriptor;
    const TIcebergSchema* iceberg_schema = nullptr;
    std::shared_ptr<TPaimonDeletionFile> paimon_deletion_file;
};

// HdfsScannerContext carries all state needed by an HdfsScanner from creation
// through close().  It was formed by merging the former HdfsScannerParams (the
// immutable input struct filled by HiveDataSource) with the former working-state
// struct (which held a back-pointer to params).  The merge eliminates the
// ctx.params->field double-indirection that made every callsite noisy.
//
// Pointer-based zero-copy: HdfsScannerContext is passed by pointer into scanners
// (never copied), so non-copyable unique_ptr members (predicates, split tasks)
// live directly in this struct.  HiveDataSource keeps a shared template
// (_scanner_ctx) and _init_scanner() assigns its pointer; per-range fields are
// overwritten in-place without copying.
struct HdfsScannerContext {
    // ===== nested types =====
    struct ColumnInfo {
        int idx_in_chunk;
        SlotDescriptor* slot_desc;
        bool decode_needed = true;

        std::string formatted_name(bool case_sensitive) const {
            auto n = std::string(name());
            return case_sensitive ? n : boost::algorithm::to_lower_copy(n);
        }
        std::string_view name() const { return slot_desc->col_name(); }
        int32_t col_unique_id() const { return slot_desc->col_unique_id(); }
        std::string_view col_physical_name() const { return slot_desc->col_physical_name(); }
        const SlotId slot_id() const { return slot_desc->id(); }
        const TypeDescriptor& slot_type() const { return slot_desc->type(); }
    };

    // ===== per-range fields =====
    const THdfsScanRange* scan_range = nullptr;
    int32_t scan_range_id = -1;
    FileSystem* fs = nullptr;
    std::string file_path;
    int64_t file_size = -1;
    bool is_first_split = false;
    std::string table_location;
    const HdfsSplitContext* split_context = nullptr;
    DataCacheOptions datacache_options{};
    TableSpecificData table_specific;

    // ===== shared scan fields =====
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;
    const TupleDescriptor* tuple_desc = nullptr;
    HdfsScannerConjuncts conjuncts;
    HdfsScannerOptions options;
    HdfsScannerProfile profile;

    // ===== column descriptors =====
    // ---- materialized columns ----
    std::vector<SlotDescriptor*> materialize_slots;
    std::vector<int> materialize_index_in_chunk;
    std::unordered_map<SlotId, std::string> materialize_slot_default_values;

    // ---- partition columns ----
    std::vector<SlotDescriptor*> partition_slots;
    std::vector<int> partition_index_in_chunk;
    std::vector<int> _partition_index_in_hdfs_partition_columns;

    // ---- extended columns (iceberg data_seq_num etc.) ----
    std::vector<SlotDescriptor*> extended_col_slots;
    std::vector<int> extended_col_index_in_chunk;
    std::vector<int> index_in_extended_columns;

    // ===== table metadata =====
    const TupleDescriptor* min_max_tuple_desc = nullptr;
    const HiveTableDescriptor* hive_table = nullptr;
    std::vector<std::string> hive_column_names;
    std::string avro_schema_json;
    std::vector<ColumnAccessPathPtr> column_access_paths;

    // ===== working state =====
    std::vector<SlotDescriptor*> slot_descs;
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // materialized column read from parquet file
    std::vector<ColumnInfo> materialized_columns;

    // partition column
    std::vector<ColumnInfo> partition_columns;

    // ExprContexts for partition column values (evaluated from hdfs file path).
    // Filled by HiveDataSource before init(); evaluated once in _build_scanner_context()
    // to produce partition_values below.  Lifetime is managed by HiveDataSource's ObjectPool.
    std::vector<ExprContext*> partition_expr_ctxs;

    // Evaluated partition column values (constant columns, one per partition slot).
    Columns partition_values;

    // Extended column metadata (iceberg data_seq_num, etc.).
    std::vector<ColumnInfo> extended_columns;

    // ExprContexts for extended column values.  Same lifetime model as partition_expr_ctxs.
    std::vector<ExprContext*> extended_col_expr_ctxs;

    // Evaluated extended column values.
    Columns extended_values;

    bool can_use_file_record_count = false;

    // used by short circuit cases:
    // get_next just returns chunk for once.
    // and it returns EOF the next time.
    bool no_more_chunks = false;

    std::string timezone;

    HdfsScannerStats* stats = nullptr;

    std::vector<SlotDescriptor*> not_existed_slots;

    // for iceberg reserved fields
    std::vector<SlotDescriptor*> reserved_field_slots;

    std::vector<ExprContext*> conjunct_ctxs_of_non_existed_slots;

    // TODO: probably should be removed in later version.
    std::atomic<int32_t>* lazy_column_coalesce_counter = nullptr;
    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;

    // ===== infrastructure =====
    // ObjectPool for allocations built by _build_scanner_context().
    // Prefer HiveDataSource::_pool (shorter lifetime); fall back to
    // runtime_state->obj_pool() in tests and non-connector paths.
    ObjectPool* obj_pool = nullptr;

    // Embedded value structs — no pointers, no pool allocation.
    // Since ctx is pointer-passed (never copied), unique_ptr members work naturally.

    struct SplitState {
        std::vector<HdfsSplitContextPtr> split_tasks;
        bool has_split_tasks = false;
        size_t estimated_mem_usage_per_split_task = 0;
    } split;

    // Destruction order within predicates matters (C++ reverse declaration):
    //   runtime_filter_scan_range_pruner (refs into conjuncts_manager) → destroyed 1st
    //   predicate_tree / predicate_parser (raw ptrs into predicate_free_pool)
    //   predicate_free_pool (owns ColumnPredicates)
    //   conjuncts_manager → destroyed last
    struct PredicateState {
        std::unique_ptr<RuntimeScanRangePruner> runtime_filter_scan_range_pruner;
        PredicateTree predicate_tree;
        std::unique_ptr<ConnectorPredicateParser> predicate_parser;
        std::vector<std::unique_ptr<ColumnPredicate>> predicate_free_pool;
        std::unique_ptr<ScanConjunctsManager> conjuncts_manager;
    } predicates;

    // ===== methods =====
    bool is_lazy_materialization_slot(SlotId slot_id) const;

    std::string formatted_name(const std::string& name) const {
        return options.case_sensitive ? name : boost::algorithm::to_lower_copy(name);
    }

    bool can_use_count_optimization() const;

    bool can_use_min_max_optimization() const;

    void update_with_none_existed_slot(SlotDescriptor* slot);
    void update_return_count_columns();
    void update_min_max_columns();

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
    void append_or_update_min_max_column_to_chunk(ChunkPtr* chunk, size_t row_count);
    MutableColumnPtr create_min_max_value_column(SlotDescriptor* slot, const TExprMinMaxValue& value, size_t row_count);

    void append_or_update_extended_column_to_chunk(ChunkPtr* chunk, size_t row_count);
    void append_or_update_column_to_chunk(ChunkPtr* chunk, size_t row_count, const std::vector<ColumnInfo>& columns,
                                          const Columns& values);

    // if we can skip this file by evaluating conjuncts of non-existed columns with default value.
    StatusOr<bool> should_skip_by_evaluating_not_existed_slots();

    // other helper functions.
    bool can_use_dict_filter_on_slot(SlotDescriptor* slot) const;
    Status evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter);

    void merge_split_tasks();
};

struct OpenFileOptions {
    FileSystem* fs = nullptr;
    std::string file_path;
    int64_t file_size = -1;
    HdfsScannerStats* fs_stats = nullptr;
    HdfsScannerStats* app_stats = nullptr;

    // for datacache
    DataCacheOptions datacache_options;

    // for compressed text file
    CompressionTypePB compression_type = CompressionTypePB::NO_COMPRESSION;
};

class HdfsScanner {
public:
    HdfsScanner() = default;
    virtual ~HdfsScanner() = default;

    Status init(RuntimeState* runtime_state, HdfsScannerContext* scanner_ctx);
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
    virtual Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) = 0;
    virtual void do_update_counter(HdfsScannerProfile* profile);
    virtual Status reinterpret_status(const Status& st);

    // ORC and Parquet push conjunct_ctxs_by_slot into their own column-level
    // readers (lazy materialisation, dict filter, etc.) and do NOT want the base
    // class to apply them a second time.  All other formats return false so the
    // base class handles by-slot evaluation uniformly in get_next().
    virtual bool scanner_handles_predicate_by_slot_internally() const { return false; }

    // True if the scanner evaluates multi-slot predicates (conjuncts->scanner_ctxs)
    // internally inside do_get_next(), so the base class must not apply them again.
    // Scanners that return true MUST guarantee row-level correctness — the ORC
    // search-argument / Parquet statistics pass is only an approximate skip; the
    // actual predicate must still be evaluated on every returned row.
    // Future expression-driven Parquet lazy materialisation will also set this true
    // once it can interleave predicate evaluation with column loading.
    virtual bool scanner_handles_multi_slot_conjuncts_internally() const { return false; }
    void move_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks);
    bool has_split_tasks() const { return _scanner_ctx != nullptr && _scanner_ctx->split.has_split_tasks; }

    static StatusOr<std::unique_ptr<RandomAccessFile>> create_random_access_file(
            std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<CacheInputStream>& cache_input_stream, const OpenFileOptions& options);

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
    void update_hdfs_counter(HdfsScannerProfile* profile);

protected:
    HdfsScannerContext* _scanner_ctx = nullptr;
    RuntimeState* _runtime_state = nullptr;
    HdfsScannerStats _app_stats;
    HdfsScannerStats _fs_stats;
    std::unique_ptr<RandomAccessFile> _file;
    // by default it's no compression.
    CompressionTypePB _compression_type = CompressionTypePB::NO_COMPRESSION;
    std::shared_ptr<CacheInputStream> _cache_input_stream = nullptr;
    std::shared_ptr<SharedBufferedInputStream> _shared_buffered_input_stream = nullptr;
    int64_t _total_running_time = 0;

public:
    static constexpr const char* ICEBERG_ROW_ID = "_row_id";
    static constexpr const char* ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER = "_last_updated_sequence_number";
    static constexpr const char* ICEBERG_ROW_POSITION = "_pos";
    // Iceberg v3 spec reserved field IDs for row lineage columns.
    // See: https://iceberg.apache.org/spec/#reserved-field-ids
    static constexpr int32_t ICEBERG_ROW_ID_COLUMN_ID = 2147483540;
    static constexpr int32_t ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_ID = 2147483539;
};

} // namespace starrocks
