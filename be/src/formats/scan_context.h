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
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "column/column.h"
#include "column/column_access_path.h"
#include "column/global_dict/types.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/query/file_scan_split_context.h"
#include "formats/deletion_bitmap.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

class ExprContext;
class PredicateTree;
class RuntimeScanRangePruner;
struct FileScanSplitContext;
class TIcebergSchema;

struct SkipRowsContext {
    DeletionBitmapPtr deletion_bitmap;
    bool has_skip_rows() { return deletion_bitmap != nullptr && !deletion_bitmap->empty(); }
};
using SkipRowsContextPtr = std::shared_ptr<SkipRowsContext>;

struct FormatScannerStats {
    int64_t raw_rows_read = 0;
    int64_t rows_read = 0;
    int64_t late_materialize_skip_rows = 0;

    // Parquet lazy materialization: rows filtered before lazy columns were read.
    int64_t parquet_lazy_col_skip_rows = 0;
    // Parquet lazy materialization: number of lazy slots triggered via MissingColumnProvider.
    int64_t parquet_lazy_slot_triggered = 0;
    // Parquet lazy materialization: number of lazy column read operations.
    int64_t parquet_lazy_read_count = 0;
    // Parquet lazy materialization: time spent reading lazy columns.
    int64_t parquet_lazy_read_ns = 0;
    // Parquet dict-code predicate evaluation count.
    int64_t parquet_dict_code_predicate_eval_count = 0;
    // Parquet lazy materialization: row groups where every lazy slot was triggered
    // on-demand (suggests active/lazy classification could be improved).
    int64_t parquet_lazy_full_trigger_count = 0;

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

    // Parquet global-dict opt application, populated by parquet::GroupReader as
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

// Immutable scan options derived from the query plan node, embedded by value
// inside format scan contexts so they are copied together with the rest of the
// per-scanner params.
struct FormatScannerOptions {
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

// All conjunct contexts and slot metadata derived from the scan plan node.
struct FormatScannerConjuncts {
    // Clone of min_max_ctxs plus scan conjuncts, used to build the
    // ScanConjunctsManager predicate tree (Parquet row-group statistics, etc.).
    std::vector<ExprContext*> all_ctxs;

    // Multi-slot predicates (e.g. "a + b > 5") that cannot be pushed into a
    // single-column reader.  Evaluated after every column is materialised.
    std::vector<ExprContext*> scanner_ctxs;

    // Single-slot predicates pushed into ORC stripe / Parquet row-group
    // statistics filtering via min_max_tuple_desc.
    std::vector<ExprContext*> min_max_ctxs;

    // Single-slot predicates keyed by SlotId.
    std::unordered_map<SlotId, std::vector<ExprContext*>> by_slot;

    // All SlotIds appearing in any conjunct.
    std::unordered_set<SlotId> slots_in_conjunct;

    // Slots appearing in multi-field conjuncts; those slots must be fully
    // decoded even when they are not output columns.
    std::unordered_set<SlotId> slots_of_multi_field;
};

struct FormatColumnInfo {
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

struct FormatScanContext {
    FormatScannerOptions options;
    FormatScannerStats* stats = nullptr;
    const FileScanSplitContext* split_context = nullptr;

    // All conjunct contexts and slot metadata derived from the scan plan node.
    FormatScannerConjuncts conjuncts;

    // Mutable per-reader shallow copy of conjuncts.by_slot.  Missing-column
    // discovery erases entries for columns absent from the file.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // Conjuncts whose referenced slots are missing from the data file.
    std::vector<ExprContext*> conjunct_ctxs_of_non_existed_slots;

    // Materialized columns read from the data file.
    std::vector<FormatColumnInfo> materialized_columns;
    std::unordered_map<SlotId, std::string> materialize_slot_default_values;

    // Partition column metadata and pre-evaluated constant values.
    std::vector<FormatColumnInfo> partition_columns;
    Columns partition_values;

    // Extended column metadata (iceberg data_seq_num, etc.).
    std::vector<FormatColumnInfo> extended_columns;

    // Evaluated extended column values.
    Columns extended_values;

    // Columns requested by the query but absent from the data file.
    std::vector<SlotDescriptor*> not_existed_slots;

    // Reserved/system fields such as Iceberg row lineage columns.
    std::vector<SlotDescriptor*> reserved_field_slots;

    // Table column access paths for complex/variant projection.
    std::vector<ColumnAccessPathPtr> column_access_paths;

    // FE-provided global dictionary maps, if any.
    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;

    std::string timezone;

    // Scan-range data needed by format readers without depending on
    // THdfsScanRange.
    int64_t scan_range_offset = 0;
    int64_t scan_range_length = std::numeric_limits<int64_t>::max();
    int32_t scan_range_id = -1;
    std::map<int32_t, TExprMinMaxValue> min_max_values;
    std::map<int32_t, TExpr> extended_column_exprs;
    std::optional<int64_t> first_row_id;
    int64_t file_record_count = 0;
    bool has_file_record_count = false;
    bool is_first_split = false;

    // Table/lake schema used by Parquet schema evolution matching.
    const TIcebergSchema* lake_schema = nullptr;

    // Non-owning predicate state built by upper scan orchestration.
    const PredicateTree* predicate_tree = nullptr;
    RuntimeScanRangePruner* runtime_filter_scan_range_pruner = nullptr;

    // TODO: probably should be removed in later version.
    std::atomic<int32_t>* lazy_column_coalesce_counter = nullptr;

    struct SplitState {
        std::vector<FileScanSplitContextPtr> split_tasks;
        bool has_split_tasks = false;
        size_t estimated_mem_usage_per_split_task = 0;
    } split;

    bool is_lazy_materialization_slot(SlotId slot_id) const;
    bool can_use_count_optimization() const;
    bool has_count_column() const { return _count_slot.has_value(); }
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

    // If there is no partition column in the chunk, append partition column to chunk,
    // otherwise update partition column in chunk
    void append_or_update_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count);

    // Emit `output_rows` rows of `value` for the ___count___ column.
    // Idempotent: if the chunk already has a ___count___ column, this is a no-op.
    // Used by both the count-opt aggregated path (output_rows=1, value=row_count)
    // and the per-row fallback (output_rows=row_count, value=1).
    void append_or_update_count_column_to_chunk(ChunkPtr* chunk, size_t output_rows, int64_t value);
    MutableColumnPtr create_min_max_value_column(SlotDescriptor* slot_desc, const TExprMinMaxValue& value,
                                                 size_t row_count);
    void append_or_update_extended_column_to_chunk(ChunkPtr* chunk, size_t row_count);

    // Append all side columns (not-existed, partition, extended, count) at once.
    // Safe when any category is empty — each sub-function is a no-op in that case.
    Status append_side_columns_to_chunk(ChunkPtr* chunk, size_t row_count);
    void append_or_update_column_to_chunk(ChunkPtr* chunk, size_t row_count,
                                          const std::vector<FormatColumnInfo>& columns, const Columns& values);

    // if we can skip this file by evaluating conjuncts of non-existed columns with default value.
    StatusOr<bool> should_skip_by_evaluating_not_existed_slots();

    // other helper functions.
    bool can_use_dict_filter_on_slot(SlotDescriptor* slot) const;
    Status evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter);

    // Evaluate both single-slot conjuncts (conjunct_ctxs_by_slot) and multi-slot
    // conjuncts (conjuncts.scanner_ctxs) on the chunk in place.  Safe to call
    // when either category is empty — each sub-step is a no-op.
    Status evaluate_all_predicates(ChunkPtr* chunk);

    void merge_split_tasks();

private:
    // ___count___ column for COUNT(*) optimization.
    // Separated from not_existed_slots because it is an execution marker,
    // not a schema-evolution missing column.
    std::optional<SlotDescriptor*> _count_slot;
};

} // namespace starrocks
