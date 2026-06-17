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

#include <boost/algorithm/string.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/global_types.h"
#include "formats/deletion_bitmap.h"
#include "runtime/descriptors.h"

namespace starrocks {

class ExprContext;

struct SkipRowsContext {
    DeletionBitmapPtr deletion_bitmap;
    bool has_skip_rows() { return deletion_bitmap != nullptr && !deletion_bitmap->empty(); }
};
using SkipRowsContextPtr = std::shared_ptr<SkipRowsContext>;

struct FormatScannerStats {
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

    // TopN scan-range skip: data files dropped before the footer is read, because the reorder
    // slot's min/max cannot beat the current TopN runtime filter.
    int64_t topn_min_max_filtered_scan_ranges = 0;

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

    // TopN scan reorder/skip (ORDER BY <col> [ASC|DESC] LIMIT k): slot id of the leading sort key
    // used to reorder morsels and skip files by their min/max (-1 = off), plus its sort direction
    // and null ordering. Filled by HiveDataSource from the provider.
    int32_t topn_reorder_slot_id = -1;
    bool topn_reorder_desc = false;
    bool topn_reorder_nulls_first = false;
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

} // namespace starrocks
