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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/cache_options.h"
#include "column/column_access_path.h"
#include "column/global_dict/types.h"
#include "common/runtime_profile.h"
#include "compute_env/query/file_scan_split_context.h"
#include "exec/olap_scan_prepare.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "storage/primitive/column_predicate_factory.h"
#include "storage/primitive/predicate_parser.h"
#include "storage/primitive/predicate_tree/predicate_tree.h"

namespace starrocks {

class HiveTableDescriptor;
class RuntimeFilterProbeCollector;

struct HdfsScannerProfile {
    RuntimeProfile* runtime_profile = nullptr;
    RuntimeProfile::Counter* raw_rows_read_counter = nullptr;
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* late_materialize_skip_rows_counter = nullptr;

    RuntimeProfile::Counter* parquet_lazy_col_skip_rows_counter = nullptr;
    RuntimeProfile::Counter* parquet_lazy_slot_triggered_counter = nullptr;
    RuntimeProfile::Counter* parquet_lazy_read_count_counter = nullptr;
    RuntimeProfile::Counter* parquet_lazy_read_timer = nullptr;
    RuntimeProfile::Counter* parquet_lazy_full_trigger_count_counter = nullptr;
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

// Table-format-specific state carried through the scan pipeline.
struct TableSpecificData {
    std::vector<const TIcebergDeleteFile*> iceberg_delete_files;
    std::shared_ptr<TDeletionVectorDescriptor> deletion_vector_descriptor;
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
    // ===== per-range fields =====
    const THdfsScanRange* scan_range = nullptr;
    FileSystem* fs = nullptr;
    std::string file_path;
    int64_t file_size = -1;
    std::string table_location;
    DataCacheOptions datacache_options{};
    TableSpecificData table_specific;

    // ===== shared scan fields =====
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;
    const TupleDescriptor* tuple_desc = nullptr;
    FormatScanContext format_scan_context;
    HdfsScannerProfile profile;

    // ===== column descriptors =====
    // ---- materialized columns ----
    std::vector<SlotDescriptor*> materialize_slots;
    std::vector<int> materialize_index_in_chunk;

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

    // ===== working state =====
    std::vector<SlotDescriptor*> slot_descs;

    // ExprContexts for partition column values (evaluated from hdfs file path).
    // Filled by HiveDataSource before init(); evaluated once in _build_scanner_context()
    // to produce format_scan_context.partition_values.  Lifetime is managed by
    // HiveDataSource's ObjectPool.
    std::vector<ExprContext*> partition_expr_ctxs;

    // ExprContexts for extended column values.  Same lifetime model as partition_expr_ctxs.
    std::vector<ExprContext*> extended_col_expr_ctxs;

    // used by short circuit cases:
    // get_next just returns chunk for once.
    // and it returns EOF the next time.
    bool no_more_chunks = false;

    // ===== infrastructure =====
    // ObjectPool for allocations built by _build_scanner_context().
    // Prefer HiveDataSource::_pool (shorter lifetime); fall back to
    // runtime_state->obj_pool() in tests and non-connector paths.
    ObjectPool* obj_pool = nullptr;

    // Embedded value structs: no pointers, no pool allocation.
    // Since ctx is pointer-passed (never copied), unique_ptr members work naturally.

    // Destruction order within predicates matters (C++ reverse declaration):
    //   runtime_filter_scan_range_pruner (refs into conjuncts_manager) is destroyed first
    //   predicate_tree / predicate_parser (raw ptrs into predicate_free_pool)
    //   predicate_free_pool (owns ColumnPredicates)
    //   conjuncts_manager is destroyed last
    struct PredicateState {
        std::unique_ptr<RuntimeScanRangePruner> runtime_filter_scan_range_pruner;
        PredicateTree predicate_tree;
        std::unique_ptr<ConnectorPredicateParser> predicate_parser;
        std::vector<std::unique_ptr<ColumnPredicate>> predicate_free_pool;
        std::unique_ptr<ScanConjunctsManager> conjuncts_manager;
    } predicates;

    // ===== methods =====
    std::string formatted_name(const std::string& name) const {
        return format_scan_context.options.case_sensitive ? name : boost::algorithm::to_lower_copy(name);
    }
};

} // namespace starrocks
