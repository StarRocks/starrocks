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

#include "connector/hive/scanner/hdfs_scanner.h"

#include <bvar/bvar.h>

#include <algorithm>

#include "base/compression/compression_utils.h"
#include "cache/data_cache_hit_rate_counter.hpp"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/config_cache_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exprs/chunk_predicate_evaluator.h"
#include "formats/delta/deletion_vector.h"
#include "formats/file_input_stream.h"
#include "formats/reserved_columns.h"
#include "fs/hdfs_metrics.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/predicate_parser.h"

namespace starrocks {

// Files whose corrupted datacache is dropped.
static bvar::Adder<int64_t> g_datacache_corruption_removed("datacache_corruption_removed_files");
// Files read again bypassing their corrupted datacache.
static bvar::Adder<int64_t> g_datacache_corruption_retried("datacache_corruption_retried_files");

Status HdfsScanner::init(RuntimeState* runtime_state, HdfsScannerContext* scanner_ctx) {
    SCOPED_RAW_TIMER(&_total_running_time);
    _runtime_state = runtime_state;
    _scanner_ctx = scanner_ctx;
    RETURN_IF_ERROR(do_init(runtime_state, *scanner_ctx));
    return Status::OK();
}

Status HdfsScanner::_build_scanner_context() {
    HdfsScannerContext& ctx = *_scanner_ctx;

    // Clear fields that this function populates so the call is idempotent
    // (same ctx pointer may be reused across scanners in tests).
    ctx.format_scan_context.partition_values.clear();
    ctx.format_scan_context.extended_values.clear();
    ctx.format_scan_context.materialized_columns.clear();
    ctx.format_scan_context.partition_columns.clear();
    ctx.format_scan_context.extended_columns.clear();
    ctx.format_scan_context.reserved_field_slots.clear();
    ctx.format_scan_context.not_existed_slots.clear();
    ctx.format_scan_context.conjunct_ctxs_by_slot.clear();
    ctx.format_scan_context.conjunct_ctxs_of_non_existed_slots.clear();
    ctx.format_scan_context.has_file_record_count = false;
    ctx.format_scan_context.file_record_count = 0;
    ctx.format_scan_context.is_first_split = false;
    ctx.format_scan_context.first_row_id.reset();
    ctx.format_scan_context.min_max_values.clear();
    ctx.format_scan_context.extended_column_exprs.clear();
    ctx.format_scan_context.predicate_tree = nullptr;
    ctx.format_scan_context.runtime_filter_scan_range_pruner = nullptr;
    ctx.format_scan_context.runtime_filter_preds = nullptr;
    ctx.predicates.runtime_filter_preds = RuntimeFilterPredicates();

    ctx.format_scan_context.scan_range_offset = ctx.scan_range->offset;
    ctx.format_scan_context.scan_range_length = ctx.scan_range->length;
    ctx.format_scan_context.min_max_values = ctx.scan_range->min_max_values;
    if (ctx.scan_range->__isset.extended_columns) {
        ctx.format_scan_context.extended_column_exprs = ctx.scan_range->extended_columns;
    }
    if (ctx.scan_range->__isset.first_row_id) {
        ctx.format_scan_context.first_row_id = ctx.scan_range->first_row_id;
    }

    Columns& partition_values = ctx.format_scan_context.partition_values;

    // evaluate partition values; ExprContexts are owned by HiveDataSource's
    // ObjectPool, made available via ctx.partition_expr_ctxs.
    for (size_t i = 0; i < _scanner_ctx->partition_slots.size(); i++) {
        int part_col_idx = _scanner_ctx->_partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_column, ctx.partition_expr_ctxs[part_col_idx]->evaluate(nullptr));
        DCHECK(partition_value_column->is_constant());
        partition_values.emplace_back(std::move(partition_value_column));
    }

    // evaluate extended column values
    Columns& extended_values = ctx.format_scan_context.extended_values;
    for (size_t i = 0; i < _scanner_ctx->extended_col_slots.size(); i++) {
        int extended_col_idx = _scanner_ctx->index_in_extended_columns[i];
        ASSIGN_OR_RETURN(auto extended_value_column, ctx.extended_col_expr_ctxs[extended_col_idx]->evaluate(nullptr));
        DCHECK(extended_value_column->is_constant());
        extended_values.emplace_back(std::move(extended_value_column));
    }

    // conjunct_ctxs_by_slot is a mutable per-scanner shallow copy of by_slot;
    // update_with_none_existed_slot() erases entries when columns are absent.
    ctx.format_scan_context.conjunct_ctxs_by_slot = _scanner_ctx->format_scan_context.conjuncts.by_slot;

    // build columns of materialized and partition.
    for (size_t i = 0; i < _scanner_ctx->materialize_slots.size(); i++) {
        auto* slot = _scanner_ctx->materialize_slots[i];
        if (formats::is_reserved_scan_column_name(slot->col_name())) {
            ctx.format_scan_context.reserved_field_slots.emplace_back(slot);
        } else {
            FormatColumnInfo column;
            column.slot_desc = slot;
            column.idx_in_chunk = _scanner_ctx->materialize_index_in_chunk[i];
            // A slot must be decoded eagerly if it is an output column OR if it
            // appears in a multi-field conjunct that the reader cannot push down.
            column.decode_needed = slot->is_output_column() ||
                                   _scanner_ctx->format_scan_context.conjuncts.slots_of_multi_field.count(slot->id());
            ctx.format_scan_context.materialized_columns.emplace_back(column);
        }
    }

    for (size_t i = 0; i < _scanner_ctx->partition_slots.size(); i++) {
        auto* slot = _scanner_ctx->partition_slots[i];
        FormatColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_ctx->partition_index_in_chunk[i];
        ctx.format_scan_context.partition_columns.emplace_back(column);
    }

    for (size_t i = 0; i < _scanner_ctx->extended_col_slots.size(); i++) {
        auto* slot = _scanner_ctx->extended_col_slots[i];
        FormatColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_ctx->extended_col_index_in_chunk[i];
        ctx.format_scan_context.extended_columns.emplace_back(column);
    }

    ctx.slot_descs = _scanner_ctx->tuple_desc->slots();
    ctx.format_scan_context.timezone = _runtime_state->timezone();
    ctx.format_scan_context.stats = &_app_stats;

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_scanner_ctx->format_scan_context.conjuncts.all_ctxs;
    opts.tuple_desc = _scanner_ctx->tuple_desc;
    // Must use the fragment-scoped pool (runtime_state->obj_pool()), not
    // _scanner_ctx->obj_pool.  BoxedExpr::expr_context() deep-copies Expr
    // nodes into this pool, and those nodes are later referenced by cloned
    // ExprContexts inside ColumnExprPredicate (which live in the predicates
    // struct and outlive the data source's _pool).  Using any shorter-lived
    // pool would free the Expr nodes before the predicates are destroyed,
    // causing the same use-after-free pattern described in HdfsScanner::close().
    opts.obj_pool = _runtime_state->obj_pool();
    opts.runtime_filters = _scanner_ctx->runtime_filter_collector;
    // Used when resolving runtime filters; see HdfsScannerContext::driver_sequence.
    opts.driver_sequence = _scanner_ctx->driver_sequence;
    opts.runtime_state = _runtime_state;
    opts.enable_column_expr_predicate = true;
    opts.is_olap_scan = false;
    opts.pred_tree_params = _runtime_state->fragment_runtime_state()->pred_tree_params();

    ctx.predicates.conjuncts_manager = std::make_unique<ScanConjunctsManager>(opts);
    RETURN_IF_ERROR(ctx.predicates.conjuncts_manager->parse_conjuncts());
    ctx.predicates.predicate_parser =
            std::make_unique<ConnectorPredicateParser>(&_scanner_ctx->tuple_desc->decoded_slots());
    ASSIGN_OR_RETURN(ctx.predicates.predicate_tree,
                     ctx.predicates.conjuncts_manager->get_predicate_tree(ctx.predicates.predicate_parser.get(),
                                                                          ctx.predicates.predicate_free_pool));
    ctx.predicates.runtime_filter_scan_range_pruner = std::make_unique<RuntimeScanRangePruner>(
            ctx.predicates.predicate_parser.get(), ctx.predicates.conjuncts_manager->unarrived_runtime_filters());
    ctx.format_scan_context.predicate_tree = &ctx.predicates.predicate_tree;
    ctx.format_scan_context.runtime_filter_scan_range_pruner = ctx.predicates.runtime_filter_scan_range_pruner.get();

    // Storage-layer runtime filter predicates, so format readers can probe rows during
    // decode instead of leaving the whole row-level filtering to the scan operator.
    // get_runtime_filter_predicates() leaves has_push_down_to_storage() false here
    // (opts.is_olap_scan == false), so the operator-level probe keeps running: this
    // pushdown is best-effort -- a predicate is dropped when its slot is absent from
    // the file or cannot be served by the reader -- and must not be treated as complete.
    ctx.format_scan_context.driver_sequence = _scanner_ctx->driver_sequence;
    if (config::parquet_runtime_filter_push_down_enable && _runtime_state->enable_join_runtime_filter_pushdown() &&
        _scanner_ctx->runtime_filter_collector != nullptr) {
        // Same fragment-scoped pool as opts.obj_pool above: the predicates are borrowed
        // by ctx.predicates and must outlive it.
        ASSIGN_OR_RETURN(ctx.predicates.runtime_filter_preds,
                         ctx.predicates.conjuncts_manager->get_runtime_filter_predicates(
                                 _runtime_state->obj_pool(), ctx.predicates.predicate_parser.get()));
        if (!ctx.predicates.runtime_filter_preds.empty()) {
            ctx.format_scan_context.runtime_filter_preds = &ctx.predicates.runtime_filter_preds;
        }
    }

    ctx.format_scan_context.update_return_count_columns();
    if (ctx.scan_range->__isset.record_count && ctx.scan_range->delete_files.empty()) {
        ctx.format_scan_context.has_file_record_count = true;
        ctx.format_scan_context.file_record_count = ctx.scan_range->record_count;
    }
    if (ctx.scan_range->__isset.is_first_split) {
        ctx.format_scan_context.is_first_split = ctx.scan_range->is_first_split;
    }

    ctx.format_scan_context.update_min_max_columns();
    return Status::OK();
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_total_running_time);
    RETURN_IF_CANCELLED(_runtime_state);

    if (_scanner_ctx->no_more_chunks) {
        return Status::EndOfFile("");
    }

    // short circuit for ___count___ optimization.
    if (_scanner_ctx->format_scan_context.can_use_count_optimization()) {
        int64_t file_record_count = 0;
        if (_scanner_ctx->format_scan_context.is_first_split) {
            file_record_count = _scanner_ctx->format_scan_context.file_record_count;
        }
        // append_side_columns_to_chunk fills per-row count (value=1),
        // partition, and extended columns first.  The next call overwrites the
        // count column with the aggregated file record count.
        RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(chunk, 1));
        _scanner_ctx->format_scan_context.append_or_update_count_column_to_chunk(chunk, 1, file_record_count);
        _scanner_ctx->no_more_chunks = true;
        _app_stats.rows_read += 1;
        return Status::OK();
    }

    // short circuit for min/max optimization.
    if (_scanner_ctx->format_scan_context.can_use_min_max_optimization()) {
        // 3 means we output 3 values: min, max, and null
        const size_t row_count = 3;
        (*chunk)->set_num_rows(row_count);
        RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(chunk, row_count));
        _scanner_ctx->no_more_chunks = true;
        _app_stats.rows_read += row_count;
        return Status::OK();
    }

    RETURN_IF_ERROR(_runtime_state->check_mem_limit("get chunk from scanner"));
    Status status = do_get_next(runtime_state, chunk);
    if (!status.ok() && !status.is_end_of_file()) {
        status = _handle_datacache_error(runtime_state, status, [&]() {
            // Only empty chunks were returned before the error, so read until some rows or EOF to pass it.
            Status st;
            do {
                RETURN_IF_CANCELLED(_runtime_state);
                (*chunk)->reset();
                st = do_get_next(runtime_state, chunk);
            } while (st.ok() && (*chunk)->num_rows() == 0);
            return st;
        });
    }
    if (status.ok()) {
    } else if (status.is_end_of_file()) {
        // do nothing.
    } else {
        LOG(ERROR) << "failed to read file: " << _scanner_ctx->file_path;
    }
    _app_stats.rows_read += (*chunk)->num_rows();
    return status;
}

Status HdfsScanner::open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_total_running_time);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    RETURN_IF_ERROR(_build_scanner_context());
    // short circuit for ___count___ optimization.
    // short circuit for min/max optimization.
    if (_scanner_ctx->format_scan_context.can_use_count_optimization() ||
        _scanner_ctx->format_scan_context.can_use_min_max_optimization()) {
        return Status::OK();
    }
    Status status = do_open(runtime_state);
    if (!status.ok()) {
        // Reopening runs do_open() again, nothing else to redo.
        status = _handle_datacache_error(runtime_state, status, [] { return Status::OK(); });
    }
    RETURN_IF_ERROR(status);
    VLOG_FILE << "open file success: " << _scanner_ctx->file_path << ", scan range = ["
              << _scanner_ctx->scan_range->offset << ","
              << (_scanner_ctx->scan_range->length + _scanner_ctx->scan_range->offset)
              << "], candidate node = " << _scanner_ctx->scan_range->candidate_node;
    return Status::OK();
}

void HdfsScanner::close() noexcept {
    if (!_runtime_state) {
        return;
    }
    // short circuit for ___count___ optimization.
    // short circuit for min/max optimization.
    if (_scanner_ctx->format_scan_context.can_use_count_optimization() ||
        _scanner_ctx->format_scan_context.can_use_min_max_optimization()) {
        return;
    }
    VLOG_FILE << "close file success: " << _scanner_ctx->file_path << ", scan range = ["
              << _scanner_ctx->scan_range->offset << ","
              << (_scanner_ctx->scan_range->length + _scanner_ctx->scan_range->offset)
              << "], rows = " << _app_stats.rows_read;

    bool expect = false;
    if (!_closed.compare_exchange_strong(expect, true)) return;
    update_counter();
    do_close(_runtime_state);
    _file.reset(nullptr);
}

Status HdfsScanner::open_random_access_file() {
    formats::FileInputStreamOptions options{.fs = _scanner_ctx->fs,
                                            .file_path = _scanner_ctx->file_path,
                                            .file_size = _scanner_ctx->file_size,
                                            .fs_stats = &_fs_stats,
                                            .app_stats = &_app_stats,
                                            .datacache_options = _scanner_ctx->datacache_options,
                                            .compression_type = _compression_type};

    ASSIGN_OR_RETURN(_file,
                     formats::create_random_access_file(_shared_buffered_input_stream, _cache_input_stream, options));
    if (_cache_input_stream) {
        _cache_input_stream->set_peer_cache_node(_scanner_ctx->scan_range->candidate_node);
    }
    return Status::OK();
}

bool HdfsScanner::_may_be_caused_by_datacache(const Status& status) {
    if (!config::external_table_clear_corrupted_cache_data || _datacache_error_handled) {
        return false;
    }
    if (_cache_input_stream == nullptr || _scanner_ctx->datacache_options.enable_cache_select) {
        return false;
    }
    // Errors of decoding a corrupted copy, e.g. InvalidArgument of a failed decompression.
    if (!status.is_corruption() && !status.is_internal_error() && !status.is_not_supported() &&
        !status.is_invalid_argument()) {
        return false;
    }
    // Some content must come from the datacache, peers included since their blocks are populated locally.
    const auto& stats = _cache_input_stream->stats();
    return stats.read_block_cache_bytes + stats.read_peer_cache_bytes > 0;
}

Status HdfsScanner::_handle_datacache_error(RuntimeState* runtime_state, const Status& status,
                                            const std::function<Status()>& redo) {
    if (!_may_be_caused_by_datacache(status)) {
        return status;
    }
    _datacache_error_handled = true;
    std::shared_ptr<CacheInputStream> cache_input_stream = _cache_input_stream;
    // A reopened file is read from its start, so reopen only if no row is returned yet.
    if (!support_reopen_without_datacache() || _app_stats.rows_read > 0) {
        // The corruption cannot be verified, drop the copy anyway so that the next read recovers.
        _drop_datacache(cache_input_stream.get(), status, false);
        return status;
    }
    Status st = _reopen_without_datacache(runtime_state);
    if (st.ok()) {
        st = redo();
    }
    if (st.ok() || st.is_end_of_file()) {
        // The file is read correctly bypassing the datacache, so its cached copy is corrupted.
        _drop_datacache(cache_input_stream.get(), status, true);
        return st;
    }
    // Not caused by the datacache, keep it. Return the new error, e.g. a 404 lets the FE refresh and retry.
    LOG(WARNING) << "read file again bypassing datacache still fails, keep its datacache: " << _scanner_ctx->file_path
                 << ", error: " << st << ", datacache read error: " << status;
    return st;
}

void HdfsScanner::_drop_datacache(CacheInputStream* cache_input_stream, const Status& status, bool verified) {
    size_t removed_blocks = cache_input_stream->remove_cached_blocks();
    if (removed_blocks > 0) {
        g_datacache_corruption_removed << 1;
    }
    const auto& stats = cache_input_stream->stats();
    LOG(WARNING) << "drop corrupted datacache of file: " << _scanner_ctx->file_path
                 << ", file_size: " << _scanner_ctx->file_size
                 << ", modification_time: " << _scanner_ctx->datacache_options.modification_time
                 << ", verified: " << verified << ", removed_blocks: " << removed_blocks
                 << ", read_local_cache_bytes: " << stats.read_block_cache_bytes
                 << ", read_peer_cache_bytes: " << stats.read_peer_cache_bytes
                 << ", rows_read: " << _app_stats.rows_read << ", error: " << status;
}

Status HdfsScanner::_reopen_without_datacache(RuntimeState* runtime_state) {
    // Close the reader first, it holds raw pointers to the file and streams.
    do_close(runtime_state);
    _file.reset();
    _shared_buffered_input_stream.reset();
    // Keep the first stream for its profile counters.
    _bypassed_cache_input_stream = std::move(_cache_input_stream);
    // Bypass all caches of the file, and rebuild the context changed by the first open.
    _scanner_ctx->datacache_options.enable_datacache = false;
    _scanner_ctx->format_scan_context.options.use_file_metacache = false;
    _scanner_ctx->format_scan_context.options.use_file_pagecache = false;
    RETURN_IF_ERROR(_build_scanner_context());
    g_datacache_corruption_retried << 1;
    LOG(WARNING) << "read file again bypassing datacache: " << _scanner_ctx->file_path;
    return do_open(runtime_state);
}

void HdfsScanner::do_update_iceberg_v2_counter(RuntimeProfile* parent_profile, const std::string& parent_name) {
    const std::string ICEBERG_TIMER = "IcebergV2FormatTimer";
    ADD_CHILD_COUNTER(parent_profile, ICEBERG_TIMER, TUnit::NONE, parent_name);

    RuntimeProfile::Counter* delete_build_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeleteFileBuildTime", TUnit::TIME_NS, ICEBERG_TIMER);
    RuntimeProfile::Counter* delete_file_build_filter_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeleteFileBuildFilterTime", TUnit::TIME_NS, ICEBERG_TIMER);
    RuntimeProfile::Counter* delete_file_per_scan_counter =
            ADD_CHILD_COUNTER(parent_profile, "DeleteFilesPerScan", TUnit::UNIT, ICEBERG_TIMER);

    COUNTER_UPDATE(delete_build_timer, _app_stats.iceberg_delete_file_build_ns);
    COUNTER_UPDATE(delete_file_build_filter_timer, _app_stats.build_rowid_filter_ns);
    COUNTER_UPDATE(delete_file_per_scan_counter, _app_stats.iceberg_delete_files_per_scan);
}

void HdfsScanner::do_update_deletion_vector_build_counter(RuntimeProfile* parent_profile) {
    if (_app_stats.deletion_vector_build_count == 0) {
        return;
    }
    const std::string DV_TIMER = formats::DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);

    RuntimeProfile::Counter* delete_build_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildTime", TUnit::TIME_NS, DV_TIMER);

    RuntimeProfile::Counter* delete_file_per_scan_counter =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildCount", TUnit::UNIT, DV_TIMER);

    COUNTER_UPDATE(delete_build_timer, _app_stats.deletion_vector_build_ns);

    COUNTER_UPDATE(delete_file_per_scan_counter, _app_stats.deletion_vector_build_count);
}

void HdfsScanner::do_update_deletion_vector_filter_counter(RuntimeProfile* parent_profile) {
    const std::string DV_TIMER = formats::DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);

    RuntimeProfile::Counter* delete_file_build_filter_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildRowIdFilterTime", TUnit::TIME_NS, DV_TIMER);
    COUNTER_UPDATE(delete_file_build_filter_timer, _app_stats.build_rowid_filter_ns);
}

int64_t HdfsScanner::estimated_mem_usage() const {
    if (_scanner_ctx != nullptr && _scanner_ctx->format_scan_context.split.estimated_mem_usage_per_split_task != 0) {
        return _scanner_ctx->format_scan_context.split.estimated_mem_usage_per_split_task;
    }
    if (_shared_buffered_input_stream != nullptr) {
        return _shared_buffered_input_stream->estimated_mem_usage();
    }
    // return 0 if we don't know estimated memory usage with high confidence.
    return 0;
}

void HdfsScanner::update_hdfs_counter(HdfsScannerProfile* profile) {
    if (_file == nullptr) return;
    static const char* const kHdfsIOProfileSectionPrefix = "HdfsIOMetrics";

    auto res = _file->get_numeric_statistics();
    if (!res.ok()) return;

    std::unique_ptr<io::NumericStatistics> statistics = std::move(res).value();
    if (statistics == nullptr || statistics->size() == 0) return;

    RuntimeProfile* runtime_profile = profile->runtime_profile;
    ADD_COUNTER(profile->runtime_profile, kHdfsIOProfileSectionPrefix, TUnit::NONE);

    for (int64_t i = 0, sz = statistics->size(); i < sz; i++) {
        auto&& name = statistics->name(i);
        if (name == HdfsReadMetricsKey::kTotalOpenFSTimeNs || name == HdfsReadMetricsKey::kTotalOpenFileTimeNs) {
            auto&& counter = ADD_CHILD_COUNTER(runtime_profile, name, TUnit::TIME_NS, kHdfsIOProfileSectionPrefix);
            COUNTER_UPDATE(counter, statistics->value(i));
        } else if (name == HdfsReadMetricsKey::kTotalBytesRead || name == HdfsReadMetricsKey::kTotalLocalBytesRead ||
                   name == HdfsReadMetricsKey::kTotalShortCircuitBytesRead ||
                   name == HdfsReadMetricsKey::kTotalZeroCopyBytesRead) {
            auto&& counter = ADD_CHILD_COUNTER(runtime_profile, name, TUnit::BYTES, kHdfsIOProfileSectionPrefix);
            COUNTER_UPDATE(counter, statistics->value(i));
        } else if (name == HdfsReadMetricsKey::kTotalHedgedReadOps ||
                   name == HdfsReadMetricsKey::kTotalHedgedReadOpsInCurThread ||
                   name == HdfsReadMetricsKey::kTotalHedgedReadOpsWin) {
            auto&& counter = ADD_CHILD_COUNTER(runtime_profile, name, TUnit::UNIT, kHdfsIOProfileSectionPrefix);
            COUNTER_UPDATE(counter, statistics->value(i));
        }
    }
}

void HdfsScanner::do_update_counter(HdfsScannerProfile* profile) {}

Status HdfsScanner::reinterpret_status(const Status& st) {
    auto msg = fmt::format("file = {}", _scanner_ctx->file_path);

    Status ret = st;
    // After catching the AWS 404 file not found error and returning it to the FE,
    // the FE will refresh the file information of table and re-execute the SQL operation.
    if (st.is_io_error() && st.message().find("404") != std::string_view::npos) {
        ret = Status::RemoteFileNotFound(st.message());
    }

    return ret.clone_and_append(msg);
}

void HdfsScanner::update_counter() {
    HdfsScannerProfile* profile = &_scanner_ctx->profile;
    if (profile->runtime_profile == nullptr) return;

    update_hdfs_counter(profile);

    COUNTER_UPDATE(profile->reader_init_timer, _app_stats.reader_init_ns);
    COUNTER_UPDATE(profile->raw_rows_read_counter, _app_stats.raw_rows_read);
    COUNTER_UPDATE(profile->rows_read_counter, _app_stats.rows_read);
    COUNTER_UPDATE(profile->late_materialize_skip_rows_counter, _app_stats.late_materialize_skip_rows);
    COUNTER_UPDATE(profile->parquet_lazy_col_skip_rows_counter, _app_stats.parquet_lazy_col_skip_rows);
    COUNTER_UPDATE(profile->parquet_lazy_slot_triggered_counter, _app_stats.parquet_lazy_slot_triggered);
    COUNTER_UPDATE(profile->parquet_lazy_read_count_counter, _app_stats.parquet_lazy_read_count);
    COUNTER_UPDATE(profile->parquet_lazy_read_timer, _app_stats.parquet_lazy_read_ns);
    COUNTER_UPDATE(profile->parquet_lazy_full_trigger_count_counter, _app_stats.parquet_lazy_full_trigger_count);
    COUNTER_UPDATE(profile->expr_filter_timer, _app_stats.expr_filter_ns);
    COUNTER_UPDATE(profile->column_read_timer, _app_stats.column_read_ns);
    COUNTER_UPDATE(profile->column_convert_timer, _app_stats.column_convert_ns);

    DataCacheHitRateCounter::instance()->update_page_cache_stat(_app_stats.page_cache_read_counter,
                                                                _app_stats.page_read_counter);

    // Report the datacache counters of the first read if the file is read again.
    const auto& cache_input_stream = _cache_input_stream ? _cache_input_stream : _bypassed_cache_input_stream;
    if (cache_input_stream) {
        const CacheInputStream::Stats& stats = cache_input_stream->stats();
        COUNTER_UPDATE(profile->datacache_read_counter, stats.read_block_cache_count);
        COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_block_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_timer, stats.read_block_cache_ns);
        COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_error_counter, stats.read_cache_error_count);
        COUNTER_UPDATE(profile->datacache_read_peer_bytes, stats.read_peer_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_peer_counter, stats.read_peer_cache_count);
        COUNTER_UPDATE(profile->datacache_read_peer_timer, stats.read_peer_cache_ns);
        COUNTER_UPDATE(profile->datacache_skip_read_peer_counter, stats.skip_read_peer_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_read_peer_bytes, stats.skip_read_peer_cache_bytes);
        COUNTER_UPDATE(profile->datacache_write_counter, stats.write_block_cache_count);
        COUNTER_UPDATE(profile->datacache_write_bytes, stats.write_block_cache_bytes);
        COUNTER_UPDATE(profile->datacache_write_timer, stats.write_block_cache_ns);
        COUNTER_UPDATE(profile->datacache_write_fail_counter, stats.write_cache_fail_count);
        COUNTER_UPDATE(profile->datacache_write_fail_bytes, stats.write_cache_fail_bytes);
        COUNTER_UPDATE(profile->datacache_skip_write_counter, stats.skip_write_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_write_bytes, stats.skip_write_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_block_buffer_counter, stats.read_block_buffer_count);
        COUNTER_UPDATE(profile->datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);

        if (_scanner_ctx->datacache_options.enable_cache_select) {
            // For cache select, we will update load datacache metrics
            _runtime_state->update_num_datacache_read_bytes(stats.read_block_cache_bytes);
            _runtime_state->update_num_datacache_read_time_ns(stats.read_block_cache_ns);
            _runtime_state->update_num_datacache_write_bytes(stats.write_block_cache_bytes);
            _runtime_state->update_num_datacache_write_time_ns(stats.write_block_cache_ns);
            _runtime_state->update_num_datacache_count(1);
        } else {
            // For none cache select sql, we will update DataCache app hit rate
            DataCacheHitRateCounter::instance()->update_block_cache_stat(stats.read_block_cache_bytes,
                                                                         _fs_stats.bytes_read);
        }
    }
    if (_shared_buffered_input_stream) {
        COUNTER_UPDATE(profile->shared_buffered_shared_io_count, _shared_buffered_input_stream->shared_io_count());
        COUNTER_UPDATE(profile->shared_buffered_shared_io_bytes, _shared_buffered_input_stream->shared_io_bytes());
        COUNTER_UPDATE(profile->shared_buffered_shared_align_io_bytes,
                       _shared_buffered_input_stream->shared_align_io_bytes());
        COUNTER_UPDATE(profile->shared_buffered_shared_io_timer, _shared_buffered_input_stream->shared_io_timer());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_count, _shared_buffered_input_stream->direct_io_count());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_bytes, _shared_buffered_input_stream->direct_io_bytes());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_timer, _shared_buffered_input_stream->direct_io_timer());
    }

    {
        COUNTER_UPDATE(profile->app_io_timer, _app_stats.io_ns);
        COUNTER_UPDATE(profile->app_io_counter, _app_stats.io_count);
        COUNTER_UPDATE(profile->app_io_bytes_read_counter, _app_stats.bytes_read);
        COUNTER_UPDATE(profile->fs_bytes_read_counter, _fs_stats.bytes_read);
        COUNTER_UPDATE(profile->fs_io_timer, _fs_stats.io_ns);
        COUNTER_UPDATE(profile->fs_io_counter, _fs_stats.io_count);
    }

    // update scanner private profile.
    do_update_counter(profile);
}

void HdfsScanner::move_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    if (_scanner_ctx == nullptr) return;
    size_t max_split_size = 0;
    for (auto& t : _scanner_ctx->format_scan_context.split.split_tasks) {
        size_t size = (t->end_offset - t->start_offset);
        max_split_size = std::max(max_split_size, size);
        split_tasks->emplace_back(std::move(t));
    }
    if (split_tasks->size() > 0) {
        _scanner_ctx->format_scan_context.split.estimated_mem_usage_per_split_task = 3 * max_split_size / 2;
    }
}

CompressionTypePB HdfsScanner::get_compression_type_from_path(const std::string& filename) {
    ssize_t end = filename.size() - 1;
    while (end >= 0 && filename[end] != '.' && filename[end] != '/') end--;
    if (end == -1 || filename[end] == '/') return NO_COMPRESSION;
    const std::string& ext = filename.substr(end + 1);
    return CompressionUtils::to_compression_pb(ext);
}

} // namespace starrocks
