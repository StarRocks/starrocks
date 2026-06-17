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

#include "exec/hdfs_scanner/hdfs_scanner.h"

#include <algorithm>

#include "base/compression/compression_utils.h"
#include "base/compression/stream_decompressor.h"
#include "cache/data_cache_hit_rate_counter.hpp"
#include "cache/scan/cache_select_input_stream.hpp"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/config_cache_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "connector/deletion_vector/deletion_vector.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "fs/hdfs/fs_hdfs.h"
#include "io/compressed_input_stream.h"
#include "storage/primitive/predicate_parser.h"

namespace starrocks {

class CountedSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream,
                                        FormatScannerStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

    ~CountedSeekableInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read(data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        _stats->bytes_read += size;
        return _stream->read_at_fully(offset, data, size);
    }

    StatusOr<std::string_view> peek(int64_t count) override {
        auto st = _stream->peek(count);
        return st;
    }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read_at(offset, out, count));
        _stats->bytes_read += nread;
        return nread;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    FormatScannerStats* _stats;
};

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
    ctx.partition_values.clear();
    ctx.extended_values.clear();
    ctx.materialized_columns.clear();
    ctx.partition_columns.clear();
    ctx.extended_columns.clear();
    ctx.reserved_field_slots.clear();
    ctx.conjunct_ctxs_by_slot.clear();
    ctx.can_use_file_record_count = false;
    ctx.is_first_split = false;

    Columns& partition_values = ctx.partition_values;

    // evaluate partition values; ExprContexts are owned by HiveDataSource's
    // ObjectPool, made available via ctx.partition_expr_ctxs.
    for (size_t i = 0; i < _scanner_ctx->partition_slots.size(); i++) {
        int part_col_idx = _scanner_ctx->_partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_column, ctx.partition_expr_ctxs[part_col_idx]->evaluate(nullptr));
        DCHECK(partition_value_column->is_constant());
        partition_values.emplace_back(std::move(partition_value_column));
    }

    // evaluate extended column values
    Columns& extended_values = ctx.extended_values;
    for (size_t i = 0; i < _scanner_ctx->extended_col_slots.size(); i++) {
        int extended_col_idx = _scanner_ctx->index_in_extended_columns[i];
        ASSIGN_OR_RETURN(auto extended_value_column, ctx.extended_col_expr_ctxs[extended_col_idx]->evaluate(nullptr));
        DCHECK(extended_value_column->is_constant());
        extended_values.emplace_back(std::move(extended_value_column));
    }

    // conjunct_ctxs_by_slot is a mutable per-scanner shallow copy of by_slot;
    // update_with_none_existed_slot() erases entries when columns are absent.
    ctx.conjunct_ctxs_by_slot = _scanner_ctx->conjuncts.by_slot;

    // build columns of materialized and partition.
    for (size_t i = 0; i < _scanner_ctx->materialize_slots.size(); i++) {
        auto* slot = _scanner_ctx->materialize_slots[i];
        if (slot->col_name() == ICEBERG_ROW_ID || slot->col_name() == "_row_source_id" ||
            slot->col_name() == "_scan_range_id" || slot->col_name() == ICEBERG_ROW_POSITION ||
            slot->col_name() == ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER) {
            ctx.reserved_field_slots.emplace_back(slot);
        } else {
            FormatColumnInfo column;
            column.slot_desc = slot;
            column.idx_in_chunk = _scanner_ctx->materialize_index_in_chunk[i];
            // A slot must be decoded eagerly if it is an output column OR if it
            // appears in a multi-field conjunct that the reader cannot push down.
            column.decode_needed =
                    slot->is_output_column() || _scanner_ctx->conjuncts.slots_of_multi_field.count(slot->id());
            ctx.materialized_columns.emplace_back(column);
        }
    }

    for (size_t i = 0; i < _scanner_ctx->partition_slots.size(); i++) {
        auto* slot = _scanner_ctx->partition_slots[i];
        FormatColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_ctx->partition_index_in_chunk[i];
        ctx.partition_columns.emplace_back(column);
    }

    for (size_t i = 0; i < _scanner_ctx->extended_col_slots.size(); i++) {
        auto* slot = _scanner_ctx->extended_col_slots[i];
        FormatColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_ctx->extended_col_index_in_chunk[i];
        ctx.extended_columns.emplace_back(column);
    }

    ctx.slot_descs = _scanner_ctx->tuple_desc->slots();
    ctx.timezone = _runtime_state->timezone();
    ctx.stats = &_app_stats;

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_scanner_ctx->conjuncts.all_ctxs;
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
    opts.runtime_state = _runtime_state;
    opts.enable_column_expr_predicate = true;
    opts.is_olap_scan = false;
    opts.pred_tree_params = _runtime_state->fragment_ctx()->pred_tree_params();

    ctx.predicates.conjuncts_manager = std::make_unique<ScanConjunctsManager>(opts);
    RETURN_IF_ERROR(ctx.predicates.conjuncts_manager->parse_conjuncts());
    ctx.predicates.predicate_parser =
            std::make_unique<ConnectorPredicateParser>(&_scanner_ctx->tuple_desc->decoded_slots());
    ASSIGN_OR_RETURN(ctx.predicates.predicate_tree,
                     ctx.predicates.conjuncts_manager->get_predicate_tree(ctx.predicates.predicate_parser.get(),
                                                                          ctx.predicates.predicate_free_pool));
    ctx.predicates.runtime_filter_scan_range_pruner = std::make_unique<RuntimeScanRangePruner>(
            ctx.predicates.predicate_parser.get(), ctx.predicates.conjuncts_manager->unarrived_runtime_filters());

    ctx.update_return_count_columns();
    if (ctx.scan_range->__isset.record_count && ctx.scan_range->delete_files.empty()) {
        ctx.can_use_file_record_count = true;
    }
    if (ctx.scan_range->__isset.is_first_split) {
        ctx.is_first_split = ctx.scan_range->is_first_split;
    }

    ctx.update_min_max_columns();
    return Status::OK();
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_total_running_time);
    RETURN_IF_CANCELLED(_runtime_state);

    if (_scanner_ctx->no_more_chunks) {
        return Status::EndOfFile("");
    }

    // short circuit for ___count___ optimization.
    if (_scanner_ctx->can_use_count_optimization()) {
        int64_t file_record_count = 0;
        if (_scanner_ctx->is_first_split) {
            file_record_count = _scanner_ctx->scan_range->record_count;
        }
        _scanner_ctx->append_or_update_count_column_to_chunk(chunk, file_record_count);
        _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, 1);
        _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, 1);
        _scanner_ctx->no_more_chunks = true;
        _app_stats.rows_read += 1;
        return Status::OK();
    }

    // short circuit for min/max optimization.
    if (_scanner_ctx->can_use_min_max_optimization()) {
        // 3 means we output 3 values: min, max, and null
        const size_t row_count = 3;
        (*chunk)->set_num_rows(row_count);
        _scanner_ctx->append_or_update_min_max_column_to_chunk(chunk, row_count);
        _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, row_count);
        _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, row_count);
        _scanner_ctx->no_more_chunks = true;
        _app_stats.rows_read += row_count;
        return Status::OK();
    }

    RETURN_IF_ERROR(_runtime_state->check_mem_limit("get chunk from scanner"));
    Status status = do_get_next(runtime_state, chunk);
    if (status.ok()) {
        // Simple scanners (Text, Avro, JSON, JNI) cannot push single-slot predicates
        // into their column readers, so the base class applies them here uniformly.
        // ORC and Parquet return true because they handle by-slot evaluation
        // internally via lazy materialisation and dict-filter pipelines.
        if (!scanner_handles_predicate_by_slot_internally()) {
            SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
            Filter chunk_filter;
            RETURN_IF_ERROR(_scanner_ctx->evaluate_on_conjunct_ctxs_by_slot(chunk, &chunk_filter));
        }
        // Multi-slot predicates (e.g. "a + b > 5") evaluated here for formats that
        // cannot handle them internally (Text, Avro, JSON, JNI).  ORC and Parquet
        // evaluate them inside do_get_next() after all columns are materialised and
        // return true from scanner_handles_multi_slot_conjuncts_internally().
        if (!scanner_handles_multi_slot_conjuncts_internally() && !_scanner_ctx->conjuncts.scanner_ctxs.empty()) {
            SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
            RETURN_IF_ERROR(
                    ChunkPredicateEvaluator::eval_conjuncts(_scanner_ctx->conjuncts.scanner_ctxs, (*chunk).get()));
        }
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
    if (_scanner_ctx->can_use_count_optimization() || _scanner_ctx->can_use_min_max_optimization()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(do_open(runtime_state));
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
    if (_scanner_ctx->can_use_count_optimization() || _scanner_ctx->can_use_min_max_optimization()) {
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

StatusOr<std::unique_ptr<RandomAccessFile>> HdfsScanner::create_random_access_file(
        std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<CacheInputStream>& cache_input_stream, const OpenFileOptions& options) {
    ASSIGN_OR_RETURN(std::unique_ptr<RandomAccessFile> raw_file, options.fs->new_random_access_file(options.file_path))
    int64_t file_size = options.file_size;
    if (file_size < 0) {
        ASSIGN_OR_RETURN(file_size, raw_file->stream()->get_size());
    }
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, options.fs_stats);

    shared_buffered_input_stream = std::make_shared<SharedBufferedInputStream>(input_stream, filename, file_size);
    const SharedBufferedInputStream::CoalesceOptions shared_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(shared_options);
    input_stream = shared_buffered_input_stream;

    // input_stream = CacheInputStream(input_stream)
    const DataCacheOptions& datacache_options = options.datacache_options;
    if (datacache_options.enable_datacache) {
        if (datacache_options.enable_cache_select) {
            cache_input_stream = std::make_shared<CacheSelectInputStream>(
                    shared_buffered_input_stream, filename, file_size, datacache_options.modification_time);
        } else {
            cache_input_stream = std::make_shared<CacheInputStream>(shared_buffered_input_stream, filename, file_size,
                                                                    datacache_options.modification_time);
            cache_input_stream->set_enable_populate_cache(datacache_options.enable_populate_datacache);
            cache_input_stream->set_enable_async_populate_mode(datacache_options.enable_datacache_async_populate_mode);
            cache_input_stream->set_enable_cache_io_adaptor(datacache_options.enable_datacache_io_adaptor);
            cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
            input_stream = cache_input_stream;
        }
        cache_input_stream->set_priority(datacache_options.datacache_priority);
        cache_input_stream->set_ttl_seconds(datacache_options.datacache_ttl_seconds);
        shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());
    }

    // if compression
    // input_stream = DecompressInputStream(input_stream)
    if (options.compression_type != CompressionTypePB::NO_COMPRESSION) {
        using DecompressorPtr = std::shared_ptr<StreamDecompressor>;
        ASSIGN_OR_RETURN(auto dec, StreamDecompressor::create_decompressor(options.compression_type));
        auto compressed_input_stream =
                std::make_shared<io::CompressedInputStream>(input_stream, DecompressorPtr(dec.release()));
        input_stream = std::make_shared<io::CompressedSeekableInputStream>(compressed_input_stream);
    }

    // input_stream = CountedInputStream(input_stream)
    // NOTE: make sure `CountedInputStream` is last applied, so io time can be accurately timed.
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, options.app_stats);

    // so wrap function is f(x) = (CountedInputStream (CacheInputStream (DecompressInputStream (CountedInputStream x))))
    auto file = std::make_unique<RandomAccessFile>(input_stream, filename);
    file->set_size(file_size);
    return file;
}

Status HdfsScanner::open_random_access_file() {
    OpenFileOptions options{.fs = _scanner_ctx->fs,
                            .file_path = _scanner_ctx->file_path,
                            .file_size = _scanner_ctx->file_size,
                            .fs_stats = &_fs_stats,
                            .app_stats = &_app_stats,
                            .datacache_options = _scanner_ctx->datacache_options,
                            .compression_type = _compression_type};

    ASSIGN_OR_RETURN(_file, create_random_access_file(_shared_buffered_input_stream, _cache_input_stream, options));
    if (_cache_input_stream) {
        _cache_input_stream->set_peer_cache_node(_scanner_ctx->scan_range->candidate_node);
    }
    return Status::OK();
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
    const std::string DV_TIMER = DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);

    RuntimeProfile::Counter* delete_build_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildTime", TUnit::TIME_NS, DV_TIMER);

    RuntimeProfile::Counter* delete_file_per_scan_counter =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildCount", TUnit::UNIT, DV_TIMER);

    COUNTER_UPDATE(delete_build_timer, _app_stats.deletion_vector_build_ns);

    COUNTER_UPDATE(delete_file_per_scan_counter, _app_stats.deletion_vector_build_count);
}

void HdfsScanner::do_update_deletion_vector_filter_counter(RuntimeProfile* parent_profile) {
    const std::string DV_TIMER = DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);

    RuntimeProfile::Counter* delete_file_build_filter_timer =
            ADD_CHILD_COUNTER(parent_profile, "DeletionVectorBuildRowIdFilterTime", TUnit::TIME_NS, DV_TIMER);
    COUNTER_UPDATE(delete_file_build_filter_timer, _app_stats.build_rowid_filter_ns);
}

int64_t HdfsScanner::estimated_mem_usage() const {
    if (_scanner_ctx != nullptr && _scanner_ctx->split.estimated_mem_usage_per_split_task != 0) {
        return _scanner_ctx->split.estimated_mem_usage_per_split_task;
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
    COUNTER_UPDATE(profile->expr_filter_timer, _app_stats.expr_filter_ns);
    COUNTER_UPDATE(profile->column_read_timer, _app_stats.column_read_ns);
    COUNTER_UPDATE(profile->column_convert_timer, _app_stats.column_convert_ns);

    DataCacheHitRateCounter::instance()->update_page_cache_stat(_app_stats.page_cache_read_counter,
                                                                _app_stats.page_read_counter);

    if (_scanner_ctx->datacache_options.enable_datacache && _cache_input_stream) {
        const CacheInputStream::Stats& stats = _cache_input_stream->stats();
        COUNTER_UPDATE(profile->datacache_read_counter, stats.read_block_cache_count);
        COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_block_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_timer, stats.read_block_cache_ns);
        COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
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
    for (auto& t : _scanner_ctx->split.split_tasks) {
        size_t size = (t->end_offset - t->start_offset);
        max_split_size = std::max(max_split_size, size);
        split_tasks->emplace_back(std::move(t));
    }
    if (split_tasks->size() > 0) {
        _scanner_ctx->split.estimated_mem_usage_per_split_task = 3 * max_split_size / 2;
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
