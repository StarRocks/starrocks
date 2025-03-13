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

#include "exec/hdfs_scanner.h"

#include "cache/block_cache/block_cache_hit_rate_counter.hpp"
#include "column/column_helper.h"
#include "connector/deletion_vector/deletion_vector.h"
#include "exec/exec_node.h"
#include "fs/hdfs/fs_hdfs.h"
#include "io/cache_select_input_stream.hpp"
#include "io/compressed_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "pipeline/fragment_context.h"
#include "runtime/global_dict/parser.h"
#include "storage/predicate_parser.h"
#include "storage/runtime_range_pruner.hpp"
#include "util/compression/compression_utils.h"
#include "util/compression/stream_compression.h"

namespace starrocks {

class CountedSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream, HdfsScanStats* stats)
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
    HdfsScanStats* _stats;
};

bool HdfsScannerParams::is_lazy_materialization_slot(SlotId slot_id) const {
    // if there is no conjuncts, then there is no lazy materialization slot.
    // we have to read up all fields.
    if (conjunct_ctxs_by_slot.size() == 0 && scanner_conjunct_ctxs.size() == 0) {
        return false;
    }
    if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
        return false;
    }
    if (slots_in_conjunct.find(slot_id) != slots_in_conjunct.end()) {
        return false;
    }
    return true;
}

Status HdfsScanner::init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    SCOPED_RAW_TIMER(&_total_running_time);
    _runtime_state = runtime_state;
    _scanner_params = scanner_params;

    RETURN_IF_ERROR(do_init(runtime_state, scanner_params));

    return Status::OK();
}

Status HdfsScanner::_build_scanner_context() {
    HdfsScannerContext& ctx = _scanner_ctx;
    Columns& partition_values = ctx.partition_values;

    // evaluate partition values.
    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        int part_col_idx = _scanner_params._partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_column,
                         _scanner_params.partition_values[part_col_idx]->evaluate(nullptr));
        DCHECK(partition_value_column->is_constant());
        partition_values.emplace_back(std::move(partition_value_column));
    }

    // evaluate extended column values
    Columns& extended_values = ctx.extended_values;
    for (size_t i = 0; i < _scanner_params.extended_col_slots.size(); i++) {
        int extended_col_idx = _scanner_params.index_in_extended_columns[i];
        ASSIGN_OR_RETURN(auto extended_value_column,
                         _scanner_params.extended_col_values[extended_col_idx]->evaluate(nullptr));
        DCHECK(extended_value_column->is_constant());
        extended_values.emplace_back(std::move(extended_value_column));
    }

    ctx.conjunct_ctxs_by_slot = _scanner_params.conjunct_ctxs_by_slot;

    // build columns of materialized and partition.
    for (size_t i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        auto* slot = _scanner_params.materialize_slots[i];

        // if `can_use_any_column`, we can set this column to non-existed column without reading it.
        if (_scanner_params.can_use_any_column) {
            ctx.update_with_none_existed_slot(slot);
        } else {
            HdfsScannerContext::ColumnInfo column;
            column.slot_desc = slot;
            column.idx_in_chunk = _scanner_params.materialize_index_in_chunk[i];
            column.decode_needed =
                    slot->is_output_column() || _scanner_params.slots_of_mutli_slot_conjunct.find(slot->id()) !=
                                                        _scanner_params.slots_of_mutli_slot_conjunct.end();
            ctx.materialized_columns.emplace_back(std::move(column));
        }
    }

    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        auto* slot = _scanner_params.partition_slots[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_params.partition_index_in_chunk[i];
        ctx.partition_columns.emplace_back(std::move(column));
    }

    for (size_t i = 0; i < _scanner_params.extended_col_slots.size(); i++) {
        auto* slot = _scanner_params.extended_col_slots[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = _scanner_params.extended_col_index_in_chunk[i];
        ctx.extended_columns.emplace_back(std::move(column));
    }

    ctx.slot_descs = _scanner_params.tuple_desc->slots();
    ctx.scan_range = _scanner_params.scan_range;
    ctx.runtime_filter_collector = _scanner_params.runtime_filter_collector;
    ctx.min_max_conjunct_ctxs = _scanner_params.min_max_conjunct_ctxs;
    ctx.min_max_tuple_desc = _scanner_params.min_max_tuple_desc;
    ctx.hive_column_names = _scanner_params.hive_column_names;
    ctx.case_sensitive = _scanner_params.case_sensitive;
    ctx.orc_use_column_names = _scanner_params.orc_use_column_names;
    ctx.can_use_any_column = _scanner_params.can_use_any_column;
    ctx.can_use_min_max_count_opt = _scanner_params.can_use_min_max_count_opt;
    ctx.use_file_metacache = _scanner_params.use_file_metacache;
    ctx.timezone = _runtime_state->timezone();
    ctx.iceberg_schema = _scanner_params.iceberg_schema;
    ctx.stats = &_app_stats;
    ctx.lazy_column_coalesce_counter = _scanner_params.lazy_column_coalesce_counter;
    ctx.split_context = _scanner_params.split_context;
    ctx.enable_split_tasks = _scanner_params.enable_split_tasks;
    ctx.connector_max_split_size = _scanner_params.connector_max_split_size;
    ctx.global_dictmaps = _scanner_params.global_dictmaps;
    ctx.parquet_bloom_filter_enable = _scanner_params.parquet_bloom_filter_enable;
    ctx.parquet_page_index_enable = _scanner_params.parquet_page_index_enable;

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_scanner_params.all_conjunct_ctxs;
    opts.tuple_desc = _scanner_params.tuple_desc;
    opts.obj_pool = _runtime_state->obj_pool();
    opts.runtime_filters = _scanner_params.runtime_filter_collector;
    opts.runtime_state = _runtime_state;
    opts.enable_column_expr_predicate = true;
    opts.is_olap_scan = false;
    opts.pred_tree_params = _runtime_state->fragment_ctx()->pred_tree_params();
    ctx.conjuncts_manager = std::make_unique<ScanConjunctsManager>(std::move(opts));
    RETURN_IF_ERROR(ctx.conjuncts_manager->parse_conjuncts());
    auto* predicate_parser =
            opts.obj_pool->add(new ConnectorPredicateParser(&_scanner_params.tuple_desc->decoded_slots()));
    ASSIGN_OR_RETURN(ctx.predicate_tree,
                     ctx.conjuncts_manager->get_predicate_tree(predicate_parser, ctx.predicate_free_pool));
    ctx.rf_scan_range_pruner = opts.obj_pool->add(
            new RuntimeScanRangePruner(predicate_parser, ctx.conjuncts_manager->unarrived_runtime_filters()));

    return Status::OK();
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_total_running_time);
    RETURN_IF_CANCELLED(_runtime_state);
    RETURN_IF_ERROR(_runtime_state->check_mem_limit("get chunk from scanner"));
    Status status = do_get_next(runtime_state, chunk);
    if (status.ok()) {
        if (!_scanner_params.scanner_conjunct_ctxs.empty()) {
            SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_scanner_params.scanner_conjunct_ctxs, (*chunk).get()));
        }
    } else if (status.is_end_of_file()) {
        // do nothing.
    } else {
        LOG(ERROR) << "failed to read file: " << _scanner_params.path;
    }
    _app_stats.rows_read += (*chunk)->num_rows();
    return status;
}

Status HdfsScanner::open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_total_running_time);
    if (_opened) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_scanner_context());
    RETURN_IF_ERROR(do_open(runtime_state));
    _opened = true;
    VLOG_FILE << "open file success: " << _scanner_params.path << ", scan range = ["
              << _scanner_params.scan_range->offset << ","
              << (_scanner_params.scan_range->length + _scanner_params.scan_range->offset) << "]";
    return Status::OK();
}

void HdfsScanner::close() noexcept {
    if (!_runtime_state) {
        return;
    }
    VLOG_FILE << "close file success: " << _scanner_params.path << ", scan range = ["
              << _scanner_params.scan_range->offset << ","
              << (_scanner_params.scan_range->length + _scanner_params.scan_range->offset)
              << "], rows = " << _app_stats.rows_read;

    bool expect = false;
    if (!_closed.compare_exchange_strong(expect, true)) return;
    update_counter();
    do_close(_runtime_state);
    _file.reset(nullptr);
}

StatusOr<std::unique_ptr<RandomAccessFile>> HdfsScanner::create_random_access_file(
        std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<io::CacheInputStream>& cache_input_stream, const OpenFileOptions& options) {
    ASSIGN_OR_RETURN(std::unique_ptr<RandomAccessFile> raw_file, options.fs->new_random_access_file(options.path))
    int64_t file_size = options.file_size;
    if (file_size < 0) {
        ASSIGN_OR_RETURN(file_size, raw_file->stream()->get_size());
    }
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, options.fs_stats);

    shared_buffered_input_stream = std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions shared_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(shared_options);
    input_stream = shared_buffered_input_stream;

    // input_stream = CacheInputStream(input_stream)
    const DataCacheOptions& datacache_options = options.datacache_options;
    if (datacache_options.enable_datacache) {
        if (datacache_options.enable_cache_select) {
            cache_input_stream = std::make_shared<io::CacheSelectInputStream>(
                    shared_buffered_input_stream, filename, file_size, datacache_options.modification_time);
        } else {
            cache_input_stream = std::make_shared<io::CacheInputStream>(shared_buffered_input_stream, filename,
                                                                        file_size, datacache_options.modification_time);
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
        using DecompressorPtr = std::shared_ptr<StreamCompression>;
        std::unique_ptr<StreamCompression> dec;
        RETURN_IF_ERROR(StreamCompression::create_decompressor(options.compression_type, &dec));
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
    OpenFileOptions options{.fs = _scanner_params.fs,
                            .path = _scanner_params.path,
                            .file_size = _scanner_params.file_size,
                            .fs_stats = &_fs_stats,
                            .app_stats = &_app_stats,
                            .datacache_options = _scanner_params.datacache_options,
                            .compression_type = _compression_type};

    ASSIGN_OR_RETURN(_file, create_random_access_file(_shared_buffered_input_stream, _cache_input_stream, options));
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
    if (_scanner_ctx.estimated_mem_usage_per_split_task != 0) {
        return _scanner_ctx.estimated_mem_usage_per_split_task;
    }
    if (_shared_buffered_input_stream != nullptr) {
        return _shared_buffered_input_stream->estimated_mem_usage();
    }
    // return 0 if we don't know estimated memory usage with high confidence.
    return 0;
}

void HdfsScanner::update_hdfs_counter(HdfsScanProfile* profile) {
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

void HdfsScanner::do_update_counter(HdfsScanProfile* profile) {}

Status HdfsScanner::reinterpret_status(const Status& st) {
    auto msg = fmt::format("file = {}", _scanner_params.path);

    Status ret = st;
    // After catching the AWS 404 file not found error and returning it to the FE,
    // the FE will refresh the file information of table and re-execute the SQL operation.
    if (st.is_io_error() && st.message().find("404") != std::string_view::npos) {
        ret = Status::RemoteFileNotFound(st.message());
    }

    return ret.clone_and_append(msg);
}

void HdfsScanner::update_counter() {
    HdfsScanProfile* profile = _scanner_params.profile;
    if (profile == nullptr) return;

    update_hdfs_counter(profile);

    COUNTER_UPDATE(profile->reader_init_timer, _app_stats.reader_init_ns);
    COUNTER_UPDATE(profile->raw_rows_read_counter, _app_stats.raw_rows_read);
    COUNTER_UPDATE(profile->rows_read_counter, _app_stats.rows_read);
    COUNTER_UPDATE(profile->late_materialize_skip_rows_counter, _app_stats.late_materialize_skip_rows);
    COUNTER_UPDATE(profile->expr_filter_timer, _app_stats.expr_filter_ns);
    COUNTER_UPDATE(profile->column_read_timer, _app_stats.column_read_ns);
    COUNTER_UPDATE(profile->column_convert_timer, _app_stats.column_convert_ns);

    if (_scanner_params.datacache_options.enable_datacache && _cache_input_stream) {
        const io::CacheInputStream::Stats& stats = _cache_input_stream->stats();
        COUNTER_UPDATE(profile->datacache_read_counter, stats.read_cache_count);
        COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_timer, stats.read_cache_ns);
        COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
        COUNTER_UPDATE(profile->datacache_write_counter, stats.write_cache_count);
        COUNTER_UPDATE(profile->datacache_write_bytes, stats.write_cache_bytes);
        COUNTER_UPDATE(profile->datacache_write_timer, stats.write_cache_ns);
        COUNTER_UPDATE(profile->datacache_write_fail_counter, stats.write_cache_fail_count);
        COUNTER_UPDATE(profile->datacache_write_fail_bytes, stats.write_cache_fail_bytes);
        COUNTER_UPDATE(profile->datacache_skip_write_counter, stats.skip_write_cache_count);
        COUNTER_UPDATE(profile->datacache_skip_write_bytes, stats.skip_write_cache_bytes);
        COUNTER_UPDATE(profile->datacache_read_block_buffer_counter, stats.read_block_buffer_count);
        COUNTER_UPDATE(profile->datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);

        if (_scanner_params.datacache_options.enable_cache_select) {
            // For cache select, we will update load datacache metrics
            _runtime_state->update_num_datacache_read_bytes(stats.read_cache_bytes);
            _runtime_state->update_num_datacache_read_time_ns(stats.read_cache_ns);
            _runtime_state->update_num_datacache_write_bytes(stats.write_cache_bytes);
            _runtime_state->update_num_datacache_write_time_ns(stats.write_cache_ns);
            _runtime_state->update_num_datacache_count(1);
        } else {
            // For none cache select sql, we will update DataCache app hit rate
            BlockCacheHitRateCounter::instance()->update(stats.read_cache_bytes, _fs_stats.bytes_read);
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

void HdfsScannerContext::update_with_none_existed_slot(SlotDescriptor* slot) {
    not_existed_slots.push_back(slot);
    SlotId slot_id = slot->id();
    if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
        for (ExprContext* expr_ctx : conjunct_ctxs_by_slot[slot_id]) {
            conjunct_ctxs_of_non_existed_slots.emplace_back(expr_ctx);
        }
        conjunct_ctxs_by_slot.erase(slot_id);
    }
}

Status HdfsScannerContext::update_materialized_columns(const std::unordered_set<std::string>& names) {
    std::vector<ColumnInfo> updated_columns;

    // special handling for ___count__ optimization.
    {
        for (auto& column : materialized_columns) {
            if (column.name() == "___count___") {
                return_count_column = true;
                break;
            }
        }

        if (return_count_column && materialized_columns.size() != 1) {
            return Status::InternalError("Plan inconsistency. ___count___ column should be unique.");
        }
    }

    for (auto& column : materialized_columns) {
        auto col_name = column.formatted_name(case_sensitive);
        // if `can_use_any_column`, we can set this column to non-existed column without reading it.
        if (names.find(col_name) == names.end()) {
            update_with_none_existed_slot(column.slot_desc);
        } else {
            updated_columns.emplace_back(column);
        }
    }

    materialized_columns.swap(updated_columns);
    return Status::OK();
}

Status HdfsScannerContext::append_or_update_not_existed_columns_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty()) return Status::OK();
    ChunkPtr& ck = (*chunk);

    // special handling for ___count___ optimization
    {
        for (auto* slot_desc : not_existed_slots) {
            if (slot_desc->col_name() == "___count___") {
                return_count_column = true;
                break;
            }
        }
        if (return_count_column && not_existed_slots.size() != 1) {
            return Status::InternalError("Plan inconsistency. ___count___ column should be unique.");
        }
    }

    if (return_count_column) {
        auto* slot_desc = not_existed_slots[0];
        TypeDescriptor desc;
        desc.type = TYPE_BIGINT;
        auto col = ColumnHelper::create_column(desc, slot_desc->is_nullable());
        col->append_datum(int64_t(1));
        col->assign(row_count, 0);
        ck->append_or_update_column(std::move(col), slot_desc->id());
    } else {
        for (auto* slot_desc : not_existed_slots) {
            auto col = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            if (row_count > 0) {
                col->append_default(row_count);
            }
            ck->append_or_update_column(std::move(col), slot_desc->id());
        }
    }
    ck->set_num_rows(row_count);
    return Status::OK();
}

void HdfsScannerContext::append_or_update_count_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty() || row_count < 0) return;
    ChunkPtr& ck = (*chunk);
    auto* slot_desc = not_existed_slots[0];
    TypeDescriptor desc;
    desc.type = TYPE_BIGINT;
    auto col = ColumnHelper::create_column(desc, slot_desc->is_nullable());
    col->append_datum(int64_t(row_count));
    ck->append_or_update_column(std::move(col), slot_desc->id());
    ck->set_num_rows(1);
}

Status HdfsScannerContext::evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter) {
    size_t chunk_size = (*chunk)->num_rows();
    if (conjunct_ctxs_by_slot.size()) {
        filter->assign(chunk_size, 1);
        for (auto& it : conjunct_ctxs_by_slot) {
            ASSIGN_OR_RETURN(chunk_size, ExecNode::eval_conjuncts_into_filter(it.second, chunk->get(), filter));
            if (chunk_size == 0) {
                (*chunk)->set_num_rows(0);
                return Status::OK();
            }
        }
        if (chunk_size != 0 && chunk_size != (*chunk)->num_rows()) {
            (*chunk)->filter(*filter);
        }
    }
    return Status::OK();
}

StatusOr<bool> HdfsScannerContext::should_skip_by_evaluating_not_existed_slots() {
    if (not_existed_slots.size() == 0) return false;

    // build chunk for evaluation.
    ChunkPtr chunk = std::make_shared<Chunk>();
    RETURN_IF_ERROR(append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    // do evaluation.
    {
        SCOPED_RAW_TIMER(&stats->expr_filter_ns);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(conjunct_ctxs_of_non_existed_slots, chunk.get()));
    }
    return !(chunk->has_rows());
}

void HdfsScannerContext::append_or_update_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    append_or_update_column_to_chunk(chunk, row_count, partition_columns, partition_values);
}

void HdfsScannerContext::append_or_update_extended_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    append_or_update_column_to_chunk(chunk, row_count, extended_columns, extended_values);
}

void HdfsScannerContext::append_or_update_column_to_chunk(ChunkPtr* chunk, size_t row_count,
                                                          const std::vector<ColumnInfo>& columns,
                                                          const Columns& values) {
    if (columns.size() == 0) return;

    ChunkPtr& ck = (*chunk);
    for (size_t i = 0; i < columns.size(); i++) {
        SlotDescriptor* slot_desc = columns[i].slot_desc;
        DCHECK(values[i]->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(values[i]);
        ColumnPtr data_column = const_column->data_column();
        auto chunk_column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());

        if (row_count > 0) {
            if (data_column->is_nullable()) {
                chunk_column->append_nulls(1);
            } else {
                chunk_column->append(*data_column, 0, 1);
            }
            chunk_column->assign(row_count, 0);
        }
        ck->append_or_update_column(std::move(chunk_column), slot_desc->id());
    }
    ck->set_num_rows(row_count);
}

bool HdfsScannerContext::can_use_dict_filter_on_slot(SlotDescriptor* slot) const {
    if (!slot->type().is_string_type()) {
        return false;
    }
    SlotId slot_id = slot->id();
    if (conjunct_ctxs_by_slot.find(slot_id) == conjunct_ctxs_by_slot.end()) {
        return false;
    }
    for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
        const Expr* root_expr = ctx->root();
        if (root_expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            std::string is_null_str;
            if (root_expr->is_null_scalar_function(is_null_str)) {
                return false;
            }
        }
    }
    return true;
}

void HdfsScannerContext::merge_split_tasks() {
    if (split_tasks.size() < 2) return;

    // NOTE: the prerequisites of `split_tasks` are
    // 1. all ranges in it are sorted
    // 2. and none of them is overlapped.
    std::vector<HdfsSplitContextPtr> new_split_tasks;

    auto do_merge = [&](size_t start, size_t end) {
        auto start_ctx = split_tasks[start].get();
        auto end_ctx = split_tasks[end].get();
        auto new_ctx = start_ctx->clone();
        new_ctx->split_start = start_ctx->split_start;
        new_ctx->split_end = end_ctx->split_end;
        new_split_tasks.emplace_back(std::move(new_ctx));
    };

    size_t head = 0;
    for (size_t i = 1; i < split_tasks.size(); i++) {
        bool cut = false;

        auto prev_ctx = split_tasks[i - 1].get();
        auto ctx = split_tasks[i].get();
        auto head_ctx = split_tasks[head].get();

        if ((ctx->split_start != prev_ctx->split_end) ||
            (ctx->split_end - head_ctx->split_start > connector_max_split_size)) {
            cut = true;
        }

        if (cut) {
            do_merge(head, i - 1);
            head = i;
        }
    }
    do_merge(head, split_tasks.size() - 1);

    // handle the tail stripe, if it's small and consecutive, merge it to the last one.
    size_t new_size = new_split_tasks.size();
    if (new_size >= 2) {
        auto tail_ctx = new_split_tasks[new_size - 1].get();
        size_t tail_size = (tail_ctx->split_end - tail_ctx->split_start);
        if ((tail_size * 2) < connector_max_split_size) {
            auto last_ctx = new_split_tasks[new_size - 2].get();
            if (last_ctx->split_end == tail_ctx->split_start) {
                last_ctx->split_end = tail_ctx->split_end;
                new_split_tasks.pop_back();
            }
        }
    }

    split_tasks.swap(new_split_tasks);
}
void HdfsScanner::move_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    size_t max_split_size = 0;
    for (auto& t : _scanner_ctx.split_tasks) {
        size_t size = (t->split_end - t->split_start);
        max_split_size = std::max(max_split_size, size);
        split_tasks->emplace_back(std::move(t));
    }
    if (split_tasks->size() > 0) {
        _scanner_ctx.estimated_mem_usage_per_split_task = 3 * max_split_size / 2;
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
