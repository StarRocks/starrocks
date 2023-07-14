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

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "io/compressed_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "util/compression/stream_compression.h"

static constexpr int64_t ROW_FORMAT_ESTIMATED_MEMORY_USAGE = 32LL * 1024 * 1024;

namespace starrocks {

class CountedSeekableInputStream : public io::SeekableInputStreamWrapper {
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
        if (st.ok()) {
            _stats->io_count += 1;
            _stats->bytes_read += count;
        }
        return st;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    HdfsScanStats* _stats;
};

bool HdfsScannerParams::is_lazy_materialization_slot(SlotId slot_id) const {
    // if there is no conjuncts, then there is no lazy materialization slot.
    // we have to read up all fields.
    if (conjunct_ctxs_by_slot.size() == 0 && conjunct_ctxs.size() == 0) {
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
    _runtime_state = runtime_state;
    _scanner_params = scanner_params;
    Status status = do_init(runtime_state, scanner_params);
    return status;
}

Status HdfsScanner::_build_scanner_context() {
    HdfsScannerContext& ctx = _scanner_ctx;
    std::vector<ColumnPtr>& partition_values = ctx.partition_values;

    // evaluate partition values.
    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        int part_col_idx = _scanner_params._partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_column,
                         _scanner_params.partition_values[part_col_idx]->evaluate(nullptr));
        DCHECK(partition_value_column->is_constant());
        partition_values.emplace_back(std::move(partition_value_column));
    }

    // build columns of materialized and partition.
    for (size_t i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        auto* slot = _scanner_params.materialize_slots[i];

        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.col_idx = _scanner_params.materialize_index_in_chunk[i];
        column.col_type = slot->type();
        column.slot_id = slot->id();
        column.col_name = slot->col_name();

        ctx.materialized_columns.emplace_back(std::move(column));
    }

    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        auto* slot = _scanner_params.partition_slots[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.col_idx = _scanner_params.partition_index_in_chunk[i];
        column.col_type = slot->type();
        column.slot_id = slot->id();
        column.col_name = slot->col_name();

        ctx.partition_columns.emplace_back(std::move(column));
    }

    ctx.tuple_desc = _scanner_params.tuple_desc;
    ctx.conjunct_ctxs_by_slot = _scanner_params.conjunct_ctxs_by_slot;
    ctx.scan_ranges = _scanner_params.scan_ranges;
    ctx.runtime_filter_collector = _scanner_params.runtime_filter_collector;
    ctx.min_max_conjunct_ctxs = _scanner_params.min_max_conjunct_ctxs;
    ctx.min_max_tuple_desc = _scanner_params.min_max_tuple_desc;
    ctx.hive_column_names = _scanner_params.hive_column_names;
    ctx.case_sensitive = _scanner_params.case_sensitive;
    ctx.can_use_any_column = _scanner_params.can_use_any_column;
    ctx.can_use_min_max_count_opt = _scanner_params.can_use_min_max_count_opt;
    ctx.timezone = _runtime_state->timezone();
    ctx.iceberg_schema = _scanner_params.iceberg_schema;
    ctx.stats = &_stats;
    ctx.lazy_column_coalesce_counter = _scanner_params.lazy_column_coalesce_counter;

    return Status::OK();
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_total_running_time);
    RETURN_IF_CANCELLED(_runtime_state);
    Status status = do_get_next(runtime_state, chunk);
    if (status.ok()) {
        if (!_scanner_params.conjunct_ctxs.empty() && _scanner_params.eval_conjunct_ctxs) {
            SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_scanner_params.conjunct_ctxs, (*chunk).get()));
        }
    } else if (status.is_end_of_file()) {
        // do nothing.
    } else {
        LOG(ERROR) << "failed to read file: " << _scanner_params.path;
    }
    _stats.num_rows_read += (*chunk)->num_rows();
    return status;
}

Status HdfsScanner::open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_total_running_time);
    if (_opened) {
        return Status::OK();
    }
    _build_scanner_context();
    auto status = do_open(runtime_state);
    if (status.ok()) {
        _opened = true;
        if (_scanner_params.open_limit != nullptr) {
            _scanner_params.open_limit->fetch_add(1, std::memory_order_relaxed);
        }
        VLOG_FILE << "open file success: " << _scanner_params.path;
    }
    return status;
}

void HdfsScanner::close(RuntimeState* runtime_state) noexcept {
    DCHECK(!has_pending_token());
    bool expect = false;
    if (!_closed.compare_exchange_strong(expect, true)) return;
    update_counter();
    do_close(runtime_state);
    _file.reset(nullptr);
    if (_opened && _scanner_params.open_limit != nullptr) {
        _scanner_params.open_limit->fetch_sub(1, std::memory_order_relaxed);
    }
}

void HdfsScanner::finalize() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

void HdfsScanner::enter_pending_queue() {
    _pending_queue_sw.start();
}

uint64_t HdfsScanner::exit_pending_queue() {
    return _pending_queue_sw.reset();
}

Status HdfsScanner::open_random_access_file() {
    CHECK(_file == nullptr) << "File has already been opened";
    ASSIGN_OR_RETURN(std::unique_ptr<RandomAccessFile> raw_file,
                     _scanner_params.fs->new_random_access_file(_scanner_params.path))
    const int64_t file_size = _scanner_params.file_size;
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &_fs_stats);

    // if compression
    // input_stream = DecompressInputStream(input_stream)
    if (_compression_type != CompressionTypePB::NO_COMPRESSION) {
        using DecompressorPtr = std::shared_ptr<StreamCompression>;
        std::unique_ptr<StreamCompression> dec;
        RETURN_IF_ERROR(StreamCompression::create_decompressor(_compression_type, &dec));
        auto compressed_input_stream =
                std::make_shared<io::CompressedInputStream>(input_stream, DecompressorPtr(dec.release()));
        input_stream = std::make_shared<io::CompressedSeekableInputStream>(compressed_input_stream);
    }
    // input_stream = SharedBufferedInputStream(input_stream)
    if (_compression_type == CompressionTypePB::NO_COMPRESSION) {
        _shared_buffered_input_stream =
                std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
        io::SharedBufferedInputStream::CoalesceOptions options = {
                .max_dist_size = config::io_coalesce_read_max_distance_size,
                .max_buffer_size = config::io_coalesce_read_max_buffer_size};
        _shared_buffered_input_stream->set_coalesce_options(options);
        input_stream = _shared_buffered_input_stream;

        // input_stream = CacheInputStream(input_stream)
        if (_scanner_params.use_block_cache) {
            _cache_input_stream = std::make_shared<io::CacheInputStream>(input_stream, filename, file_size);
            _cache_input_stream->set_enable_populate_cache(_scanner_params.enable_populate_block_cache);
            _shared_buffered_input_stream->set_align_size(_cache_input_stream->get_align_size());
            input_stream = _cache_input_stream;
        }
    }
    // input_stream = CountedInputStream(input_stream)
    // NOTE: make sure `CountedInputStream` is last applied, so io time can be accurately timed.
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &_stats);

    // so wrap function is f(x) = (CountedInputStream (CacheInputStream (DecompressInputStream (CountedInputStream x))))
    _file = std::make_unique<RandomAccessFile>(input_stream, filename);
    _file->set_size(file_size);
    return Status::OK();
}

int64_t HdfsScanner::estimated_mem_usage() const {
    if (_shared_buffered_input_stream == nullptr) {
        // don't read data in columnar format(such as CSV format), usually in a fixed size.
        return ROW_FORMAT_ESTIMATED_MEMORY_USAGE;
    }
    return _shared_buffered_input_stream->estimated_mem_usage();
}

void HdfsScanner::update_hdfs_counter(HdfsScanProfile* profile) {
    static const char* const kHdfsIOProfileSectionPrefix = "HdfsIO";
    if (_file == nullptr) return;

    auto res = _file->get_numeric_statistics();
    if (!res.ok()) return;

    std::unique_ptr<io::NumericStatistics> statistics = std::move(res).value();
    if (statistics == nullptr || statistics->size() == 0) return;

    RuntimeProfile* runtime_profile = profile->runtime_profile;
    ADD_TIMER(runtime_profile, kHdfsIOProfileSectionPrefix);
    for (int64_t i = 0, sz = statistics->size(); i < sz; i++) {
        auto&& name = statistics->name(i);
        auto&& counter = ADD_CHILD_COUNTER(runtime_profile, name, TUnit::UNIT, kHdfsIOProfileSectionPrefix);
        COUNTER_UPDATE(counter, statistics->value(i));
    }
}

void HdfsScanner::do_update_counter(HdfsScanProfile* profile) {}

void HdfsScanner::update_counter() {
    HdfsScanProfile* profile = _scanner_params.profile;
    if (profile == nullptr) return;

    update_hdfs_counter(profile);

    COUNTER_UPDATE(profile->reader_init_timer, _stats.reader_init_ns);
    COUNTER_UPDATE(profile->rows_read_counter, _stats.raw_rows_read);
    COUNTER_UPDATE(profile->expr_filter_timer, _stats.expr_filter_ns);
    COUNTER_UPDATE(profile->column_read_timer, _stats.column_read_ns);
    COUNTER_UPDATE(profile->column_convert_timer, _stats.column_convert_ns);

    if (_scanner_params.use_block_cache && _cache_input_stream) {
        const io::CacheInputStream::Stats& stats = _cache_input_stream->stats();
        COUNTER_UPDATE(profile->block_cache_read_counter, stats.read_cache_count);
        COUNTER_UPDATE(profile->block_cache_read_bytes, stats.read_cache_bytes);
        COUNTER_UPDATE(profile->block_cache_read_timer, stats.read_cache_ns);
        COUNTER_UPDATE(profile->block_cache_write_counter, stats.write_cache_count);
        COUNTER_UPDATE(profile->block_cache_write_bytes, stats.write_cache_bytes);
        COUNTER_UPDATE(profile->block_cache_write_timer, stats.write_cache_ns);
        COUNTER_UPDATE(profile->block_cache_write_fail_counter, stats.write_cache_fail_count);
        COUNTER_UPDATE(profile->block_cache_write_fail_bytes, stats.write_cache_fail_bytes);
    }
    if (_shared_buffered_input_stream) {
        COUNTER_UPDATE(profile->shared_buffered_shared_io_count, _shared_buffered_input_stream->shared_io_count());
        COUNTER_UPDATE(profile->shared_buffered_shared_io_bytes, _shared_buffered_input_stream->shared_io_bytes());
        COUNTER_UPDATE(profile->shared_buffered_shared_io_timer, _shared_buffered_input_stream->shared_io_timer());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_count, _shared_buffered_input_stream->direct_io_count());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_bytes, _shared_buffered_input_stream->direct_io_bytes());
        COUNTER_UPDATE(profile->shared_buffered_direct_io_timer, _shared_buffered_input_stream->direct_io_timer());
    }

    {
        COUNTER_UPDATE(profile->app_io_timer, _stats.io_ns);
        COUNTER_UPDATE(profile->app_io_counter, _stats.io_count);
        COUNTER_UPDATE(profile->app_io_bytes_read_counter, _stats.bytes_read);
        COUNTER_UPDATE(profile->fs_bytes_read_counter, _fs_stats.bytes_read);
        COUNTER_UPDATE(profile->fs_io_timer, _fs_stats.io_ns);
        COUNTER_UPDATE(profile->fs_io_counter, _fs_stats.io_count);
    }

    // update scanner private profile.
    do_update_counter(profile);
}

void HdfsScannerContext::update_materialized_columns(const std::unordered_set<std::string>& names) {
    std::vector<ColumnInfo> updated_columns;

    for (auto& column : materialized_columns) {
        auto col_name = column.formated_col_name(case_sensitive);
        // if `can_use_any_column`, we can set this column to non-existed column without reading it.
        if (names.find(col_name) == names.end() || can_use_any_column) {
            not_existed_slots.push_back(column.slot_desc);
            SlotId slot_id = column.slot_id;
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot[slot_id]) {
                    conjunct_ctxs_of_non_existed_slots.emplace_back(ctx);
                }
                conjunct_ctxs_by_slot.erase(slot_id);
            }
        } else {
            updated_columns.emplace_back(column);
        }
    }

    materialized_columns.swap(updated_columns);
}

void HdfsScannerContext::update_not_existed_columns_of_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty() || row_count <= 0) return;

    ChunkPtr& ck = (*chunk);
    for (auto* slot_desc : not_existed_slots) {
        ck->get_column_by_slot_id(slot_desc->id())->append_default(row_count);
    }
}

void HdfsScannerContext::append_not_existed_columns_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.size() == 0) return;

    ChunkPtr& ck = (*chunk);
    ck->set_num_rows(row_count);
    for (auto* slot_desc : not_existed_slots) {
        auto col = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        if (row_count > 0) {
            col->append_default(row_count);
        }
        ck->append_column(std::move(col), slot_desc->id());
    }
}

Status HdfsScannerContext::evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter) {
    size_t chunk_size = (*chunk)->num_rows();
    if (conjunct_ctxs_by_slot.size()) {
        filter->assign(chunk_size, 1);
        for (auto& it : conjunct_ctxs_by_slot) {
            ASSIGN_OR_RETURN(chunk_size, ExecNode::eval_conjuncts_into_filter(it.second, chunk->get(), filter));
            if (chunk_size == 0) {
                break;
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
    append_not_existed_columns_to_chunk(&chunk, 1);
    // do evaluation.
    {
        SCOPED_RAW_TIMER(&stats->expr_filter_ns);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(conjunct_ctxs_of_non_existed_slots, chunk.get()));
    }
    return !(chunk->has_rows());
}

void HdfsScannerContext::update_partition_column_of_chunk(ChunkPtr* chunk, size_t row_count) {
    if (partition_columns.empty() || row_count <= 0) return;

    ChunkPtr& ck = (*chunk);
    for (size_t i = 0; i < partition_columns.size(); i++) {
        SlotDescriptor* slot_desc = partition_columns[i].slot_desc;
        DCHECK(partition_values[i]->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_values[i]);
        ColumnPtr data_column = const_column->data_column();
        auto chunk_part_column = ck->get_column_by_slot_id(slot_desc->id());

        if (data_column->is_nullable()) {
            chunk_part_column->append_nulls(1);
        } else {
            chunk_part_column->append(*data_column, 0, 1);
        }
        chunk_part_column->assign(row_count, 0);
    }
}

void HdfsScannerContext::append_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count) {
    if (partition_columns.size() == 0) return;

    ChunkPtr& ck = (*chunk);
    ck->set_num_rows(row_count);

    for (size_t i = 0; i < partition_columns.size(); i++) {
        SlotDescriptor* slot_desc = partition_columns[i].slot_desc;
        DCHECK(partition_values[i]->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_values[i]);
        ColumnPtr data_column = const_column->data_column();
        auto chunk_part_column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());

        if (row_count > 0) {
            if (data_column->is_nullable()) {
                chunk_part_column->append_nulls(1);
            } else {
                chunk_part_column->append(*data_column, 0, 1);
            }
            chunk_part_column->assign(row_count, 0);
        }
        ck->append_column(std::move(chunk_part_column), slot_desc->id());
    }
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

} // namespace starrocks
