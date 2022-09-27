// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/hdfs_scanner.h"

#include <boost/algorithm/string.hpp>

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "io/compressed_input_stream.h"
#include "util/compression/stream_compression.h"

namespace starrocks::vectorized {

class CountedSeekableInputStream : public io::SeekableInputStreamWrapper {
public:
    explicit CountedSeekableInputStream(std::shared_ptr<io::SeekableInputStream> stream,
                                        vectorized::HdfsScanStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

    ~CountedSeekableInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read(data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read_at(offset, data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        RETURN_IF_ERROR(_stream->read_at_fully(offset, data, size));
        _stats->bytes_read += size;
        return Status::OK();
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    vectorized::HdfsScanStats* _stats;
};

Status HdfsScanner::init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _runtime_state = runtime_state;
    _scanner_params = scanner_params;

    // which columsn do we need to scan.
    for (const auto& slot : _scanner_params.materialize_slots) {
        _scanner_columns.emplace_back(slot->col_name());
    }

    // No need to clone following conjunct ctxs. It's cloned & released from outside.
    _min_max_conjunct_ctxs = _scanner_params.min_max_conjunct_ctxs;
    _conjunct_ctxs = _scanner_params.conjunct_ctxs;
    _conjunct_ctxs_by_slot = _scanner_params.conjunct_ctxs_by_slot;

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
    ctx.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    ctx.scan_ranges = _scanner_params.scan_ranges;
    ctx.runtime_filter_collector = _scanner_params.runtime_filter_collector;
    ctx.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    ctx.min_max_tuple_desc = _scanner_params.min_max_tuple_desc;
    ctx.case_sensitive = _scanner_params.case_sensitive;
    ctx.timezone = _runtime_state->timezone();
    ctx.stats = &_stats;

    return Status::OK();
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    RETURN_IF_CANCELLED(_runtime_state);
    Status status = do_get_next(runtime_state, chunk);
    if (status.ok()) {
        if (!_conjunct_ctxs.empty()) {
            SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
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
        LOG(INFO) << "open file success: " << _scanner_params.path;
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
    _raw_file.reset(nullptr);
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
    ASSIGN_OR_RETURN(_raw_file, _scanner_params.fs->new_random_access_file(_scanner_params.path))
    auto stream = std::make_shared<CountedSeekableInputStream>(_raw_file->stream(), &_stats);
    std::shared_ptr<io::SeekableInputStream> input_stream = stream;
    if (_compression_type != CompressionTypePB::NO_COMPRESSION) {
        using DecompressorPtr = std::shared_ptr<StreamCompression>;
        std::unique_ptr<StreamCompression> dec;
        RETURN_IF_ERROR(StreamCompression::create_decompressor(_compression_type, &dec));
        auto compressed_input_stream =
                std::make_shared<io::CompressedInputStream>(stream, DecompressorPtr(dec.release()));
        input_stream = std::make_shared<io::CompressedSeekableInputStream>(compressed_input_stream);
    }

    if (_scanner_params.use_block_cache) {
        _cache_input_stream = std::make_shared<io::CacheInputStream>(_raw_file->filename(), input_stream);
        _file = std::make_unique<RandomAccessFile>(_cache_input_stream, _raw_file->filename());
    } else {
        _file = std::make_unique<RandomAccessFile>(input_stream, _raw_file->filename());
    }
    _file->set_size(_scanner_params.file_size);
    return Status::OK();
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
    COUNTER_UPDATE(profile->bytes_read_counter, _stats.bytes_read);
    COUNTER_UPDATE(profile->expr_filter_timer, _stats.expr_filter_ns);
    COUNTER_UPDATE(profile->io_timer, _stats.io_ns);
    COUNTER_UPDATE(profile->io_counter, _stats.io_count);
    COUNTER_UPDATE(profile->column_read_timer, _stats.column_read_ns);
    COUNTER_UPDATE(profile->column_convert_timer, _stats.column_convert_ns);

    if (_scanner_params.use_block_cache) {
        const io::CacheInputStream::Stats& stats = _cache_input_stream->stats();
        COUNTER_UPDATE(profile->block_cache_read_counter, stats.read_cache_count);
        COUNTER_UPDATE(profile->block_cache_read_bytes, stats.read_cache_bytes);
        COUNTER_UPDATE(profile->block_cache_read_timer, stats.read_cache_ns);
        COUNTER_UPDATE(profile->block_cache_write_counter, stats.write_cache_count);
        COUNTER_UPDATE(profile->block_cache_write_bytes, stats.write_cache_bytes);
        COUNTER_UPDATE(profile->block_cache_write_timer, stats.write_cache_ns);
    }
    // update scanner private profile.
    do_update_counter(profile);
}

void HdfsScannerContext::set_columns_from_file(const std::unordered_set<std::string>& names) {
    for (auto& column : materialized_columns) {
        auto col_name = column.formated_col_name(case_sensitive);
        if (names.find(col_name) == names.end()) {
            not_existed_slots.push_back(column.slot_desc);
            SlotId slot_id = column.slot_id;
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot[slot_id]) {
                    conjunct_ctxs_of_non_existed_slots.emplace_back(ctx);
                }
                conjunct_ctxs_by_slot.erase(slot_id);
            }
        }
    }
}

void HdfsScannerContext::update_not_existed_columns_of_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
    if (not_existed_slots.empty() || row_count <= 0) return;

    ChunkPtr& ck = (*chunk);
    for (auto* slot_desc : not_existed_slots) {
        ck->get_column_by_slot_id(slot_desc->id())->append_default(row_count);
    }
}

void HdfsScannerContext::append_not_existed_columns_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
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

void HdfsScannerContext::update_partition_column_of_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
    if (partition_columns.empty() || row_count <= 0) return;

    ChunkPtr& ck = (*chunk);
    for (size_t i = 0; i < partition_columns.size(); i++) {
        SlotDescriptor* slot_desc = partition_columns[i].slot_desc;
        DCHECK(partition_values[i]->is_constant());
        auto* const_column = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(partition_values[i]);
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

void HdfsScannerContext::append_partition_column_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
    if (partition_columns.size() == 0) return;

    ChunkPtr& ck = (*chunk);
    ck->set_num_rows(row_count);

    for (size_t i = 0; i < partition_columns.size(); i++) {
        SlotDescriptor* slot_desc = partition_columns[i].slot_desc;
        DCHECK(partition_values[i]->is_constant());
        auto* const_column = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(partition_values[i]);
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

} // namespace starrocks::vectorized
