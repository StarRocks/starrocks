// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/hdfs_scanner.h"

#include <hdfs/hdfs.h>

#include <memory>

#include "env/env_hdfs.h"
#include "exec/exec_node.h"
#include "exec/parquet/file_reader.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

Status HdfsScanner::init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _runtime_state = runtime_state;
    _scanner_params = scanner_params;

    // which columsn do we need to scan.
    for (const auto& slot : _scanner_params.materialize_slots) {
        _scanner_columns.emplace_back(slot->col_name());
    }

    // general conjuncts.
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_scanner_params.conjunct_ctxs, runtime_state, &_conjunct_ctxs));

    // slot id conjuncts.
    const auto& conjunct_ctxs_by_slot = _scanner_params.conjunct_ctxs_by_slot;
    if (!conjunct_ctxs_by_slot.empty()) {
        for (auto iter = conjunct_ctxs_by_slot.begin(); iter != conjunct_ctxs_by_slot.end(); ++iter) {
            SlotId slot_id = iter->first;
            _conjunct_ctxs_by_slot.insert({slot_id, std::vector<ExprContext*>()});
            RETURN_IF_ERROR(Expr::clone_if_not_exists(iter->second, runtime_state, &_conjunct_ctxs_by_slot[slot_id]));
        }
    }

    // min/max conjuncts.
    RETURN_IF_ERROR(
            Expr::clone_if_not_exists(_scanner_params.min_max_conjunct_ctxs, runtime_state, &_min_max_conjunct_ctxs));

    Status status = do_init(runtime_state, scanner_params);
    return status;
}

void HdfsScanner::_build_file_read_param() {
    HdfsFileReaderParam& param = _file_read_param;
    std::vector<ColumnPtr>& partition_values = param.partition_values;

    // evaluate partition values.
    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        int part_col_idx = _scanner_params._partition_index_in_hdfs_partition_columns[i];
        auto partition_value_column = _scanner_params.partition_values[part_col_idx]->evaluate(nullptr);
        DCHECK(partition_value_column->is_constant());
        partition_values.emplace_back(std::move(partition_value_column));
    }

    // build columns of materialized and partition.
    for (size_t i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        auto* slot = _scanner_params.materialize_slots[i];

        HdfsFileReaderParam::ColumnInfo column;
        column.slot_desc = slot;
        column.col_idx = _scanner_params.materialize_index_in_chunk[i];
        column.col_type = slot->type();
        column.slot_id = slot->id();
        column.col_name = slot->col_name();

        param.materialized_columns.emplace_back(std::move(column));
    }

    for (size_t i = 0; i < _scanner_params.partition_slots.size(); i++) {
        auto* slot = _scanner_params.partition_slots[i];
        HdfsFileReaderParam::ColumnInfo column;
        column.slot_desc = slot;
        column.col_idx = _scanner_params.partition_index_in_chunk[i];
        column.col_type = slot->type();
        column.slot_id = slot->id();
        column.col_name = slot->col_name();

        param.partition_columns.emplace_back(std::move(column));
    }

    param.tuple_desc = _scanner_params.tuple_desc;
    param.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    param.scan_ranges = _scanner_params.scan_ranges;
    param.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    param.min_max_tuple_desc = _scanner_params.min_max_tuple_desc;
    param.timezone = _runtime_state->timezone();
    param.stats = &_stats;
}

Status HdfsScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
#ifndef BE_TEST
    SCOPED_TIMER(_scanner_params.parent->_scan_timer);
#endif
    Status status = do_get_next(runtime_state, chunk);
    if (status.ok()) {
        if (!_conjunct_ctxs.empty()) {
            SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
            ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
        }
    } else if (status.is_end_of_file()) {
        // do nothing.
    } else {
        LOG(ERROR) << "failed to read file: " << _scanner_params.fs->file_name();
    }
    return status;
}

Status HdfsScanner::open(RuntimeState* runtime_state) {
    if (_is_open) {
        return Status::OK();
    }
    _build_file_read_param();
    auto status = do_open(runtime_state);
    if (status.ok()) {
        _is_open = true;
        LOG(INFO) << "open file success: " << _scanner_params.fs->file_name();
    }
    return status;
}

Status HdfsScanner::close(RuntimeState* runtime_state) {
    if (_is_closed) {
        return Status::OK();
    }
    Expr::close(_conjunct_ctxs, runtime_state);
    Expr::close(_min_max_conjunct_ctxs, runtime_state);
    for (auto& it : _conjunct_ctxs_by_slot) {
        Expr::close(it.second, runtime_state);
    }
    auto status = do_close(runtime_state);
    if (status.ok()) {
        _is_closed = true;
    }
    return status;
}

#ifndef BE_TEST
struct HdfsReadStats {
    int64_t bytes_total_read = 0;
    int64_t bytes_read_local = 0;
    int64_t bytes_read_short_circuit = 0;
    int64_t bytes_read_dn_cache = 0;
    int64_t bytes_read_remote = 0;
};

static void get_hdfs_statistics(hdfsFile file, HdfsReadStats* stats) {
    struct hdfsReadStatistics* hdfs_stats = nullptr;
    auto res = hdfsFileGetReadStatistics(file, &hdfs_stats);
    if (res == 0) {
        stats->bytes_total_read += hdfs_stats->totalBytesRead;
        stats->bytes_read_local += hdfs_stats->totalLocalBytesRead;
        stats->bytes_read_short_circuit += hdfs_stats->totalShortCircuitBytesRead;
        stats->bytes_read_dn_cache += hdfs_stats->totalZeroCopyBytesRead;
        stats->bytes_read_remote += hdfs_stats->totalBytesRead - hdfs_stats->totalLocalBytesRead;

        hdfsFileFreeReadStatistics(hdfs_stats);
    }
    hdfsFileClearReadStatistics(file);
}
#endif

void HdfsScanner::update_counter() {
#ifndef BE_TEST
    HdfsReadStats hdfs_stats;
    auto hdfs_file = down_cast<HdfsRandomAccessFile*>(_scanner_params.fs.get())->hdfs_file();
    // Hdfslib only supports obtaining statistics of hdfs file system.
    // For other systems such as s3, calling this function will cause be crash.
    if (_scanner_params.parent->_is_hdfs_fs) {
        get_hdfs_statistics(hdfs_file, &hdfs_stats);
    }

    COUNTER_UPDATE(_scanner_params.parent->_bytes_total_read, hdfs_stats.bytes_total_read);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_local, hdfs_stats.bytes_read_local);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_short_circuit, hdfs_stats.bytes_read_short_circuit);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_dn_cache, hdfs_stats.bytes_read_dn_cache);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_remote, hdfs_stats.bytes_read_remote);
#endif
}

Status HdfsParquetScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status HdfsParquetScanner::do_open(RuntimeState* runtime_state) {
    // create file reader
    _reader = std::make_shared<parquet::FileReader>(_scanner_params.fs.get(),
                                                    _scanner_params.scan_ranges[0]->file_length);
#ifndef BE_TEST
    SCOPED_TIMER(_scanner_params.parent->_reader_init_timer);
#endif
    RETURN_IF_ERROR(_reader->init(_file_read_param));
    return Status::OK();
}

Status HdfsParquetScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
#ifndef BE_TEST
    SCOPED_TIMER(_scanner_params.parent->_scan_timer);
#endif
    Status status = _reader->get_next(chunk);
    return status;
}

void HdfsParquetScanner::update_counter() {
    HdfsScanner::update_counter();

#ifndef BE_TEST
    COUNTER_UPDATE(_scanner_params.parent->_raw_rows_counter, _stats.raw_rows_read);
    COUNTER_UPDATE(_scanner_params.parent->_expr_filter_timer, _stats.expr_filter_ns);
    COUNTER_UPDATE(_scanner_params.parent->_io_timer, _stats.io_ns);
    COUNTER_UPDATE(_scanner_params.parent->_io_counter, _stats.io_count);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_from_disk_counter, _stats.bytes_read_from_disk);
    COUNTER_UPDATE(_scanner_params.parent->_column_read_timer, _stats.column_read_ns);
    COUNTER_UPDATE(_scanner_params.parent->_column_convert_timer, _stats.column_convert_ns);
    COUNTER_UPDATE(_scanner_params.parent->_value_decode_timer, _stats.value_decode_ns);
    COUNTER_UPDATE(_scanner_params.parent->_level_decode_timer, _stats.level_decode_ns);
    COUNTER_UPDATE(_scanner_params.parent->_page_read_timer, _stats.page_read_ns);
    COUNTER_UPDATE(_scanner_params.parent->_footer_read_timer, _stats.footer_read_ns);
    COUNTER_UPDATE(_scanner_params.parent->_column_reader_init_timer, _stats.column_reader_init_ns);
    COUNTER_UPDATE(_scanner_params.parent->_group_chunk_read_timer, _stats.group_chunk_read_ns);
    COUNTER_UPDATE(_scanner_params.parent->_group_dict_filter_timer, _stats.group_dict_filter_ns);
    COUNTER_UPDATE(_scanner_params.parent->_group_dict_decode_timer, _stats.group_dict_decode_ns);
#endif
}

void HdfsFileReaderParam::set_columns_from_file(const std::unordered_set<std::string>& names) {
    for (auto& column : materialized_columns) {
        if (names.find(column.col_name) == names.end()) {
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

void HdfsFileReaderParam::append_not_exised_columns_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
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

bool HdfsFileReaderParam::should_skip_by_evaluating_not_existed_slots() {
    if (not_existed_slots.size() == 0) return false;

    // build chunk for evaluation.
    ChunkPtr chunk = std::make_shared<Chunk>();
    append_not_exised_columns_to_chunk(&chunk, 1);
    // do evaluation.
    {
        SCOPED_RAW_TIMER(&stats->expr_filter_ns);
        ExecNode::eval_conjuncts(conjunct_ctxs_of_non_existed_slots, chunk.get());
    }
    return !(chunk->has_rows());
}

void HdfsFileReaderParam::append_partition_column_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count) {
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

bool HdfsFileReaderParam::can_use_dict_filter_on_slot(SlotDescriptor* slot) const {
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
