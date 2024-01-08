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

#include "formats/parquet/group_reader.h"

#include <memory>
#include <sstream>

#include "column/column_helper.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/page_index_reader.h"
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"
#include "utils.h"

namespace starrocks::parquet {

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, const std::set<int64_t>* need_skip_rowids,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _need_skip_rowids(need_skip_rowids), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
}

Status GroupReader::init() {
    // the calling order matters, do not change unless you know why.
    RETURN_IF_ERROR(_init_column_readers());
    _process_columns_and_conjunct_ctxs();

    return Status::OK();
}

Status GroupReader::prepare() {
    RETURN_IF_ERROR(_rewrite_conjunct_ctxs_to_predicates(&_is_group_filtered));
    _init_read_chunk();
    _range = SparseRange<uint64_t>(_row_group_first_row, _row_group_first_row + _row_group_metadata->num_rows);
    if (config::parquet_page_index_enable) {
        auto page_index_reader = std::make_unique<PageIndexReader>(this, _param.file, _column_readers,
                                                                   _row_group_metadata, _param.min_max_conjunct_ctxs);
        ASSIGN_OR_RETURN(bool flag, page_index_reader->generate_read_range(_range));
        if (flag && !_is_group_filtered) {
            page_index_reader->select_column_offset_index();
        }
    }

    if (!_is_group_filtered) {
        _range_iter = _range.new_iterator();
    }
    return Status::OK();
}

Status GroupReader::_do_get_next(ChunkPtr* chunk, size_t* row_count) {
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    _read_chunk->reset();
    size_t count = *row_count;
    bool has_more_filter = !_left_conjunct_ctxs.empty();
    Status status;

    ChunkPtr active_chunk = _create_read_chunk(_active_column_indices);
    {
        size_t rows_to_skip = _column_reader_opts.context->rows_to_skip;
        _column_reader_opts.context->rows_to_skip = 0;

        SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
        // read data into active_chunk
        _column_reader_opts.context->filter = nullptr;
        status = _read(_active_column_indices, &count, &active_chunk);
        _param.stats->raw_rows_read += count;
        _raw_rows_read += count;
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }

        _column_reader_opts.context->rows_to_skip = rows_to_skip;
    }

    bool has_filter = false;
    int32_t chunk_size = -1;
    Filter chunk_filter(count, 1);
    DCHECK_EQ(active_chunk->num_rows(), count);

    // dict filter chunk
    {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
        ASSIGN_OR_RETURN(has_filter, _filter_chunk_with_dict_filter(&active_chunk, &chunk_filter));
    }

    // row id filter
    if ((nullptr != _need_skip_rowids) && !_need_skip_rowids->empty()) {
        int64_t current_chunk_base_row = _row_group_first_row + _raw_rows_read - count;
        {
            SCOPED_RAW_TIMER(&_param.stats->iceberg_delete_file_build_filter_ns);
            auto start_str = _need_skip_rowids->lower_bound(current_chunk_base_row);
            auto end_str = _need_skip_rowids->upper_bound(current_chunk_base_row + count - 1);
            for (; start_str != end_str; start_str++) {
                chunk_filter[*start_str - current_chunk_base_row] = 0;
                has_filter = true;
            }
        }
    }

    // other filter that not dict
    if (has_more_filter) {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        ASSIGN_OR_RETURN(chunk_size,
                         ExecNode::eval_conjuncts_into_filter(_left_conjunct_ctxs, active_chunk.get(), &chunk_filter));
        has_filter = true;
    }

    if (has_filter) {
        size_t hit_count = chunk_size >= 0 ? chunk_size : SIMD::count_nonzero(chunk_filter.data(), count);
        if (hit_count == 0) {
            active_chunk->set_num_rows(0);
        } else if (hit_count != count) {
            active_chunk->filter_range(chunk_filter, 0, count);
        }
        active_chunk->check_or_die();
    }

    size_t active_rows = active_chunk->num_rows();
    if (active_rows > 0 && !_lazy_column_indices.empty()) {
        _lazy_column_needed = true;
        ChunkPtr lazy_chunk = _create_read_chunk(_lazy_column_indices);
        RETURN_IF_ERROR(_lazy_skip_rows(_lazy_column_indices, lazy_chunk, *row_count));

        SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
        // read data into lazy chunk
        _column_reader_opts.context->filter = has_filter ? &chunk_filter : nullptr;
        Status st = _read(_lazy_column_indices, &count, &lazy_chunk);
        if (!st.ok() && !st.is_end_of_file()) {
            return st;
        }
        if (has_filter) {
            lazy_chunk->filter_range(chunk_filter, 0, lazy_chunk->num_rows());
        }
        if (lazy_chunk->num_rows() != active_rows) {
            return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                             active_rows, lazy_chunk->num_rows()));
        }
        active_chunk->merge(std::move(*lazy_chunk));
    } else if (active_rows == 0) {
        _param.stats->late_materialize_skip_rows += count;
        _column_reader_opts.context->rows_to_skip += count;
        *row_count = 0;
        return status;
    }

    // We don't care about the column order as they will be reordered in HiveDataSource
    _read_chunk->swap_chunk(*active_chunk);
    *row_count = _read_chunk->num_rows();

    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    // convert from _read_chunk to chunk.
    RETURN_IF_ERROR(_fill_dst_chunk(_read_chunk, chunk));

    _column_reader_opts.context->filter = nullptr;
    return status;
}

Status GroupReader::_do_get_next_new(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    _read_chunk->reset();

    ChunkPtr active_chunk = _create_read_chunk(_active_column_indices);
    // to complicity with _do_get_next will break and return even active_row is all filtered.
    // but a better choice is don't return until really have some results.
    while (true) {
        if (!_range_iter.has_more()) {
            *row_count = 0;
            return Status::EndOfFile("");
        }

        auto r = _range_iter.next(*row_count);
        auto count = r.span_size();
        _param.stats->raw_rows_read += count;

        active_chunk->reset();

        bool has_filter = false;
        Filter chunk_filter(count, 1);

        // row id filter
        if ((nullptr != _need_skip_rowids) && !_need_skip_rowids->empty()) {
            {
                SCOPED_RAW_TIMER(&_param.stats->iceberg_delete_file_build_filter_ns);
                auto start_str = _need_skip_rowids->lower_bound(r.begin());
                auto end_str = _need_skip_rowids->upper_bound(r.end() - 1);

                for (; start_str != end_str; start_str++) {
                    chunk_filter[*start_str - r.begin()] = 0;
                    has_filter = true;
                }
                if (SIMD::count_nonzero(chunk_filter.data(), count) == 0) {
                    continue;
                }
            }
        }

        // we really have predicate to run round by round
        if (!_dict_column_indices.empty() || !_left_no_dict_filter_conjuncts_by_slot.empty()) {
            has_filter = true;
            ASSIGN_OR_RETURN(size_t hit_count, _read_range_round_by_round(r, &chunk_filter, &active_chunk));
            if (hit_count == 0) {
                _param.stats->late_materialize_skip_rows += count;
                continue;
            }
            active_chunk->filter_range(chunk_filter, 0, count);
        } else if (has_filter) {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, &chunk_filter, &active_chunk));
            active_chunk->filter_range(chunk_filter, 0, count);
        } else {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, nullptr, &active_chunk));
        }

        // deal with lazy columns
        if (!_lazy_column_indices.empty()) {
            _lazy_column_needed = true;
            ChunkPtr lazy_chunk = _create_read_chunk(_lazy_column_indices);

            SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);

            if (has_filter) {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, &chunk_filter, &lazy_chunk));
                lazy_chunk->filter_range(chunk_filter, 0, count);
            } else {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, nullptr, &lazy_chunk));
            }

            if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_chunk));
        }

        _read_chunk->swap_chunk(*active_chunk);
        *row_count = _read_chunk->num_rows();

        SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
        // convert from _read_chunk to chunk.
        RETURN_IF_ERROR(_fill_dst_chunk(_read_chunk, chunk));
        break;
    }

    return _range_iter.has_more() ? Status::OK() : Status::EndOfFile("");
}

Status GroupReader::_read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range,
                                const Filter* filter, ChunkPtr* chunk) {
    if (read_columns.empty()) {
        return Status::OK();
    }

    for (int col_idx : read_columns) {
        auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        RETURN_IF_ERROR(
                _column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id).get()));
    }

    return Status::OK();
}

StatusOr<size_t> GroupReader::_read_range_round_by_round(const Range<uint64_t>& range, Filter* filter,
                                                         ChunkPtr* chunk) {
    const std::vector<int>& read_order = _column_read_order_ctx->get_column_read_order();
    size_t round_cost = 0;
    DeferOp defer([&]() { _column_read_order_ctx->update_ctx(round_cost); });
    size_t hit_count = 0;
    for (int col_idx : read_order) {
        auto& column = _param.read_cols[col_idx];
        round_cost += _column_read_order_ctx->get_column_cost(col_idx);
        SlotId slot_id = column.slot_id;
        RETURN_IF_ERROR(
                _column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id).get()));

        if (std::find(_dict_column_indices.begin(), _dict_column_indices.end(), col_idx) !=
            _dict_column_indices.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
            for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
                RETURN_IF_ERROR(_column_readers[slot_id]->filter_dict_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                             filter, sub_field_path, 0));
                hit_count = SIMD::count_nonzero(*filter);
                if (hit_count == 0) {
                    return hit_count;
                }
            }
        }

        if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) != _left_no_dict_filter_conjuncts_by_slot.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            std::vector<ExprContext*> ctxs = _left_no_dict_filter_conjuncts_by_slot.at(slot_id);
            auto temp_chunk = std::make_shared<Chunk>();
            temp_chunk->columns().reserve(1);
            ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_id);
            temp_chunk->append_column(column, slot_id);
            ASSIGN_OR_RETURN(hit_count, ExecNode::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter));
            if (hit_count == 0) {
                break;
            }
        }
    }

    return hit_count;
}

void GroupReader::close() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    if (_lazy_column_needed) {
        _param.lazy_column_coalesce_counter->fetch_add(1, std::memory_order_relaxed);
    } else {
        _param.lazy_column_coalesce_counter->fetch_sub(1, std::memory_order_relaxed);
    }
    _param.stats->group_min_round_cost = _param.stats->group_min_round_cost == 0
                                                 ? _column_read_order_ctx->get_min_round_cost()
                                                 : std::min(_param.stats->group_min_round_cost,
                                                            int64_t(_column_read_order_ctx->get_min_round_cost()));
    _column_readers.clear();
}

Status GroupReader::_init_column_readers() {
    // ColumnReaderOptions is used by all column readers in one row group
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.timezone = _param.timezone;
    opts.case_sensitive = _param.case_sensitive;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    opts.context = _obj_pool.add(new ColumnReaderContext);
    for (const auto& column : _param.read_cols) {
        RETURN_IF_ERROR(_create_column_reader(column));
    }
    return Status::OK();
}

Status GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node =
            _param.file_metadata->schema().get_stored_column_by_field_idx(column.field_idx_in_parquet);
    {
        SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
        if (column.t_iceberg_schema_field == nullptr) {
            RETURN_IF_ERROR(
                    ColumnReader::create(_column_reader_opts, schema_node, column.col_type_in_chunk, &column_reader));
        } else {
            RETURN_IF_ERROR(ColumnReader::create(_column_reader_opts, schema_node, column.col_type_in_chunk,
                                                 column.t_iceberg_schema_field, &column_reader));
        }

        if (column.col_type_in_chunk.is_complex_type()) {
            // For complex type columns, we need parse def & rep levels.
            // For OptionalColumnReader, by default, we will not parse it's def level for performance. But if
            // column is a complex type, we have to parse def level to calculate nullability.
            column_reader->set_need_parse_levels(true);
        }
    }
    _column_readers[column.slot_id] = std::move(column_reader);
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    int read_col_idx = 0;

    for (auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id;
        if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                std::vector<std::string> sub_field_path;
                if (_try_to_use_dict_filter(column, ctx, sub_field_path, column.decode_needed)) {
                    _use_as_dict_filter_column(read_col_idx, slot_id, sub_field_path);
                } else {
                    _left_conjunct_ctxs.emplace_back(ctx);
                    // used for struct col, some dict filter conjunct pushed down to leaf some left
                    if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) ==
                        _left_no_dict_filter_conjuncts_by_slot.end()) {
                        _left_no_dict_filter_conjuncts_by_slot.insert({slot_id, std::vector<ExprContext*>{ctx}});
                    } else {
                        _left_no_dict_filter_conjuncts_by_slot[slot_id].emplace_back(ctx);
                    }
                }
            }
            _active_column_indices.emplace_back(read_col_idx);
        } else {
            if (config::parquet_late_materialization_enable) {
                _lazy_column_indices.emplace_back(read_col_idx);
            } else {
                _active_column_indices.emplace_back(read_col_idx);
            }
        }
        ++read_col_idx;
    }

    std::unordered_map<int, size_t> col_cost;
    size_t all_cost = 0;
    for (int col_idx : _active_column_indices) {
        size_t flat_size = _param.read_cols[col_idx].col_type_in_chunk.get_flat_size();
        col_cost.insert({col_idx, flat_size});
        all_cost += flat_size;
    }
    _column_read_order_ctx =
            std::make_unique<ColumnReadOrderCtx>(_active_column_indices, all_cost, std::move(col_cost));

    if (_active_column_indices.empty()) {
        _active_column_indices.swap(_lazy_column_indices);
    }
}

bool GroupReader::_try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                          std::vector<std::string>& sub_field_path, bool is_decode_needed) {
    const Expr* root_expr = ctx->root();
    std::vector<std::vector<std::string>> subfields;
    root_expr->get_subfields(&subfields);

    for (int i = 1; i < subfields.size(); i++) {
        if (subfields[i] != subfields[0]) {
            return false;
        }
    }

    if (subfields.size() != 0) {
        sub_field_path = subfields[0];
    }

    if (_column_readers[column.slot_id]->try_to_use_dict_filter(ctx, is_decode_needed, column.slot_id, sub_field_path,
                                                                0)) {
        return true;
    } else {
        return false;
    }
}

ChunkPtr GroupReader::_create_read_chunk(const std::vector<int>& column_indices) {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(column_indices.size());
    for (auto col_idx : column_indices) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id;
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    return chunk;
}

void GroupReader::collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                    ColumnIOType type) {
    int64_t end = 0;
    // collect io of active column
    for (const auto& index : _active_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id;
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, type, true);
    }

    // collect io of lazy column
    for (const auto& index : _lazy_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id;
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, type, false);
    }
    *end_offset = end;
}

void GroupReader::_init_read_chunk() {
    const auto& slots = _param.tuple_desc->slots();
    std::vector<SlotDescriptor*> read_slots;
    for (const auto& column : _param.read_cols) {
        int chunk_index = column.col_idx_in_chunk;
        read_slots.emplace_back(slots[chunk_index]);
    }
    size_t chunk_size = _param.chunk_size;
    _read_chunk = ChunkHelper::new_chunk(read_slots, chunk_size);
    _init_chunk_dict_column(&_read_chunk);
}

Status GroupReader::_read(const std::vector<int>& read_columns, size_t* row_count, ChunkPtr* chunk) {
    if (read_columns.empty()) {
        *row_count = 0;
        return Status::OK();
    }

    size_t count = *row_count;
    size_t real_count = count;
    for (int col_idx : read_columns) {
        auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        _column_reader_opts.context->next_row = 0;
        count = *row_count;
        Status status = _column_readers[slot_id]->next_batch(&count, (*chunk)->get_column_by_slot_id(slot_id).get());
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }
        real_count = col_idx == read_columns[0] ? count : real_count;
        if (UNLIKELY(real_count != count)) {
            return Status::InternalError(strings::Substitute("Unmatched row count, $0", _param.file->filename()));
        }
    }

    if (count != *row_count) {
        *row_count = count;
        return Status::EndOfFile("");
    }

    *row_count = count;
    return Status::OK();
}

Status GroupReader::_lazy_skip_rows(const std::vector<int>& read_columns, const ChunkPtr& chunk, size_t chunk_size) {
    auto& ctx = _column_reader_opts.context;
    if (ctx->rows_to_skip == 0) {
        return Status::OK();
    }

    size_t rows_to_skip = ctx->rows_to_skip;
    Filter empty_filter(1, 0);
    ctx->filter = &empty_filter;
    for (int col_idx : read_columns) {
        auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        _column_reader_opts.context->next_row = 0;

        ctx->rows_to_skip = rows_to_skip;
        while (ctx->rows_to_skip > 0) {
            size_t to_read = std::min(ctx->rows_to_skip, chunk_size);
            auto temp_column = chunk->get_column_by_slot_id(slot_id)->clone_empty();
            RETURN_IF_ERROR(_column_readers[slot_id]->next_batch(&to_read, temp_column.get()));
        }
    }

    ctx->rows_to_skip = 0;
    return Status::OK();
}

void GroupReader::_use_as_dict_filter_column(int col_idx, SlotId slot_id, std::vector<std::string>& sub_field_path) {
    _dict_column_indices.emplace_back(col_idx);
    if (_dict_column_sub_field_paths.find(col_idx) == _dict_column_sub_field_paths.end()) {
        _dict_column_sub_field_paths.insert({col_idx, std::vector<std::vector<std::string>>({sub_field_path})});
    } else {
        _dict_column_sub_field_paths[col_idx].emplace_back(sub_field_path);
    }
}

Status GroupReader::_rewrite_conjunct_ctxs_to_predicates(bool* is_group_filtered) {
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            RETURN_IF_ERROR(
                    _column_readers[slot_id]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path, 0));
        }
    }

    return Status::OK();
}

void GroupReader::_init_chunk_dict_column(ChunkPtr* chunk) {
    // replace dict filter column
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            _column_readers[slot_id]->init_dict_column((*chunk)->get_column_by_slot_id(slot_id), sub_field_path, 0);
        }
    }
}

StatusOr<bool> GroupReader::_filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter) {
    if (_dict_column_indices.size() == 0) {
        return false;
    }
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            RETURN_IF_ERROR(_column_readers[slot_id]->filter_dict_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                         filter, sub_field_path, 0));
        }
    }
    return true;
}

Status GroupReader::_fill_dst_chunk(const ChunkPtr& read_chunk, ChunkPtr* chunk) {
    read_chunk->check_or_die();
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id;
        RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                  read_chunk->get_column_by_slot_id(slot_id)));
    }
    read_chunk->check_or_die();
    return Status::OK();
}

} // namespace starrocks::parquet
