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

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "utils.h"

namespace starrocks::parquet {

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
}

GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    // If GroupReader is filtered by statistics, it's _has_prepared = false
    if (_has_prepared) {
        if (_lazy_column_needed) {
            _param.lazy_column_coalesce_counter->fetch_add(1, std::memory_order_relaxed);
        } else {
            _param.lazy_column_coalesce_counter->fetch_sub(1, std::memory_order_relaxed);
        }
        _param.stats->group_min_round_cost = _param.stats->group_min_round_cost == 0
                                                     ? _column_read_order_ctx->get_min_round_cost()
                                                     : std::min(_param.stats->group_min_round_cost,
                                                                int64_t(_column_read_order_ctx->get_min_round_cost()));
    }
}

Status GroupReader::init() {
    // Create column readers and bind ParquetField & ColumnChunkMetaData(except complex type) to each ColumnReader
    RETURN_IF_ERROR(_create_column_readers());
    _process_columns_and_conjunct_ctxs();
    _range = SparseRange<uint64_t>(_row_group_first_row, _row_group_first_row + _row_group_metadata->num_rows);
    return Status::OK();
}

Status GroupReader::prepare() {
    RETURN_IF_ERROR(_prepare_column_readers());
    // we need deal with page index first, so that it can work on collect_io_range,
    // and pageindex's io has been collected in FileReader

    if (_range.span_size() != get_row_group_metadata()->num_rows) {
        for (const auto& pair : _column_readers) {
            pair.second->select_offset_index(_range, _row_group_first_row);
        }
    }

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
    if (config::parquet_coalesce_read_enable && _param.sb_stream != nullptr) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);
        int32_t counter = _param.lazy_column_coalesce_counter->load(std::memory_order_relaxed);
        if (counter >= 0 || !config::io_coalesce_adaptive_lazy_active) {
            _param.stats->group_active_lazy_coalesce_together += 1;
        } else {
            _param.stats->group_active_lazy_coalesce_seperately += 1;
        }
        _set_end_offset(end_offset);
        RETURN_IF_ERROR(_param.sb_stream->set_io_ranges(ranges, counter >= 0));
    }

    RETURN_IF_ERROR(_rewrite_conjunct_ctxs_to_predicates(&_is_group_filtered));
    _init_read_chunk();

    if (!_is_group_filtered) {
        _range_iter = _range.new_iterator();
    }

    _has_prepared = true;
    return Status::OK();
}

const tparquet::ColumnChunk* GroupReader::get_chunk_metadata(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_chunk_metadata();
}

ColumnReader* GroupReader::get_column_reader(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second.get();
}

const ParquetField* GroupReader::get_column_parquet_field(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_column_parquet_field();
}

const tparquet::RowGroup* GroupReader::get_row_group_metadata() const {
    return _row_group_metadata;
}

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
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
        if (nullptr != _skip_rows_ctx && _skip_rows_ctx->has_skip_rows()) {
            {
                SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
                ASSIGN_OR_RETURN(has_filter,
                                 _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), chunk_filter));

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

            if (has_filter) {
                Range<uint64_t> lazy_read_range = r.filter(&chunk_filter);
                // if all data is filtered, we have skipped early.
                DCHECK(lazy_read_range.span_size() > 0);
                Filter lazy_filter = {chunk_filter.begin() + lazy_read_range.begin() - r.begin(),
                                      chunk_filter.begin() + lazy_read_range.end() - r.begin()};
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, lazy_read_range, &lazy_filter, &lazy_chunk));
                lazy_chunk->filter_range(lazy_filter, 0, lazy_read_range.span_size());
            } else {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, nullptr, &lazy_chunk));
            }

            if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_chunk));
        }

        *row_count = active_chunk->num_rows();

        SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
        // convert from _read_chunk to chunk.
        RETURN_IF_ERROR(_fill_dst_chunk(active_chunk, chunk));
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
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));
    }

    return Status::OK();
}

StatusOr<size_t> GroupReader::_read_range_round_by_round(const Range<uint64_t>& range, Filter* filter,
                                                         ChunkPtr* chunk) {
    const std::vector<int>& read_order = _column_read_order_ctx->get_column_read_order();
    size_t round_cost = 0;
    double first_selectivity = -1;
    DeferOp defer([&]() { _column_read_order_ctx->update_ctx(round_cost, first_selectivity); });
    size_t hit_count = 0;
    for (int col_idx : read_order) {
        auto& column = _param.read_cols[col_idx];
        round_cost += _column_read_order_ctx->get_column_cost(col_idx);
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));

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
        first_selectivity = first_selectivity < 0 ? hit_count * 1.0 / filter->size() : first_selectivity;
    }

    return hit_count;
}

Status GroupReader::_create_column_readers() {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    // ColumnReaderOptions is used by all column readers in one row group
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.file_meta_data = _param.file_metadata;
    opts.timezone = _param.timezone;
    opts.case_sensitive = _param.case_sensitive;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    for (const auto& column : _param.read_cols) {
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

    // create for partition values
    if (_param.partition_columns != nullptr && _param.partition_values != nullptr) {
        for (size_t i = 0; i < _param.partition_columns->size(); i++) {
            const auto& column = (*_param.partition_columns)[i];
            const auto* slot_desc = column.slot_desc;
            const auto value = (*_param.partition_values)[i];
            _column_readers.emplace(slot_desc->id(), std::make_unique<FixedValueColumnReader>(value->get(0)));
        }
    }

    // create for not existed column
    if (_param.not_existed_slots != nullptr) {
        for (size_t i = 0; i < _param.not_existed_slots->size(); i++) {
            const auto* slot = (*_param.not_existed_slots)[i];
            _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(kNullDatum));
        }
    }
    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
        if (column.t_lake_schema_field == nullptr) {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type()));
        } else {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type(),
                                                         column.t_lake_schema_field));
        }
        if (_param.global_dictmaps->contains(column.slot_id())) {
            ASSIGN_OR_RETURN(
                    column_reader,
                    ColumnReaderFactory::create(std::move(column_reader), _param.global_dictmaps->at(column.slot_id()),
                                                column.slot_id(), _row_group_metadata->num_rows));
        }
        if (column_reader == nullptr) {
            // this shouldn't happen but guard
            return Status::InternalError("No valid column reader.");
        }
    }
    return column_reader;
}

Status GroupReader::_prepare_column_readers() const {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    for (const auto& [slot_id, column_reader] : _column_readers) {
        RETURN_IF_ERROR(column_reader->prepare());
        if (column_reader->get_column_parquet_field() != nullptr &&
            column_reader->get_column_parquet_field()->is_complex_type()) {
            // For complex type columns, we need parse def & rep levels.
            // For OptionalColumnReader, by default, we will not parse it's def level for performance. But if
            // column is a complex type, we have to parse def level to calculate nullability.
            column_reader->set_need_parse_levels(true);
        }
    }
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    int read_col_idx = 0;

    for (auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
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
            _column_readers[slot_id]->set_can_lazy_decode(true);
        }
        ++read_col_idx;
    }

    std::unordered_map<int, size_t> col_cost;
    size_t all_cost = 0;
    for (int col_idx : _active_column_indices) {
        size_t flat_size = _param.read_cols[col_idx].slot_type().get_flat_size();
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

    if (_column_readers[column.slot_id()]->try_to_use_dict_filter(ctx, is_decode_needed, column.slot_id(),
                                                                  sub_field_path, 0)) {
        return true;
    } else {
        return false;
    }
}

ChunkPtr GroupReader::_create_read_chunk(const std::vector<int>& column_indices) {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(column_indices.size());
    for (auto col_idx : column_indices) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id();
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    return chunk;
}

void GroupReader::collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                    ColumnIOTypeFlags types) {
    int64_t end = 0;
    // collect io of active column
    for (const auto& index : _active_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, true);
    }

    // collect io of lazy column
    for (const auto& index : _lazy_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, false);
    }
    *end_offset = end;
}

void GroupReader::_init_read_chunk() {
    std::vector<SlotDescriptor*> read_slots;
    for (const auto& column : _param.read_cols) {
        read_slots.emplace_back(column.slot_desc);
    }
    size_t chunk_size = _param.chunk_size;
    _read_chunk = ChunkHelper::new_chunk(read_slots, chunk_size);
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
        SlotId slot_id = column.slot_id();
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            if (*is_group_filtered) {
                return Status::OK();
            }
            RETURN_IF_ERROR(
                    _column_readers[slot_id]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path, 0));
        }
    }

    return Status::OK();
}

StatusOr<bool> GroupReader::_filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter) {
    if (_dict_column_indices.size() == 0) {
        return false;
    }
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id();
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            RETURN_IF_ERROR(_column_readers[slot_id]->filter_dict_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                         filter, sub_field_path, 0));
        }
    }
    return true;
}

Status GroupReader::_fill_dst_chunk(ChunkPtr& read_chunk, ChunkPtr* chunk) {
    read_chunk->check_or_die();
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                  read_chunk->get_column_by_slot_id(slot_id)));
    }
    read_chunk->check_or_die();
    return Status::OK();
}

} // namespace starrocks::parquet
