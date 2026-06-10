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

#include "formats/parquet/column_materializer.h"

#include <fmt/format.h>

#include <algorithm>

#include "column/chunk.h"
#include "common/config.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"

namespace starrocks::parquet {

void ColumnMaterializer::clear_classification() {
    _active_column_indices.clear();
    _lazy_column_indices.clear();
    _active_slot_ids.clear();
    _lazy_slot_ids.clear();
    _dict_column_indices.clear();
    _dict_column_sub_field_paths.clear();
    _post_read_conjuncts_by_slot.clear();
    _column_read_order_ctx.reset();
}

void ColumnMaterializer::add_active_column(int col_idx) {
    _active_column_indices.push_back(col_idx);
    _active_slot_ids.push_back(_param.read_cols[col_idx].slot_id());
}

void ColumnMaterializer::add_lazy_column(int col_idx) {
    _lazy_column_indices.push_back(col_idx);
    _lazy_slot_ids.push_back(_param.read_cols[col_idx].slot_id());
}

void ColumnMaterializer::promote_lazy_to_active() {
    _active_column_indices.swap(_lazy_column_indices);
    _active_slot_ids.swap(_lazy_slot_ids);
}

void ColumnMaterializer::rebuild_read_order_ctx() {
    std::unordered_map<int, size_t> col_cost;
    size_t all_cost = 0;
    for (int col_idx : _active_column_indices) {
        size_t flat_size = _param.read_cols[col_idx].slot_type().get_flat_size();
        col_cost[col_idx] = flat_size;
        all_cost += flat_size;
    }
    _column_read_order_ctx =
            std::make_unique<ColumnReadOrderCtx>(_active_column_indices, all_cost, std::move(col_cost));
}

void ColumnMaterializer::add_dict_filter_column(int col_idx, std::vector<std::string>& sub_field_path) {
    _dict_column_indices.emplace_back(col_idx);
    if (_dict_column_sub_field_paths.find(col_idx) == _dict_column_sub_field_paths.end()) {
        _dict_column_sub_field_paths.insert({col_idx, std::vector<std::vector<std::string>>({sub_field_path})});
    } else {
        _dict_column_sub_field_paths[col_idx].emplace_back(sub_field_path);
    }
}

void ColumnMaterializer::add_post_read_conjunct(SlotId slot_id, ExprContext* ctx) {
    _post_read_conjuncts_by_slot[slot_id].push_back(ctx);
}

Status ColumnMaterializer::init_read_chunk() {
    std::vector<SlotDescriptor*> read_slots;
    read_slots.reserve(_param.read_cols.size());
    for (const auto& column : _param.read_cols) {
        read_slots.emplace_back(column.slot_desc);
    }
    if (!_param.scanner_ctx->reserved_field_slots.empty()) {
        for (auto* slot : _param.scanner_ctx->reserved_field_slots) {
            read_slots.push_back(slot);
        }
    }
    ASSIGN_OR_RETURN(_read_chunk, ChunkHelper::new_chunk_checked(read_slots, _param.chunk_size));
    return Status::OK();
}

ChunkPtr ColumnMaterializer::create_active_chunk() const {
    return create_read_chunk(_active_slot_ids, true);
}

ChunkPtr ColumnMaterializer::create_lazy_chunk() const {
    return create_read_chunk(_lazy_slot_ids, false);
}

ChunkPtr ColumnMaterializer::create_read_chunk(const std::vector<SlotId>& slot_ids,
                                               bool include_reserved_fields) const {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(slot_ids.size());
    for (SlotId slot_id : slot_ids) {
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    if (include_reserved_fields && !_param.scanner_ctx->reserved_field_slots.empty()) {
        for (const auto* slot : _param.scanner_ctx->reserved_field_slots) {
            ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot->id());
            chunk->append_column(column, slot->id());
        }
    }
    return chunk;
}

Status ColumnMaterializer::read_slot(SlotId slot_id, const Range<uint64_t>& range, const Filter* filter,
                                     ChunkPtr* chunk) {
    RETURN_IF_ERROR((*_column_readers)[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));
    _slot_cache[slot_id] = {(*chunk)->get_column_by_slot_id(slot_id)};
    return Status::OK();
}

Status ColumnMaterializer::read_active_range(const Range<uint64_t>& range, const Filter* filter, ChunkPtr* chunk) {
    return read_range(_active_column_indices, range, filter, chunk);
}

Status ColumnMaterializer::read_lazy_range(const Range<uint64_t>& range, const Filter* filter, ChunkPtr* chunk) {
    return read_range(_lazy_column_indices, range, filter, chunk, true);
}

StatusOr<size_t> ColumnMaterializer::read_active_range_round_by_round(const Range<uint64_t>& range, Filter* filter,
                                                                      ChunkPtr* chunk) {
    DCHECK(_column_read_order_ctx != nullptr);
    const std::vector<int>& read_order = _column_read_order_ctx->get_column_read_order();
    size_t round_cost = 0;
    double first_selectivity = -1;
    DeferOp defer([&]() { _column_read_order_ctx->update_ctx(round_cost, first_selectivity); });
    size_t hit_count = 0;

    if (!_param.scanner_ctx->reserved_field_slots.empty()) {
        for (const auto* slot : _param.scanner_ctx->reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(read_slot(slot_id, range, filter, chunk));
            if (_post_read_conjuncts_by_slot.find(slot_id) != _post_read_conjuncts_by_slot.end()) {
                SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
                std::vector<ExprContext*> ctxs = _post_read_conjuncts_by_slot.at(slot_id);
                ASSIGN_OR_RETURN(hit_count, eval_slot_conjuncts(ctxs, slot_id, chunk, filter));
                if (hit_count == 0) {
                    break;
                }
            }
        }
    }
    for (int col_idx : read_order) {
        auto& column = _param.read_cols[col_idx];
        round_cost += _column_read_order_ctx->get_column_cost(col_idx);
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(read_slot(slot_id, range, filter, chunk));

        if (std::find(_dict_column_indices.begin(), _dict_column_indices.end(), col_idx) !=
            _dict_column_indices.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
            for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
                RETURN_IF_ERROR(filter_dict_column(slot_id, (*chunk)->get_column_by_slot_id(slot_id), filter,
                                                   sub_field_path, 0));
                hit_count = SIMD::count_nonzero(*filter);
                if (hit_count == 0) {
                    return hit_count;
                }
            }
        }

        if (_post_read_conjuncts_by_slot.find(slot_id) != _post_read_conjuncts_by_slot.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            std::vector<ExprContext*> ctxs = _post_read_conjuncts_by_slot.at(slot_id);
            ASSIGN_OR_RETURN(hit_count, eval_slot_conjuncts(ctxs, slot_id, chunk, filter));
            if (hit_count == 0) {
                break;
            }
        }
        first_selectivity = first_selectivity < 0 ? hit_count * 1.0 / filter->size() : first_selectivity;
    }

    return hit_count;
}

Status ColumnMaterializer::rewrite_dict_conjuncts_to_predicate(bool* is_group_filtered) {
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id();
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            if (*is_group_filtered) {
                return Status::OK();
            }
            RETURN_IF_ERROR((*_column_readers)[slot_id]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered,
                                                                                            sub_field_path, 0));
        }
    }

    return Status::OK();
}

Status ColumnMaterializer::filter_dict_column(SlotId slot_id, ColumnPtr& column, Filter* filter,
                                              const std::vector<std::string>& sub_field_path, const size_t& layer) {
    return (*_column_readers)[slot_id]->filter_dict_column(column, filter, sub_field_path, layer);
}

StatusOr<size_t> ColumnMaterializer::eval_slot_conjuncts(const std::vector<ExprContext*>& ctxs, SlotId slot_id,
                                                         ChunkPtr* chunk, Filter* filter) {
    auto temp_chunk = std::make_shared<Chunk>();
    temp_chunk->columns().reserve(1);
    ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_id);
    temp_chunk->append_column(column, slot_id);
    return ExecNode::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter);
}

Status ColumnMaterializer::read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range,
                                      const Filter* filter, ChunkPtr* chunk, bool ignore_reserved_field) {
    if (read_columns.empty() && _param.scanner_ctx->reserved_field_slots.empty()) {
        return Status::OK();
    }
    if (!ignore_reserved_field && !_param.scanner_ctx->reserved_field_slots.empty()) {
        for (const auto& slot : _param.scanner_ctx->reserved_field_slots) {
            RETURN_IF_ERROR(read_slot(slot->id(), range, filter, chunk));
        }
    }

    for (int col_idx : read_columns) {
        const auto& column = _param.read_cols[col_idx];
        RETURN_IF_ERROR(read_slot(column.slot_id(), range, filter, chunk));
    }

    return Status::OK();
}

Status ColumnMaterializer::fill_dst_column(SlotId slot_id, ColumnPtr& dst, ColumnPtr& src) {
    return (*_column_readers)[slot_id]->fill_dst_column(dst, src);
}

void ColumnMaterializer::collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end,
                                           ColumnIOTypeFlags types) {
    for (const auto& index : _active_column_indices) {
        const auto& column = _param.read_cols[index];
        (*_column_readers)[column.slot_id()]->collect_column_io_range(ranges, end, types, true);
    }

    for (const auto& index : _lazy_column_indices) {
        const auto& column = _param.read_cols[index];
        (*_column_readers)[column.slot_id()]->collect_column_io_range(ranges, end, types, false);
    }
}

Status ColumnMaterializer::read_lazy_columns(const Range<uint64_t>& full_range,
                                             const Range<uint64_t>& post_filter_range, const Filter& post_filter,
                                             bool has_filter, ChunkPtr& active_chunk) {
    ChunkPtr lazy_chunk = create_lazy_chunk();
    if (has_filter) {
        RETURN_IF_ERROR(read_lazy_range(post_filter_range, &post_filter, &lazy_chunk));
        lazy_chunk->filter_range(post_filter, 0, post_filter_range.span_size());
    } else {
        RETURN_IF_ERROR(read_lazy_range(full_range, nullptr, &lazy_chunk));
    }
    if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
        return Status::InternalError(fmt::format("Unmatched row count, active_rows={}, lazy_rows={}",
                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
    }
    active_chunk->merge(std::move(*lazy_chunk));
    _lazy_column_needed = true;
    return Status::OK();
}

Status ColumnMaterializer::emit_physical_columns(ChunkPtr& active_chunk, ChunkPtr* dst,
                                                 const std::unordered_set<SlotId>* skip_slots) {
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        if (skip_slots && skip_slots->count(slot_id)) continue;
        RETURN_IF_ERROR(fill_dst_column(slot_id, (*dst)->get_column_by_slot_id(slot_id),
                                        active_chunk->get_column_by_slot_id(slot_id)));
    }
    if (!_param.scanner_ctx->reserved_field_slots.empty()) {
        for (const auto* slot : _param.scanner_ctx->reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(fill_dst_column(slot_id, (*dst)->get_column_by_slot_id(slot_id),
                                            active_chunk->get_column_by_slot_id(slot_id)));
        }
    }
    return Status::OK();
}

void ColumnMaterializer::classify_columns(bool* out_has_reserved_field_filter) {
    *out_has_reserved_field_filter = false;
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    int read_col_idx = 0;
    for (auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        auto it = conjunct_ctxs_by_slot.find(slot_id);
        if (it != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : it->second) {
                std::vector<std::string> sub_field_path;
                if (_try_use_dict_filter(read_col_idx, column, ctx, sub_field_path)) {
                    add_dict_filter_column(read_col_idx, sub_field_path);
                } else {
                    add_post_read_conjunct(slot_id, ctx);
                }
            }
            add_active_column(read_col_idx);
        } else if (config::parquet_late_materialization_enable) {
            add_lazy_column(read_col_idx);
            (*_column_readers)[slot_id]->set_can_lazy_decode(true);
        } else {
            add_active_column(read_col_idx);
        }
        ++read_col_idx;
    }

    if (!_param.scanner_ctx->reserved_field_slots.empty()) {
        for (auto* slot : _param.scanner_ctx->reserved_field_slots) {
            SlotId slot_id = slot->id();
            auto it = conjunct_ctxs_by_slot.find(slot_id);
            if (it != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : it->second) {
                    add_post_read_conjunct(slot_id, ctx);
                }
                *out_has_reserved_field_filter = true;
            }
        }
    }

    rebuild_read_order_ctx();
}

bool ColumnMaterializer::_try_use_dict_filter(int col_idx, const GroupReaderParam::Column& column, ExprContext* ctx,
                                              std::vector<std::string>& sub_field_path) {
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
    return (*_column_readers)[column.slot_id()]->try_to_use_dict_filter(ctx, column.decode_needed, column.slot_id(),
                                                                        sub_field_path, 0);
}

Status ColumnMaterializer::materialize_slot(SlotId slot_id, const Range<uint64_t>& range, const Filter* filter) {
    if (_slot_cache.find(slot_id) != _slot_cache.end()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(
            (*_column_readers)[slot_id]->read_range(range, filter, _read_chunk->get_column_by_slot_id(slot_id)));
    _slot_cache[slot_id] = {_read_chunk->get_column_by_slot_id(slot_id)};
    return Status::OK();
}

} // namespace starrocks::parquet
