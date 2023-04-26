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

#include "runtime/chunk_cursor.h"

#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/sort_exec_exprs.h"
#include "exprs/expr_context.h"

namespace starrocks {

ChunkCursor::ChunkCursor(ChunkSupplier chunk_supplier, ChunkProbeSupplier chunk_probe_supplier,
                         ChunkHasSupplier chunk_has_supplier, const std::vector<ExprContext*>* exprs,
                         const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first, bool is_pipeline)
        : _chunk_supplier(std::move(chunk_supplier)),
          _chunk_probe_supplier(std::move(chunk_probe_supplier)),
          _chunk_has_supplier(std::move(chunk_has_supplier)),
          _current_pos(-1),
          _sort_exprs(exprs),
          _is_pipeline(is_pipeline) {
    DCHECK_EQ(_sort_exprs->size(), is_asc->size());
    DCHECK_EQ(is_asc->size(), is_null_first->size());

    size_t col_num = is_asc->size();
    _sort_order_flag.resize(col_num);
    _null_first_flag.resize(col_num);
    for (size_t i = 0; i < col_num; ++i) {
        _sort_order_flag[i] = (*is_asc)[i] ? 1 : -1;
        if ((*is_asc)[i]) {
            _null_first_flag[i] = (*is_null_first)[i] ? -1 : 1;
        } else {
            _null_first_flag[i] = (*is_null_first)[i] ? 1 : -1;
        }
    }

    if (!_is_pipeline) {
        _reset_with_next_chunk();
    }
}

ChunkCursor::~ChunkCursor() = default;

bool ChunkCursor::operator<(const ChunkCursor& cursor) const {
    DCHECK_EQ(_current_order_by_columns.size(), cursor._current_order_by_columns.size());
    // both cursors must be pointing to valid data.
    DCHECK(_current_pos >= 0 && _current_chunk != nullptr);
    DCHECK(cursor._current_pos >= 0 && cursor._current_chunk != nullptr);
    const size_t number_of_order_by_columns = _current_order_by_columns.size();
    bool is_ahead = true;
    for (size_t col_index = 0; col_index < number_of_order_by_columns; ++col_index) {
        const auto& left_col = _current_order_by_columns[col_index];
        const auto& right_col = cursor._current_order_by_columns[col_index];
        int cmp = left_col->compare_at(_current_pos, cursor._current_pos, *right_col, _null_first_flag[col_index]);
        if (cmp != 0) {
            if (_sort_order_flag[col_index] > 0) {
                is_ahead = cmp < 0;
            } else {
                is_ahead = cmp > 0;
            }
            break;
        }
    }
    return is_ahead;
}

bool ChunkCursor::is_valid() const {
    return _current_pos >= 0 && _current_chunk != nullptr;
}

void ChunkCursor::next() {
    if (_current_chunk == nullptr) {
        return;
    }
    ++_current_pos;
    if (_current_pos >= _current_chunk->num_rows()) {
        _reset_with_next_chunk();
        if (_current_chunk != nullptr) {
            ++_current_pos;
        }
    }
}

bool ChunkCursor::has_next() {
    if (_current_chunk == nullptr) {
        return false;
    }

    if ((_current_pos + 1) < _current_chunk->num_rows()) {
        return true;
    }

    return false;
}

void ChunkCursor::next_for_pipeline() {
    if (_current_chunk == nullptr) {
        return;
    }
    ++_current_pos;
    if (_current_pos >= _current_chunk->num_rows()) {
        next_chunk_for_pipeline();
        if (_current_chunk != nullptr) {
            ++_current_pos;
        }
    }
}

ChunkPtr ChunkCursor::clone_empty_chunk(size_t reserved_row_number) const {
    if (_current_chunk == nullptr) {
        return nullptr;
    } else {
        return _current_chunk->clone_empty_with_slot(reserved_row_number);
    }
}

bool ChunkCursor::copy_current_row_to(Chunk* dest) const {
    dest->append(*_current_chunk, _current_pos, 1);
    return true;
}

Status ChunkCursor::chunk_supplier(Chunk** chunk) {
    return _chunk_supplier(chunk);
}

bool ChunkCursor::chunk_probe_supplier(Chunk** chunk) {
    return _chunk_probe_supplier(chunk);
}

bool ChunkCursor::chunk_has_supplier() {
    return _chunk_has_supplier();
}

void ChunkCursor::_reset_with_next_chunk() {
    _current_order_by_columns.clear();
    Chunk* tmp_chunk = nullptr;
    _chunk_supplier(&tmp_chunk);
    _current_chunk.reset(tmp_chunk);
    _current_pos = -1;
    if (_current_chunk == nullptr) {
        return;
    }

    // prepare order by columns
    _current_order_by_columns.reserve(_sort_exprs->size());
    for (ExprContext* expr_ctx : *_sort_exprs) {
        auto col = EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), _current_chunk.get());
        _current_order_by_columns.push_back(std::move(col));
    }
}

void ChunkCursor::next_chunk_for_pipeline() {
    _current_order_by_columns.clear();
    Chunk* tmp_chunk = nullptr;
    _chunk_probe_supplier(&tmp_chunk);
    _current_chunk.reset(tmp_chunk);
    _current_pos = -1;
    if (_current_chunk == nullptr) {
        return;
    }
    DCHECK(!_current_chunk->is_empty());

    // prepare order by columns
    _current_order_by_columns.reserve(_sort_exprs->size());
    for (ExprContext* expr_ctx : *_sort_exprs) {
        auto col = EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), _current_chunk.get());
        _current_order_by_columns.push_back(std::move(col));
    }
}

SimpleChunkSortCursor::SimpleChunkSortCursor(ChunkProvider chunk_provider, const std::vector<ExprContext*>* sort_exprs)
        : _chunk_provider(std::move(chunk_provider)), _sort_exprs(sort_exprs) {}

bool SimpleChunkSortCursor::is_data_ready() {
    if (!_data_ready && !_chunk_provider(nullptr, nullptr)) {
        return false;
    }
    _data_ready = true;
    return true;
}

std::pair<ChunkUniquePtr, Columns> SimpleChunkSortCursor::try_get_next() {
    DCHECK(_data_ready);
    DCHECK(_sort_exprs);

    if (_eos) {
        return {nullptr, Columns{}};
    }
    ChunkUniquePtr chunk = nullptr;
    if (!_chunk_provider(&chunk, &_eos) || !chunk) {
        return {nullptr, Columns{}};
    }
    if (!chunk || chunk->is_empty()) {
        return {nullptr, Columns{}};
    }

    Columns sort_columns;
    for (ExprContext* expr : *_sort_exprs) {
        // TODO: handle the error correctly
        auto column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunk.get());
        sort_columns.push_back(column);
    }
    return {std::move(chunk), std::move(sort_columns)};
}

bool SimpleChunkSortCursor::is_eos() {
    return _eos;
}

} // namespace starrocks
