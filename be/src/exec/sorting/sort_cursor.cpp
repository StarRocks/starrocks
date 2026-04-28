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

#include "exec/sorting/sort_cursor.h"

#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/expr_context.h"

namespace starrocks {

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
