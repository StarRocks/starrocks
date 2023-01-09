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

#include "exec/pipeline/set/union_const_source_operator.h"

#include "column/column_helper.h"

namespace starrocks::pipeline {

StatusOr<ChunkPtr> UnionConstSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    DCHECK(0 <= _next_processed_row_index && _next_processed_row_index < _rows_total);

    auto chunk = std::make_shared<Chunk>();

    size_t rows_count = std::min(static_cast<size_t>(state->chunk_size()), _rows_total - _next_processed_row_index);
    size_t columns_count = _dst_slots.size();

    for (size_t col_i = 0; col_i < columns_count; col_i++) {
        const auto* dst_slot = _dst_slots[col_i];

        ColumnPtr dst_column = ColumnHelper::create_column(dst_slot->type(), dst_slot->is_nullable());
        dst_column->reserve(rows_count);

        for (size_t row_i = 0; row_i < rows_count; row_i++) {
            // Each const_expr_list is projected to ONE dest row.
            DCHECK_EQ(_const_expr_lists[_next_processed_row_index + row_i].size(), columns_count);

            ASSIGN_OR_RETURN(ColumnPtr src_column,
                             _const_expr_lists[_next_processed_row_index + row_i][col_i]->evaluate(nullptr));

            RETURN_IF_HAS_ERROR(_const_expr_lists[_next_processed_row_index + row_i]);
            auto cur_row_dst_column =
                    ColumnHelper::move_column(dst_slot->type(), dst_slot->is_nullable(), src_column, 1);
            dst_column->append(*cur_row_dst_column, 0, 1);
        }

        chunk->append_column(std::move(dst_column), dst_slot->id());
    }

    _next_processed_row_index += rows_count;

    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

Status UnionConstSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    for (const vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::prepare(exprs, state));
    }

    for (const vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }

    return Status::OK();
}

void UnionConstSourceOperatorFactory::close(RuntimeState* state) {
    for (const vector<ExprContext*>& exprs : _const_expr_lists) {
        Expr::close(exprs, state);
    }

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
