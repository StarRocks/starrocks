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

#include "exprs/map_expr.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "util/raw_container.h"

namespace starrocks {

StatusOr<ColumnPtr> MapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    if (UNLIKELY(_children.size() % 2 == 1)) {
        return Status::RuntimeError(fmt::format("Map expressions' keys and values should be in pair."));
    }
    const size_t num_pairs = _children.size();
    size_t num_rows = 1;
    // when num_pairs == 0, we should generate right num_rows.
    if (num_pairs == 0 && chunk) {
        num_rows = chunk->num_rows();
    }

    bool all_const = true;
    Columns pairs_columns(num_pairs);
    for (size_t i = 0; i < num_pairs; i++) {
        ASSIGN_OR_RETURN(auto col, _children[i]->evaluate_checked(context, chunk));
        num_rows = std::max(num_rows, col->size());
        all_const &= col->is_constant();
        pairs_columns[i] = std::move(col);
    }

    for (size_t i = 0; i < num_pairs; i++) {
        pairs_columns[i] = ColumnHelper::cast_to_nullable_column(
                ColumnHelper::unfold_const_column(_type.children[i % 2], num_rows, pairs_columns[i]));
    }

    auto key_col = ColumnHelper::create_column(_type.children[0], true);
    auto value_col = ColumnHelper::create_column(_type.children[1], true);
    auto offsets = UInt32Column::create();
    uint32_t curr_offset = 0;
    offsets->append(curr_offset);
    if (num_pairs > 2) {
        for (size_t i = 0; i < num_rows; ++i) {
            auto unique_pairs = 0;
            for (auto id = 0; id < num_pairs; id += 2) {
                bool unique = true;
                // keep the last identical key
                for (auto fid = id + 2; unique && (fid < num_pairs); fid += 2) {
                    if (pairs_columns[id]->equals(i, *pairs_columns[fid], i)) {
                        unique = false;
                    }
                }
                unique_pairs += unique;
                if (unique) {
                    key_col->append(*pairs_columns[id], i, 1);
                    value_col->append(*pairs_columns[id + 1], i, 1);
                }
            }
            curr_offset += unique_pairs;
            offsets->append(curr_offset);
        }
    } else if (num_pairs > 0) { // avoid copying for only one pair
        key_col = pairs_columns[0]->as_mutable_ptr();
        value_col = pairs_columns[1]->as_mutable_ptr();
        for (size_t i = 0; i < num_rows; ++i) {
            curr_offset++;
            offsets->append(curr_offset);
        }
    } else { // {}
        for (size_t i = 0; i < num_rows; ++i) {
            offsets->append(curr_offset);
        }
    }

    auto res = MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets));

    if (all_const) {
        res->assign(num_rows, 0);
    }
    return res;
}

Expr* MapExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::MAP_EXPR, node.node_type);
    return new MapExpr(node);
}

} // namespace starrocks
