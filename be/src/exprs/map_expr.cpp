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

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "util/raw_container.h"

namespace starrocks {

StatusOr<ColumnPtr> MapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    DCHECK_EQ(2, _children.size());
    ASSIGN_OR_RETURN(ColumnPtr key_col, _children[0]->evaluate_checked(context, chunk));
    ASSIGN_OR_RETURN(ColumnPtr value_col, _children[1]->evaluate_checked(context, chunk));
    size_t num_rows = std::max(key_col->size(), value_col->size());
    // No optimization for const column now.
    key_col = ColumnHelper::unfold_const_column(_children[0]->type(), num_rows, key_col);
    value_col = ColumnHelper::unfold_const_column(_children[1]->type(), num_rows, value_col);
    key_col = ColumnHelper::cast_to_nullable_column(key_col);
    value_col = ColumnHelper::cast_to_nullable_column(value_col);
    // fake offsets, just for creating map column.
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(num_rows);
    return std::make_shared<MapColumn>(std::move(key_col), std::move(value_col), std::move(offsets));
}

Expr* MapExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::MAP_EXPR, node.node_type);
    return new MapExpr(node);
}

} // namespace starrocks
