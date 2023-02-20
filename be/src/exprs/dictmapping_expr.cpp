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

#include "exprs/dictmapping_expr.h"

namespace starrocks {
DictMappingExpr::DictMappingExpr(const TExprNode& node) : Expr(node, false) {}

StatusOr<ColumnPtr> DictMappingExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    // TODO: record rewrite info in ExprContext
    // If dict_func_expr is nullptr, then it means that this DictExpr has not been rewritten.
    // But in some cases we need to evaluate the original expression directly
    // (usually column_expr_predicate).
    if (dict_func_expr == nullptr) {
        return get_child(1)->evaluate_checked(context, ptr);
    } else {
        return dict_func_expr->evaluate_checked(context, ptr);
    }
}

} // namespace starrocks
