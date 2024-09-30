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
<<<<<<< HEAD
=======
    // children == 2: DictExpr(string column, string expression) or DictExpr(array column, array column)
    // children == 3: DictExpr(array column, string expression, array expression)
    // do array-expresion first, then string expression
    if (_children.size() == 2) {
        auto target_column = ptr->get_column_by_slot_id(slot_id());
        auto data_column = ColumnHelper::get_data_column(target_column.get());

        if (data_column->is_binary()) {
            DCHECK(dict_func_expr == nullptr);
            return get_child(1)->evaluate_checked(context, ptr);
        } else if (dict_func_expr != nullptr) {
            return dict_func_expr->evaluate_checked(context, ptr);
        } else {
            return Status::InternalError("unreachable path, dict_func_expr shouldn't be nullptr");
        }
    } else if (_children.size() == 3) {
        // array -> string
        ASSIGN_OR_RETURN(auto str, _children[2]->evaluate_checked(context, ptr));
        Chunk cc;
        cc.append_column(str, slot_id());
        if (dict_func_expr != nullptr) {
            return dict_func_expr->evaluate_checked(context, &cc);
        } else {
            return Status::InternalError("unreachable path, array_dict_func_expr shouldn't be nullptr");
        }
    }

    return Status::InternalError(fmt::format("unreachable path, dict children size: {}", _children.size()));
>>>>>>> 4d9cdb7dac ([Enhancement] optimize array_contains/array_position function (#50912))
}

} // namespace starrocks
