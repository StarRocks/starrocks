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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {
DictMappingExpr::DictMappingExpr(const TExprNode& node) : Expr(node, false) {}

Status DictMappingExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    if (scope != FunctionContext::FunctionStateScope::FRAGMENT_LOCAL || !_open_rewrite) {
        return Status::OK();
    }

    return state->mutable_dict_optimize_parser()->rewrite_expr(context, this, _output_id);
}

StatusOr<ColumnPtr> DictMappingExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    if (ptr == nullptr || ptr->is_empty()) {
        return get_child(1)->evaluate_checked(context, ptr);
    }
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
}

} // namespace starrocks
