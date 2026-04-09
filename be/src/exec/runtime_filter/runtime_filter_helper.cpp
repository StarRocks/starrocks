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

#include "exec/runtime_filter/runtime_filter_helper.h"

#include "column/column.h"
#include "column/column_helper.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "exprs/min_max_predicate.h"
#include "runtime/runtime_filter.h"
#include "types/logical_type_infra.h"

namespace starrocks {

StatusOr<ExprContext*> RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(ObjectPool* pool,
                                                                                      ExprContext* conjunct,
                                                                                      Chunk* chunk) {
    auto left_child = conjunct->root()->get_child(0);
    auto right_child = conjunct->root()->get_child(1);
    // all of the child(1) in expr is in build chunk
    ASSIGN_OR_RETURN(auto res, conjunct->evaluate(right_child, chunk));
    DCHECK_EQ(res->size(), 1);
    ColumnPtr col;
    if (res->is_constant()) {
        col = res;
    } else if (res->is_nullable()) {
        if (res->is_null(0)) {
            col = ColumnHelper::create_const_null_column(1);
        } else {
            auto data_col = down_cast<NullableColumn*>(res->as_mutable_raw_ptr())->data_column();
            col = ConstColumn::create(std::move(data_col), 1);
        }
    } else {
        col = ConstColumn::create(std::move(res), 1);
    }

    auto literal = pool->add(new VectorizedLiteral(std::move(col), right_child->type()));
    auto new_expr = conjunct->root()->clone(pool);
    auto new_left = left_child->clone(pool);
    new_expr->clear_children();
    new_expr->add_child(new_left);
    new_expr->add_child(literal);
    auto expr = pool->add(new ExprContext(new_expr));
    expr->set_build_from_only_in_filter(true);
    return expr;
}

void RuntimeFilterHelper::create_min_max_value_predicate(ObjectPool* pool, SlotId slot_id, LogicalType slot_type,
                                                         const RuntimeFilter* filter, Expr** min_max_predicate) {
    *min_max_predicate = nullptr;
    if (filter == nullptr) return;
    if (filter->get_min_max_filter() == nullptr) return;
    // TODO, if you want to enable it for string, pls adapt for low-cardinality string
    if (slot_type == TYPE_CHAR || slot_type == TYPE_VARCHAR) return;
    auto res = type_dispatch_filter(slot_type, (Expr*)nullptr, MinMaxPredicateBuilder(pool, slot_id, filter));
    *min_max_predicate = res;
}

} // namespace starrocks
