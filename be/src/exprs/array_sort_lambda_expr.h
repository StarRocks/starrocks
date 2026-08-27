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

#pragma once

#include <atomic>
#include <unordered_set>

#include "column/nullable_column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
// array_sort_lambda(array, lambda function)
// Sorts array using a custom lambda comparator function

class ArraySortLambdaExpr final : public Expr {
public:
    ArraySortLambdaExpr(const TExprNode& node);

    // for tests
    explicit ArraySortLambdaExpr(TypeDescriptor type);

    Status prepare(RuntimeState* state, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    ArraySortLambdaExpr(const ArraySortLambdaExpr& other)
            : Expr(other),
              _outer_common_exprs(other._outer_common_exprs),
              _initial_required_slots(other._initial_required_slots),
              _comparator_validated(other._comparator_validated.load(std::memory_order_relaxed)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArraySortLambdaExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    std::string debug_string() const override;

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override;

    Status do_for_each_child(const std::function<Status(Expr*)>& callback) override;

private:
    StatusOr<ColumnPtr> evaluate_lambda_expr(ExprContext* context, Chunk* chunk, const Column* data_column);

    Status validate_strict_weak_ordering(ExprContext* context, Chunk* chunk, const ArrayColumn* array_col);

    Status check_lambda_only_depends_on_args(const LambdaFunction* lambda_function);
    // use map to make sure the order of execution
    std::map<SlotId, Expr*> _outer_common_exprs;
    // the slots initially required for lambda function evaluation, excluding lambda arguments,
    // other common expressions can be evaluated based on these slots.
    std::unordered_set<SlotId> _initial_required_slots;
    // Set once the lambda comparator has passed the strict weak ordering validation, so that
    // later chunks skip it. The Expr tree is shared by every pipeline driver of the fragment
    // (a driver only clones its ExprContext), so this is read and written concurrently and
    // must stay a lock-free flag. Only the OK verdict is cached: an error Status owns heap
    // state, and publishing one through a shared member races with a driver that is copying
    // it out at the same time (use-after-free). A failing comparator fails the query anyway,
    // so re-running the cheap sampled validation on every chunk until then costs nothing.
    std::atomic<bool> _comparator_validated{false};
};
} // namespace starrocks
