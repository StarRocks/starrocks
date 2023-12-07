// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

// The children of lambda function include 3 parts: lambda expr, lambda arguments and optimal common sub expressions,
// the layout may be:
// lambda_expr, arg_1, arg_2..., sub_expr_slot_id_1, sub_expr_slot_id_2, ... sub_expr_1, sub_expr_2, ...
// taking  (x,y) -> x*2 + x*2 + y as example, 3 parts are listed below:
//      lambda expr      | lambda argument | common sub expression |
// slot[1] + slot[1] + y |    x, y         |   slot[1], x * 2      |
// Note the common sub expressions should be evaluated in order before the lambda expr.

class LambdaFunction final : public Expr {
public:
    LambdaFunction(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new LambdaFunction(*this)); }

    Status prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    // the slot ids of lambda expression may be originally from the arguments of this lambda function
    // or its parent lambda functions, or captured columns, remove the first one.
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->insert(slot_ids->end(), _captured_slot_ids.begin(), _captured_slot_ids.end());
        return _captured_slot_ids.size();
    }

    int get_lambda_arguments_ids(std::vector<SlotId>* ids) {
        ids->assign(_arguments_ids.begin(), _arguments_ids.end());
        return _arguments_ids.size();
    }

private:
    std::vector<SlotId> _captured_slot_ids;
    std::vector<SlotId> _arguments_ids;
    std::vector<SlotId> _common_sub_expr_ids;
    std::vector<Expr*> _common_sub_expr;
    int _common_sub_expr_num;
    bool _is_prepared = false;
};
} // namespace starrocks::vectorized
