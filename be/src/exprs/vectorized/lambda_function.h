// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

// LambdaFunction has various children such as lambda expression, arg_0, arg_1,....

class LambdaFunction final : public Expr {
public:
    LambdaFunction(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new LambdaFunction(*this)); }

    Status prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) override;

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override;

    // the slot ids of lambda expression may be originally from the arguments of this lambda function
    // or its parent lambda functions, or captured columns, remove the first one.
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->assign(captured_slot_ids.begin(), captured_slot_ids.end());
        return captured_slot_ids.size();
    }

    int get_lambda_arguments_ids(std::vector<SlotId>* ids) {
        ids->assign(arguments_ids.begin(), arguments_ids.end());
        return arguments_ids.size();
    }

    int get_unused_arguments_index(std::vector<SlotId>* ids) {
        ids->assign(unused_arguments_index.begin(), unused_arguments_index.end());
        return unused_arguments_index.size();
    }

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

private:
    std::vector<SlotId> captured_slot_ids;
    std::vector<SlotId> arguments_ids;
    std::vector<SlotId> unused_arguments_index;

};
} // namespace starrocks::vectorized
