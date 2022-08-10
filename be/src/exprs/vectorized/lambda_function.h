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

class LambdaFunction final : public Expr {
public:
    LambdaFunction(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new LambdaFunction(*this)); }

    Status prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context);

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override;

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->assign(captured_slot_ids.begin(),captured_slot_ids.end());
        return captured_slot_ids.size();
    }

private:
    std::vector<SlotId> captured_slot_ids;
};
} // namespace starrocks::vectorized
