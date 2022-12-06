// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    // the slot ids of lambda expression may be originally from the arguments of this lambda function
    // or its parent lambda functions, or captured columns, remove the first one.
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->assign(_captured_slot_ids.begin(), _captured_slot_ids.end());
        return _captured_slot_ids.size();
    }

    int get_lambda_arguments_ids(std::vector<SlotId>* ids) {
        ids->assign(_arguments_ids.begin(), _arguments_ids.end());
        return _arguments_ids.size();
    }

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

private:
    std::vector<SlotId> _captured_slot_ids;
    std::vector<SlotId> _arguments_ids;
};
} // namespace starrocks::vectorized
