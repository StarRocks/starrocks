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

#include "common/object_pool.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"

namespace starrocks {

class VectorizedFunctionCallExpr final : public Expr {
public:
    explicit VectorizedFunctionCallExpr(const TExprNode& node);

    ~VectorizedFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedFunctionCallExpr(*this)); }

protected:
    Status prepare(RuntimeState* state, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_constant() const override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    const FunctionDescriptor* _fn_desc{nullptr};

    bool _is_returning_random_value = false;
};

} // namespace starrocks
