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

#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

class StateCombinator;

class AggStateFunctionCallExpr final : public Expr {
public:
    explicit AggStateFunctionCallExpr(const TExprNode& node);

    ~AggStateFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new AggStateFunctionCallExpr(*this)); }

protected:
    Status prepare(RuntimeState* state, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    std::vector<bool> _arg_nullables;
    std::shared_ptr<StateCombinator> _state_combinator = nullptr;
};

} // namespace starrocks
