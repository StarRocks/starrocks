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

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/function_call_expr.h"
#include "exprs/jit/jit_functions.h"
#include "exprs/jit/jit_wrapper.h"
#include "gen_cpp/Exprs_types.h"
#include "llvm/IR/IRBuilder.h"

namespace starrocks {

class JITExpr final : public Expr {
public:
    static JITExpr* create(ObjectPool* pool, Expr* expr);

    JITExpr(ObjectPool* pool, const TExprNode& node, Expr* expr);

    ~JITExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return JITExpr::create(pool, _expr); }

protected:
    /**
     * @brief Prepare the expression.
     */
    Status prepare(RuntimeState* state, ExprContext* context) override;

    /**
     * @brief Open the expression, including:
     * 1. Compile the expression into native code and retrieve the function pointer.
     * 2. Create a function context and set the function pointer.
     */
    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    /**
     * @brief Evaluate the expression using the function context, which contains the compiled function pointer.
     */
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    /**
     * @brief Evaluate the expression, remove the compiled function.
     */
    void close() override;

private:
    ObjectPool* _pool;
    // The original expression.
    Expr* _expr;
};

} // namespace starrocks
