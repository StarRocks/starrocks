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

#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/function_call_expr.h"
#include "exprs/jit/jit_engine.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

class JITExpr final : public Expr {
public:
    static JITExpr* create(ObjectPool* pool, Expr* expr);

    JITExpr(const TExprNode& node, Expr* expr);

    ~JITExpr() override;

    Expr* clone(ObjectPool* pool) const override { return JITExpr::create(pool, _expr); }

    bool is_jit_compiled() { return _jit_function != nullptr && !_jit_expr_name.empty(); }

protected:
    /**
     * @brief Prepare the expression, including:
     * 1. Compile the expression into native code and retrieve the function pointer.
     * 2. Create a function context and set the function pointer.
     */
    Status prepare(RuntimeState* state, ExprContext* context) override;

    /**
     * @brief Evaluate the expression using the function context, which contains the compiled function pointer.
     */
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    Expr* _expr; // The original expression.
    bool _is_prepared = false;
    JITScalarFunction _jit_function = nullptr;
    std::function<void()> _delete_cache_handle = nullptr;
    std::string _jit_expr_name;
};

} // namespace starrocks
