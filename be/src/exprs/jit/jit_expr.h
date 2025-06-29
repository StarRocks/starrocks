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

    ~JITExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return JITExpr::create(pool, _expr); }

    bool is_jit_compiled() { return _jit_function != nullptr; }

    void set_uncompilable_children(RuntimeState* state);

    Status prepare_impl(RuntimeState* state, ExprContext* context);

protected:
    // Compile the expression into native code and retrieve the function pointer.
    // if compile failed, fallback to original expr.
    Status prepare(RuntimeState* state, ExprContext* context) override;

    // Evaluate the expression using the compiled function.
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    // The original expression.
    Expr* _expr;
    bool _is_prepared = false;
    JITScalarFunction _jit_function = nullptr;
    std::unique_ptr<JitObjectCache> _jit_obj_cache;
};

} // namespace starrocks
