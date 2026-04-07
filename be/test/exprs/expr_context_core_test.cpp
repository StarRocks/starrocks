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

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

class ExprContextTestExpr final : public Expr {
public:
    explicit ExprContextTestExpr(bool register_fn_ctx = false)
            : Expr(TypeDescriptor(TYPE_INT)), _register_fn_ctx(register_fn_ctx) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ExprContextTestExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++_evaluate_calls;
        return ColumnHelper::create_const_column<TYPE_INT>(7, 1);
    }

    Status prepare(RuntimeState* state, ExprContext* context) override {
        RETURN_IF_ERROR(Expr::prepare(state, context));
        if (_register_fn_ctx) {
            _fn_ctx_idx = context->register_func(state, FunctionContext::TypeDesc(TypeDescriptor(TYPE_INT)), {});
        }
        return Status::OK();
    }

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override {
        ++_close_calls;
        Expr::close(state, context, scope);
    }

    int fn_ctx_idx() const { return _fn_ctx_idx; }
    int close_calls() const { return _close_calls; }
    int evaluate_calls() const { return _evaluate_calls; }

private:
    bool _register_fn_ctx = false;
    int _fn_ctx_idx = -1;
    int _close_calls = 0;
    int _evaluate_calls = 0;
};

} // namespace

TEST(ExprContextCoreTest, EvaluateResizesConstToInputRows) {
    ExprContextTestExpr expr;
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());

    constexpr size_t num_rows = 4;
    Chunk chunk;
    auto input_column = ColumnHelper::create_const_column<TYPE_INT>(1, num_rows);
    chunk.append_column(std::move(input_column), 0);

    auto result = context.evaluate(&chunk);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(num_rows, result.value()->size());
}

TEST(ExprContextCoreTest, EvaluateWithNullChunkUsesDummyChunk) {
    ExprContextTestExpr expr;
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());

    auto result = context.evaluate(nullptr);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(1, result.value()->size());
    EXPECT_EQ(1, expr.evaluate_calls());
}

TEST(ExprContextCoreTest, CloneCopiesFunctionContexts) {
    ExprContextTestExpr expr(true);
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    ASSERT_GE(expr.fn_ctx_idx(), 0);

    int fragment_local_state = 123;
    context.fn_context(expr.fn_ctx_idx())->set_function_state(FunctionContext::FRAGMENT_LOCAL, &fragment_local_state);

    ObjectPool pool;
    ExprContext* cloned = nullptr;
    ASSERT_TRUE(context.clone(&state, &pool, &cloned).ok());
    ASSERT_NE(nullptr, cloned);

    auto* original_fn_ctx = context.fn_context(expr.fn_ctx_idx());
    auto* cloned_fn_ctx = cloned->fn_context(expr.fn_ctx_idx());
    EXPECT_NE(original_fn_ctx, cloned_fn_ctx);
    EXPECT_EQ(&fragment_local_state, cloned_fn_ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

TEST(ExprContextCoreTest, CloseIsIdempotent) {
    ExprContextTestExpr expr;
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());

    context.close(&state);
    context.close(&state);
    EXPECT_EQ(1, expr.close_calls());
}

} // namespace starrocks
