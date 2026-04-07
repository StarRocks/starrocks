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

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

class SimpleExpr final : public Expr {
public:
    explicit SimpleExpr(TypeDescriptor type, bool is_constant = true)
            : Expr(std::move(type)), _is_constant(is_constant) {}

    SimpleExpr(TypeDescriptor type, TExprNodeType::type node_type, TExprOpcode::type opcode) : Expr(std::move(type)) {
        _node_type = node_type;
        _opcode = opcode;
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new SimpleExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++evaluate_calls;
        return ColumnHelper::create_const_column<TYPE_INT>(42, 1);
    }

    bool is_constant() const override { return _is_constant; }

    int evaluate_calls = 0;

private:
    bool _is_constant = true;
};

} // namespace

TEST(ExprCoreTest, CopyDeepCopiesChildren) {
    ObjectPool pool;
    auto* root = pool.add(new SimpleExpr(TypeDescriptor(TYPE_INT)));
    auto* child = pool.add(new SimpleExpr(TypeDescriptor(TYPE_INT)));
    auto* grandchild = pool.add(new SimpleExpr(TypeDescriptor(TYPE_INT)));
    child->add_child(grandchild);
    root->add_child(child);

    auto* copied_root = Expr::copy(&pool, root);
    ASSERT_NE(root, copied_root);
    ASSERT_EQ(1, copied_root->get_num_children());

    auto* copied_child = copied_root->get_child(0);
    ASSERT_NE(child, copied_child);
    ASSERT_EQ(1, copied_child->get_num_children());

    auto* copied_grandchild = copied_child->get_child(0);
    EXPECT_NE(grandchild, copied_grandchild);
}

TEST(ExprCoreTest, TypeWithoutCastSkipsCastOpcode) {
    SimpleExpr child(TypeDescriptor(TYPE_INT), TExprNodeType::INT_LITERAL, TExprOpcode::INVALID_OPCODE);
    SimpleExpr cast(TypeDescriptor(TYPE_INT), TExprNodeType::CAST_EXPR, TExprOpcode::CAST);
    cast.add_child(&child);

    EXPECT_EQ(TExprNodeType::INT_LITERAL, Expr::type_without_cast(&cast));
    EXPECT_EQ(&child, Expr::expr_without_cast(&cast));
}

TEST(ExprCoreTest, EvaluateConstCachesResult) {
    SimpleExpr expr{TypeDescriptor(TYPE_INT)};
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());

    auto first = expr.evaluate_const(&context);
    auto second = expr.evaluate_const(&context);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    EXPECT_EQ(1, expr.evaluate_calls);
    EXPECT_EQ(first.value().get(), second.value().get());
}

} // namespace starrocks
