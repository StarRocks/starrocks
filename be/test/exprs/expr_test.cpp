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

#include "exprs/expr.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "runtime/current_thread.h"
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

// Throws like an allocation the memory hook refuses, and records whether the memory limit was being checked.
class BadAllocExpr final : public Expr {
public:
    explicit BadAllocExpr(TypeDescriptor type) : Expr(std::move(type)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new BadAllocExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++evaluate_calls;
        limit_checked = CurrentThread::is_catched();
        throw std::bad_alloc();
    }

    bool is_constant() const override { return true; }

    int evaluate_calls = 0;
    bool limit_checked = false;
};

} // namespace

TEST(ExprTest, CopyDeepCopiesChildren) {
    ObjectPool pool;
    auto* root = pool.add(new SimpleExpr(TYPE_INT_DESC));
    auto* child = pool.add(new SimpleExpr(TYPE_INT_DESC));
    auto* grandchild = pool.add(new SimpleExpr(TYPE_INT_DESC));
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

TEST(ExprTest, TypeWithoutCastSkipsCastOpcode) {
    SimpleExpr child(TYPE_INT_DESC, TExprNodeType::INT_LITERAL, TExprOpcode::INVALID_OPCODE);
    SimpleExpr cast(TYPE_INT_DESC, TExprNodeType::CAST_EXPR, TExprOpcode::CAST);
    cast.add_child(&child);

    EXPECT_EQ(TExprNodeType::INT_LITERAL, Expr::type_without_cast(&cast));
    EXPECT_EQ(&child, Expr::expr_without_cast(&cast));
}

TEST(ExprTest, GeoTypesDoNotRequireLiteralNode) {
    for (auto type : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        SimpleExpr expr{TypeDescriptor(type)};
        EXPECT_EQ(type, expr.type().type);
        EXPECT_FALSE(expr.is_slotref());
    }
}

TEST(ExprTest, FileTypeDoesNotRequireLiteralNode) {
    SimpleExpr expr{TypeDescriptor(TYPE_FILE)};
    EXPECT_EQ(TYPE_FILE, expr.type().type);
    EXPECT_FALSE(expr.is_slotref());
    EXPECT_FALSE(expr.is_cast_expr());
}

TEST(ExprTest, EvaluateConstCachesResult) {
    SimpleExpr expr{TYPE_INT_DESC};
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

// Folding a constant checks the memory limit and turns a refused allocation into an error.
TEST(ExprTest, EvaluateConstChecksMemoryLimit) {
    BadAllocExpr expr{TYPE_INT_DESC};
    RuntimeState state;
    ExprContext context(&expr);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());

    StatusOr<ColumnPtr> first = Status::OK();
    EXPECT_NO_THROW(first = expr.evaluate_const(&context));
    EXPECT_TRUE(expr.limit_checked);
    ASSERT_FALSE(first.ok());
    EXPECT_TRUE(first.status().is_mem_limit_exceeded()) << first.status();

    // the error is kept, the constant is not evaluated again
    auto second = expr.evaluate_const(&context);
    EXPECT_FALSE(second.ok());
    EXPECT_EQ(1, expr.evaluate_calls);
}

// The values of an IN list are evaluated in open() under the memory limit too.
TEST(ExprTest, InConstPredicateOpenChecksMemoryLimit) {
    TExprNode node;
    node.node_type = TExprNodeType::IN_PRED;
    node.type = TYPE_BOOLEAN_DESC.to_thrift();
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_child_type(TPrimitiveType::INT);
    node.in_predicate.__set_is_not_in(false);

    ObjectPool pool;
    auto* in_pred = pool.add(new VectorizedInConstPredicate<TYPE_INT>(node));
    in_pred->add_child(pool.add(new ColumnRef(TYPE_INT_DESC, 1)));
    auto* value = pool.add(new BadAllocExpr(TYPE_INT_DESC));
    in_pred->add_child(value);

    RuntimeState state;
    ExprContext context(in_pred);
    ASSERT_TRUE(context.prepare(&state).ok());
    Status st = Status::OK();
    EXPECT_NO_THROW(st = context.open(&state));
    EXPECT_TRUE(value->limit_checked);
    EXPECT_TRUE(st.is_mem_limit_exceeded()) << st;
}

} // namespace starrocks
