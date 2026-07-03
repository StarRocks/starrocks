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

#include <string>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/arrow_function_call.h"
#include "exprs/expr.h"
#include "exprs/expr_factory.h"
#include "exprs/java_function_call_expr.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

class MarkerExpr final : public Expr {
public:
    MarkerExpr(const TExprNode& node, int marker) : Expr(node), _marker(marker) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MarkerExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        return ColumnHelper::create_const_column<TYPE_INT>(_marker, 1);
    }

    int marker() const { return _marker; }

private:
    int _marker;
};

int g_post_hook_calls = 0;

void reset_hook_counters() {
    g_post_hook_calls = 0;
}

Status post_hook_handle_all(ObjectPool* pool, const TExprNode& node, Expr** expr, RuntimeState*) {
    ++g_post_hook_calls;
    *expr = pool->add(new MarkerExpr(node, 202));
    return Status::OK();
}

Status post_hook_handle_dict_only(ObjectPool* pool, const TExprNode& node, Expr** expr, RuntimeState*) {
    ++g_post_hook_calls;
    if (node.node_type == TExprNodeType::DICT_EXPR || node.node_type == TExprNodeType::DICT_QUERY_EXPR ||
        node.node_type == TExprNodeType::DICTIONARY_GET_EXPR) {
        *expr = pool->add(new MarkerExpr(node, 303));
    }
    return Status::OK();
}

TExprNode make_function_call_node(const std::string& function_name,
                                  TFunctionBinaryType::type binary_type = TFunctionBinaryType::BUILTIN) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(0);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_is_nullable(true);

    TFunction fn;
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    fn.__set_name(fn_name);
    fn.__set_binary_type(binary_type);
    node.__set_fn(fn);
    return node;
}

class ExprFactoryCoreTest : public ::testing::Test {
protected:
    void TearDown() override { ExprFactory::set_non_core_create_post_hook(nullptr); }
};

TEST_F(ExprFactoryCoreTest, UnsupportedCastReturnsError) {
    TExprNode cast_expr;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.__set_opcode(TExprOpcode::CAST);
    cast_expr.num_children = 1;
    cast_expr.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    cast_expr.__isset.child_type = false;

    TExpr texpr;
    texpr.nodes.emplace_back(cast_expr);

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    Status st = ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state);
    ASSERT_FALSE(st.ok());
}

TEST_F(ExprFactoryCoreTest, SrjarFunctionCallHasPriorityOverCoreCondition) {
    reset_hook_counters();
    ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);

    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("if", TFunctionBinaryType::SRJAR));

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state).ok());
    ASSERT_EQ(0, g_post_hook_calls);

    ASSERT_NE(nullptr, dynamic_cast<JavaFunctionCallExpr*>(root_expr));
}

TEST_F(ExprFactoryCoreTest, PythonFunctionCallHasPriorityOverCoreCondition) {
    reset_hook_counters();
    ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);

    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("if", TFunctionBinaryType::PYTHON));

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state).ok());
    ASSERT_EQ(0, g_post_hook_calls);

    ASSERT_NE(nullptr, dynamic_cast<ArrowFunctionCallExpr*>(root_expr));
}

TEST_F(ExprFactoryCoreTest, FunctionCallCoreConditionHasPriorityOverPostHook) {
    reset_hook_counters();
    ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);

    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("ifnull"));

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state).ok());
    ASSERT_EQ(0, g_post_hook_calls);
    ASSERT_NE(nullptr, root_expr);
    ASSERT_EQ(nullptr, dynamic_cast<MarkerExpr*>(root_expr));
}

TEST_F(ExprFactoryCoreTest, FunctionCallPostHookHandlesFallback) {
    reset_hook_counters();
    ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);

    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("non_core_fallback"));

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state).ok());
    ASSERT_EQ(1, g_post_hook_calls);

    auto* marker_expr = dynamic_cast<MarkerExpr*>(root_expr);
    ASSERT_NE(nullptr, marker_expr);
    EXPECT_EQ(202, marker_expr->marker());
}

TEST_F(ExprFactoryCoreTest, DictNodeDelegatesToPostHook) {
    reset_hook_counters();
    ExprFactory::set_non_core_create_post_hook(post_hook_handle_dict_only);

    TExprNode node;
    node.__set_node_type(TExprNodeType::DICT_EXPR);
    node.__set_num_children(0);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));

    TExpr texpr;
    texpr.nodes.emplace_back(node);

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state).ok());
    ASSERT_EQ(1, g_post_hook_calls);

    auto* marker_expr = dynamic_cast<MarkerExpr*>(root_expr);
    ASSERT_NE(nullptr, marker_expr);
    EXPECT_EQ(303, marker_expr->marker());
}

} // namespace

} // namespace starrocks
