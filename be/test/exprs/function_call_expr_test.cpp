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

#include "exprs/function_call_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "butil/time.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/agg_state_function_call_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class VectorizedFunctionCallExprTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    RuntimeState _runtime_state;
    TExprNode expr_node;
};

namespace {

TAggStateDesc make_sum_agg_state_desc() {
    TAggStateDesc desc;
    std::vector<TTypeDesc> arg_types = {gen_type_desc(TPrimitiveType::INT)};
    desc.__set_agg_func_name("sum");
    desc.__set_arg_types(arg_types);
    desc.__set_ret_type(gen_type_desc(TPrimitiveType::BIGINT));
    desc.__set_result_nullable(false);
    desc.__set_func_version(1);
    return desc;
}

TExprNode make_agg_state_function_node(const std::string& function_name, int num_children,
                                       std::vector<TTypeDesc> arg_types) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(num_children);
    node.__set_type(gen_type_desc(TPrimitiveType::BIGINT));
    node.__set_is_nullable(false);

    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name(function_name);

    TFunction function;
    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types(std::move(arg_types));
    function.__set_ret_type(gen_type_desc(TPrimitiveType::BIGINT));
    function.__set_has_var_args(false);
    function.__set_agg_state_desc(make_sum_agg_state_desc());
    node.__set_fn(function);
    return node;
}

TExprNode make_int_literal_node(int64_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_num_children(0);
    node.__set_type(gen_type_desc(TPrimitiveType::INT));
    node.__set_is_nullable(false);

    TIntLiteral int_literal;
    int_literal.__set_value(value);
    node.__set_int_literal(int_literal);
    return node;
}

TExprNode make_pi_function_node() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(0);
    node.__set_type(gen_type_desc(TPrimitiveType::DOUBLE));
    node.__set_is_nullable(false);

    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("pi");

    TFunction function;
    std::vector<TTypeDesc> arg_types;
    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types(arg_types);
    function.__set_ret_type(gen_type_desc(TPrimitiveType::DOUBLE));
    function.__set_has_var_args(false);
    function.__set_fid(10010);
    node.__set_fn(function);
    return node;
}

void assert_bigint_column_value(const ColumnPtr& column, int64_t expected) {
    ASSERT_EQ(1, column->size());
    ASSERT_FALSE(column->is_nullable());
    auto values = ColumnHelper::cast_to<TYPE_BIGINT>(column);
    ASSERT_EQ(expected, values->get_data()[0]);
}

} // namespace

TEST_F(VectorizedFunctionCallExprTest, mathPiExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("pi");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10010);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());

    {
        double value = ColumnHelper::get_const_value<TYPE_DOUBLE>(result);
        ASSERT_EQ(M_PI, value);
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathModExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10252);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);

    expr.add_child(&col1);
    expr.add_child(&col2);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (const int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathLeastExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10282);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);
    MockVectorizedExpr<TYPE_INT> col3(expr_node, 5, 20);
    MockVectorizedExpr<TYPE_INT> col4(expr_node, 5, 1);
    MockVectorizedExpr<TYPE_INT> col5(expr_node, 5, 15);
    MockVectorizedExpr<TYPE_INT> col6(expr_node, 5, 2);
    MockVectorizedExpr<TYPE_INT> col7(expr_node, 5, 5);

    expr.add_child(&col1);
    expr.add_child(&col2);
    expr.add_child(&col3);
    expr.add_child(&col4);
    expr.add_child(&col5);
    expr.add_child(&col6);
    expr.add_child(&col7);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(result);

        for (const int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathNullGreatestExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10292);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);
    MockNullVectorizedExpr<TYPE_INT> col3(expr_node, 5, 20);
    MockVectorizedExpr<TYPE_INT> col4(expr_node, 5, 1);
    MockVectorizedExpr<TYPE_INT> col5(expr_node, 5, 15);
    MockVectorizedExpr<TYPE_INT> col6(expr_node, 5, 2);
    MockVectorizedExpr<TYPE_INT> col7(expr_node, 5, 5);

    expr.add_child(&col1);
    expr.add_child(&col2);
    expr.add_child(&col3);
    expr.add_child(&col4);
    expr.add_child(&col5);
    expr.add_child(&col6);
    expr.add_child(&col7);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));
    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (int k = 0; k < result->size(); ++k) {
            if (k % 2) {
                ASSERT_TRUE(result->is_null(k));
            } else {
                ASSERT_FALSE(result->is_null(k));
            }
        }

        for (const int& j : value->get_data()) {
            ASSERT_EQ(20, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, prepareFaileCase) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);

    expr.add_child(&col1);

    ExprContext exprContext(&expr);
    exprContext._is_clone = true;

    ASSERT_FALSE(expr.prepare(nullptr, &exprContext).ok());
    exprContext.close(nullptr);
}

TEST_F(VectorizedFunctionCallExprTest, prepare_close) {
    TFunction func;
    func.__set_fid(60010); // like
    func.__set_binary_type(TFunctionBinaryType::BUILTIN);

    TExprNode expr_node;
    expr_node.__set_fn(func);
    expr_node.__set_opcode(TExprOpcode::ADD);
    expr_node.__set_child_type(TPrimitiveType::INT);
    expr_node.__set_node_type(TExprNodeType::BINARY_PRED);
    expr_node.__set_num_children(2);
    expr_node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));

    VectorizedFunctionCallExpr expr(expr_node);
    ColumnRef col1(TypeDescriptor::create_varbinary_type(10), 1);
    ColumnRef col2(TypeDescriptor::create_varbinary_type(10), 2);
    expr.add_child(&col1);
    expr.add_child(&col2);

    ExprContext expr_context(&expr);
    Status st = expr_context.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());
    st = expr_context.open(&_runtime_state);
    ASSERT_TRUE(st.ok());
    expr_context.close(&_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, aggStateFunctionCallDispatchesToDedicatedExpr) {
    TExpr texpr;
    texpr.nodes.emplace_back(make_agg_state_function_node("sum_state", 1, {gen_type_desc(TPrimitiveType::INT)}));
    texpr.nodes.emplace_back(make_int_literal_node(7));

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_OK(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state));
    ASSERT_NE(nullptr, dynamic_cast<AggStateFunctionCallExpr*>(root_expr));
}

TEST_F(VectorizedFunctionCallExprTest, normalFunctionCallStillDispatchesToVectorizedFunctionCallExpr) {
    TExpr texpr;
    texpr.nodes.emplace_back(make_pi_function_node());

    ObjectPool pool;
    RuntimeState state;
    Expr* root_expr = nullptr;
    ASSERT_OK(ExprFactory::create_expr_tree(&pool, texpr, &root_expr, &state));
    ASSERT_NE(nullptr, dynamic_cast<VectorizedFunctionCallExpr*>(root_expr));
}

TEST_F(VectorizedFunctionCallExprTest, aggStateFunctionCallEvaluatesStateAndMerge) {
    AggStateFunctionCallExpr root_expr(
            make_agg_state_function_node("sum_state_merge", 1, {gen_type_desc(TPrimitiveType::BIGINT)}));
    AggStateFunctionCallExpr state_expr(
            make_agg_state_function_node("sum_state", 1, {gen_type_desc(TPrimitiveType::INT)}));
    MockVectorizedExpr<TYPE_INT> input_expr(make_int_literal_node(7), 1, 7);
    state_expr.add_child(&input_expr);
    root_expr.add_child(&state_expr);

    ExprContext expr_context(&root_expr);
    ASSERT_OK(expr_context.prepare(&_runtime_state));
    ASSERT_OK(expr_context.open(&_runtime_state));

    auto result_or = root_expr.evaluate_checked(&expr_context, nullptr);
    ASSERT_TRUE(result_or.ok()) << result_or.status();
    assert_bigint_column_value(result_or.value(), 7);

    expr_context.close(&_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, aggStateFunctionCallEvaluatesStateUnionAndMerge) {
    AggStateFunctionCallExpr root_expr(
            make_agg_state_function_node("sum_state_merge", 1, {gen_type_desc(TPrimitiveType::BIGINT)}));
    AggStateFunctionCallExpr union_expr(make_agg_state_function_node(
            "sum_state_union", 2, {gen_type_desc(TPrimitiveType::BIGINT), gen_type_desc(TPrimitiveType::BIGINT)}));
    AggStateFunctionCallExpr state_expr1(
            make_agg_state_function_node("sum_state", 1, {gen_type_desc(TPrimitiveType::INT)}));
    MockVectorizedExpr<TYPE_INT> input_expr1(make_int_literal_node(7), 1, 7);
    AggStateFunctionCallExpr state_expr2(
            make_agg_state_function_node("sum_state", 1, {gen_type_desc(TPrimitiveType::INT)}));
    MockVectorizedExpr<TYPE_INT> input_expr2(make_int_literal_node(5), 1, 5);

    state_expr1.add_child(&input_expr1);
    state_expr2.add_child(&input_expr2);
    union_expr.add_child(&state_expr1);
    union_expr.add_child(&state_expr2);
    root_expr.add_child(&union_expr);

    ExprContext expr_context(&root_expr);
    ASSERT_OK(expr_context.prepare(&_runtime_state));
    ASSERT_OK(expr_context.open(&_runtime_state));

    auto result_or = root_expr.evaluate_checked(&expr_context, nullptr);
    ASSERT_TRUE(result_or.ok()) << result_or.status();
    assert_bigint_column_value(result_or.value(), 12);

    expr_context.close(&_runtime_state);
}

} // namespace starrocks
