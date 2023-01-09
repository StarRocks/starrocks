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

#include "butil/time.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/cast_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

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

    ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());

    {
        double value = ColumnHelper::get_const_value<TYPE_DOUBLE>(result);
        ASSERT_EQ(M_PI, value);
    }

    Expr::close(expr_ctxs, &_runtime_state);
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

    ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    Expr::close(expr_ctxs, &_runtime_state);
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

    ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(result);

        for (int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    Expr::close(expr_ctxs, &_runtime_state);
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

    ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
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

        for (int& j : value->get_data()) {
            ASSERT_EQ(20, j);
        }
    }

    Expr::close(expr_ctxs, &_runtime_state);
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

} // namespace starrocks
