// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/function_call_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <math.h>

#include <vector>

#include "butil/time.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace vectorized {

class VectorizedFunctionCallExprTest : public ::testing::Test {
public:
    void SetUp() {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    RuntimeState runtime_state = RuntimeState(TQueryGlobals());
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

    WARN_IF_ERROR(Expr::prepare(expr_ctxs, &runtime_state), "");
    WARN_IF_ERROR(Expr::open(expr_ctxs, &runtime_state), "");

    ColumnPtr result = exprContext.evaluate(nullptr).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());

    {
        double value = ColumnHelper::get_const_value<TYPE_DOUBLE>(result);
        ASSERT_EQ(M_PI, value);
    }

    exprContext.close(nullptr);
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

    WARN_IF_ERROR(Expr::prepare(expr_ctxs, &runtime_state), "");
    WARN_IF_ERROR(Expr::open(expr_ctxs, &runtime_state), "");

    ColumnPtr result = exprContext.evaluate(nullptr).value();

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (int j = 0; j < value->get_data().size(); ++j) {
            ASSERT_EQ(1, value->get_data()[j]);
        }
    }

    exprContext.close(nullptr);
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

    WARN_IF_ERROR(Expr::prepare(expr_ctxs, &runtime_state), "");
    WARN_IF_ERROR(Expr::open(expr_ctxs, &runtime_state), "");

    ColumnPtr result = exprContext.evaluate(nullptr).value();

    ASSERT_FALSE(result->is_constant());
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(result);

        for (int j = 0; j < value->get_data().size(); ++j) {
            ASSERT_EQ(1, value->get_data()[j]);
        }
    }

    Expr::close(expr_ctxs, &runtime_state);
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

    WARN_IF_ERROR(Expr::prepare(expr_ctxs, &runtime_state), "");
    WARN_IF_ERROR(Expr::open(expr_ctxs, &runtime_state), "");

    ColumnPtr result = exprContext.evaluate(nullptr).value();

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

        for (int j = 0; j < value->get_data().size(); ++j) {
            ASSERT_EQ(20, value->get_data()[j]);
        }
    }

    exprContext.close(nullptr);
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

} // namespace vectorized
} // namespace starrocks
