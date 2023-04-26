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

#include "exprs/in_predicate.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class VectorizedInPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::IN_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedInPredicateTest, sliceInTrue) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.opcode = TExprOpcode::FILTER_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.in_predicate.is_not_in = false;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    std::string v1("test1");
    std::string v2("test2");
    std::string v3("test3");
    std::string v4("test4");
    std::string v5("test5");
    std::string v6("test6");

    Slice s1(v1);
    Slice s2(v2);
    Slice s3(v3);
    Slice s4(v4);
    Slice s5(v5);
    Slice s6(v6);

    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
    MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
    MockConstVectorizedExpr<TYPE_VARCHAR> col3(expr_node, s3);
    MockConstVectorizedExpr<TYPE_VARCHAR> col4(expr_node, s4);
    MockConstVectorizedExpr<TYPE_VARCHAR> col5(expr_node, s1);
    MockConstVectorizedExpr<TYPE_VARCHAR> col6(expr_node, s6);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    expr->_children.push_back(&col3);
    expr->_children.push_back(&col4);
    expr->_children.push_back(&col5);
    expr->_children.push_back(&col6);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedInPredicateTest, dateInFalse) {
    expr_node.child_type = TPrimitiveType::DATETIME;
    expr_node.opcode = TExprOpcode::FILTER_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);
    expr_node.in_predicate.is_not_in = false;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_DATETIME> col1(expr_node, 10, TimestampValue::create(2020, 6, 8, 12, 20, 30));
    MockConstVectorizedExpr<TYPE_DATETIME> col2(expr_node, TimestampValue::create(2020, 6, 8, 13, 20, 30));
    MockConstVectorizedExpr<TYPE_DATETIME> col3(expr_node, TimestampValue::create(2020, 6, 8, 14, 20, 30));
    MockConstVectorizedExpr<TYPE_DATETIME> col4(expr_node, TimestampValue::create(2020, 6, 8, 15, 20, 30));
    MockConstVectorizedExpr<TYPE_DATETIME> col5(expr_node, TimestampValue::create(2021, 6, 8, 12, 20, 30));
    MockConstVectorizedExpr<TYPE_DATETIME> col6(expr_node, TimestampValue::create(2022, 6, 8, 12, 20, 30));

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    expr->_children.push_back(&col3);
    expr->_children.push_back(&col4);
    expr->_children.push_back(&col5);
    expr->_children.push_back(&col6);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_FALSE(v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedInPredicateTest, intNotInTrue) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.opcode = TExprOpcode::FILTER_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.in_predicate.is_not_in = true;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 0);
    MockConstVectorizedExpr<TYPE_INT> col2(expr_node, 1);
    MockConstVectorizedExpr<TYPE_INT> col3(expr_node, 2);
    MockConstVectorizedExpr<TYPE_INT> col4(expr_node, 3);
    MockConstVectorizedExpr<TYPE_INT> col5(expr_node, 4);
    MockConstVectorizedExpr<TYPE_INT> col6(expr_node, 5);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    expr->_children.push_back(&col3);
    expr->_children.push_back(&col4);
    expr->_children.push_back(&col5);
    expr->_children.push_back(&col6);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedInPredicateTest, nullSliceIn) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.opcode = TExprOpcode::FILTER_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.in_predicate.is_not_in = false;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    std::string v1("test1");
    std::string v2("test2");

    Slice s1(v1);
    Slice s2(v2);

    MockNullVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
    MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
    MockConstVectorizedExpr<TYPE_VARCHAR> col3(expr_node, s1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    expr->_children.push_back(&col3);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_nullable());

        auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(
                ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2 == 0) {
                ASSERT_FALSE(ptr->is_null(j));
                ASSERT_TRUE(v->get_data()[j]);
            } else {
                ASSERT_TRUE(ptr->is_null(j));
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, sliceNotInNull) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.opcode = TExprOpcode::FILTER_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.in_predicate.is_not_in = true;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    std::string v1("test1");
    std::string v2("test2");
    std::string v3("test3");

    Slice s1(v1);
    Slice s2(v2);
    Slice s3(v2);

    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
    MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
    MockNullVectorizedExpr<TYPE_VARCHAR> col3(expr_node, 1, s3);
    col3.all_null = true;
    col3.only_null = true;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    expr->_children.push_back(&col3);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_nullable());

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }
    }
}

TEST_F(VectorizedInPredicateTest, inConstPred) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.opcode = TExprOpcode::FILTER_NOT_IN;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.in_predicate.is_not_in = true;

    auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

    std::string v2("test2");
    Slice s2(v2);

    auto mock_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(s2, 5);
    MockExpr col1(expr_node, mock_col);

    MockNullVectorizedExpr<TYPE_VARCHAR> col2(expr_node, 1, s2);
    col2.all_null = true;
    col2.only_null = true;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->size() == 5);
    }
}

} // namespace starrocks
