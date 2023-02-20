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

#include "exprs/array_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <utility>

#include "column/column_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {

class ArrayExprTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { _objpool.clear(); }

    MockExpr* new_mock_expr(ColumnPtr value, const LogicalType& type) {
        return new_mock_expr(std::move(value), TypeDescriptor(type));
    }

    MockExpr* new_mock_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        MockExpr* e = _objpool.add(new MockExpr(node, std::move(value)));
        return e;
    }

private:
    ObjectPool _objpool;
};

// NOLINTNEXTLINE
TEST_F(ArrayExprTest, test_evaluate) {
    TypeDescriptor type_arr_int;
    type_arr_int.type = LogicalType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = LogicalType::TYPE_INT;

    TypeDescriptor type_arr_str;
    type_arr_str.type = LogicalType::TYPE_ARRAY;
    type_arr_str.children.emplace_back();
    type_arr_str.children.back().type = LogicalType::TYPE_VARCHAR;
    type_arr_str.children.back().len = 10;

    // []
    {
        std::unique_ptr<Expr> expr(ExprsTestHelper::create_array_expr(type_arr_int));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        ASSERT_TRUE(result->is_array());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // [1, 2, 4]
    // [3, 4, 8]
    // [6, 8, 12]
    {
        std::unique_ptr<Expr> expr(ExprsTestHelper::create_array_expr(type_arr_int));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({1, 3, 6}), LogicalType::TYPE_INT));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({2, 4, 8}), LogicalType::TYPE_INT));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({4, 8, 12}), LogicalType::TYPE_INT));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(3, result->size());

        // row [1,2, 4]
        EXPECT_EQ(1, result->get(0).get_array()[0].get_int32());
        EXPECT_EQ(2, result->get(0).get_array()[1].get_int32());
        EXPECT_EQ(4, result->get(0).get_array()[2].get_int32());

        // row [3,4,8]
        EXPECT_EQ(3, result->get(1).get_array()[0].get_int32());
        EXPECT_EQ(4, result->get(1).get_array()[1].get_int32());
        EXPECT_EQ(8, result->get(1).get_array()[2].get_int32());

        // row [6,8,12]
        EXPECT_EQ(6, result->get(2).get_array()[0].get_int32());
        EXPECT_EQ(8, result->get(2).get_array()[1].get_int32());
        EXPECT_EQ(12, result->get(2).get_array()[2].get_int32());
    }

    // [1, 2, 4]
    // [3, 4, NULL]
    // [6, 8, 12]
    {
        std::unique_ptr<Expr> expr(ExprsTestHelper::create_array_expr(type_arr_int));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({1, 3, 6}), LogicalType::TYPE_INT));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({2, 4, 8}), LogicalType::TYPE_INT));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_nullable_column<int32_t>({4, 0, 12}, {0, 1, 0}),
                                      LogicalType::TYPE_INT));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(3, result->size());

        // row [1,2, 4]
        EXPECT_EQ(1, result->get(0).get_array()[0].get_int32());
        EXPECT_EQ(2, result->get(0).get_array()[1].get_int32());
        EXPECT_EQ(4, result->get(0).get_array()[2].get_int32());

        // row [3,4,NULL]
        EXPECT_EQ(3, result->get(1).get_array()[0].get_int32());
        EXPECT_EQ(4, result->get(1).get_array()[1].get_int32());
        EXPECT_TRUE(result->get(1).get_array()[2].is_null());

        // row [6,8,12]
        EXPECT_EQ(6, result->get(2).get_array()[0].get_int32());
        EXPECT_EQ(8, result->get(2).get_array()[1].get_int32());
        EXPECT_EQ(12, result->get(2).get_array()[2].get_int32());
    }

    // ["a", "", "x"]
    // ["ab", "bcd", NULL]
    // ["", "xyz", "x"]
    {
        TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
        type_varchar.len = 10;
        std::unique_ptr<Expr> expr(ExprsTestHelper::create_array_expr(type_arr_str));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<Slice>({"a", "ab", ""}), type_varchar));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<Slice>({"", "bcd", "xyz"}), type_varchar));
        expr->add_child(
                new_mock_expr(ColumnTestHelper::build_nullable_column<Slice>({"x", "", "x"}, {0, 1, 0}), type_varchar));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(3, result->size());

        // row ["a", "", "x"]
        EXPECT_EQ("a", result->get(0).get_array()[0].get_slice());
        EXPECT_EQ("", result->get(0).get_array()[1].get_slice());
        EXPECT_EQ("x", result->get(0).get_array()[2].get_slice());

        // row ["ab", "bcd", NULL]
        EXPECT_EQ("ab", result->get(1).get_array()[0].get_slice());
        EXPECT_EQ("bcd", result->get(1).get_array()[1].get_slice());
        EXPECT_TRUE(result->get(1).get_array()[2].is_null());

        // row ['', "xyz", "x"]
        EXPECT_EQ("", result->get(2).get_array()[0].get_slice());
        EXPECT_EQ("xyz", result->get(2).get_array()[1].get_slice());
        EXPECT_EQ("x", result->get(2).get_array()[2].get_slice());
    }
}

} // namespace starrocks
