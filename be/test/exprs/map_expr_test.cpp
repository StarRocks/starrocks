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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <utility>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "exprs/array_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "testutil/parallel_test.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {

class MapExprTest : public ::testing::Test {
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
TEST_F(MapExprTest, test_evaluate) {
    TypeDescriptor type_map_int_str;
    type_map_int_str.type = LogicalType::TYPE_MAP;
    type_map_int_str.children.emplace_back();
    type_map_int_str.children.back().type = LogicalType::TYPE_INT;
    type_map_int_str.children.emplace_back();
    type_map_int_str.children.back().type = LogicalType::TYPE_VARCHAR;
    type_map_int_str.children.back().len = 10;

    // {}
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        ASSERT_TRUE(result->is_map());
        EXPECT_EQ(0, result->get(0).get_map().size());
    }

    // one key-value pair
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({1, 3, 6}), LogicalType::TYPE_INT));
        TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
        type_varchar.len = 10;
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<Slice>({"a", "ab", ""}), type_varchar));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(3, result->size());
        ASSERT_EQ("{1:'a'}, {3:'ab'}, {6:''}", result->debug_string());
    }

    // more key-value pairs with duplicated keys
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<int32_t>({1, 3, 6}), LogicalType::TYPE_INT));
        TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
        type_varchar.len = 10;
        expr->add_child(new_mock_expr(ColumnTestHelper::build_column<Slice>({"a", "ab", ""}), type_varchar));

        expr->add_child(new_mock_expr(ColumnTestHelper::build_nullable_column<int32_t>({4, 0, 6}, {0, 1, 0}),
                                      LogicalType::TYPE_INT));

        expr->add_child(
                new_mock_expr(ColumnTestHelper::build_nullable_column<Slice>({"x", "", "x"}, {0, 1, 0}), type_varchar));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(3, result->size());
        ASSERT_EQ("{1:'a',4:'x'}, {3:'ab',NULL:NULL}, {6:'x'}", result->debug_string());
    }
}

// NOLINTNEXTLINE
TEST_F(MapExprTest, test_const_evaluate) {
    TypeDescriptor type_map_int_str;
    type_map_int_str.type = LogicalType::TYPE_MAP;
    type_map_int_str.children.emplace_back();
    type_map_int_str.children.back().type = LogicalType::TYPE_INT;
    type_map_int_str.children.emplace_back();
    type_map_int_str.children.back().type = LogicalType::TYPE_VARCHAR;
    type_map_int_str.children.back().len = 10;

    // {}
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        ASSERT_TRUE(result->is_map());
        EXPECT_EQ(0, result->get(0).get_map().size());
    }

    // one key-value pair
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_INT>(1, 1), LogicalType::TYPE_INT));
        TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
        type_varchar.len = 10;
        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>("a", 1), type_varchar));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_map());
        ASSERT_EQ("{1:'a'}", result->debug_string());
    }

    // more key-value pairs with duplicated keys
    {
        auto expr(ExprsTestHelper::create_map_expr(type_map_int_str));
        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_INT>(1, 1), LogicalType::TYPE_INT));
        TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
        type_varchar.len = 10;
        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>("a", 1), type_varchar));

        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_INT>(4, 1), LogicalType::TYPE_INT));
        expr->add_child(
                new_mock_expr(ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>("x", 1), type_varchar));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_map());
        ASSERT_EQ("{1:'a',4:'x'}", result->debug_string());
    }
}

// NOLINTNEXTLINE
GROUP_SLOW_PARALLEL_TEST(MapExprOverflowTest, test_flatten_bytes_exceed_uint32_capacity) {
    // Two key-value pairs (4 children) whose value columns share one 2100-row x 1MB
    // varchar column, so the flattened value column is ~4.4GB and overflows
    // BinaryColumn's uint32 offsets. Expect an explicit CapacityLimitExceed error.
    // Transient peak memory is ~13GB due to bytes vector growth.
    TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
    type_varchar.len = 1048576;
    TypeDescriptor type_map;
    type_map.type = LogicalType::TYPE_MAP;
    type_map.children.push_back(type_varchar);
    type_map.children.push_back(type_varchar);

    const size_t num_rows = 2100;
    const std::string payload(1024 * 1024, 'x');
    auto fat_column_builder = BinaryColumn::create();
    auto key1_column_builder = BinaryColumn::create();
    auto key2_column_builder = BinaryColumn::create();
    fat_column_builder->reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        fat_column_builder->append(Slice(payload));
        key1_column_builder->append(Slice("k1"));
        key2_column_builder->append(Slice("k2"));
    }
    BinaryColumn::Ptr fat_column = std::move(fat_column_builder);
    BinaryColumn::Ptr key1_column = std::move(key1_column_builder);
    BinaryColumn::Ptr key2_column = std::move(key2_column_builder);

    ObjectPool pool;
    std::unique_ptr<Expr> expr(ExprsTestHelper::create_map_expr(type_map));
    expr->add_child(pool.add(new MockExpr(type_varchar, key1_column)));
    expr->add_child(pool.add(new MockExpr(type_varchar, fat_column)));
    expr->add_child(pool.add(new MockExpr(type_varchar, key2_column)));
    expr->add_child(pool.add(new MockExpr(type_varchar, fat_column)));

    auto result = expr->evaluate_checked(nullptr, nullptr);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_capacity_limit_exceeded());
}

} // namespace starrocks