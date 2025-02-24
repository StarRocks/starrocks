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

#include "exprs/map_element_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/map_column.h"
#include "column/type_traits.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/column_test_helper.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {
namespace {

MutableColumnPtr const_int_column(int32_t value, size_t size = 1) {
    auto data = Int32Column::create();
    data->append(value);
    return ConstColumn::create(std::move(data), size);
}

MutableColumnPtr const_varchar_column(const std::string& value, size_t size = 1) {
    auto data = BinaryColumn::create();
    data->append_string(value);
    return ConstColumn::create(std::move(data), size);
}

std::unique_ptr<Expr> create_map_element_expr(const TTypeDesc& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::MAP_ELEMENT_EXPR);
    node.__set_is_nullable(true);
    node.__set_type(type);
    node.__set_num_children(0);

    auto* expr = MapElementExprFactory::from_thrift(node);
    return std::unique_ptr<Expr>(expr);
}

std::unique_ptr<Expr> create_map_element_expr(const TypeDescriptor& type) {
    return create_map_element_expr(type.to_thrift());
}

} // anonymous namespace

class MapElementExprTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { _objpool.clear(); }

    FakeConstExpr* new_fake_const_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        FakeConstExpr* e = _objpool.add(new FakeConstExpr(node));
        e->_column = std::move(value);
        return e;
    }

    MockColumnExpr* new_fake_col_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        auto* e = _objpool.add(new MockColumnExpr(node, value));
        return e;
    }

private:
    ObjectPool _objpool;
};

// NOLINTNEXTLINE
TEST_F(MapElementExprTest, test_map_int_int) {
    TypeDescriptor type_map_int_int;
    type_map_int_int.type = LogicalType::TYPE_MAP;
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_int_int, false);

    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)1] = (int32_t)44;
    map1[(int32_t)2] = (int32_t)55;
    map1[(int32_t)4] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(int32_t)2] = (int32_t)77;
    map2[(int32_t)3] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(int32_t)2] = (int32_t)99;
    column->append_datum(map3);

    column->append_datum(DatumMap());

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //
    // Query:
    //   select c0[2]
    //
    // Outputs:
    //   22
    //   55
    //   77
    //   99
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_int_int));
        expr->add_child(new_fake_const_expr(const_int_column(2, column->size()), type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_FALSE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));

        EXPECT_EQ(22, result->get(0).get_int32());
        EXPECT_EQ(55, result->get(1).get_int32());
        EXPECT_EQ(77, result->get(2).get_int32());
        EXPECT_EQ(99, result->get(3).get_int32());
    }

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //
    // Query:
    //   select c0[3]
    //
    // Outputs:
    //   33
    //   NULL
    //   88
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_int_int));
        expr->add_child(new_fake_const_expr(const_int_column(3, column->size()), type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));

        EXPECT_EQ(33, result->get(0).get_int32());
        EXPECT_EQ(88, result->get(2).get_int32());
    }

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //
    // Query:
    //   select c0[3/3/3/0,0]
    //
    // Outputs:
    //   33
    //   NULL
    //   88
    //   NULL
    //   NULL

    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_int_int));
        auto type = TypeDescriptor(LogicalType::TYPE_INT);
        expr->add_child(new_fake_col_expr(ColumnTestHelper::build_column<int32_t>({3, 3, 3, 0, 0}), type));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));

        EXPECT_EQ(33, result->get(0).get_int32());
        EXPECT_EQ(88, result->get(2).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(MapElementExprTest, test_map_varchar_int) {
    TypeDescriptor type_map_varchar_int;
    type_map_varchar_int.type = LogicalType::TYPE_MAP;
    type_map_varchar_int.children.resize(2);
    type_map_varchar_int.children[0].type = LogicalType::TYPE_VARCHAR;
    type_map_varchar_int.children[0].len = 10;
    type_map_varchar_int.children[1].type = LogicalType::TYPE_INT;

    TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
    type_varchar.len = 10;

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_varchar_int, true);

    DatumMap map;
    map[(Slice) "a"] = (int32_t)11;
    map[(Slice) "b"] = (int32_t)22;
    map[(Slice) "c"] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "a"] = (int32_t)44;
    map1[(Slice) "b"] = (int32_t)55;
    map1[(Slice) "d"] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(Slice) "b"] = (int32_t)77;
    map2[(Slice) "c"] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(Slice) "b"] = (int32_t)99;
    column->append_datum(map3);

    column->append_datum(DatumMap());
    column->append_nulls(1);

    // Inputs:
    //   c0
    // --------
    //   [a->11, b->22, c->33]
    //   [a->44, b->55, d->66]
    //   [b->77, c->88]
    //   [b->99]
    //   [NULL]
    //   NULL
    //
    // Query:
    //   select c0["b"]
    //
    // Outputs:
    //   22
    //   55
    //   77
    //   99
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_varchar_int));
        expr->add_child(new_fake_const_expr(const_varchar_column("b", column->size()), type_varchar));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(6, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_FALSE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
        EXPECT_TRUE(result->is_null(5));

        EXPECT_EQ(22, result->get(0).get_int32());
        EXPECT_EQ(55, result->get(1).get_int32());
        EXPECT_EQ(77, result->get(2).get_int32());
        EXPECT_EQ(99, result->get(3).get_int32());
    }

    // Inputs:
    //   c0
    // --------
    //   [a->11, b->22, c->33]
    //   [a->44, b->55, d->66]
    //   [b->77, c->88]
    //   [b->99]
    //   [NULL]
    //   NULL
    //
    // Query:
    //   select c0[3]
    //
    // Outputs:
    //   33
    //   NULL
    //   88
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_varchar_int));
        expr->add_child(new_fake_const_expr(const_varchar_column("c", column->size()), type_varchar));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(6, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
        EXPECT_TRUE(result->is_null(5));

        EXPECT_EQ(33, result->get(0).get_int32());
        EXPECT_EQ(88, result->get(2).get_int32());
    }
}

TEST_F(MapElementExprTest, test_map_const) {
    TypeDescriptor type_map_int_int;
    type_map_int_int.type = LogicalType::TYPE_MAP;
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_int_int, false);

    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)1] = (int32_t)44;
    map1[(int32_t)2] = (int32_t)55;
    map1[(int32_t)4] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(int32_t)2] = (int32_t)77;
    map2[(int32_t)3] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(int32_t)2] = (int32_t)99;
    column->append_datum(map3);

    column->append_datum(DatumMap());

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //
    // Query:
    //   select c0[null]
    //
    // Outputs:
    //   null
    //   null
    //   null
    //   null
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_int_int));
        expr->add_child(new_fake_const_expr(ColumnHelper::create_const_null_column(column->size()), type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
    }
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_const_expr(ColumnHelper::create_const_null_column(column->size()), type_int));
        expr->add_child(new_fake_const_expr(ColumnHelper::create_const_null_column(column->size()), type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->only_null());
    }
    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //
    // Query:
    //   select c0[3]
    //
    // Outputs:
    //   33

    {
        ColumnPtr const_map = ConstColumn::create(column->clone(), column->size());
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(const_map, type_map_int_int));
        expr->add_child(new_fake_const_expr(const_int_column(3, column->size()), type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        // corner test
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_constant());
        EXPECT_EQ(33, result->get(0).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(MapElementExprTest, test_const_map_int_variable_int) {
    TypeDescriptor type_map_int_int;
    type_map_int_int.type = LogicalType::TYPE_MAP;
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_int_int, false);

    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);
    ColumnPtr const_column = ConstColumn::create(column->clone(), 1);

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //
    //   c1
    // ----------
    // 1, 2, 3
    //
    // Query:
    //   select c0[idx]
    //
    // Outputs:
    //   11
    //   22
    //   33
    RunTimeColumnType<TYPE_INT>::Ptr key = RunTimeColumnType<TYPE_INT>::create();
    key->append(1);
    key->append(2);
    key->append(3);

    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(const_column, type_map_int_int));
        expr->add_child(new_fake_col_expr(key, type_int));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(3, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));

        EXPECT_EQ(11, result->get(0).get_int32());
        EXPECT_EQ(22, result->get(1).get_int32());
        EXPECT_EQ(33, result->get(2).get_int32());
    }
}
// NOLINTNEXTLINE
TEST_F(MapElementExprTest, test_map_null_key) {
    TypeDescriptor type_map_varchar_int;
    type_map_varchar_int.type = LogicalType::TYPE_MAP;
    type_map_varchar_int.children.resize(2);
    type_map_varchar_int.children[0].type = LogicalType::TYPE_VARCHAR;
    type_map_varchar_int.children[0].len = 10;
    type_map_varchar_int.children[1].type = LogicalType::TYPE_INT;

    TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
    type_varchar.len = 10;

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_varchar_int, false);

    DatumMap map;
    map[(Slice) "a"] = (int32_t)11;
    map[(Slice) "b"] = (int32_t)22;
    map[(Slice) "c"] = (int32_t)33;
    column->append_datum(map);

    MapColumn* map_column = down_cast<MapColumn*>(column.get());
    map_column->offsets_column()->append(6);
    map_column->keys_column()->append_datum(Datum(Slice("a")));
    map_column->keys_column()->append_datum(Datum(Slice("b")));
    map_column->keys_column()->append_nulls(1);
    map_column->values_column()->append_datum(Datum(44));
    map_column->values_column()->append_datum(Datum(55));
    map_column->values_column()->append_datum(Datum(66));

    // Inputs:
    //   c0
    // --------
    //   [a->11, b->22, c->33]
    //   [a->44, b->55, null->66]
    //
    // Query:
    //   select c0[null]
    //
    // Outputs:
    //   Null
    //   66
    {
        std::unique_ptr<Expr> expr = create_map_element_expr(type_int);

        expr->add_child(new_fake_col_expr(column, type_map_varchar_int));
        expr->add_child(new_fake_const_expr(ColumnHelper::create_const_null_column(1), type_varchar));
        ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
        ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(2, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));

        EXPECT_EQ(66, result->get(1).get_int32());
    }
}

} // namespace starrocks
