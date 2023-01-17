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
#include "column/map_column.h"

namespace starrocks {
namespace {

class FakeConstExpr : public starrocks::Expr {
public:
    explicit FakeConstExpr(const TExprNode& dummy) : Expr(dummy) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _column; }

    Expr* clone(ObjectPool*) const override { return nullptr; }

    ColumnPtr _column;
};

ColumnPtr const_int_column(int32_t value, size_t size = 1) {
    auto data = Int32Column::create();
    data->append(value);
    return ConstColumn::create(std::move(data), size);
}

ColumnPtr const_varchar_column(const std::string& value, size_t size = 1) {
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

    auto column = ColumnHelper::create_column(type_map_int_int, false);

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

        expr->add_child(new_fake_const_expr(column, type_map_int_int));
        expr->add_child(new_fake_const_expr(const_int_column(2), type_int));

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

        expr->add_child(new_fake_const_expr(column, type_map_int_int));
        expr->add_child(new_fake_const_expr(const_int_column(3), type_int));

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

    auto column = ColumnHelper::create_column(type_map_varchar_int, true);

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

        expr->add_child(new_fake_const_expr(column, type_map_varchar_int));
        expr->add_child(new_fake_const_expr(const_varchar_column("b"), type_varchar));

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

        expr->add_child(new_fake_const_expr(column, type_map_varchar_int));
        expr->add_child(new_fake_const_expr(const_varchar_column("c"), type_varchar));

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

} // namespace starrocks
