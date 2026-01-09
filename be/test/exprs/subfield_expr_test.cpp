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

#include "exprs/subfield_expr.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/struct_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

namespace {

std::unique_ptr<Expr> create_subfield_expr(const TypeDescriptor& type,
                                           const std::vector<std::string>& used_subfield_name) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SUBFIELD_EXPR);
    node.__set_is_nullable(true);
    node.__set_type(type.to_thrift());
    node.__set_num_children(0);
    node.__set_used_subfield_names(used_subfield_name);

    auto* expr = SubfieldExprFactory::from_thrift(node);
    return std::unique_ptr<Expr>(expr);
}

} // anonymous namespace

class SubfieldExprTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { _objpool.clear(); }

    FakeConstExpr* new_fake_const_expr(MutableColumnPtr&& value, const TypeDescriptor& type) {
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

TEST_F(SubfieldExprTest, subfield_test) {
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_VARCHAR);
    struct_type.field_names.emplace_back("name");

    auto column = ColumnHelper::create_column(struct_type, false);

    DatumStruct datum_struct_1;
    datum_struct_1.push_back(1);
    datum_struct_1.push_back("smith");
    column->append_datum(datum_struct_1);

    DatumStruct datum_struct_2;
    datum_struct_2.push_back(2);
    datum_struct_2.push_back("cruise");
    column->append_datum(datum_struct_2);

    {
        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"id"});
        expr->add_child(new_fake_const_expr(column->clone(), struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_numeric());
        EXPECT_EQ(2, subfield_column->size());
        EXPECT_EQ("1", subfield_column->debug_item(0));
        EXPECT_EQ("2", subfield_column->debug_item(1));
    }

    {
        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"name"});
        expr->add_child(new_fake_const_expr(column->clone(), struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_binary());
        EXPECT_EQ(2, subfield_column->size());
        EXPECT_EQ("'smith'", subfield_column->debug_item(0));
        EXPECT_EQ("'cruise'", subfield_column->debug_item(1));
    }
}

TEST_F(SubfieldExprTest, subfield_null_test) {
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_VARCHAR);
    struct_type.field_names.emplace_back("name");
    {
        auto column = ColumnHelper::create_column(struct_type, false);

        DatumStruct datum_struct_1;
        datum_struct_1.push_back(1);
        datum_struct_1.push_back("smith");
        column->append_datum(datum_struct_1);

        column->append_nulls(1);

        DatumStruct datum_struct_3;
        datum_struct_3.push_back(3);
        datum_struct_3.push_back("cruise");
        column->append_datum(datum_struct_3);

        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"id"});
        expr->add_child(new_fake_const_expr(std::move(column), struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_numeric());
        EXPECT_EQ(3, subfield_column->size());
        EXPECT_EQ("1", subfield_column->debug_item(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ("3", subfield_column->debug_item(2));
    }

    {
        auto column = ColumnHelper::create_column(struct_type, true);

        DatumStruct datum_struct_1;
        datum_struct_1.push_back(1);
        datum_struct_1.push_back("smith");
        column->append_datum(datum_struct_1);

        column->append_nulls(1);

        DatumStruct datum_struct_3;
        datum_struct_3.push_back(3);
        datum_struct_3.push_back("cruise");
        column->append_datum(datum_struct_3);

        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"id"});
        expr->add_child(new_fake_const_expr(std::move(column), struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_numeric());
        EXPECT_EQ(3, subfield_column->size());
        EXPECT_EQ("1", subfield_column->debug_item(0));
        EXPECT_EQ("0", subfield_column->debug_item(1));
        EXPECT_EQ("3", subfield_column->debug_item(2));
    }
}

TEST_F(SubfieldExprTest, subfield_clone_test) {
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_VARCHAR);
    struct_type.field_names.emplace_back("name");
    auto column = ColumnHelper::create_column(struct_type, false);

    DatumStruct datum_struct_1;
    datum_struct_1.push_back(1);
    datum_struct_1.push_back("smith");
    column->append_datum(datum_struct_1);

    column->append_nulls(1);

    DatumStruct datum_struct_3;
    datum_struct_3.push_back(3);
    datum_struct_3.push_back("cruise");
    column->append_datum(datum_struct_3);

    std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"id"});
    expr->add_child(new_fake_const_expr(column->clone(), struct_type));
    auto result = expr->evaluate(nullptr, nullptr);
    EXPECT_TRUE(result->is_nullable());
    auto subfield_column = ColumnHelper::get_data_column(result.get());
    EXPECT_TRUE(subfield_column->is_numeric());
    EXPECT_EQ(3, subfield_column->size());
    EXPECT_EQ("1", subfield_column->debug_item(0));
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ("3", subfield_column->debug_item(2));

    // Column must be cloned
    auto struct_column = down_cast<StructColumn*>(column.get());
    EXPECT_TRUE(ColumnHelper::get_data_column(struct_column->field_column("id").get()) != subfield_column);
}

TEST_F(SubfieldExprTest, subfield_multi_level_test) {
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("level1");
    struct_type.children.emplace_back(LogicalType::TYPE_STRUCT);
    struct_type.children[1].field_names.emplace_back("level2");
    struct_type.children[1].children.emplace_back(LogicalType::TYPE_VARCHAR);
    auto column = ColumnHelper::create_column(struct_type, false);

    DatumStruct datum_struct_1_level1;
    datum_struct_1_level1.push_back(1);
    DatumStruct datum_struct_1_level2;
    datum_struct_1_level2.push_back("smith");
    datum_struct_1_level1.push_back(datum_struct_1_level2);
    column->append_datum(datum_struct_1_level1);

    column->append_nulls(1);

    DatumStruct datum_struct_3_level1;
    datum_struct_3_level1.push_back(3);
    DatumStruct datum_struct_3_level2;
    datum_struct_3_level2.push_back("cruise");
    datum_struct_3_level1.push_back(datum_struct_3_level2);
    column->append_datum(datum_struct_3_level1);

    std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"level1", "level2"});
    expr->add_child(new_fake_const_expr(std::move(column), struct_type));
    auto result = expr->evaluate(nullptr, nullptr);
    EXPECT_TRUE(result->is_nullable());
    EXPECT_EQ(3, result->size());
    EXPECT_EQ("'smith'", result->debug_item(0));
    EXPECT_EQ("NULL", result->debug_item(1));
    EXPECT_EQ("'cruise'", result->debug_item(2));
}

TEST_F(SubfieldExprTest, get_subfields_with_nested_expr_test) {
    // Test case for issue #67555: ensure get_subfields correctly counts nested subfields
    // When a SubfieldExpr has a child expression (e.g., cast), it should count both
    // the subfield itself and any subfields from children
    
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("name");
    struct_type.children.emplace_back(LogicalType::TYPE_VARCHAR);
    
    // Create a subfield expression
    std::unique_ptr<Expr> subfield_expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), {"id"});
    
    // Create a fake constant expression to act as a child (simulating a cast or other expression)
    auto column = ColumnHelper::create_column(struct_type, false);
    DatumStruct datum_struct;
    datum_struct.push_back(1);
    datum_struct.push_back("test");
    column->append_datum(datum_struct);
    
    subfield_expr->add_child(new_fake_const_expr(std::move(column), struct_type));
    
    // Test get_subfields returns correct count
    std::vector<std::vector<std::string>> subfields;
    int count = subfield_expr->get_subfields(&subfields);
    
    // Should return 1 (for the subfield itself) + 0 (FakeConstExpr has no subfields) = 1
    // But the fix changes it to properly call parent's implementation
    EXPECT_EQ(1, count);
    EXPECT_EQ(1, subfields.size());
    EXPECT_EQ(std::vector<std::string>({"id"}), subfields[0]);
}

TEST_F(SubfieldExprTest, get_subfields_nested_subfield_test) {
    // Test case for nested subfield expressions (subfield of subfield)
    TypeDescriptor outer_struct_type;
    outer_struct_type.type = LogicalType::TYPE_STRUCT;
    outer_struct_type.field_names.emplace_back("inner");
    outer_struct_type.children.emplace_back(LogicalType::TYPE_STRUCT);
    outer_struct_type.children[0].field_names.emplace_back("value");
    outer_struct_type.children[0].children.emplace_back(LogicalType::TYPE_INT);
    
    // Create nested subfield expressions: outer.inner and then inner.value
    std::unique_ptr<Expr> inner_subfield = create_subfield_expr(
        outer_struct_type.children[0], {"inner"});
    
    std::unique_ptr<Expr> outer_subfield = create_subfield_expr(
        TypeDescriptor(LogicalType::TYPE_INT), {"value"});
    
    auto column = ColumnHelper::create_column(outer_struct_type, false);
    DatumStruct outer_datum;
    DatumStruct inner_datum;
    inner_datum.push_back(42);
    outer_datum.push_back(inner_datum);
    column->append_datum(outer_datum);
    
    inner_subfield->add_child(new_fake_const_expr(std::move(column), outer_struct_type));
    outer_subfield->add_child(inner_subfield.release());
    
    // Test get_subfields with nested subfield expressions
    std::vector<std::vector<std::string>> subfields;
    int count = outer_subfield->get_subfields(&subfields);
    
    // Should return 2: one for outer subfield, one for inner subfield
    // After the fix, this should correctly account for both levels
    EXPECT_EQ(2, count);
    EXPECT_EQ(2, subfields.size());
}

} // namespace starrocks