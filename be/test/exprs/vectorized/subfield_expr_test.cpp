// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/subfield_expr.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/struct_column.h"

namespace starrocks::vectorized {

namespace {
class FakeConstExpr : public starrocks::Expr {
public:
    explicit FakeConstExpr(const TExprNode& dummy) : Expr(dummy) {}

    ColumnPtr evaluate(ExprContext*, Chunk*) override { return _column; }

    Expr* clone(ObjectPool*) const override { return nullptr; }

    ColumnPtr _column;
};

std::unique_ptr<Expr> create_subfield_expr(const TypeDescriptor& type, const std::string& used_subfield_name) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SUBFIELD_EXPR);
    node.__set_is_nullable(true);
    node.__set_type(type.to_thrift());
    node.__set_num_children(0);
    node.__set_used_subfield_name(used_subfield_name);

    auto* expr = SubfieldExprFactory::from_thrift(node);
    return std::unique_ptr<Expr>(expr);
}

} // anonymous namespace

class SubfieldExprTest : public ::testing::Test {
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

TEST_F(SubfieldExprTest, subfield_test) {
    TypeDescriptor struct_type;
    struct_type.type = LogicalType::TYPE_STRUCT;
    struct_type.children.emplace_back(LogicalType::TYPE_INT);
    struct_type.field_names.emplace_back("id");
    struct_type.children.emplace_back(LogicalType::TYPE_VARCHAR);
    struct_type.field_names.emplace_back("name");
    struct_type.selected_fields.push_back(true);
    struct_type.selected_fields.push_back(true);

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
        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), "id");
        expr->add_child(new_fake_const_expr(column, struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_numeric());
        EXPECT_EQ(2, subfield_column->size());
        EXPECT_EQ("1", subfield_column->debug_item(0));
        EXPECT_EQ("2", subfield_column->debug_item(1));
    }

    {
        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), "name");
        expr->add_child(new_fake_const_expr(column, struct_type));
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
    struct_type.selected_fields.push_back(true);
    struct_type.selected_fields.push_back(true);
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

        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), "id");
        expr->add_child(new_fake_const_expr(column, struct_type));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        auto subfield_column = ColumnHelper::get_data_column(result.get());
        EXPECT_TRUE(subfield_column->is_numeric());
        EXPECT_EQ(3, subfield_column->size());
        EXPECT_EQ("1", subfield_column->debug_item(0));
        EXPECT_EQ("0", subfield_column->debug_item(1));
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

        std::unique_ptr<Expr> expr = create_subfield_expr(TypeDescriptor(LogicalType::TYPE_INT), "id");
        expr->add_child(new_fake_const_expr(column, struct_type));
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

} // namespace starrocks::vectorized