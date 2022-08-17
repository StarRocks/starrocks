// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/array_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "exprs/vectorized/mock_vectorized_expr.h"

namespace starrocks::vectorized {

namespace {

ColumnPtr build_int_column(const std::vector<int>& values) {
    auto data = Int32Column::create();
    data->append_numbers(values.data(), values.size() * sizeof(int32_t));
    return data;
}

ColumnPtr build_int_column(const std::vector<int>& values, const std::vector<uint8_t>& nullflags) {
    DCHECK_EQ(values.size(), nullflags.size());
    auto null = NullColumn::create();
    null->append_numbers(nullflags.data(), nullflags.size());

    auto data = build_int_column(values);

    return NullableColumn::create(std::move(data), std::move(null));
}

ColumnPtr build_string_column(const std::vector<Slice>& values) {
    auto data = BinaryColumn::create();
    data->append_strings(values);
    return data;
}

ColumnPtr build_string_column(const std::vector<Slice>& values, const std::vector<uint8_t>& nullflags) {
    DCHECK_EQ(values.size(), nullflags.size());
    auto null = NullColumn::create();
    null->append_numbers(nullflags.data(), nullflags.size());

    auto data = build_string_column(values);

    return NullableColumn::create(std::move(data), std::move(null));
}

std::unique_ptr<Expr> create_array_expr(const TTypeDesc& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::ARRAY_EXPR);
    node.__set_is_nullable(true);
    node.__set_type(type);
    node.__set_num_children(0);

    auto* expr = ArrayExprFactory::from_thrift(node);
    return std::unique_ptr<Expr>(expr);
}

std::unique_ptr<Expr> create_array_expr(const TypeDescriptor& type) {
    return create_array_expr(type.to_thrift());
}

} // anonymous namespace

class ArrayExprTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { _objpool.clear(); }

    MockExpr* new_mock_expr(ColumnPtr value, const PrimitiveType& type) {
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
    type_arr_int.type = PrimitiveType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = PrimitiveType::TYPE_INT;

    TypeDescriptor type_arr_str;
    type_arr_str.type = PrimitiveType::TYPE_ARRAY;
    type_arr_str.children.emplace_back();
    type_arr_str.children.back().type = PrimitiveType::TYPE_VARCHAR;
    type_arr_str.children.back().len = 10;

    // []
    {
        std::unique_ptr<Expr> expr(create_array_expr(type_arr_int));
        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_EQ(1, result->size());
        ASSERT_TRUE(result->is_array());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // [1, 2, 4]
    // [3, 4, 8]
    // [6, 8, 12]
    {
        std::unique_ptr<Expr> expr(create_array_expr(type_arr_int));
        expr->add_child(new_mock_expr(build_int_column({1, 3, 6}), PrimitiveType::TYPE_INT));
        expr->add_child(new_mock_expr(build_int_column({2, 4, 8}), PrimitiveType::TYPE_INT));
        expr->add_child(new_mock_expr(build_int_column({4, 8, 12}), PrimitiveType::TYPE_INT));
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
        std::unique_ptr<Expr> expr(create_array_expr(type_arr_int));
        expr->add_child(new_mock_expr(build_int_column({1, 3, 6}), PrimitiveType::TYPE_INT));
        expr->add_child(new_mock_expr(build_int_column({2, 4, 8}), PrimitiveType::TYPE_INT));
        expr->add_child(new_mock_expr(build_int_column({4, 0, 12}, {0, 1, 0}), PrimitiveType::TYPE_INT));
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
        TypeDescriptor type_varchar(PrimitiveType::TYPE_VARCHAR);
        type_varchar.len = 10;
        std::unique_ptr<Expr> expr(create_array_expr(type_arr_str));
        expr->add_child(new_mock_expr(build_string_column({"a", "ab", ""}), type_varchar));
        expr->add_child(new_mock_expr(build_string_column({"", "bcd", "xyz"}), type_varchar));
        expr->add_child(new_mock_expr(build_string_column({"x", "", "x"}, {0, 1, 0}), type_varchar));
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

} // namespace starrocks::vectorized
