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

#include <gtest/gtest.h>

#include <memory>
#include <optional>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/binary_predicate.h"

namespace starrocks {

namespace {

class FixedResultExpr final : public Expr {
public:
    FixedResultExpr(const TExprNode& node, ColumnPtr column) : Expr(node), _column(std::move(column)) {}

    Expr* clone(ObjectPool* /*pool*/) const override { return new FixedResultExpr(*this); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* /*context*/, Chunk* /*chunk*/) override { return _column; }

    bool is_constant() const override { return false; }

private:
    ColumnPtr _column;
};

TExprNode make_binary_pred_node(TExprOpcode::type opcode) {
    TExprNode node;
    node.__set_opcode(opcode);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_num_children(2);
    node.__set_child_type(TPrimitiveType::INT);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_is_nullable(true);
    return node;
}

ColumnPtr make_int_column(std::initializer_list<std::optional<int32_t>> values) {
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    bool has_null = false;
    for (const auto& value : values) {
        if (value.has_value()) {
            data->append(value.value());
            nulls->append(0);
        } else {
            data->append(0);
            nulls->append(1);
            has_null = true;
        }
    }
    if (has_null) {
        return NullableColumn::create(std::move(data), std::move(nulls));
    }
    return data;
}

void assert_bool_column(const ColumnPtr& result, std::initializer_list<std::optional<uint8_t>> expected) {
    ASSERT_EQ(expected.size(), result->size());
    const auto* values = down_cast<const BooleanColumn*>(ColumnHelper::get_data_column(result.get()));
    size_t idx = 0;
    for (const auto& expected_value : expected) {
        if (!expected_value.has_value()) {
            ASSERT_TRUE(result->is_null(idx));
        } else {
            ASSERT_FALSE(result->is_null(idx));
            ASSERT_EQ(expected_value.value(), values->get_data()[idx]);
        }
        ++idx;
    }
}

} // namespace

TEST(BinaryPredicateCoreTest, eq_and_ne_numeric) {
    TExprNode eq_node = make_binary_pred_node(TExprOpcode::EQ);
    std::unique_ptr<Expr> eq_expr(VectorizedBinaryPredicateFactory::from_thrift(eq_node));

    FixedResultExpr lhs(eq_node, make_int_column({1, 2, 3}));
    FixedResultExpr rhs(eq_node, make_int_column({1, 0, 3}));
    eq_expr->add_child(&lhs);
    eq_expr->add_child(&rhs);
    assert_bool_column(eq_expr->evaluate(nullptr, nullptr), {1, 0, 1});

    TExprNode ne_node = make_binary_pred_node(TExprOpcode::NE);
    std::unique_ptr<Expr> ne_expr(VectorizedBinaryPredicateFactory::from_thrift(ne_node));
    ne_expr->add_child(&lhs);
    ne_expr->add_child(&rhs);
    assert_bool_column(ne_expr->evaluate(nullptr, nullptr), {0, 1, 0});
}

TEST(BinaryPredicateCoreTest, eq_handles_nullable_inputs) {
    TExprNode node = make_binary_pred_node(TExprOpcode::EQ);
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(node));

    FixedResultExpr lhs(node, make_int_column({1, std::nullopt, 3, std::nullopt}));
    FixedResultExpr rhs(node, make_int_column({1, 2, std::nullopt, std::nullopt}));
    expr->add_child(&lhs);
    expr->add_child(&rhs);

    assert_bool_column(expr->evaluate(nullptr, nullptr), {1, std::nullopt, std::nullopt, std::nullopt});
}

TEST(BinaryPredicateCoreTest, eq_supports_const_input) {
    TExprNode node = make_binary_pred_node(TExprOpcode::EQ);
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(node));

    FixedResultExpr lhs(node, ColumnHelper::create_const_column<TYPE_INT>(5, 3));
    FixedResultExpr rhs(node, make_int_column({5, 7, 5}));
    expr->add_child(&lhs);
    expr->add_child(&rhs);

    assert_bool_column(expr->evaluate(nullptr, nullptr), {1, 0, 1});
}

} // namespace starrocks
