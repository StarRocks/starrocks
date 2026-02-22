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
#include "exprs/compound_predicate.h"
#include "runtime/descriptors.h"

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

TExprNode make_compound_node(TExprOpcode::type opcode, int num_children) {
    TExprNode node;
    node.__set_opcode(opcode);
    node.__set_num_children(num_children);
    node.__set_child_type(TPrimitiveType::BOOLEAN);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    node.__set_is_nullable(true);
    return node;
}

ColumnPtr make_bool_column(std::initializer_list<std::optional<uint8_t>> values) {
    auto data = BooleanColumn::create();
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
    const auto* values = static_cast<const BooleanColumn*>(ColumnHelper::get_data_column(result.get()));
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

TEST(CompoundPredicateCoreTest, and_without_null) {
    TExprNode node = make_compound_node(TExprOpcode::COMPOUND_AND, 2);
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(node));

    FixedResultExpr left(node, make_bool_column({1, 1, 0, 0}));
    FixedResultExpr right(node, make_bool_column({1, 0, 1, 0}));
    expr->add_child(&left);
    expr->add_child(&right);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(result->is_nullable());
    assert_bool_column(result, {1, 0, 0, 0});
}

TEST(CompoundPredicateCoreTest, and_with_null) {
    TExprNode node = make_compound_node(TExprOpcode::COMPOUND_AND, 2);
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(node));

    FixedResultExpr left(node, make_bool_column({std::nullopt, 1, 0, std::nullopt}));
    FixedResultExpr right(node, make_bool_column({0, std::nullopt, std::nullopt, 1}));
    expr->add_child(&left);
    expr->add_child(&right);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_bool_column(result, {0, std::nullopt, 0, std::nullopt});
}

TEST(CompoundPredicateCoreTest, or_with_null) {
    TExprNode node = make_compound_node(TExprOpcode::COMPOUND_OR, 2);
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(node));

    FixedResultExpr left(node, make_bool_column({std::nullopt, 1, 0, std::nullopt}));
    FixedResultExpr right(node, make_bool_column({1, std::nullopt, std::nullopt, 0}));
    expr->add_child(&left);
    expr->add_child(&right);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_bool_column(result, {1, 1, std::nullopt, std::nullopt});
}

TEST(CompoundPredicateCoreTest, not_with_null) {
    TExprNode node = make_compound_node(TExprOpcode::COMPOUND_NOT, 1);
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(node));

    FixedResultExpr input(node, make_bool_column({1, 0, std::nullopt}));
    expr->add_child(&input);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_bool_column(result, {0, 1, std::nullopt});
}

} // namespace starrocks
