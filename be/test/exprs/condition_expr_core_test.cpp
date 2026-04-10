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
#include "exprs/condition_expr.h"

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

TExprNode make_condition_node(TPrimitiveType::type type, int num_children) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(num_children);
    node.__set_child_type(type);
    node.__set_type(gen_type_desc(type));
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

void assert_int_column(const ColumnPtr& result, std::initializer_list<std::optional<int32_t>> expected) {
    ASSERT_EQ(expected.size(), result->size());
    const auto* values = down_cast<const Int32Column*>(ColumnHelper::get_data_column(result.get()));
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

TEST(ConditionExprCoreTest, if_selects_branch_with_nullable_predicate) {
    TExprNode node = make_condition_node(TPrimitiveType::INT, 3);
    std::unique_ptr<Expr> expr(VectorizedConditionExprFactory::create_if_expr(node));
    ASSERT_NE(expr, nullptr);

    FixedResultExpr pred(node, make_bool_column({1, 0, std::nullopt, 1}));
    FixedResultExpr lhs(node, make_int_column({10, 20, std::nullopt, 40}));
    FixedResultExpr rhs(node, make_int_column({100, 200, 300, std::nullopt}));
    expr->add_child(&pred);
    expr->add_child(&lhs);
    expr->add_child(&rhs);

    assert_int_column(expr->evaluate(nullptr, nullptr), {10, 200, 300, 40});
}

TEST(ConditionExprCoreTest, ifnull_with_const_fallback) {
    TExprNode node = make_condition_node(TPrimitiveType::INT, 2);
    std::unique_ptr<Expr> expr(VectorizedConditionExprFactory::create_if_null_expr(node));
    ASSERT_NE(expr, nullptr);

    FixedResultExpr lhs(node, make_int_column({1, std::nullopt, 3, std::nullopt}));
    FixedResultExpr rhs(node, ColumnHelper::create_const_column<TYPE_INT>(7, 4));
    expr->add_child(&lhs);
    expr->add_child(&rhs);

    assert_int_column(expr->evaluate(nullptr, nullptr), {1, 7, 3, 7});
}

TEST(ConditionExprCoreTest, coalesce_picks_first_non_null) {
    TExprNode node = make_condition_node(TPrimitiveType::INT, 3);
    std::unique_ptr<Expr> expr(VectorizedConditionExprFactory::create_coalesce_expr(node));
    ASSERT_NE(expr, nullptr);

    FixedResultExpr c0(node, make_int_column({std::nullopt, std::nullopt, 3}));
    FixedResultExpr c1(node, make_int_column({10, std::nullopt, std::nullopt}));
    FixedResultExpr c2(node, ColumnHelper::create_const_column<TYPE_INT>(99, 3));
    expr->add_child(&c0);
    expr->add_child(&c1);
    expr->add_child(&c2);

    assert_int_column(expr->evaluate(nullptr, nullptr), {10, 99, 3});
}

} // namespace starrocks
