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

#include "exprs/in_const_predicate.hpp"

#include <gtest/gtest.h>

#include "exprs/column_ref.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class InConstPredicateTest : public ::testing::Test {
public:
    void SetUp() override {}

protected:
    TExpr _create_not_in_null_mid(SlotId slot_id, const std::vector<int32_t>& values, bool has_null);

    ObjectPool _pool;
    RuntimeState _runtime_state;
};

TExpr InConstPredicateTest::_create_not_in_null_mid(SlotId slot_id, const std::vector<int32_t>& values, bool has_null) {
    std::vector<TExprNode> nodes;

    nodes.emplace_back(ExprsTestHelper::create_not_in_pred_node(values.size() + 1 + has_null));
    nodes.emplace_back(ExprsTestHelper::create_slot_expr_node(0, slot_id, ExprsTestHelper::IntTTypeDesc, true));
    for (size_t i = 0; i < values.size() / 2; i++) {
        nodes.emplace_back(ExprsTestHelper::create_int_literal(values[i], ExprsTestHelper::IntTTypeDesc, false));
    }
    if (has_null) {
        nodes.emplace_back(ExprsTestHelper::create_null_literal(ExprsTestHelper::IntTTypeDesc));
    }
    for (size_t i = values.size() / 2; i < values.size(); i++) {
        nodes.emplace_back(ExprsTestHelper::create_int_literal(values[i], ExprsTestHelper::IntTTypeDesc, false));
    }

    TExpr t_expr;
    t_expr.nodes = nodes;
    return t_expr;
}

TEST_F(InConstPredicateTest, in_open_with_null) {
    std::vector<int32_t> values{1, 3, 5, 7, 9};
    TExpr in_texpr = ExprsTestHelper::create_in_pred_texpr(0, values, true);
    std::vector<TExpr> exprs{in_texpr};
    std::vector<ExprContext*> expr_ctxs;
    ASSERT_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(&_pool, &_runtime_state, &exprs, &expr_ctxs));
    ASSERT_EQ(expr_ctxs.size(), 1);

    const auto* in_const_pred = reinterpret_cast<const VectorizedInConstPredicate<TYPE_INT>*>(expr_ctxs[0]->root());
    ColumnPtr col = in_const_pred->get_all_values();
    ASSERT_EQ(col->debug_string(), "[9, 5, 1, 7, 3]");
}

TEST_F(InConstPredicateTest, not_in_open_with_null) {
    std::vector<int32_t> values{1, 3, 5, 7, 9};
    TExpr in_texpr = ExprsTestHelper::create_not_in_pred_texpr(0, values, true);
    std::vector<TExpr> exprs{in_texpr};
    std::vector<ExprContext*> expr_ctxs;
    ASSERT_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(&_pool, &_runtime_state, &exprs, &expr_ctxs));
    ASSERT_EQ(expr_ctxs.size(), 1);

    const auto* in_const_pred = reinterpret_cast<const VectorizedInConstPredicate<TYPE_INT>*>(expr_ctxs[0]->root());
    ColumnPtr col = in_const_pred->get_all_values();
    ASSERT_EQ(col->debug_string(), "[NULL]");
}

TEST_F(InConstPredicateTest, not_in_open_with_null_break) {
    std::vector<int32_t> values{1, 3, 5, 7, 9};
    TExpr in_texpr = _create_not_in_null_mid(0, values, true);
    std::vector<TExpr> exprs{in_texpr};
    std::vector<ExprContext*> expr_ctxs;
    ASSERT_OK(ExprsTestHelper::create_and_open_conjunct_ctxs(&_pool, &_runtime_state, &exprs, &expr_ctxs));
    ASSERT_EQ(expr_ctxs.size(), 1);

    const auto* in_const_pred = reinterpret_cast<const VectorizedInConstPredicate<TYPE_INT>*>(expr_ctxs[0]->root());
    ColumnPtr col = in_const_pred->get_all_values();
    ASSERT_EQ(col->debug_string(), "[NULL]");
}

TEST_F(InConstPredicateTest, clone) {
    ColumnRef* col_ref = _pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    VectorizedInConstPredicateBuilder builder(&_runtime_state, &_pool, col_ref);
    builder.set_null_in_set(true);
    builder.use_as_join_runtime_filter();
    Status st = builder.create();

    std::vector<int32_t> values{1, 3, 5, 7, 9};
    ColumnPtr col = ColumnTestHelper::build_column(values);
    builder.add_values(col, 0);

    ExprContext* expr_ctx = builder.get_in_const_predicate();
    Expr* new_expr = expr_ctx->root()->clone(&_pool);
    auto* new_in_const_pred = (VectorizedInConstPredicate<TYPE_INT>*)new_expr;

    ASSERT_TRUE(new_in_const_pred->null_in_set());
    ASSERT_TRUE(new_in_const_pred->is_join_runtime_filter());
    const auto& new_values = new_in_const_pred->hash_set();
    ASSERT_EQ(new_values.size(), 5);
}

} // namespace starrocks