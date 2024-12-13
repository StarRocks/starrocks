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
#include "types/logical_type.h"

namespace starrocks {

class InConstPredicateTest : public ::testing::Test {
public:
    void SetUp() override {}

protected:
    ObjectPool _pool;
    RuntimeState _runtime_state;
};

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