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

#include "exprs/min_max_predicate.h"

#include <gtest/gtest.h>

#include "exprs/runtime_filter.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class MinMaxPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        _rf.bloom_filter().init(100);
        _rf.insert(10);
        _rf.insert(20);

        _nullable_rf.bloom_filter().init(100);
        _nullable_rf.insert(10);
        _nullable_rf.insert(20);
        _nullable_rf.insert_null();

        _chunk = std::make_shared<Chunk>();
        _slot_id = 1;
    }

protected:
    ComposedRuntimeFilter<TYPE_INT> _rf;
    ComposedRuntimeFilter<TYPE_INT> _nullable_rf;
    ObjectPool _pool;
    ChunkPtr _chunk;
    ColumnPtr _column;
    SlotId _slot_id;
};

// rt has no null, col is not nullable
TEST_F(MinMaxPredicateTest, rt_has_no_null_1) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    _column = ColumnTestHelper::build_column<int32_t>(values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 1, 0]");
}

// rt has no null, col is nullable, there is no null in column
TEST_F(MinMaxPredicateTest, rt_has_no_null_2) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    std::vector<uint8_t> null_values = {0, 0, 0, 0, 0};
    _column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 1, 0]");
}

// rt has no null, col is nullable, there is null in column
TEST_F(MinMaxPredicateTest, rt_has_no_null_3) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    std::vector<uint8_t> null_values = {0, 0, 0, 1, 0};
    _column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 0, 0]");
}

// rt has no null, col is only null
TEST_F(MinMaxPredicateTest, rt_has_no_null_4) {
    _column = ColumnHelper::create_const_null_column(5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 0, 0, 0, 0]");
}

// rt has no null, col is const not null value, and exist in rt
TEST_F(MinMaxPredicateTest, rt_has_no_null_5) {
    _column = ColumnHelper::create_const_column<TYPE_INT>(15, 5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[1, 1, 1, 1, 1]");
}

// rt has no null, col is const not null value, and not exist in rt
TEST_F(MinMaxPredicateTest, rt_has_no_null_6) {
    _column = ColumnHelper::create_const_column<TYPE_INT>(30, 5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 0, 0, 0, 0]");
}

// rt has null, col is not nullable
TEST_F(MinMaxPredicateTest, rt_has_null_1) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    _column = ColumnTestHelper::build_column<int32_t>(values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 1, 0]");
}

// rt has null, col is nullable, there is no null in col
TEST_F(MinMaxPredicateTest, rt_has_null_2) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    std::vector<uint8_t> null_values = {0, 0, 0, 0, 0};
    _column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 1, 0]");
}

// rt has null, col is nullable, there is null in col
TEST_F(MinMaxPredicateTest, rt_has_null_3) {
    std::vector<int32_t> values = {1, 10, 15, 20, 25};
    std::vector<uint8_t> null_values = {0, 0, 0, 0, 1};
    _column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 1, 1, 1, 1]");
}

// rt has null, col is only null
TEST_F(MinMaxPredicateTest, rt_has_null_4) {
    _column = ColumnHelper::create_const_null_column(5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[1, 1, 1, 1, 1]");
}

// rt has null, col is const not null value, and exist in rt
TEST_F(MinMaxPredicateTest, rt_has_null_5) {
    _column = ColumnHelper::create_const_column<TYPE_INT>(15, 5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[1, 1, 1, 1, 1]");
}

// rt has null, col is const not null value, and not exist in rt
TEST_F(MinMaxPredicateTest, rt_has_null_6) {
    _column = ColumnHelper::create_const_column<TYPE_INT>(30, 5);
    _chunk->append_column(_column, _slot_id);

    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    auto st = expr->evaluate_checked(nullptr, _chunk.get());

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(st.value()->debug_string(), "[0, 0, 0, 0, 0]");
}

TEST_F(MinMaxPredicateTest, debug_string) {
    Expr* expr1 = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    Expr* expr2 = MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>();
    ASSERT_EQ(expr1->debug_string(),
              "MinMaxPredicate (type=5, slot_id=1, has_null=0, min=10, max=20, expr( type=INT "
              "node-type=RUNTIME_FILTER_MIN_MAX_EXPR codegen=false))");
    ASSERT_EQ(expr2->debug_string(),
              "MinMaxPredicate (type=5, slot_id=1, has_null=1, min=10, max=20, expr( type=INT "
              "node-type=RUNTIME_FILTER_MIN_MAX_EXPR codegen=false))");
}

TEST_F(MinMaxPredicateTest, clone) {
    Expr* expr = MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>();
    Expr* clone_expr = expr->clone(&_pool);
    ASSERT_EQ(clone_expr->debug_string(),
              "MinMaxPredicate (type=5, slot_id=1, has_null=0, min=10, max=20, expr( type=INT "
              "node-type=RUNTIME_FILTER_MIN_MAX_EXPR codegen=false))");
}

TEST_F(MinMaxPredicateTest, other) {
    auto* expr1 = reinterpret_cast<MinMaxPredicate<TYPE_INT>*>(
            MinMaxPredicateBuilder(&_pool, _slot_id, &_rf).operator()<TYPE_INT>());
    ASSERT_FALSE(expr1->is_constant());
    ASSERT_FALSE(expr1->is_bound({}));
    ASSERT_FALSE(expr1->has_null());

    auto* expr2 = reinterpret_cast<MinMaxPredicate<TYPE_INT>*>(
            MinMaxPredicateBuilder(&_pool, _slot_id, &_nullable_rf).operator()<TYPE_INT>());
    ASSERT_TRUE(expr2->has_null());

    std::vector<SlotId> slot_ids;
    auto ret = expr2->get_slot_ids(&slot_ids);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(slot_ids, std::vector<SlotId>{1});
}

} // namespace starrocks
