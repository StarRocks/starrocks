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
#include <vector>

#include "exprs/agg/base_aggregate_test.h"

namespace starrocks {

class CountingInt32Column final : public CowFactory<ColumnFactory<FixedLengthColumnBase<int32_t>, CountingInt32Column>,
                                                    CountingInt32Column, Column> {
    friend class CowFactory<ColumnFactory<FixedLengthColumnBase<int32_t>, CountingInt32Column>, CountingInt32Column,
                            Column>;

public:
    MutableColumnPtr clone_empty() const override { return create(); }

    MutableColumnPtr clone() const override {
        auto column = clone_empty();
        column->append(*this, 0, size());
        return column;
    }

    uint32_t serialize(size_t idx, uint8_t* pos) const override {
        ++_serialize_count;
        return FixedLengthColumnBase<int32_t>::serialize(idx, pos);
    }

    size_t serialize_count() const { return _serialize_count; }

private:
    mutable size_t _serialize_count = 0;
};

class MaxMinByTest : public testing::Test {
public:
    void SetUp() override {
        _context.reset(FunctionContext::create_test_context());
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }

    void TearDown() override {
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
        _context.reset();
    }

protected:
    FunctionContext* context() { return _context.get(); }

private:
    std::unique_ptr<FunctionContext> _context;
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

TEST_F(MaxMinByTest, maxByBatchSingleStateSerializesWinnerOnce) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto values = CountingInt32Column::create();
    values->get_data() = {10, 20, 30, 40};
    auto keys = Int32Column::create();
    keys->get_data() = {1, 2, 3, 4};

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(context(), keys->size(), columns, aggr_state->state());

    EXPECT_EQ(1, values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(40, result->get_data()[0]);
}

TEST_F(MaxMinByTest, minByBatchSingleStateSerializesLastTiedWinnerOnce) {
    const AggregateFunction* func = get_aggregate_function("min_by", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto values = CountingInt32Column::create();
    values->get_data() = {10, 20, 30};
    auto keys = Int32Column::create();
    keys->get_data() = {2, 1, 1};

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(context(), keys->size(), columns, aggr_state->state());

    EXPECT_EQ(1, values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(30, result->get_data()[0]);
}

TEST_F(MaxMinByTest, maxByStringKeySerializesFirstTiedWinnerOnce) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_VARCHAR, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto values = CountingInt32Column::create();
    values->get_data() = {10, 20, 30, 40};
    auto keys = BinaryColumn::create();
    std::vector<Slice> key_values{{"a"}, {"z"}, {"m"}, {"z"}};
    keys->append_strings(key_values.data(), key_values.size());

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(context(), keys->size(), columns, aggr_state->state());

    EXPECT_EQ(1, values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(20, result->get_data()[0]);
}

TEST_F(MaxMinByTest, maxByMaterializesAtMostOnceForEachChunk) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto first_values = CountingInt32Column::create();
    first_values->get_data() = {10, 20};
    auto first_keys = Int32Column::create();
    first_keys->get_data() = {1, 4};
    const Column* first_columns[] = {first_values.get(), first_keys.get()};
    func->update_batch_single_state(context(), first_keys->size(), first_columns, aggr_state->state());
    EXPECT_EQ(1, first_values->serialize_count());

    auto losing_values = CountingInt32Column::create();
    losing_values->get_data() = {30, 40};
    auto losing_keys = Int32Column::create();
    losing_keys->get_data() = {2, 3};
    const Column* losing_columns[] = {losing_values.get(), losing_keys.get()};
    func->update_batch_single_state(context(), losing_keys->size(), losing_columns, aggr_state->state());
    EXPECT_EQ(0, losing_values->serialize_count());

    auto winning_values = CountingInt32Column::create();
    winning_values->get_data() = {50, 60};
    auto winning_keys = Int32Column::create();
    winning_keys->get_data() = {5, 6};
    const Column* winning_columns[] = {winning_values.get(), winning_keys.get()};
    func->update_batch_single_state(context(), winning_keys->size(), winning_columns, aggr_state->state());
    EXPECT_EQ(1, winning_values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(60, result->get_data()[0]);
}

TEST_F(MaxMinByTest, maxBySkipsNullValueBeforeSelectingChunkWinner) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto value_data = CountingInt32Column::create();
    value_data->get_data() = {10, 20, 30};
    auto* counted_values = value_data.get();
    auto value_nulls = NullColumn::create();
    value_nulls->get_data() = {0, 1, 0};
    auto values = NullableColumn::create(std::move(value_data), std::move(value_nulls));
    auto keys = Int32Column::create();
    keys->get_data() = {1, 3, 2};

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(context(), keys->size(), columns, aggr_state->state());

    EXPECT_EQ(1, counted_values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(30, result->get_data()[0]);
}

TEST_F(MaxMinByTest, maxByV2KeepsNullValueForChunkWinner) {
    const AggregateFunction* func = get_aggregate_function("max_by_v2", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto value_data = CountingInt32Column::create();
    value_data->get_data() = {10, 20, 30};
    auto* counted_values = value_data.get();
    auto value_nulls = NullColumn::create();
    value_nulls->get_data() = {0, 1, 0};
    auto values = NullableColumn::create(std::move(value_data), std::move(value_nulls));
    auto keys = Int32Column::create();
    keys->get_data() = {1, 3, 2};

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state(context(), keys->size(), columns, aggr_state->state());

    EXPECT_EQ(0, counted_values->serialize_count());

    auto result = NullableColumn::create(Int32Column::create(), NullColumn::create());
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_TRUE(result->is_null(0));
}

TEST_F(MaxMinByTest, maxByFrameSerializesWinnerOnce) {
    const AggregateFunction* func = get_aggregate_function("max_by", TYPE_INT, TYPE_INT, false);
    ASSERT_NE(nullptr, func);
    auto aggr_state = ManagedAggrState::create(context(), func);

    auto values = CountingInt32Column::create();
    values->get_data() = {10, 20, 30, 40};
    auto keys = Int32Column::create();
    keys->get_data() = {100, 2, 4, 200};

    const Column* columns[] = {values.get(), keys.get()};
    func->update_batch_single_state_with_frame(context(), aggr_state->state(), columns, 0, keys->size(), 1, 3);

    EXPECT_EQ(1, values->serialize_count());

    auto result = Int32Column::create();
    func->finalize_to_column(context(), aggr_state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(30, result->get_data()[0]);
}

} // namespace starrocks
