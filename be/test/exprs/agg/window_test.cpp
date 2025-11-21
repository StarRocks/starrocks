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
#include "exprs/agg/window.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "exprs/agg/aggregate_factory.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/counting_allocator.h"
#include "testutil/function_utils.h"

namespace starrocks {

class LagWindowTest : public testing::Test {
public:
    LagWindowTest() = default;

    void SetUp() override {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
        _allocator = std::make_unique<CountingAllocatorWithHook>();
    }
    void TearDown() override {
        delete utils;
        _allocator.reset();
    }

private:
    FunctionUtils* utils{};
    FunctionContext* ctx{};
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

class ManagedAggrState {
public:
    ~ManagedAggrState() { _func->destroy(_ctx, _state); }
    static std::unique_ptr<ManagedAggrState> create(FunctionContext* ctx, const AggregateFunction* func) {
        return std::make_unique<ManagedAggrState>(ctx, func);
    }
    AggDataPtr state() { return _state; }

private:
    ManagedAggrState(FunctionContext* ctx, const AggregateFunction* func) : _ctx(ctx), _func(func) {
        _state = _mem_pool.allocate_aligned(func->size(), func->alignof_size());
        _func->create(_ctx, _state);
    }
    FunctionContext* _ctx;
    const AggregateFunction* _func;
    MemPool _mem_pool;
    AggDataPtr _state;
};

static inline Columns build_lag_args(const ColumnPtr& value, int64_t offset, const ColumnPtr& default_val) {
    auto offset_col = ColumnHelper::create_const_column<TYPE_BIGINT>(offset, value->size());
    Columns cols;
    cols.emplace_back(value);       // arg0 : value column
    cols.emplace_back(offset_col);  // arg1 : offset
    cols.emplace_back(default_val); // arg2 : default
    return cols;
}

static inline Columns build_args_with_custom_offset(const ColumnPtr& value, const ColumnPtr& offset_const,
                                                    const ColumnPtr& default_val) {
    Columns cols;
    cols.emplace_back(value);        // arg0 : value column
    cols.emplace_back(offset_const); // arg1 : offset (custom const, may be nullable)
    cols.emplace_back(default_val);  // arg2 : default
    return cols;
}

TEST_F(LagWindowTest, test_basic_lag) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(0, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        int32_t expected = (row == 0) ? 0 : (row == 1) ? 10 : (row == 2) ? 0 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_lag_ignore_nulls) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        int32_t expected = (row == 0) ? 99 : (row == 1) ? 10 : (row == 2) ? 10 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_default_value_is_col) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(i + 1);
        default_null_col->append(0);
    }
    ColumnPtr default_col = NullableColumn::create(default_data_col, default_null_col);

    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());
    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        int32_t expected = (row == 0) ? 1 : (row == 1) ? 10 : (row == 2) ? 0 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_default_value_is_col_and_ignore_nulls) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(i + 1);
        default_null_col->append(0);
    }

    ColumnPtr default_col = NullableColumn::create(default_data_col, default_null_col);

    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        int32_t expected = (row == 0) ? 1 : (row == 1) ? 10 : (row == 2) ? 10 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_default_value_is_null) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr col_ptr = NullableColumn::create(default_data_col, default_null_col);
    auto default_col = ConstColumn::create(col_ptr->as_mutable_ptr(), col_ptr->size());
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        int32_t expected = (row == 0) ? 0 : (row == 1) ? 10 : (row == 2) ? 0 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_default_value_is_null_ignore_nulls) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr col_ptr = NullableColumn::create(default_data_col, default_null_col);
    auto default_col = ConstColumn::create(col_ptr->as_mutable_ptr(), col_ptr->size());
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        int32_t expected = (row == 0) ? 0 : (row == 1) ? 10 : (row == 2) ? 10 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_default_col_is_null_ignore_nulls) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr default_col = NullableColumn::create(default_data_col, default_null_col);
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        int32_t expected = (row == 0) ? 0 : (row == 1) ? 10 : (row == 2) ? 10 : (row == 3) ? 30 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_lead_default_col_is_null_ignore_nulls) {
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    Int32Column::Ptr default_data_col = Int32Column::create();
    NullColumn::Ptr default_null_col = NullColumn::create();

    for (int i = 0; i < 5; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr default_col = NullableColumn::create(default_data_col, default_null_col);
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lead", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row + offset;
        int64_t frame_end = frame_start + 1; // half-open [start, end)

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        int32_t expected = (row == 0) ? 0 : (row == 1) ? 30 : (row == 2) ? 40 : (row == 3) ? 0 : -1;

        if (lag_state->is_null) {
            ASSERT_EQ(expected, 0) << "row=" << row;
        } else {
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

// New tests to improve branch coverage for lead/lag implementation

TEST_F(LagWindowTest, test_lag_offset_is_null_sets_zero) {
    // value column with mixed nulls
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    // Build a nullable const offset column -> offset treated as 0
    Int64Column::Ptr off_data = Int64Column::create();
    NullColumn::Ptr off_null = NullColumn::create();
    off_data->append(0);
    off_null->append(1); // NULL offset
    ColumnPtr off_nullable = NullableColumn::create(off_data, off_null);
    ColumnPtr offset_const = ConstColumn::create(off_nullable->as_mutable_ptr(), value_col->size());

    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    Columns args = build_args_with_custom_offset(value_col, offset_const, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        // offset=0 => frame_start=row, frame_end=row+1 -> read current row
        int64_t frame_start = row;
        int64_t frame_end = frame_start + 1;
        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);
        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        // Expected equals current row's value; if current is NULL, result is null (not default)
        if (row == 1) {
            ASSERT_TRUE(lag_state->is_null) << "row=" << row;
        } else {
            ASSERT_FALSE(lag_state->is_null) << "row=" << row;
            int32_t expected = (row == 0) ? 10 : (row == 2) ? 30 : 40;
            ASSERT_EQ(expected, lag_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_lead_ignore_nulls_basic) {
    // value column with mixed nulls
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(data_col, null_col);
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(77, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lead_func = get_aggregate_function("lead_in", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lead_func);
    lead_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        // lead(ignore nulls): frame_start=row+offset, frame_end=frame_start+1
        int64_t frame_start = row + offset;
        int64_t frame_end = frame_start + 1;
        lead_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                        frame_end);

        auto* lead_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        int32_t expected = (row == 0) ? 30 : (row == 1) ? 30 : (row == 2) ? 40 : 77;
        if (expected == 77) {
            ASSERT_FALSE(lead_state->is_null) << "row=" << row;
            ASSERT_EQ(expected, lead_state->value) << "row=" << row;
        } else {
            ASSERT_FALSE(lead_state->is_null) << "row=" << row;
            ASSERT_EQ(expected, lead_state->value) << "row=" << row;
        }
    }
}

TEST_F(LagWindowTest, test_non_const_default_out_of_range_oob) {
    // value column with mixed nulls
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    // default column: empty (size=0) -> def_col_ok=false when out-of-range, expect NULL
    Int32Column::Ptr def_data = Int32Column::create();
    NullColumn::Ptr def_null = NullColumn::create();
    ColumnPtr default_col = NullableColumn::create(def_data, def_null);

    const int64_t offset = 1;
    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    // row=0 triggers out_of_range (frame_start=-1)
    int64_t frame_start = -1;
    int64_t frame_end = 0;
    lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, value_col->size(),
                                                   frame_start, frame_end);
    auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
    ASSERT_TRUE(lag_state->is_null) << "expected NULL due to default column OOB";
}

TEST_F(LagWindowTest, test_non_const_default_out_of_range_inbounds) {
    // value column with mixed nulls
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    // default column: sized properly
    Int32Column::Ptr def_data = Int32Column::create();
    NullColumn::Ptr def_null = NullColumn::create();
    def_data->append(100);
    def_null->append(0);
    def_data->append(200);
    def_null->append(0);
    def_data->append(300);
    def_null->append(0);
    def_data->append(400);
    def_null->append(0);
    ColumnPtr default_col = NullableColumn::create(def_data, def_null);

    const int64_t offset = 1;
    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    // row=0 triggers out_of_range; current_row_index = frame_end - 1 + offset = 0 - 1 + 1 = 0
    // For lag: current_row_index = 0 + offset = 1
    int64_t frame_start = -1;
    int64_t frame_end = 0;
    lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, value_col->size(),
                                                   frame_start, frame_end);
    auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
    ASSERT_FALSE(lag_state->is_null);
    ASSERT_EQ(100, lag_state->value) << "expect default_col[row=1] as fallback";
}

TEST_F(LagWindowTest, test_normal_window_null_value_no_default_applied) {
    // value column with a null at row=1; default constant should NOT apply in normal-window path
    Int32Column::Ptr data_col = Int32Column::create();
    NullColumn::Ptr null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1); // NULL at row=1
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(data_col, null_col);

    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(123, value_col->size());
    const int64_t offset = 1;
    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;
        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);
        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/false>*>(state->state());
        // At row=2, target row is row=1 which is NULL; expect is_null=true (default NOT applied in normal window)
        if (row == 2) {
            ASSERT_TRUE(lag_state->is_null) << "default should not apply in normal window when value is null";
        }
    }
}

TEST_F(LagWindowTest, test_lag_array_default_const_ignore_nulls_fallback) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    ArrayColumn::Ptr arr = ArrayColumn::create(elem, offs);
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    ArrayColumn::Ptr def_arr = ArrayColumn::create(def_elem, def_offs);
    def_arr->append_datum(DatumArray{(int32_t)99, (int32_t)100});
    ColumnPtr default_const = ConstColumn::create(def_arr->as_mutable_ptr(), value_col->size());

    const int64_t offset = 2;
    Columns args = build_lag_args(value_col, offset, default_const);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* func = get_aggregate_function("lag_in", TYPE_ARRAY, TYPE_ARRAY, true);
    auto state = ManagedAggrState::create(ctx, func);
    func->reset(ctx, args, state->state());

    int64_t N = value_col->size();
    int64_t row = 0;
    int64_t frame_start = row - offset;
    int64_t frame_end = frame_start + 1;
    func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start, frame_end);

    auto* s = reinterpret_cast<LeadLagState<TYPE_ARRAY, true>*>(state->state());
    ASSERT_FALSE(s->default_is_null);
    ASSERT_TRUE(s->default_value_is_constant);
    ASSERT_FALSE(s->is_null);
    auto res = s->value->get(0).get<DatumArray>();
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res[0].get<int32_t>(), 99);
    ASSERT_EQ(res[1].get<int32_t>(), 100);
}

TEST_F(LagWindowTest, test_lead_array_default_const_non_ignore_outside_window) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    ArrayColumn::Ptr arr = ArrayColumn::create(elem, offs);
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    ArrayColumn::Ptr def_arr = ArrayColumn::create(def_elem, def_offs);
    def_arr->append_datum(DatumArray{(int32_t)7, (int32_t)8});
    ColumnPtr default_const = ConstColumn::create(def_arr->as_mutable_ptr(), value_col->size());

    const int64_t offset = 1;
    Columns args = build_lag_args(value_col, offset, default_const);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* func = get_aggregate_function("lead", TYPE_ARRAY, TYPE_ARRAY, true);
    auto state = ManagedAggrState::create(ctx, func);
    func->reset(ctx, args, state->state());

    int64_t N = value_col->size();
    int64_t row = N - 1;
    int64_t frame_start = row + offset;
    int64_t frame_end = frame_start + 1;
    func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start, frame_end);

    auto* s = reinterpret_cast<LeadLagState<TYPE_ARRAY, false>*>(state->state());
    ASSERT_FALSE(s->is_null);
    auto res = s->value->get(0).get<DatumArray>();
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res[0].get<int32_t>(), 7);
    ASSERT_EQ(res[1].get<int32_t>(), 8);
}

TEST_F(LagWindowTest, test_array_non_const_default_out_of_range_sets_null_ignore_nulls) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    ArrayColumn::Ptr arr = ArrayColumn::create(elem, offs);
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    ArrayColumn::Ptr def_arr = ArrayColumn::create(def_elem, def_offs);
    def_arr->append_datum(DatumArray{(int32_t)500});
    def_arr->append_datum(DatumArray{(int32_t)600});
    ColumnPtr default_col = NullableColumn::create(def_arr->as_mutable_ptr(), NullColumn::create(2, 0));

    const int64_t offset = 1;
    Columns args = build_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* func = get_aggregate_function("lead_in", TYPE_ARRAY, TYPE_ARRAY, true);
    auto state = ManagedAggrState::create(ctx, func);
    func->reset(ctx, args, state->state());

    int64_t N = value_col->size();
    int64_t row = N - 1;
    int64_t frame_start = row + offset;
    int64_t frame_end = frame_start + 1;
    func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start, frame_end);

    auto* s = reinterpret_cast<LeadLagState<TYPE_ARRAY, true>*>(state->state());
    ASSERT_TRUE(s->is_null);
}

} // namespace starrocks