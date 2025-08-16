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

} // namespace starrocks