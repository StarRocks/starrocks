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
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "exec/analytor.h"
#include "exprs/agg/aggregate_factory.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/runtime_state.h"
#include "testutil/function_utils.h"

namespace starrocks {

class LeadLagWindowTest : public testing::Test {
public:
    LeadLagWindowTest() = default;

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

static inline Columns build_lead_lag_args(const ColumnPtr& value, int64_t offset, const ColumnPtr& default_val) {
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

TEST_F(LeadLagWindowTest, test_basic_lag) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(0, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_default_value_is_col_and_ignore_nulls) {
    // lag_in (ignoreNulls=true) with non-constant default column [1, 2, 3, 4]
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();
    for (int i = 0; i < 4; ++i) {
        default_data_col->append(i + 1);
        default_null_col->append(0);
    }
    ColumnPtr default_col = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, /*is_nullable*/ true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    // row0: no previous non-null -> default_col[0]=1
    // row1: previous non-null is row0=10
    // row2: skip null row1, previous non-null is row0=10
    // row3: previous non-null is row2=30
    int32_t expected_vals[] = {1, 10, 10, 30};
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(),
                                                       /*peer_group_start*/ 0,
                                                       /*peer_group_end*/ N, frame_start, frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        ASSERT_FALSE(lag_state->is_null) << "row=" << row;
        ASSERT_EQ(expected_vals[row], lag_state->value) << "row=" << row;
    }
}

TEST_F(LeadLagWindowTest, test_lag_ignore_nulls) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_default_value_is_col) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(i + 1);
        default_null_col->append(0);
    }
    ColumnPtr default_col = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));

    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_lag_large_binary) {
    auto data_col = LargeBinaryColumn::create();
    auto null_col = NullColumn::create();

    data_col->append(Slice("a"));
    null_col->append(0);
    data_col->append(Slice("b"));
    null_col->append(0);
    data_col->append(Slice("c"));
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("z"), value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_VARCHAR, TYPE_VARCHAR, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    std::vector<std::string> expected{"z", "a", "b"};
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_VARCHAR, /*ignoreNulls=*/false>*>(state->state());
        ASSERT_FALSE(lag_state->is_null) << "row=" << row;
        Slice value = AggDataTypeTraits<TYPE_VARCHAR>::get_ref(lag_state->value);
        ASSERT_EQ(expected[row], value.to_string()) << "row=" << row;
    }
}

TEST_F(LeadLagWindowTest, test_default_value_is_null) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    auto col_ptr = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));
    size_t size = col_ptr->size();
    auto default_col = ConstColumn::create(std::move(col_ptr), size);
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_default_value_is_null_ignore_nulls) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    auto col_ptr = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));
    size_t size = col_ptr->size();
    auto default_col = ConstColumn::create(std::move(col_ptr), size);
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_default_col_is_null_ignore_nulls) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();

    for (int i = 0; i < 4; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr default_col = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_lead_default_col_is_null_ignore_nulls) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto default_data_col = Int32Column::create();
    auto default_null_col = NullColumn::create();

    for (int i = 0; i < 5; ++i) {
        default_data_col->append(0);
        default_null_col->append(1);
    }
    ColumnPtr default_col = NullableColumn::create(std::move(default_data_col), std::move(default_null_col));
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_lag_offset_is_null_sets_zero) {
    // value column with mixed nulls
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    // Build a nullable const offset column -> offset treated as 0
    auto off_data = Int64Column::create();
    auto off_null = NullColumn::create();
    off_data->append(0);
    off_null->append(1); // NULL offset
    ColumnPtr off_nullable = NullableColumn::create(std::move(off_data), std::move(off_null));
    size_t size = off_nullable->size();
    ColumnPtr offset_const = ConstColumn::create(std::move(off_nullable), size);

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

TEST_F(LeadLagWindowTest, test_lead_ignore_nulls_basic) {
    // value column with mixed nulls
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();

    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(77, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

// `lead ... IGNORE NULLS` must not treat a missing future non-null as "use the default" while the
// partition may still grow. `is_window_result_ready` is the contract Analytor will wait on.
//
// Frame encoding matches streaming N FOLLOWING: frame_end = current + offset + 1 (half-open).
TEST_F(LeadLagWindowTest, test_lead_ignore_nulls_readiness_waits_for_future_nonnull) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(0);
    null_col->append(1);
    data_col->append(0);
    null_col->append(1);
    data_col->append(10);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;
    const int64_t current = 0;
    const int64_t frame_start = current + offset;
    const int64_t frame_end = frame_start + 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    const AggregateFunction* lead_func = get_aggregate_function("lead_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lead_func);
    lead_func->reset(ctx, args, state->state());

    // Only the first NULL is "buffered": no non-null after current.
    ASSERT_FALSE(lead_func->is_window_result_ready(ctx, state->state(), args, /*partition_start=*/0,
                                                   /*available_end=*/1, frame_start, frame_end,
                                                   /*partition_is_complete=*/false));
    // Second row is also NULL.
    ASSERT_FALSE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, 2, frame_start, frame_end, false));
    // 10 is now in range (current, available_end) = (0, 3).
    ASSERT_TRUE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, 3, frame_start, frame_end, false));
}

TEST_F(LeadLagWindowTest, test_lead_ignore_nulls_readiness_offset2_needs_two_nonnulls) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(0);
    null_col->append(1); // 0
    data_col->append(10);
    null_col->append(0); // 1
    data_col->append(0);
    null_col->append(1); // 2
    data_col->append(20);
    null_col->append(0); // 3
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 2;
    const int64_t current = 0;
    const int64_t frame_start = current + offset;
    const int64_t frame_end = frame_start + 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    const AggregateFunction* lead_func = get_aggregate_function("lead_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lead_func);
    lead_func->reset(ctx, args, state->state());

    // Through index 2: only one non-null (10) after current.
    ASSERT_FALSE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, 3, frame_start, frame_end, false));
    // 20 arrives: two non-nulls after current.
    ASSERT_TRUE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, 4, frame_start, frame_end, false));
}

TEST_F(LeadLagWindowTest, test_lead_ignore_nulls_readiness_complete_partition_uses_default) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(0);
    null_col->append(1);
    data_col->append(0);
    null_col->append(1);
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;
    const int64_t current = 0;
    const int64_t frame_start = current + offset;
    const int64_t frame_end = frame_start + 1;
    const int64_t n = static_cast<int64_t>(value_col->size());

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    const AggregateFunction* lead_func = get_aggregate_function("lead_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lead_func);
    lead_func->reset(ctx, args, state->state());

    ASSERT_FALSE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, n, frame_start, frame_end, false));
    // EOS: not enough non-nulls, but the default is now a valid result.
    ASSERT_TRUE(lead_func->is_window_result_ready(ctx, state->state(), args, 0, n, frame_start, frame_end, true));
}

TEST_F(LeadLagWindowTest, test_lag_ignore_nulls_readiness_always_true) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;
    const int64_t current = 0;
    const int64_t frame_start = current - offset;
    const int64_t frame_end = frame_start + 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    // LAG only reads history; the physical frame being present is enough.
    ASSERT_TRUE(lag_func->is_window_result_ready(ctx, state->state(), args, 0, 1, frame_start, frame_end, false));
}

TEST_F(LeadLagWindowTest, test_non_const_default_out_of_range_oob) {
    // value column with mixed nulls
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    // default column: empty (size=0) -> def_col_ok=false when out-of-range, expect NULL
    auto def_data = Int32Column::create();
    auto def_null = NullColumn::create();
    ColumnPtr default_col = NullableColumn::create(std::move(def_data), std::move(def_null));

    const int64_t offset = 1;
    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_non_const_default_out_of_range_inbounds) {
    // value column with mixed nulls
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(10);
    null_col->append(0);
    data_col->append(0);
    null_col->append(1);
    data_col->append(30);
    null_col->append(0);
    data_col->append(40);
    null_col->append(0);
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    // default column: sized properly
    auto def_data = Int32Column::create();
    auto def_null = NullColumn::create();
    def_data->append(100);
    def_null->append(0);
    def_data->append(200);
    def_null->append(0);
    def_data->append(300);
    def_null->append(0);
    def_data->append(400);
    def_null->append(0);
    ColumnPtr default_col = NullableColumn::create(std::move(def_data), std::move(def_null));

    const int64_t offset = 1;
    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_normal_window_null_value_no_default_applied) {
    // value column with a null at row=1; default constant should NOT apply in normal-window path
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
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
    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_lag_array_default_const_ignore_nulls_fallback) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    auto arr = ArrayColumn::create(std::move(elem), std::move(offs));
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    auto def_arr = ArrayColumn::create(std::move(def_elem), std::move(def_offs));
    def_arr->append_datum(DatumArray{(int32_t)99, (int32_t)100});
    size_t size = def_arr->size();
    ColumnPtr default_const = ConstColumn::create(std::move(def_arr), size);

    const int64_t offset = 2;
    Columns args = build_lead_lag_args(value_col, offset, default_const);
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

TEST_F(LeadLagWindowTest, test_lead_array_default_const_non_ignore_outside_window) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    auto arr = ArrayColumn::create(std::move(elem), std::move(offs));
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    auto def_arr = ArrayColumn::create(std::move(def_elem), std::move(def_offs));
    def_arr->append_datum(DatumArray{(int32_t)7, (int32_t)8});
    size_t size = def_arr->size();
    ColumnPtr default_const = ConstColumn::create(std::move(def_arr), size);

    const int64_t offset = 1;
    Columns args = build_lead_lag_args(value_col, offset, default_const);
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

TEST_F(LeadLagWindowTest, test_array_non_const_default_out_of_range_sets_null_ignore_nulls) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    auto arr = ArrayColumn::create(std::move(elem), std::move(offs));
    arr->append_datum(DatumArray{(int32_t)10});
    arr->append_datum(DatumArray{(int32_t)20});
    arr->append_datum(DatumArray{(int32_t)30});
    arr->append_datum(DatumArray{(int32_t)40});
    ColumnPtr value_col = arr;

    auto def_elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto def_offs = UInt32Column::create();
    auto def_arr = ArrayColumn::create(std::move(def_elem), std::move(def_offs));
    def_arr->append_datum(DatumArray{(int32_t)500});
    def_arr->append_datum(DatumArray{(int32_t)600});
    auto null_col = NullColumn::create(2, 0);
    ColumnPtr default_col = NullableColumn::create(std::move(def_arr), std::move(null_col));

    const int64_t offset = 1;
    Columns args = build_lead_lag_args(value_col, offset, default_col);
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

TEST_F(LeadLagWindowTest, test_lag_large_binary_non_const_default) {
    // LargeBinaryColumn as value, regular BinaryColumn as non-const default.
    // This tests the fix for wrong down_cast on columns[2] when columns[0] is LargeBinaryColumn.
    auto data_col = LargeBinaryColumn::create();
    auto null_col = NullColumn::create();

    data_col->append(Slice("alpha"));
    null_col->append(0);
    data_col->append(Slice("beta"));
    null_col->append(0);
    data_col->append(Slice("gamma"));
    null_col->append(0);

    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));

    // Default column is regular BinaryColumn (not LargeBinaryColumn)
    auto def_data_col = BinaryColumn::create();
    auto def_null_col = NullColumn::create();
    def_data_col->append(Slice("d0"));
    def_null_col->append(0);
    def_data_col->append(Slice("d1"));
    def_null_col->append(0);
    def_data_col->append(Slice("d2"));
    def_null_col->append(0);
    ColumnPtr default_col = NullableColumn::create(std::move(def_data_col), std::move(def_null_col));

    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag", TYPE_VARCHAR, TYPE_VARCHAR, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    const int64_t N = value_col->size();
    // row0: out of range -> default_col[0]="d0"
    // row1: lag(1) -> row0="alpha"
    // row2: lag(1) -> row1="beta"
    std::vector<std::string> expected{"d0", "alpha", "beta"};
    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;

        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);

        auto* lag_state = reinterpret_cast<LeadLagState<TYPE_VARCHAR, /*ignoreNulls=*/false>*>(state->state());
        ASSERT_FALSE(lag_state->is_null) << "row=" << row;
        Slice value = AggDataTypeTraits<TYPE_VARCHAR>::get_ref(lag_state->value);
        ASSERT_EQ(expected[row], value.to_string()) << "row=" << row;
    }
}

TEST_F(LeadLagWindowTest, test_lag_ignore_nulls_all_null_needs_no_retention) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    const int64_t N = 8;
    for (int i = 0; i < N; ++i) {
        data_col->append(0);
        null_col->append(1); // all NULL
    }
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;
        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);
        ASSERT_EQ(std::nullopt, lag_func->get_min_retained_position(ctx, state->state())) << "row=" << row;
    }
}

// With regularly-occurring non-nulls the retained watermark tracks the most recent non-null (never
// pinned to the partition start), and reset_state_for_contraction shifts it into the operator's
// post-eviction coordinates.
TEST_F(LeadLagWindowTest, test_lag_ignore_nulls_watermark_tracks_recent_nonnull) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    const int64_t N = 8;
    for (int i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            data_col->append((i + 1) * 10);
            null_col->append(0);
        } else {
            data_col->append(0);
            null_col->append(1);
        }
    }
    ColumnPtr value_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto default_col = ColumnHelper::create_const_column<TYPE_INT>(99, value_col->size());
    const int64_t offset = 1;

    Columns args = build_lead_lag_args(value_col, offset, default_col);
    std::vector<const Column*> raw_cols{args[0].get(), args[1].get(), args[2].get()};

    const AggregateFunction* lag_func = get_aggregate_function("lag_in", TYPE_INT, TYPE_INT, true);
    auto state = ManagedAggrState::create(ctx, lag_func);
    lag_func->reset(ctx, args, state->state());

    for (int64_t row = 0; row < N; ++row) {
        int64_t frame_start = row - offset;
        int64_t frame_end = frame_start + 1;
        lag_func->update_batch_single_state_with_frame(ctx, state->state(), raw_cols.data(), 0, N, frame_start,
                                                       frame_end);
        auto* s = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
        auto wm = lag_func->get_min_retained_position(ctx, state->state());
        ASSERT_EQ(s->target_not_null_index, wm) << "row=" << row;
        ASSERT_GE(wm.value(), row - 2) << "row=" << row; // bounded near current, not pinned to 0
    }

    auto* s = reinterpret_cast<LeadLagState<TYPE_INT, /*ignoreNulls=*/true>*>(state->state());
    int64_t before = s->target_not_null_index;
    lag_func->reset_state_for_contraction(ctx, state->state(), 2);
    ASSERT_EQ(before - 2, s->target_not_null_index);
}

// ===================================================================================================
// End-to-end correctness: drive the real Analytor operator for `lag(col, offset) IGNORE NULLS` and
// verify the emitted output column matches an independent reference implementation, with the streaming
// + watermark-eviction path both DISABLED (legacy materializing) and ENABLED. This is what exercises
// Analytor::_remove_unused_rows + reset_state_for_contraction under real chunking/eviction, which the
// function-level tests above never touch.
// ===================================================================================================
namespace {

using OptInt = std::optional<int32_t>;

TTypeDesc make_scalar_ttype(TPrimitiveType::type t) {
    TTypeDesc desc;
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar;
    scalar.__set_type(t);
    node.__set_scalar_type(scalar);
    desc.types.push_back(node);
    return desc;
}

TExprNode make_slot_ref(TupleId tuple, SlotId slot, const TTypeDesc& ttype) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(ttype);
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot);
    slot_ref.__set_tuple_id(tuple);
    node.__set_slot_ref(slot_ref);
    return node;
}

TExprNode make_bigint_literal(int64_t v) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_type(make_scalar_ttype(TPrimitiveType::BIGINT));
    node.__set_num_children(0);
    node.__set_is_nullable(false);
    TIntLiteral lit;
    lit.__set_value(v);
    node.__set_int_literal(lit);
    return node;
}

TExprNode make_null_literal(const TTypeDesc& ttype) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(ttype);
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    return node;
}

// lag(<slot>, offset, NULL) IGNORE NULLS, ROWS UNBOUNDED PRECEDING AND <offset> PRECEDING, single partition.
TPlanNode make_lag_tnode(TupleId in_tuple_id, SlotId col_slot_id, int64_t offset) {
    const TTypeDesc int_type = make_scalar_ttype(TPrimitiveType::INT);
    TExpr fn_call;
    TExprNode agg;
    agg.__set_node_type(TExprNodeType::AGG_EXPR);
    agg.__set_num_children(3);
    agg.__set_type(int_type);
    agg.__set_has_nullable_child(true);
    agg.__set_is_nullable(true);
    {
        TAggregateExpr agg_expr;
        agg_expr.__set_is_merge_agg(false);
        agg.__set_agg_expr(agg_expr);
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_function_name("lag");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_arg_types(std::vector<TTypeDesc>{int_type});
        fn.__set_ret_type(int_type);
        fn.__set_has_var_args(false);
        fn.__set_ignore_nulls(true);
        agg.__set_fn(fn);
    }
    fn_call.nodes.push_back(agg);
    fn_call.nodes.push_back(make_slot_ref(in_tuple_id, col_slot_id, int_type));
    fn_call.nodes.push_back(make_bigint_literal(offset));
    fn_call.nodes.push_back(make_null_literal(int_type));

    TAnalyticWindow window;
    window.__set_type(TAnalyticWindowType::ROWS);
    {
        TAnalyticWindowBoundary end;
        end.__set_type(TAnalyticWindowBoundaryType::PRECEDING);
        end.__set_rows_offset_value(offset);
        window.__set_window_end(end); // window_start unset => UNBOUNDED PRECEDING
    }
    TAnalyticNode anode;
    anode.__set_window(window);
    anode.__set_buffered_tuple_id(in_tuple_id);
    anode.analytic_functions.push_back(fn_call);

    TPlanNode tnode;
    tnode.__set_node_id(0);
    tnode.__set_node_type(TPlanNodeType::ANALYTIC_EVAL_NODE);
    tnode.__set_limit(-1);
    tnode.__set_analytic_node(anode);
    return tnode;
}

// lead(<slot>, offset, NULL) IGNORE NULLS, ROWS UNBOUNDED PRECEDING AND <offset> FOLLOWING,
// single partition.
TPlanNode make_lead_tnode(TupleId in_tuple_id, SlotId col_slot_id, int64_t offset) {
    const TTypeDesc int_type = make_scalar_ttype(TPrimitiveType::INT);
    TExpr fn_call;
    TExprNode agg;
    agg.__set_node_type(TExprNodeType::AGG_EXPR);
    agg.__set_num_children(3);
    agg.__set_type(int_type);
    agg.__set_has_nullable_child(true);
    agg.__set_is_nullable(true);
    {
        TAggregateExpr agg_expr;
        agg_expr.__set_is_merge_agg(false);
        agg.__set_agg_expr(agg_expr);
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_function_name("lead");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_arg_types(std::vector<TTypeDesc>{int_type});
        fn.__set_ret_type(int_type);
        fn.__set_has_var_args(false);
        fn.__set_ignore_nulls(true);
        agg.__set_fn(fn);
    }
    fn_call.nodes.push_back(agg);
    fn_call.nodes.push_back(make_slot_ref(in_tuple_id, col_slot_id, int_type));
    fn_call.nodes.push_back(make_bigint_literal(offset));
    fn_call.nodes.push_back(make_null_literal(int_type));

    TAnalyticWindow window;
    window.__set_type(TAnalyticWindowType::ROWS);
    {
        TAnalyticWindowBoundary end;
        end.__set_type(TAnalyticWindowBoundaryType::FOLLOWING);
        end.__set_rows_offset_value(offset);
        window.__set_window_end(end); // window_start unset => UNBOUNDED PRECEDING
    }
    TAnalyticNode anode;
    anode.__set_window(window);
    anode.__set_buffered_tuple_id(in_tuple_id);
    anode.analytic_functions.push_back(fn_call);

    TPlanNode tnode;
    tnode.__set_node_id(0);
    tnode.__set_node_type(TPlanNodeType::ANALYTIC_EVAL_NODE);
    tnode.__set_limit(-1);
    tnode.__set_analytic_node(anode);
    return tnode;
}

// Reference: lag(x, offset) IGNORE NULLS with NULL default.
std::vector<OptInt> ref_lag_ignore_nulls(const std::vector<OptInt>& in, int64_t offset) {
    std::vector<OptInt> out(in.size());
    std::vector<int32_t> seen; // non-null values strictly before current, in order
    for (size_t i = 0; i < in.size(); ++i) {
        if (static_cast<int64_t>(seen.size()) >= offset) {
            out[i] = seen[seen.size() - offset];
        } else {
            out[i] = std::nullopt;
        }
        if (in[i].has_value()) {
            seen.push_back(*in[i]);
        }
    }
    return out;
}

// Reference: lead(x, offset) IGNORE NULLS with NULL default.
std::vector<OptInt> ref_lead_ignore_nulls(const std::vector<OptInt>& in, int64_t offset) {
    std::vector<OptInt> out(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        int64_t remaining = offset;
        for (size_t j = i + 1; j < in.size(); ++j) {
            if (in[j].has_value() && --remaining == 0) {
                out[i] = in[j];
                break;
            }
        }
    }
    return out;
}

ColumnPtr build_nullable_int_column(const std::vector<OptInt>& vals, size_t begin, size_t count) {
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    for (size_t i = 0; i < count; ++i) {
        const OptInt& v = vals[begin + i];
        data->append(v.has_value() ? *v : 0);
        nulls->append(v.has_value() ? 0 : 1);
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

// Drive the real Analytor and return the lag output for each input row (in input order).
std::vector<OptInt> run_analytor_lag(const std::vector<OptInt>& input, int64_t offset, bool streaming,
                                     int64_t chunk_rows) {
    config::pipeline_analytic_enable_ignore_nulls_streaming = streaming;
    // Small eviction batch so the streaming path actually evicts/contracts many times over the run.
    config::pipeline_analytic_removable_chunk_num = 2;

    ObjectPool pool;
    TDescriptorTableBuilder dtb;
    {
        TTupleDescriptorBuilder in_tuple;
        in_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("v").build());
        in_tuple.build(&dtb);
        TTupleDescriptorBuilder out_tuple;
        out_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("lag_v").build());
        out_tuple.build(&dtb);
    }
    auto* state = pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
    DescriptorTbl* desc_tbl = nullptr;
    CHECK(DescriptorTbl::create(state, &pool, dtb.desc_tbl(), &desc_tbl, config::vector_chunk_size).ok());
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();

    const TupleId in_tuple_id = 0;
    const TupleId out_tuple_id = 1;
    const SlotId col_slot_id = desc_tbl->get_tuple_descriptor(in_tuple_id)->slots()[0]->id();
    const SlotId res_slot_id = desc_tbl->get_tuple_descriptor(out_tuple_id)->slots()[0]->id();
    TupleDescriptor* result_tuple = desc_tbl->get_tuple_descriptor(out_tuple_id);

    TPlanNode tnode = make_lag_tnode(in_tuple_id, col_slot_id, offset);
    RuntimeProfile profile("Analytor");
    auto analytor = std::make_shared<Analytor>(tnode, result_tuple, false);
    CHECK(analytor->prepare(state, &pool, &profile).ok());
    const auto process_mode = profile.get_info_string("ProcessMode");
    CHECK(process_mode.has_value());
    CHECK(streaming ? process_mode->find("Streaming/") == 0 : process_mode->find("Materializing/") == 0)
            << "lag ProcessMode=" << *process_mode << " streaming=" << streaming;
    CHECK(analytor->open(state).ok());

    std::vector<OptInt> out;
    auto collect = [&](const ChunkPtr& chunk) {
        if (chunk == nullptr) return;
        ColumnPtr res = chunk->get_column_by_slot_id(res_slot_id);
        for (size_t i = 0; i < res->size(); ++i) {
            if (res->is_null(i)) {
                out.push_back(std::nullopt);
            } else {
                out.push_back(static_cast<int32_t>(res->get(i).get_int32()));
            }
        }
    };

    size_t fed = 0;
    while (fed < input.size()) {
        const size_t n = std::min<size_t>(chunk_rows, input.size() - fed);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(build_nullable_int_column(input, fed, n), col_slot_id);
        CHECK(analytor->process(state, chunk).ok());
        fed += n;
        while (ChunkPtr o = analytor->poll_chunk_buffer()) collect(o);
    }
    CHECK(analytor->finish_process(state).ok());
    while (ChunkPtr o = analytor->poll_chunk_buffer()) collect(o);
    analytor->close(state);
    return out;
}

// Drive the real Analytor and return the lead output for each input row (in input order).
std::vector<OptInt> run_analytor_lead(const std::vector<OptInt>& input, int64_t offset, bool streaming,
                                      int64_t chunk_rows) {
    config::pipeline_analytic_enable_ignore_nulls_streaming = streaming;
    // Small eviction batch so the future streaming path actually evicts/contracts during these tests.
    config::pipeline_analytic_removable_chunk_num = 2;

    ObjectPool pool;
    TDescriptorTableBuilder dtb;
    {
        TTupleDescriptorBuilder in_tuple;
        in_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("v").build());
        in_tuple.build(&dtb);
        TTupleDescriptorBuilder out_tuple;
        out_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("lead_v").build());
        out_tuple.build(&dtb);
    }
    auto* state = pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
    DescriptorTbl* desc_tbl = nullptr;
    CHECK(DescriptorTbl::create(state, &pool, dtb.desc_tbl(), &desc_tbl, config::vector_chunk_size).ok());
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();

    const TupleId in_tuple_id = 0;
    const TupleId out_tuple_id = 1;
    const SlotId col_slot_id = desc_tbl->get_tuple_descriptor(in_tuple_id)->slots()[0]->id();
    const SlotId res_slot_id = desc_tbl->get_tuple_descriptor(out_tuple_id)->slots()[0]->id();
    TupleDescriptor* result_tuple = desc_tbl->get_tuple_descriptor(out_tuple_id);

    TPlanNode tnode = make_lead_tnode(in_tuple_id, col_slot_id, offset);
    RuntimeProfile profile("Analytor");
    auto analytor = std::make_shared<Analytor>(tnode, result_tuple, false);
    CHECK(analytor->prepare(state, &pool, &profile).ok());
    const auto process_mode = profile.get_info_string("ProcessMode");
    CHECK(process_mode.has_value());
    CHECK(streaming ? process_mode->find("Streaming/") == 0 : process_mode->find("Materializing/") == 0)
            << "lead ProcessMode=" << *process_mode << " streaming=" << streaming;
    CHECK(analytor->open(state).ok());

    std::vector<OptInt> out;
    auto collect = [&](const ChunkPtr& chunk) {
        if (chunk == nullptr) return;
        ColumnPtr res = chunk->get_column_by_slot_id(res_slot_id);
        for (size_t i = 0; i < res->size(); ++i) {
            if (res->is_null(i)) {
                out.push_back(std::nullopt);
            } else {
                out.push_back(static_cast<int32_t>(res->get(i).get_int32()));
            }
        }
    };

    size_t fed = 0;
    while (fed < input.size()) {
        const size_t n = std::min<size_t>(chunk_rows, input.size() - fed);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(build_nullable_int_column(input, fed, n), col_slot_id);
        CHECK(analytor->process(state, chunk).ok());
        fed += n;
        while (ChunkPtr o = analytor->poll_chunk_buffer()) collect(o);
    }
    CHECK(analytor->finish_process(state).ok());
    while (ChunkPtr o = analytor->poll_chunk_buffer()) collect(o);
    analytor->close(state);
    return out;
}

std::vector<OptInt> pattern_column(int pattern, size_t n, uint32_t seed) {
    std::vector<OptInt> v(n);
    std::mt19937 rng(seed);
    for (size_t i = 0; i < n; ++i) {
        bool is_null;
        switch (pattern) {
        case 0: // all null
            is_null = true;
            break;
        case 1: // dense
            is_null = false;
            break;
        case 2: // sparse: one non-null every 64 rows
            is_null = (i % 64 != 0);
            break;
        case 3: // head then long null tail (the watermark caveat case)
            is_null = (i != 0);
            break;
        default: // random ~1/3 null
            is_null = (rng() % 3 == 0);
            break;
        }
        v[i] = is_null ? OptInt(std::nullopt) : OptInt(static_cast<int32_t>(i + 1));
    }
    return v;
}

void expect_equal(const std::vector<OptInt>& got, const std::vector<OptInt>& expected, const std::string& tag) {
    ASSERT_EQ(expected.size(), got.size()) << tag;
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].has_value(), got[i].has_value()) << tag << " null-mismatch at row " << i;
        if (expected[i].has_value()) {
            ASSERT_EQ(*expected[i], *got[i]) << tag << " value-mismatch at row " << i;
        }
    }
}

} // namespace

// Streaming output must equal both the legacy (materializing) output and the independent reference,
// across data shapes, offsets, and chunk sizes (which shift partition/eviction boundaries).
TEST_F(LeadLagWindowTest, e2e_streaming_matches_reference_and_legacy) {
    const size_t n = 5000;
    const int patterns[] = {0, 1, 2, 3, 4};
    const int64_t offsets[] = {1, 2, 3};
    const int64_t chunk_sizes[] = {1, 7, 256, 4096};

    for (int p : patterns) {
        const auto input = pattern_column(p, n, /*seed*/ 1000 + p);
        for (int64_t off : offsets) {
            const auto expected = ref_lag_ignore_nulls(input, off);
            for (int64_t cs : chunk_sizes) {
                const std::string tag =
                        "p=" + std::to_string(p) + " off=" + std::to_string(off) + " chunk=" + std::to_string(cs);
                expect_equal(run_analytor_lag(input, off, /*streaming*/ false, cs), expected, "legacy " + tag);
                expect_equal(run_analytor_lag(input, off, /*streaming*/ true, cs), expected, "streaming " + tag);
            }
        }
    }
}

// The exact column discussed for offset 2: NULL,NULL,1,NULL,1,NULL,1,1 (values distinguished so we can
// see which physical non-null is picked). Verifies streaming and legacy agree with the reference.
TEST_F(LeadLagWindowTest, e2e_offset2_sparse_explicit) {
    std::vector<OptInt> input{std::nullopt, std::nullopt, 10, std::nullopt, 20, std::nullopt, 30, 40};
    const int64_t offset = 2;
    const auto expected = ref_lag_ignore_nulls(input, offset);
    // sanity-check the reference itself: 2nd-most-recent non-null strictly before each row.
    // rows:          0    1    2      3     4      5     6     7
    // value:         -    -    10     -     20     -     30    40
    // non-nulls before row: {} {} {}   {10}  {10}  {10,20} {10,20} {10,20,30}
    // 2nd-most-recent:  -    -    -      -     -      10     10     20
    std::vector<OptInt> hand{std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 10, 10, 20};
    expect_equal(expected, hand, "reference-self-check");

    for (int64_t cs : {1, 3, 8}) {
        expect_equal(run_analytor_lag(input, offset, false, cs), expected, "legacy cs=" + std::to_string(cs));
        expect_equal(run_analytor_lag(input, offset, true, cs), expected, "streaming cs=" + std::to_string(cs));
    }
}

TEST_F(LeadLagWindowTest, e2e_lead_ignore_nulls_matches_reference_and_legacy) {
    const std::vector<std::vector<OptInt>> inputs{
            // A future non-null arrives two chunks after the current row when chunk_rows=1.
            {std::nullopt, std::nullopt, 10},
            // The last offset rows have no possible result and must use the NULL default.
            {10, 20, 30, 40},
            // LEAD cannot determine any result until it sees the real partition end.
            {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
            // A long null gap exercises data-dependent look-ahead.
            {10, std::nullopt, std::nullopt, 20, std::nullopt, std::nullopt, 30, 40},
    };
    const int64_t offsets[] = {1, 2};
    const int64_t chunk_sizes[] = {1, 2, 3, 8};

    for (size_t input_idx = 0; input_idx < inputs.size(); ++input_idx) {
        const auto& input = inputs[input_idx];
        for (int64_t offset : offsets) {
            const auto expected = ref_lead_ignore_nulls(input, offset);
            for (int64_t chunk_rows : chunk_sizes) {
                const std::string tag = "input=" + std::to_string(input_idx) + " offset=" + std::to_string(offset) +
                                        " chunk_rows=" + std::to_string(chunk_rows);
                const auto legacy = run_analytor_lead(input, offset, /*streaming=*/false, chunk_rows);
                const auto streaming = run_analytor_lead(input, offset, /*streaming=*/true, chunk_rows);
                expect_equal(legacy, expected, "legacy " + tag);
                expect_equal(streaming, expected, "streaming " + tag);
                expect_equal(streaming, legacy, "streaming-vs-legacy " + tag);
            }
        }
    }
}

} // namespace starrocks
