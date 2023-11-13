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

#pragma once
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"

namespace starrocks {

template <typename State>
class WindowFunction : public AggregateFunctionStateHelper<State> {
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* column, size_t start,
                                  size_t size) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** column,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void batch_finalize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }
};

template <LogicalType LT, typename State, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class ValueWindowFunction : public WindowFunction<State> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    /// The dst column has been resized.
    void get_values_helper(ConstAggDataPtr __restrict state, Column* dst, size_t start, size_t end) const {
        DCHECK_GT(end, start);
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        if (AggregateFunctionStateHelper<State>::data(state).is_null) {
            for (size_t i = start; i < end; ++i) {
                nullable_column->set_null(i);
            }
            return;
        }

        Column* data_column = nullable_column->mutable_data_column();
        auto* column = down_cast<InputColumnType*>(data_column);
        auto value = AggregateFunctionStateHelper<State>::data(state).value;
        for (size_t i = start; i < end; ++i) {
            AggDataTypeTraits<LT>::assign_value(column, i, value);
        }
    }
};

template <LogicalType LT, typename State, typename T>
class ValueWindowFunction<LT, State, T, StringLTGuard<LT>> : public WindowFunction<State> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    /// TODO: do not hack the string type
    /// The dst BinaryColumn hasn't been resized, because the underlying _bytes and _offsets column couldn't be resized.
    void get_values_helper(ConstAggDataPtr __restrict state, Column* dst, size_t start, size_t end) const {
        DCHECK_GT(end, start);
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        if (AggregateFunctionStateHelper<State>::data(state).is_null) {
            nullable_column->append_nulls(end - start);
            return;
        }

        NullData& null_data = nullable_column->null_column_data();
        for (size_t i = start; i < end; ++i) {
            null_data.emplace_back(0);
        }

        Column* data_column = nullable_column->mutable_data_column();
        auto* column = down_cast<InputColumnType*>(data_column);
        auto value = AggregateFunctionStateHelper<State>::data(state).value;
        for (size_t i = start; i < end; ++i) {
            AggDataTypeTraits<LT>::append_value(column, value);
        }
    }
};

struct RowNumberState {
    int64_t cur_positon;
};

class RowNumberWindowFunction final : public WindowFunction<RowNumberState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).cur_positon = 0;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        this->data(state).cur_positon++;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        column->get_data()[start] = this->data(state).cur_positon;
    }

    std::string get_name() const override { return "row_number"; }
};

struct RankState {
    int64_t rank;
    int64_t count;
    int64_t peer_group_start;
};

class RankWindowFunction final : public WindowFunction<RankState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).rank = 0;
        this->data(state).count = 1;
        this->data(state).peer_group_start = -1;
    }

    void reset_state_for_contraction(FunctionContext* ctx, AggDataPtr __restrict state, size_t count) const override {
        this->data(state).peer_group_start -= count;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        int64_t peer_group_count = peer_group_end - peer_group_start;
        if (this->data(state).peer_group_start != peer_group_start) {
            this->data(state).peer_group_start = peer_group_start;
            this->data(state).rank += this->data(state).count;
        }
        this->data(state).count = peer_group_count;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).rank;
        }
    }

    std::string get_name() const override { return "rank"; }
};

struct DenseRankState {
    int64_t rank;
    int64_t peer_group_start;
};

class DenseRankWindowFunction final : public WindowFunction<DenseRankState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).rank = 0;
        this->data(state).peer_group_start = -1;
    }

    void reset_state_for_contraction(FunctionContext* ctx, AggDataPtr __restrict state, size_t count) const override {
        this->data(state).peer_group_start -= count;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (this->data(state).peer_group_start != peer_group_start) {
            this->data(state).peer_group_start = peer_group_start;
            this->data(state).rank++;
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).rank;
        }
    }

    std::string get_name() const override { return "dense_rank"; }
};

struct CumeDistState {
    int64_t rank;
    int64_t peer_group_start;
    int64_t count;
};

class CumeDistWindowFunction final : public WindowFunction<CumeDistState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& s = this->data(state);
        s.rank = 0;
        s.peer_group_start = -1;
        s.count = 1;
    }

    void reset_state_for_contraction(FunctionContext* ctx, AggDataPtr __restrict state, size_t count) const override {
        this->data(state).peer_group_start -= count;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        auto& s = this->data(state);
        if (s.peer_group_start != peer_group_start) {
            s.peer_group_start = peer_group_start;
            int64_t peer_group_count = peer_group_end - peer_group_start;
            s.rank += peer_group_count;
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto& s = this->data(state);
        auto* column = down_cast<DoubleColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = (double)s.rank / s.count;
        }
    }

    std::string get_name() const override { return "cume_dist"; }
};

struct PercentRankState : CumeDistState {
    int64_t peer_group_count;
};

class PercentRankWindowFunction final : public WindowFunction<PercentRankState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& s = this->data(state);
        s.rank = 0;
        s.peer_group_start = -1;
        s.peer_group_count = 1;
        s.count = 1;
    }

    void reset_state_for_contraction(FunctionContext* ctx, AggDataPtr __restrict state, size_t count) const override {
        this->data(state).peer_group_start -= count;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        int64_t peer_group_count = peer_group_end - peer_group_start;
        auto& s = this->data(state);
        if (s.peer_group_start != peer_group_start) {
            s.peer_group_start = peer_group_start;
            s.rank += s.peer_group_count;
        }
        s.peer_group_count = peer_group_count;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto& s = this->data(state);
        auto* column = down_cast<DoubleColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            if (s.count > 1) {
                column->get_data()[i] = (double)(s.rank - 1) / (s.count - 1);
            } else {
                column->get_data()[i] = (double)0;
            }
        }
    }

    std::string get_name() const override { return "percent_rank"; }
};

// The NTILE window function divides ordered rows in the partition into `num_buckets` ranked groups
// of as equal size as possible and returns the group id of each row starting from 1.
//
// It can not been used with the windowing clause.
// And for the implementation, it uses `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
//
// The size of buckets could be `num_partition_rows/num_buckets` (small bucket)
// or `num_partition_rows/num_buckets+1` (large bucket).
// The top `num_partition_rows%num_buckets` buckets are the large buckets.
struct NtileState {
    int64_t num_buckets = 0;

    int64_t large_bucket_size = 0;
    int64_t small_bucket_size = 0;

    int64_t num_large_buckets = 0;
    int64_t num_large_bucket_rows = 0;

    // Start from 0.
    int64_t cur_position = -1;
};

class NtileWindowFunction final : public WindowFunction<NtileState> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).num_buckets = args[0]->get(0).get_int64();

        // Start from 0 and used after increment, so set -1 before the first increment.
        this->data(state).cur_position = -1;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        auto& s = this->data(state);

        if (-1 == s.cur_position) {
            int64_t num_rows = peer_group_end - peer_group_start;

            s.small_bucket_size = num_rows / s.num_buckets;
            s.large_bucket_size = s.small_bucket_size + 1;

            s.num_large_buckets = num_rows % s.num_buckets;
            s.num_large_bucket_rows = s.num_large_buckets * s.large_bucket_size;
        }

        ++s.cur_position;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_EQ(end, start + 1);
        auto* column = down_cast<Int64Column*>(dst);
        const auto& s = this->data(state);

        if (s.cur_position < s.num_large_bucket_rows) {
            column->get_data()[start] = s.cur_position / s.large_bucket_size + 1;
        } else {
            column->get_data()[start] =
                    (s.cur_position - s.num_large_bucket_rows) / s.small_bucket_size + s.num_large_buckets + 1;
        }
    }

    std::string get_name() const override { return "ntile"; }
};

template <LogicalType LT>
struct FirstValueState {
    using T = AggDataValueType<LT>;
    T value;
    bool is_null = false;
    bool has_value = false;
};

template <LogicalType LT, bool ignoreNulls, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class FirstValueWindowFunction final : public ValueWindowFunction<LT, FirstValueState<LT>, T> {
    using InputColumnType = typename ValueWindowFunction<LT, FirstValueState<LT>, T>::InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value = {};
        this->data(state).is_null = false;
        this->data(state).has_value = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // For cases like: rows between 2 preceding and 1 preceding
        // If frame_start ge frame_end, means the frame is empty
        if (frame_start >= frame_end) {
            if (!this->data(state).has_value) {
                this->data(state).is_null = true;
            }
            return;
        }

        // only calculate once
        if (this->data(state).has_value && (!this->data(state).is_null || !ignoreNulls)) {
            return;
        }

        size_t value_index =
                !ignoreNulls ? frame_start : ColumnHelper::find_nonnull(columns[0], frame_start, frame_end);
        if (value_index == frame_end || columns[0]->is_null(value_index)) {
            this->data(state).is_null = true;
            if (!ignoreNulls) {
                this->data(state).has_value = true;
            }
        } else {
            const Column* data_column = ColumnHelper::get_data_column(columns[0]);
            const auto* column = down_cast<const InputColumnType*>(data_column);
            this->data(state).is_null = false;
            this->data(state).has_value = true;
            AggDataTypeTraits<LT>::assign_value(this->data(state).value,
                                                AggDataTypeTraits<LT>::get_row_ref(*column, value_index));
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_first_value"; }
};

template <LogicalType LT, bool ignoreNulls, typename = guard::Guard>
struct LastValueState {
    using T = AggDataValueType<LT>;
    T value;
    bool is_null = false;
    bool has_value = false;
};

template <LogicalType LT, bool ignoreNulls, typename T = RunTimeCppType<LT>>
class LastValueWindowFunction final : public ValueWindowFunction<LT, LastValueState<LT, ignoreNulls>, T> {
    using InputColumnType = typename ValueWindowFunction<LT, FirstValueState<LT>, T>::InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value = {};
        this->data(state).is_null = false;
        this->data(state).has_value = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (frame_start >= frame_end) {
            if (!this->data(state).has_value) {
                this->data(state).is_null = true;
            }
            return;
        }

        size_t value_index =
                !ignoreNulls ? frame_end - 1 : ColumnHelper::last_nonnull(columns[0], frame_start, frame_end);
        if (value_index == frame_end || columns[0]->is_null(value_index)) {
            if (ignoreNulls) {
                this->data(state).is_null = (!this->data(state).has_value);
            } else {
                this->data(state).is_null = true;
                this->data(state).has_value = true;
            }
        } else {
            const Column* data_column = ColumnHelper::get_data_column(columns[0]);
            const auto* column = down_cast<const InputColumnType*>(data_column);
            this->data(state).is_null = false;
            this->data(state).has_value = true;
            AggDataTypeTraits<LT>::assign_value(this->data(state).value,
                                                AggDataTypeTraits<LT>::get_row_ref(*column, value_index));
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_last_value"; }
};

template <LogicalType LT, typename = guard::Guard>
struct LeadLagState {
    using T = AggDataValueType<LT>;
    T value;
    int64_t offset = 0;
    T default_value;
    bool is_null = false;
    bool default_is_null = false;
};

template <LogicalType LT, bool ignoreNulls, bool isLag, typename T = RunTimeCppType<LT>>
class LeadLagWindowFunction final : public ValueWindowFunction<LT, LeadLagState<LT>, T> {
    using InputColumnType = typename ValueWindowFunction<LT, FirstValueState<LT>, T>::InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value = {};
        this->data(state).is_null = false;

        // get offset
        const Column* arg1 = args[1].get();
        DCHECK(arg1->is_constant());
        const auto* offset_column = down_cast<const ConstColumn*>(arg1);
        if (offset_column->is_nullable()) {
            this->data(state).offset = 0;
        } else {
            this->data(state).offset = ColumnHelper::get_const_value<LogicalType::TYPE_BIGINT>(arg1);
        }

        // get default value
        const Column* arg2 = args[2].get();
        DCHECK(arg2->is_constant());
        const auto* default_column = down_cast<const ConstColumn*>(arg2);
        if (default_column->is_nullable()) {
            this->data(state).default_is_null = true;
        } else {
            auto value = ColumnHelper::get_const_value<LT>(arg2);
            AggDataTypeTraits<LT>::assign_value(this->data(state).default_value, value);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // frame_start < peer_group_start is for lag function
        // frame_end > peer_group_end is for lead function
        if ((frame_start < peer_group_start) | (frame_end > peer_group_end)) {
            if (this->data(state).default_is_null) {
                this->data(state).is_null = true;
            } else {
                this->data(state).is_null = false;
                this->data(state).value = this->data(state).default_value;
            }
            return;
        }

        // for lead/lag, [peer_group_start, peer_group_end] equals to [partition_start, partition_end]
        // when lead/lag called, the whole partitoin's data has already been here, so we can just check all the way to the begining or the end
        if (ignoreNulls) {
            const int64_t offset = this->data(state).offset;
            // lead(v1 ignore nulls, <offset>) has window `ROWS BETWEEN UNBOUNDED PRECEDING AND <offset> FOLLOWING`
            //      frame_start = partition_start
            //      frame_end = current_row + <offset> + 1
            //      current_row = frame_end - 1 - <offset>
            //
            // lag(v1 ignore nulls, <offset>) has window `ROWS BETWEEN UNBOUNDED PRECEDING AND <offset> PRECEDING`
            //      frame_start = partition_start
            //      frame_end = current_row - <offset> + 1
            //      current_row = frame_end - 1 + <offset>
            int64_t current_row = frame_end - 1 + (isLag ? offset : -offset);
            if (current_row < peer_group_start) {
                current_row = peer_group_start;
            } else if (current_row >= peer_group_end) {
                current_row = peer_group_end - 1;
            }

            int64_t cnt = offset;
            size_t value_index = current_row;
            if (isLag) {
                // Look backward, find <offset>-th non-null value
                while (value_index > peer_group_start && cnt > 0) {
                    int64_t next_index = ColumnHelper::last_nonnull(columns[0], peer_group_start, value_index);
                    if (next_index == value_index) {
                        break;
                    }
                    value_index = next_index;
                    DCHECK_GE(value_index, peer_group_start);
                    cnt--;
                }
            } else {
                // Look forward, find <offset>-th non-null value
                while (value_index < peer_group_end && cnt > 0) {
                    int64_t next_index = ColumnHelper::find_nonnull(columns[0], value_index + 1, peer_group_end);
                    if (next_index == peer_group_end) {
                        break;
                    }
                    value_index = next_index;
                    DCHECK_LE(value_index, peer_group_end);
                    cnt--;
                }
            }
            DCHECK_GE(value_index, peer_group_start);
            DCHECK_LE(value_index, peer_group_end);
            if (cnt > 0 || value_index == peer_group_end || columns[0]->is_null(value_index)) {
                if (this->data(state).default_is_null) {
                    this->data(state).is_null = true;
                } else {
                    this->data(state).value = this->data(state).default_value;
                }
            } else {
                const Column* data_column = ColumnHelper::get_data_column(columns[0]);
                const auto* column = down_cast<const InputColumnType*>(data_column);
                this->data(state).is_null = false;
                AggDataTypeTraits<LT>::assign_value(this->data(state).value,
                                                    AggDataTypeTraits<LT>::get_row_ref(*column, value_index));
            }
        } else {
            if (!columns[0]->is_null(frame_end - 1)) {
                this->data(state).is_null = false;
                const Column* data_column = ColumnHelper::get_data_column(columns[0]);
                const auto* column = down_cast<const InputColumnType*>(data_column);
                AggDataTypeTraits<LT>::assign_value(this->data(state).value,
                                                    AggDataTypeTraits<LT>::get_row_ref(*column, frame_end - 1));
            } else {
                this->data(state).is_null = true;
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "lead-lag"; }
};

// result value
template <LogicalType LT>
struct AllocateSessionState {
    using T = AggDataValueType<LT>;
    int64_t session_id{};
    T last_not_null_value{};
    bool is_null{};
    bool has_value{};
    int32_t delta{};
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class SessionNumberWindowFunction final : public WindowFunction<AllocateSessionState<LT>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).session_id = 1;
        this->data(state).last_not_null_value = {};
        this->data(state).is_null = false;
        this->data(state).has_value = false;

        const Column* delta_column = args[1].get();
        DCHECK(delta_column->is_constant());
        if (!delta_column->only_null() && !delta_column->empty()) {
            this->data(state).delta = ColumnHelper::get_const_value<LogicalType::TYPE_INT>(args[1]);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        const InputColumnType* column = down_cast<const InputColumnType*>(data_column);

        DCHECK(frame_start < frame_end);
        if (frame_start > frame_end) {
            return;
        }

        auto delta = this->data(state).delta;
        size_t current_row = frame_end - 1;
        if (columns[0]->is_null(current_row)) {
            this->data(state).is_null = true;
        } else {
            auto current_value = AggDataTypeTraits<LT>::get_row_ref(*column, current_row);
            if (this->data(state).has_value && current_value - this->data(state).last_not_null_value > delta) {
                this->data(state).session_id++;
            }
            this->data(state).is_null = false;
            this->data(state).last_not_null_value = AggDataTypeTraits<LT>::get_row_ref(*column, frame_start);
        }
        this->data(state).has_value = true;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        auto& s = this->data(state);
        if (dst->is_nullable()) {
            auto* nullable_dst = down_cast<NullableColumn*>(dst);
            auto* data_column = down_cast<Int64Column*>(nullable_dst->data_column().get());
            for (size_t i = start; i < end; ++i) {
                data_column->get_data()[i] = s.session_id;
            }
            if (s.is_null) {
                for (size_t i = start; i < end; ++i) {
                    nullable_dst->set_null(i);
                }
            }
        } else {
            auto* data_column = down_cast<Int64Column*>(dst);
            for (size_t i = start; i < end; ++i) {
                data_column->get_data()[i] = s.session_id;
            }
        }
    }

    std::string get_name() const override { return "session_number"; }
};

} // namespace starrocks
