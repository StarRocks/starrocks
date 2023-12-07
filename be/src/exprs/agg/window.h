// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_helper.h"
#include "exprs/agg/aggregate.h"

namespace starrocks::vectorized {

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

    void merge_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column* column,
                                  AggDataPtr __restrict state) const override {
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

protected:
    // BinaryColumn column is special, the underlying _bytes and _offsets column don't resize
    void get_slice_values(ConstAggDataPtr __restrict state, Column* dst, size_t start, size_t end) const {
        DCHECK_GT(end, start);
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        NullData& null_data = nullable_column->null_column_data();
        if (AggregateFunctionStateHelper<State>::data(state).is_null) {
            nullable_column->append_nulls(end - start);
            return;
        }

        for (size_t i = start; i < end; ++i) {
            null_data.emplace_back(0);
        }
        Column* data_column = nullable_column->mutable_data_column();
        auto* column = down_cast<BinaryColumn*>(data_column);
        for (size_t i = start; i < end; ++i) {
            column->append(AggregateFunctionStateHelper<State>::data(state).slice());
        }
    }
};

template <PrimitiveType PT, typename State, typename T = RunTimeCppType<PT>>
class ValueWindowFunction : public WindowFunction<State> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    // The dst column has been resized.
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
        T value = AggregateFunctionStateHelper<State>::data(state).value;

        InputColumnType* column = down_cast<InputColumnType*>(data_column);
        for (size_t i = start; i < end; ++i) {
            if constexpr (PT != TYPE_HLL && PT != TYPE_OBJECT) {
                column->get_data()[i] = value;
            } else {
                // For TYPE_HLL(HLL) AND and TYPE_OBJECT(BITMAP),
                // we can't use get_data to write datas.
                *column->get_object(i) = *value;
            }
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
        Int64Column* column = down_cast<Int64Column*>(dst);
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
        Int64Column* column = down_cast<Int64Column*>(dst);
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
        Int64Column* column = down_cast<Int64Column*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).rank;
        }
    }

    std::string get_name() const override { return "dense_rank"; }
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

template <PrimitiveType PT, typename = guard::Guard>
struct FirstValueState {
    using T = RunTimeCppType<PT>;
    T value;
    bool is_null = false;
    bool has_value = false;
};

// TODO(murphy) refactor with AggDataTypeTraits
template <PrimitiveType PT>
struct FirstValueState<PT, StringPTGuard<PT>> {
    Buffer<uint8_t> buffer;
    bool is_null = false;
    bool has_value = false;

    Slice slice() const { return {buffer.data(), buffer.size()}; }
};

template <PrimitiveType PT, bool ignoreNulls, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class FirstValueWindowFunction final : public ValueWindowFunction<PT, FirstValueState<PT>, T> {
    using InputColumnType = typename ValueWindowFunction<PT, FirstValueState<PT>, T>::InputColumnType;

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
            this->data(state).is_null = true;
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
            const InputColumnType* column = down_cast<const InputColumnType*>(data_column);
            this->data(state).is_null = false;
            this->data(state).has_value = true;
            this->data(state).value = column->get_data()[value_index];
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_first_value"; }
};

template <PrimitiveType PT, bool ignoreNulls, typename = guard::Guard>
struct LastValueState {
    using T = RunTimeCppType<PT>;
    T value;
    bool is_null = false;
    bool has_value = false;
};

// TODO(murphy) refactor with AggDataTypeTraits
template <PrimitiveType PT, bool ignoreNulls>
struct LastValueState<PT, ignoreNulls, StringPTGuard<PT>> {
    Buffer<uint8_t> buffer;
    bool is_null = false;
    bool has_value = false;

    Slice slice() const { return {buffer.data(), buffer.size()}; }
};

template <PrimitiveType PT, bool ignoreNulls, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class LastValueWindowFunction final : public ValueWindowFunction<PT, LastValueState<PT, ignoreNulls>, T> {
    using InputColumnType = typename ValueWindowFunction<PT, FirstValueState<PT>, T>::InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value = {};
        this->data(state).is_null = false;
        this->data(state).has_value = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (frame_start >= frame_end) {
            this->data(state).is_null = true;
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
            const InputColumnType* column = down_cast<const InputColumnType*>(data_column);
            this->data(state).is_null = false;
            this->data(state).has_value = true;
            this->data(state).value = column->get_data()[value_index];
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_last_value"; }
};

template <PrimitiveType PT, typename = guard::Guard>
struct LeadLagState {
    using T = RunTimeCppType<PT>;
    T value;
    T default_value;
    bool is_null = false;
    bool defualt_is_null = false;
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class LeadLagWindowFunction final : public ValueWindowFunction<PT, LeadLagState<PT>, T> {
    using InputColumnType = typename ValueWindowFunction<PT, FirstValueState<PT>, T>::InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value = {};
        this->data(state).is_null = false;
        const Column* arg2 = args[2].get();
        DCHECK(arg2->is_constant());
        const auto* default_column = down_cast<const ConstColumn*>(arg2);
        if (default_column->is_nullable()) {
            this->data(state).defualt_is_null = true;
        } else {
            this->data(state).default_value = default_column->get(0).get<T>();
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // frame_end <= frame_start is for lag function
        // frame_end > peer_group_end is for lead function
        if ((frame_end <= frame_start) | (frame_end > peer_group_end)) {
            if (this->data(state).defualt_is_null) {
                this->data(state).is_null = true;
            } else {
                this->data(state).value = this->data(state).default_value;
            }
            return;
        }

        if (columns[0]->is_null(frame_end - 1)) {
            this->data(state).is_null = true;
            return;
        }

        this->data(state).is_null = false;
        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        const InputColumnType* column = down_cast<const InputColumnType*>(data_column);
        this->data(state).value = column->get_data()[frame_end - 1];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_values_helper(state, dst, start, end);
    }

    std::string get_name() const override { return "lead-lag"; }
};

// TODO(murphy) refactor with AggDataTypeTraits
template <PrimitiveType PT, bool ignoreNulls>
class FirstValueWindowFunction<PT, ignoreNulls, Slice, StringPTGuard<PT>> final
        : public WindowFunction<FirstValueState<PT>> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).buffer.clear();
        this->data(state).is_null = false;
        this->data(state).has_value = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // For cases like: rows between 2 preceding and 1 preceding
        // If frame_start ge frame_end, means the frame is empty
        if (frame_start >= frame_end) {
            this->data(state).is_null = true;
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
            const auto* column = down_cast<const BinaryColumn*>(data_column);
            Slice slice = column->get_slice(value_index);
            const auto* p = reinterpret_cast<const uint8_t*>(slice.data);
            this->data(state).buffer.insert(this->data(state).buffer.end(), p, p + slice.size);
            this->data(state).is_null = false;
            this->data(state).has_value = true;
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_slice_values(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_first_value"; }
};

// TODO(murphy) refactor with AggDataTypeTraits
template <PrimitiveType PT, bool ignoreNulls>
class LastValueWindowFunction<PT, ignoreNulls, Slice, StringPTGuard<PT>> final
        : public WindowFunction<LastValueState<PT, ignoreNulls>> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).buffer.clear();
        this->data(state).is_null = false;
        this->data(state).has_value = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (frame_start >= frame_end) {
            this->data(state).is_null = true;
            return;
        }

        size_t value_index =
                !ignoreNulls ? frame_end - 1 : ColumnHelper::last_nonnull(columns[0], frame_start, frame_end);
        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        const auto* column = down_cast<const BinaryColumn*>(data_column);
        if (value_index == frame_end || columns[0]->is_null(value_index)) {
            if (ignoreNulls) {
                this->data(state).is_null = (!this->data(state).has_value);
            } else {
                this->data(state).is_null = true;
                this->data(state).has_value = true;
            }
        } else {
            this->data(state).is_null = false;
            this->data(state).has_value = true;
            Slice slice = column->get_slice(value_index);
            const auto* p = reinterpret_cast<const uint8_t*>(slice.data);
            this->data(state).buffer.clear();
            this->data(state).buffer.insert(this->data(state).buffer.end(), p, p + slice.size);
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_slice_values(state, dst, start, end);
    }

    std::string get_name() const override { return "nullable_last_value"; }
};

template <PrimitiveType PT>
struct LeadLagState<PT, StringPTGuard<PT>> {
    Buffer<uint8_t> value;
    Buffer<uint8_t> default_value;
    bool is_null = false;
    bool defualt_is_null = false;

    Slice slice() const { return {value.data(), value.size()}; }
};

template <PrimitiveType PT>
class LeadLagWindowFunction<PT, Slice, StringPTGuard<PT>> final : public WindowFunction<LeadLagState<PT>> {
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).value.clear();
        this->data(state).is_null = false;
        const Column* arg2 = args[2].get();
        DCHECK(arg2->is_constant());
        const auto* default_column = down_cast<const ConstColumn*>(arg2);
        if (default_column->is_nullable()) {
            this->data(state).defualt_is_null = true;
        } else {
            this->data(state).default_value.clear();
            Slice slice = default_column->get(0).get<Slice>();
            const uint8_t* p = reinterpret_cast<const uint8_t*>(slice.data);
            this->data(state).default_value.insert(this->data(state).default_value.end(), p, p + slice.size);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // frame_end <= frame_start is for lag function
        // frame_end > peer_group_end is for lead function
        if ((frame_end <= frame_start) | (frame_end > peer_group_end)) {
            if (this->data(state).defualt_is_null) {
                this->data(state).is_null = true;
            } else {
                this->data(state).value = this->data(state).default_value;
            }
            return;
        }

        if (columns[0]->is_null(frame_end - 1)) {
            this->data(state).is_null = true;
            return;
        }

        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        const auto* column = down_cast<const BinaryColumn*>(data_column);
        Slice slice = column->get_slice(frame_end - 1);
        const uint8_t* p = reinterpret_cast<const uint8_t*>(slice.data);
        this->data(state).value.insert(this->data(state).value.end(), p, p + slice.size);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        this->get_slice_values(state, dst, start, end);
    }

    std::string get_name() const override { return "lead-lag"; }
};

} // namespace starrocks::vectorized
