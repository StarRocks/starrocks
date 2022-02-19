// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include <utility>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/agg/maxmin.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

template <typename T>
constexpr bool IsWindowFunctionSliceState = false;

template <>
constexpr bool IsWindowFunctionSliceState<MaxAggregateData<TYPE_VARCHAR>> = true;

template <>
constexpr bool IsWindowFunctionSliceState<MinAggregateData<TYPE_VARCHAR>> = true;

template <typename T>
struct NullableAggregateFunctionState {
    using NestedState = T;

    NullableAggregateFunctionState() : _nested_state() {}

    AggDataPtr mutable_nest_state() { return reinterpret_cast<AggDataPtr>(&_nested_state); }

    ConstAggDataPtr nested_state() const { return reinterpret_cast<ConstAggDataPtr>(&_nested_state); }

    bool is_null{true};
    T _nested_state;
};

// This class wrap an aggregate function and handle NULL value.
// If an aggregate function has at least one nullable argument, we should use this class.
// If all row all are NULL, we will return NULL.
// The State must be NullableAggregateFunctionState
template <typename State>
class NullableAggregateFunctionBase : public AggregateFunctionStateHelper<State> {
public:
    explicit NullableAggregateFunctionBase(AggregateFunctionPtr nested_function_)
            : nested_function(std::move(nested_function_)) {}

    std::string get_name() const override { return "nullable " + nested_function->get_name(); }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).is_null = true;
        nested_function->reset(ctx, args, this->data(state).mutable_nest_state());
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const Column* data_column = nullptr;
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(column);
            if (!nullable_column->null_column()->get_data()[row_num]) {
                this->data(state).is_null = false;
                data_column = nullable_column->data_column().get();
                nested_function->merge(ctx, data_column, this->data(state).mutable_nest_state(), row_num);
            }
        } else {
            this->data(state).is_null = false;
            nested_function->merge(ctx, column, this->data(state).mutable_nest_state(), row_num);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(to);
        if (LIKELY(!this->data(state).is_null)) {
            nested_function->serialize_to_column(ctx, this->data(state).nested_state(),
                                                 nullable_column->mutable_data_column());
            nullable_column->null_column_data().push_back(0);
        } else {
            nullable_column->append_default();
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (LIKELY(!this->data(state).is_null)) {
            if (to->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(to);
                nested_function->finalize_to_column(ctx, this->data(state).nested_state(),
                                                    nullable_column->mutable_data_column());
                nullable_column->null_column_data().push_back(0);
            } else {
                nested_function->finalize_to_column(ctx, this->data(state).nested_state(), to);
            }
        } else {
            to->append_default();
        }
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            serialize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    void batch_finalize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            finalize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
        if (src[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[0].get());
            if (nullable_column->has_null()) {
                dst_nullable_column->set_has_null(true);
                const NullData& src_null_data = nullable_column->immutable_null_column_data();
                size_t null_size = SIMD::count_nonzero(src_null_data);
                if (null_size == chunk_size) {
                    dst_nullable_column->append_nulls(chunk_size);
                } else {
                    NullData& dst_null_data = dst_nullable_column->null_column_data();
                    dst_null_data = src_null_data;
                    Columns src_data_columns(1);
                    src_data_columns[0] = nullable_column->data_column();
                    nested_function->convert_to_serialize_format(src_data_columns, chunk_size,
                                                                 &dst_nullable_column->data_column());
                }
            } else {
                dst_nullable_column->null_column_data().resize(chunk_size);

                Columns src_data_columns(1);
                src_data_columns[0] = nullable_column->data_column();
                nested_function->convert_to_serialize_format(src_data_columns, chunk_size,
                                                             &dst_nullable_column->data_column());
            }
        } else {
            dst_nullable_column->null_column_data().resize(chunk_size);
            nested_function->convert_to_serialize_format(src, chunk_size, &dst_nullable_column->data_column());
        }
    }

    using NestedState = typename State::NestedState;

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        // binary column couldn't call resize method like Numeric Column
        // for non-slice type, null column data has been reset to zero in AnalyticNode
        // for slice type, we need to emplace back null data
        if (!this->data(state).is_null) {
            nested_function->get_values(ctx, this->data(state).nested_state(), nullable_column->mutable_data_column(),
                                        start, end);
            if constexpr (IsWindowFunctionSliceState<NestedState>) {
                NullData& null_data = nullable_column->null_column_data();
                null_data.insert(null_data.end(), end - start, 0);
            }
        } else {
            NullData& null_data = nullable_column->null_column_data();
            if constexpr (IsWindowFunctionSliceState<NestedState>) {
                nullable_column->append_nulls(end - start);
            } else {
                for (size_t i = start; i < end; ++i) {
                    null_data[i] = 1;
                }
            }
            nullable_column->set_has_null(true);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            // TODO: optimize with simd ?
            if (filter[i] == 0) {
                merge(ctx, column, states[i] + state_offset, i);
            }
        }
    }

    void merge_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column* column,
                                  AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            merge(ctx, column, state, i);
        }
    }

protected:
    AggregateFunctionPtr nested_function;
};

template <typename State>
class NullableAggregateFunctionUnary final : public NullableAggregateFunctionBase<State> {
public:
    explicit NullableAggregateFunctionUnary(const AggregateFunctionPtr& nested_function)
            : NullableAggregateFunctionBase<State>(nested_function) {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {}

    // TODO(kks): abstract the AVX2 filter process later
    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (columns[0]->is_nullable()) {
            const auto* column = down_cast<const NullableColumn*>(columns[0]);
            const Column* data_column = &column->data_column_ref();
            const uint8_t* f_data = column->null_column()->raw_data();
            int offset = 0;
#ifdef __AVX2__
            // !important: filter must be an uint8_t container
            constexpr int batch_nums = 256 / (8 * sizeof(uint8_t));
            __m256i all0 = _mm256_setzero_si256();
            while (offset + batch_nums < chunk_size) {
                // TODO(kks): when our memory allocate could align 32-byte, we could use _mm256_load_si256
                __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + offset));
                int mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));
                // all not null
                if (mask == 0) {
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        this->data(states[i] + state_offset).is_null = false;
                        this->nested_function->update(ctx, &data_column,
                                                      this->data(states[i] + state_offset).mutable_nest_state(), i);
                    }
                    // all null
                } else if (mask == 0xffffffff) {
                } else {
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        if (f_data[i] == 0) {
                            this->data(states[i] + state_offset).is_null = false;
                            this->nested_function->update(ctx, &data_column,
                                                          this->data(states[i] + state_offset).mutable_nest_state(), i);
                        }
                    }
                }
                offset += batch_nums;
            }
#endif
            for (size_t i = offset; i < chunk_size; ++i) {
                if (f_data[i] == 0) {
                    this->data(states[i] + state_offset).is_null = false;
                    this->nested_function->update(ctx, &data_column,
                                                  this->data(states[i] + state_offset).mutable_nest_state(), i);
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                this->data(states[i] + state_offset).is_null = false;
                this->nested_function->update(ctx, columns, this->data(states[i] + state_offset).mutable_nest_state(),
                                              i);
            }
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& selection) const override {
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (columns[0]->is_nullable()) {
            const auto* column = down_cast<const NullableColumn*>(columns[0]);
            const Column* data_column = &column->data_column_ref();
            const uint8_t* f_data = column->null_column()->raw_data();
            int offset = 0;

#ifdef __AVX2__
            // !important: filter must be an uint8_t container
            constexpr int batch_nums = 256 / (8 * sizeof(uint8_t));
            __m256i all0 = _mm256_setzero_si256();
            while (offset + batch_nums < chunk_size) {
                // TODO(kks): when our memory allocate could align 32-byte, we could use _mm256_load_si256
                __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + offset));
                int mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));
                // all not null
                if (mask == 0) {
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        // TODO: optimize with simd
                        if (!selection[i]) {
                            this->data(states[i] + state_offset).is_null = false;
                            this->nested_function->update(ctx, &data_column,
                                                          this->data(states[i] + state_offset).mutable_nest_state(), i);
                        }
                    }
                    // all null
                } else if (mask == 0xffffffff) {
                } else {
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        if (!f_data[i] & !selection[i]) {
                            this->data(states[i] + state_offset).is_null = false;
                            this->nested_function->update(ctx, &data_column,
                                                          this->data(states[i] + state_offset).mutable_nest_state(), i);
                        }
                    }
                }
                offset += batch_nums;
            }
#endif

            for (size_t i = offset; i < chunk_size; ++i) {
                if (!f_data[i] & !selection[i]) {
                    this->data(states[i] + state_offset).is_null = false;
                    this->nested_function->update(ctx, &data_column,
                                                  this->data(states[i] + state_offset).mutable_nest_state(), i);
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                if (!selection[i]) {
                    this->data(states[i] + state_offset).is_null = false;
                    this->nested_function->update(ctx, columns,
                                                  this->data(states[i] + state_offset).mutable_nest_state(), i);
                }
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (columns[0]->is_nullable()) {
            const auto* column = down_cast<const NullableColumn*>(columns[0]);
            const Column* data_column = &column->data_column_ref();

            // The fast pass
            if (!column->has_null()) {
                this->data(state).is_null = false;
                this->nested_function->update_batch_single_state(ctx, chunk_size, &data_column,
                                                                 this->data(state).mutable_nest_state());
                return;
            }

            const uint8_t* f_data = column->null_column()->raw_data();
            int offset = 0;
#ifdef __AVX2__
            // !important: filter must be an uint8_t container
            constexpr int batch_nums = 256 / (8 * sizeof(uint8_t));
            __m256i all0 = _mm256_setzero_si256();
            while (offset + batch_nums < chunk_size) {
                __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + offset));
                int mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));
                // all not null
                if (mask == 0) {
                    this->data(state).is_null = false;
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        this->nested_function->update(ctx, &data_column, this->data(state).mutable_nest_state(), i);
                    }
                    // all null
                } else if (mask == 0xffffffff) {
                } else {
                    for (size_t i = offset; i < offset + batch_nums; i++) {
                        if (f_data[i] == 0) {
                            this->nested_function->update(ctx, &data_column, this->data(state).mutable_nest_state(), i);
                            this->data(state).is_null = false;
                        }
                    }
                }
                offset += batch_nums;
            }
#endif

            for (size_t i = offset; i < chunk_size; ++i) {
                if (f_data[i] == 0) {
                    this->nested_function->update(ctx, &data_column, this->data(state).mutable_nest_state(), i);
                    this->data(state).is_null = false;
                }
            }
        } else {
            this->data(state).is_null = false;
            this->nested_function->update_batch_single_state(ctx, chunk_size, columns,
                                                             this->data(state).mutable_nest_state());
        }
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        // For cases like: rows between 2 preceding and 1 preceding
        // Please refer to AnalyticNode::_update_window_batch_normal
        // If frame_start ge frame_end, means the frame is empty,
        // we could directly return.
        if (frame_start >= frame_end) {
            return;
        }

        if (columns[0]->is_nullable()) {
            const auto* column = down_cast<const NullableColumn*>(columns[0]);
            const Column* data_column = &column->data_column_ref();

            // The fast pass
            if (!column->has_null()) {
                this->data(state).is_null = false;
                this->nested_function->update_batch_single_state(ctx, this->data(state).mutable_nest_state(),
                                                                 &data_column, peer_group_start, peer_group_end,
                                                                 frame_start, frame_end);
                return;
            }

            const uint8_t* f_data = column->null_column()->raw_data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                if (f_data[i] == 0) {
                    this->data(state).is_null = false;
                    this->nested_function->update_batch_single_state(ctx, this->data(state).mutable_nest_state(),
                                                                     &data_column, peer_group_start, peer_group_end, i,
                                                                     i + 1);
                }
            }
        } else {
            this->data(state).is_null = false;
            this->nested_function->update_batch_single_state(ctx, this->data(state).mutable_nest_state(), columns,
                                                             peer_group_start, peer_group_end, frame_start, frame_end);
        }
    }
};

template <typename State>
class NullableAggregateFunctionVariadic final : public NullableAggregateFunctionBase<State> {
public:
    NullableAggregateFunctionVariadic(const AggregateFunctionPtr& nested_function)
            : NullableAggregateFunctionBase<State>(nested_function) {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto column_size = ctx->get_num_args();
        // This container stores the columns we really pass to the nested function.
        std::vector<const Column*> data_columns;
        data_columns.resize(column_size);

        for (size_t i = 0; i < column_size; i++) {
            if (columns[i]->is_nullable()) {
                const auto* column = down_cast<const NullableColumn*>(columns[i]);
                if (column->is_null(row_num)) {
                    // If at least one column has a null value in the current row,
                    // we don't process this row.
                    return;
                }
                data_columns[i] = &column->data_column_ref();
            } else {
                data_columns[i] = columns[i];
            }
        }
        this->data(state).is_null = false;
        this->nested_function->update(ctx, data_columns.data(), this->data(state).mutable_nest_state(), row_num);
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, states[i] + state_offset, i);
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& selection) const override {
        auto column_size = ctx->get_num_args();
        // This container stores the columns we really pass to the nested function.
        std::vector<const Column*> data_columns;
        data_columns.resize(column_size);

        std::vector<uint8_t> null_data_result;
        null_data_result.resize(chunk_size);

        // has_nullable_column: false, means every column is data column.
        bool has_nullable_column = false;
        bool has_null = false;
        for (size_t i = 0; i < column_size; i++) {
            if (columns[i]->is_nullable()) {
                has_nullable_column = true;
                const auto* column = down_cast<const NullableColumn*>(columns[i]);
                data_columns[i] = &column->data_column_ref();

                // compute null_datas for column that has null.
                if (columns[i]->has_null()) {
                    has_null = true;
                    auto null_data = column->null_column()->raw_data();
                    for (size_t j = 0; j < chunk_size; ++j) {
                        null_data_result[j] |= null_data[j];
                    }
                }
            } else {
                data_columns[i] = columns[i];
            }
        }

        if (has_nullable_column) {
            if (has_null) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    if (!null_data_result[i] & !selection[i]) {
                        this->data(states[i] + state_offset).is_null = false;
                        this->nested_function->update(ctx, data_columns.data(),
                                                      this->data(states[i] + state_offset).mutable_nest_state(), i);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; ++i) {
                    if (!selection[i]) {
                        this->data(states[i] + state_offset).is_null = false;
                        this->nested_function->update(ctx, data_columns.data(),
                                                      this->data(states[i] + state_offset).mutable_nest_state(), i);
                    }
                }
            }
        } else {
            // Because every column is data column, so we still use columns.
            for (size_t i = 0; i < chunk_size; ++i) {
                if (!selection[i]) {
                    this->data(states[i] + state_offset).is_null = false;
                    this->nested_function->update(ctx, columns,
                                                  this->data(states[i] + state_offset).mutable_nest_state(), i);
                }
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());

        // dst's null_column, initial with false.
        dst_nullable_column->null_column_data().resize(chunk_size);

        // dst's null_column, used to | with src's null columns to indicate result chunk's null column.
        NullData& dst_null_data = dst_nullable_column->null_column_data();

        Columns data_columns;
        data_columns.reserve(src.size());

        bool has_nullable_column = false;
        for (const auto& i : src) {
            if (i->is_nullable()) {
                has_nullable_column = true;

                const auto* nullable_column = down_cast<const NullableColumn*>(i.get());
                data_columns.emplace_back(nullable_column->data_column());
                if (i->has_null()) {
                    const NullData& src_null_data = nullable_column->immutable_null_column_data();

                    // for one row, every columns should be probing to obtain null column.
                    for (int j = 0; j < chunk_size; ++j) {
                        dst_null_data[j] |= src_null_data[j];
                    }
                }
            } else {
                data_columns.emplace_back(i);
            }
        }

        if (!has_nullable_column) {
            this->nested_function->convert_to_serialize_format(src, chunk_size, &dst_nullable_column->data_column());
        } else {
            this->nested_function->convert_to_serialize_format(data_columns, chunk_size,
                                                               &dst_nullable_column->data_column());
        }
    }
};

} // namespace starrocks::vectorized
