// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/sum.h"
#include "exprs/vectorized/arithmetic_operation.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"

namespace starrocks::vectorized {

// AvgResultPT for final result
template <PrimitiveType PT, typename = guard::Guard>
struct AvgResultTrait {
    static const PrimitiveType value = PT;
};
template <PrimitiveType PT>
struct AvgResultTrait<PT, ArithmeticPTGuard<PT>> {
    static const PrimitiveType value = TYPE_DOUBLE;
};
template <PrimitiveType PT>
struct AvgResultTrait<PT, DecimalPTGuard<PT>> {
    static const PrimitiveType value = TYPE_DECIMAL128;
};

template <PrimitiveType PT>
inline constexpr PrimitiveType AvgResultPT = AvgResultTrait<PT>::value;

// ImmediateAvgResultPT for immediate accumulated result
template <PrimitiveType PT, typename = guard::Guard>
inline constexpr PrimitiveType ImmediateAvgResultPT = PT;

template <PrimitiveType PT>
inline constexpr PrimitiveType ImmediateAvgResultPT<PT, AvgDoublePTGuard<PT>> = TYPE_DOUBLE;

template <PrimitiveType PT>
inline constexpr PrimitiveType ImmediateAvgResultPT<PT, AvgDecimal64PTGuard<PT>> = TYPE_DECIMAL64;

// Only for compile
template <PrimitiveType PT>
inline constexpr PrimitiveType ImmediateAvgResultPT<PT, BinaryPTGuard<PT>> = TYPE_DOUBLE;

template <typename T>
struct AvgAggregateState {
    T sum{};
    int64_t count = 0;
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>, PrimitiveType ImmediatePT = ImmediateAvgResultPT<PT>,
          typename ImmediateType = RunTimeCppType<ImmediatePT>>
class AvgAggregateFunction final
        : public AggregateFunctionBatchHelper<AvgAggregateState<ImmediateType>,
                                              AvgAggregateFunction<PT, T, ImmediatePT, ImmediateType>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).sum = {};
        this->data(state).count = 0;
    }

    using InputColumnType = RunTimeColumnType<PT>;
    static constexpr auto ResultPT = AvgResultPT<PT>;
    using ResultType = RunTimeCppType<ResultPT>;
    using ResultColumnType = RunTimeColumnType<ResultPT>;

    template <bool is_inc>
    void do_update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state, size_t row_num) const {
        DCHECK(!columns[0]->is_nullable());
        [[maybe_unused]] const InputColumnType* column = down_cast<const InputColumnType*>(columns[0]);
        if constexpr (is_inc) {
            if constexpr (pt_is_datetime<PT>) {
                this->data(state).sum += column->get_data()[row_num].to_unix_second();
            } else if constexpr (pt_is_date<PT>) {
                this->data(state).sum += column->get_data()[row_num].julian();
            } else if constexpr (pt_is_decimalv2<PT>) {
                this->data(state).sum += column->get_data()[row_num];
            } else if constexpr (pt_is_arithmetic<PT>) {
                this->data(state).sum += column->get_data()[row_num];
            } else if constexpr (pt_is_decimal<PT>) {
                this->data(state).sum += column->get_data()[row_num];
            } else {
                DCHECK(false) << "Invalid PrimitiveTypes for avg function";
            }
            this->data(state).count++;
        } else {
            if constexpr (pt_is_datetime<PT>) {
                this->data(state).sum -= column->get_data()[row_num].to_unix_second();
            } else if constexpr (pt_is_date<PT>) {
                this->data(state).sum -= column->get_data()[row_num].julian();
            } else if constexpr (pt_is_decimalv2<PT>) {
                this->data(state).sum -= column->get_data()[row_num];
            } else if constexpr (pt_is_arithmetic<PT>) {
                this->data(state).sum -= column->get_data()[row_num];
            } else if constexpr (pt_is_decimal<PT>) {
                this->data(state).sum -= column->get_data()[row_num];
            } else {
                DCHECK(false) << "Invalid PrimitiveTypes for avg function";
            }
            this->data(state).count--;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        do_update<true>(ctx, columns, state, row_num);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition) const override {
        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        const int64_t current_frame_last_position = current_row_position + rows_end_offset;
        if (!ignore_subtraction && previous_frame_first_position >= partition_start &&
            previous_frame_first_position < partition_end) {
            do_update<false>(ctx, columns, state, previous_frame_first_position);
        }
        if (!ignore_addition && current_frame_last_position >= partition_start &&
            current_frame_last_position < partition_end) {
            do_update<true>(ctx, columns, state, current_frame_last_position);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice slice = column->get(row_num).get_slice();
        this->data(state).sum += *reinterpret_cast<ImmediateType*>(slice.data);
        this->data(state).count += *reinterpret_cast<int64_t*>(slice.data + sizeof(ImmediateType));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();
        size_t new_size = old_size + sizeof(ImmediateType) + sizeof(int64_t);
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).sum), sizeof(ImmediateType));
        memcpy(bytes.data() + old_size + sizeof(ImmediateType), &(this->data(state).count), sizeof(int64_t));

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();

        size_t one_element_size = sizeof(ImmediateType) + sizeof(int64_t);
        bytes.resize(one_element_size * chunk_size);
        dst_column->get_offset().resize(chunk_size + 1);

        [[maybe_unused]] const InputColumnType* src_column = down_cast<const InputColumnType*>(src[0].get());
        int64_t count = 1;
        ImmediateType result = {};
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (pt_is_datetime<PT>) {
                result = src_column->get_data()[i].to_unix_second();
            } else if constexpr (pt_is_date<PT>) {
                result = src_column->get_data()[i].julian();
            } else if constexpr (pt_is_decimalv2<PT>) {
                result = src_column->get_data()[i];
            } else if constexpr (pt_is_arithmetic<PT>) {
                result = src_column->get_data()[i];
            } else if constexpr (pt_is_decimal<PT>) {
                result = src_column->get_data()[i];
            } else {
                DCHECK(false) << "Invalid PrimitiveTypes for avg function";
            }
            memcpy(bytes.data() + old_size, &result, sizeof(ImmediateType));
            memcpy(bytes.data() + old_size + sizeof(ImmediateType), &count, sizeof(int64_t));
            old_size += one_element_size;
            dst_column->get_offset()[i + 1] = old_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        // In fact, for StarRocks real query, we don't need this check.
        // But for robust, we add this check.
        if (this->data(state).count == 0) {
            return;
        }

        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        ResultType result;
        if constexpr (pt_is_decimalv2<PT>) {
            result = this->data(state).sum / DecimalV2Value(this->data(state).count, 0);
        } else if constexpr (pt_is_datetime<PT>) {
            result.from_unix_second(this->data(state).sum / this->data(state).count);
        } else if constexpr (pt_is_date<PT>) {
            result._julian = this->data(state).sum / this->data(state).count;
        } else if constexpr (pt_is_arithmetic<PT>) {
            result = this->data(state).sum / this->data(state).count;
        } else if constexpr (pt_is_decimal<PT>) {
            static_assert(pt_is_decimal128<ResultPT>, "Result type of avg on decimal32/64/128 is decimal 128");
            ResultType sum = ResultType(this->data(state).sum);
            ResultType count = ResultType(this->data(state).count);
            result = decimal_div_integer<ResultType>(sum, count, ctx->get_arg_type(0)->scale);
        } else {
            DCHECK(false) << "Invalid PrimitiveTypes for avg function";
        }
        column->append(result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        ResultColumnType* column = down_cast<ResultColumnType*>(dst);
        ResultType result;

        if constexpr (pt_is_decimalv2<PT>) {
            result = this->data(state).sum / DecimalV2Value(this->data(state).count, 0);
        } else if constexpr (pt_is_datetime<PT>) {
            result.from_unix_second(this->data(state).sum / this->data(state).count);
        } else if constexpr (pt_is_date<PT>) {
            result._julian = this->data(state).sum / this->data(state).count;
        } else if constexpr (pt_is_arithmetic<PT>) {
            result = this->data(state).sum / this->data(state).count;
        } else if constexpr (pt_is_decimal<PT>) {
            static_assert(pt_is_decimal128<ResultPT>, "Result type of avg on decimal32/64/128 is decimal 128");
            ResultType sum = ResultType(this->data(state).sum);
            ResultType count = ResultType(this->data(state).count);
            result = decimal_div_integer<ResultType>(sum, count, ctx->get_arg_type(0)->scale);
        } else {
            DCHECK(false) << "Invalid PrimitiveTypes for avg function";
        }
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    std::string get_name() const override { return "avg"; }
};
template <PrimitiveType PT, typename = DecimalPTGuard<PT>>
using DecimalAvgAggregateFunction =
        AvgAggregateFunction<PT, RunTimeCppType<PT>, TYPE_DECIMAL128, RunTimeCppType<TYPE_DECIMAL128>>;

} // namespace starrocks::vectorized
