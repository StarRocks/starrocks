// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

// AvgResultPT for final result
template <PrimitiveType PT, typename = guard::Guard>
inline constexpr PrimitiveType PercentileResultPT = PT;

template <PrimitiveType PT>
inline constexpr PrimitiveType PercentileResultPT<PT, ArithmeticPTGuard<PT>> = TYPE_DOUBLE;

template <PrimitiveType PT, typename = guard::Guard>
struct PercentileContState {
    using CppType = RunTimeCppType<PT>;
    void update(CppType item) { items.emplace_back(item); }

    std::vector<CppType> items;
    double rate = 0.0;
};

template <PrimitiveType PT>
class PercentileContAggregateFunction final
        : public AggregateFunctionBatchHelper<PercentileContState<PT>, PercentileContAggregateFunction<PT>> {
public:
    using InputCppType = RunTimeCppType<PT>;
    using InputColumnType = RunTimeColumnType<PT>;
    static constexpr auto ResultPT = PercentileResultPT<PT>;
    using ResultType = RunTimeCppType<ResultPT>;
    using ResultColumnType = RunTimeColumnType<ResultPT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).update(column.get_data()[row_num]);

        if (ctx->get_num_args() == 2) {
            const auto* rate = down_cast<const ConstColumn*>(columns[1]);
            this->data(state).rate = rate->get(row_num).get_double();
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        const Slice slice = column->get(row_num).get_slice();
        double rate = *reinterpret_cast<double*>(slice.data);
        size_t items_size = *reinterpret_cast<size_t*>(slice.data + sizeof(double));
        auto data_ptr = slice.data + sizeof(double) + sizeof(size_t);

        vector<InputCppType> res;
        vector<InputCppType>& vec = this->data(state).items;
        res.resize(vec.size() + items_size);

        std::merge(vec.begin(), vec.end(), reinterpret_cast<InputCppType*>(data_ptr),
                   reinterpret_cast<InputCppType*>(data_ptr + items_size * sizeof(InputCppType)), res.begin());
        this->data(state).items = std::move(res);
        this->data(state).rate = rate;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t items_size = this->data(state).items.size();

        // should serialize: rate_size, vector_size, all vector element.
        size_t new_size = old_size + sizeof(double) + sizeof(size_t) + items_size * sizeof(InputCppType);
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).rate), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double), &items_size, sizeof(size_t));
        memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t), this->data(state).items.data(),
               items_size * sizeof(InputCppType));
        std::sort(reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t)),
                  reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t) +
                                                  items_size * sizeof(InputCppType)));

        column->get_offset().emplace_back(new_size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        using CppType = RunTimeCppType<PT>;
        std::vector<CppType> new_vector = this->data(state).items;
        std::sort(new_vector.begin(), new_vector.end());
        const double& rate = this->data(state).rate;

        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        if (new_vector.size() == 0) {
            return;
        }
        if (new_vector.size() == 1 || rate == 1) {
            column->append(new_vector.back());
            return;
        }

        double u = (new_vector.size() - 1) * rate;
        int index = (int)u;

        [[maybe_unused]] ResultType result;
        if constexpr (pt_is_datetime<PT>) {
            result.from_unix_second(
                    new_vector[index].to_unix_second() +
                    (u - (float)index) * (new_vector[index + 1].to_unix_second() - new_vector[index].to_unix_second()));
        } else if constexpr (pt_is_date<PT>) {
            result._julian = new_vector[index]._julian +
                             (u - (double)index) * (new_vector[index + 1]._julian - new_vector[index]._julian);
        } else if constexpr (pt_is_arithmetic<PT>) {
            result = new_vector[index] + (u - (double)index) * (new_vector[index + 1] - new_vector[index]);
        } else {
            LOG(ERROR) << "Invalid PrimitiveTypes for percentile_cont function";
            return;
        }

        column->append(result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();

        if (chunk_size <= 0) {
            return;
        }

        double rate = ColumnHelper::get_const_value<TYPE_DOUBLE>(src[1]);
        InputColumnType src_column = *down_cast<const InputColumnType*>(src[0].get());
        std::sort(src_column.get_data().begin(), src_column.get_data().end());

        bytes.resize(old_size + sizeof(double) + sizeof(size_t) + chunk_size * sizeof(InputCppType));

        memcpy(bytes.data() + old_size, &rate, sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double), &chunk_size, sizeof(size_t));
        memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t), src_column.get_data().data(),
               chunk_size * sizeof(InputCppType));
    }

    std::string get_name() const override { return "percentile_cont"; }
};

} // namespace starrocks::vectorized
