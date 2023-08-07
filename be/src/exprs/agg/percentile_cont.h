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

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

// AvgResultLT for final result
template <LogicalType LT, bool isCont, typename = guard::Guard>
inline constexpr LogicalType PercentileResultLT = LT;

template <LogicalType LT>
inline constexpr LogicalType PercentileResultLT<LT, true, ArithmeticLTGuard<LT>> = TYPE_DOUBLE;

template <LogicalType LT, typename = guard::Guard>
struct PercentileState {
    using CppType = RunTimeCppType<LT>;
    void update(CppType item) { items.emplace_back(item); }

    std::vector<CppType> items;
    double rate = 0.0;
};

template <LogicalType LT, typename = guard::Guard>
class PercentileContDiscAggregateFunction
        : public AggregateFunctionBatchHelper<PercentileState<LT>, PercentileContDiscAggregateFunction<LT>> {
public:
    using InputCppType = RunTimeCppType<LT>;
    using InputColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).update(column.get_data()[row_num]);

        if (ctx->get_num_args() == 2) {
            const auto* rate = down_cast<const ConstColumn*>(columns[1]);
            this->data(state).rate = rate->get(row_num).get_double();
            DCHECK(this->data(state).rate >= 0 && this->data(state).rate <= 1);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        const Slice slice = column->get(row_num).get_slice();
        double rate = *reinterpret_cast<double*>(slice.data);
        size_t second_size = *reinterpret_cast<size_t*>(slice.data + sizeof(double));
        auto data_ptr = slice.data + sizeof(double) + sizeof(size_t);

        auto second_start = reinterpret_cast<InputCppType*>(data_ptr);
        auto second_end = reinterpret_cast<InputCppType*>(data_ptr + second_size * sizeof(InputCppType));

        // TODO(murphy) reduce the copy overhead of merge algorithm
        auto& output = this->data(state).items;
        size_t first_size = output.size();
        output.resize(first_size + second_size);
        auto first_end = output.begin() + first_size;
        std::copy(second_start, second_end, first_end);
        // TODO: optimize it with SIMD bitonic merge
        std::inplace_merge(output.begin(), first_end, output.end());

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
        pdqsort(reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t)),
                reinterpret_cast<InputCppType*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t) +
                                                items_size * sizeof(InputCppType)));

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (chunk_size <= 0) {
            return;
        }
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        double rate = ColumnHelper::get_const_value<TYPE_DOUBLE>(src[1]);
        auto src_column = *down_cast<const InputColumnType*>(src[0].get());
        InputCppType* src_data = src_column.get_data().data();
        for (auto i = 0; i < chunk_size; ++i) {
            size_t old_size = bytes.size();
            bytes.resize(old_size + sizeof(double) + sizeof(size_t) + sizeof(InputCppType));
            memcpy(bytes.data() + old_size, &rate, sizeof(double));
            *reinterpret_cast<size_t*>(bytes.data() + old_size + sizeof(double)) = 1UL;
            memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t), &src_data[i], sizeof(InputCppType));
            dst_column->get_offset().push_back(bytes.size());
        }
    }
};

template <LogicalType LT>
class PercentileContDiscAggregateFunction<LT, StringLTGuard<LT>>
        : public AggregateFunctionBatchHelper<PercentileState<LT>, PercentileContDiscAggregateFunction<LT>> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const BinaryColumn&>(*columns[0]);

        // use mem_pool to hold the slice's data, otherwise after chunk is processed, the memory of slice used is gone
        size_t element_size = column.get_data()[row_num].get_size();
        uint8_t* pos = ctx->mem_pool()->allocate(element_size);
        ctx->add_mem_usage(element_size);
        memcpy(pos, column.get_data()[row_num].get_data(), element_size);

        this->data(state).update(Slice(pos, element_size));

        if (ctx->get_num_args() == 2) {
            const auto* rate = down_cast<const ConstColumn*>(columns[1]);
            this->data(state).rate = rate->get(row_num).get_double();
            DCHECK(this->data(state).rate >= 0 && this->data(state).rate <= 1);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        //  slice == rate, vector_size, [[element0_size, element0_data],[element1_size, element1_data], ...]
        const Slice slice = column->get(row_num).get_slice();
        double rate = *reinterpret_cast<double*>(slice.data);
        size_t second_size = *reinterpret_cast<size_t*>(slice.data + sizeof(double));
        auto src_ptr = slice.data + sizeof(double) + sizeof(size_t);
        size_t elements_total_size =
                slice.get_size() - sizeof(double) - sizeof(size_t) - (second_size * sizeof(size_t));

        // used to save slice'data
        uint8_t* dest_ptr = ctx->mem_pool()->allocate(elements_total_size);
        ctx->add_mem_usage(elements_total_size);

        // TODO(murphy) reduce the copy overhead of merge algorithm
        auto& output = this->data(state).items;
        size_t first_size = output.size();
        output.reserve(first_size + second_size);
        for (size_t i = 0; i < second_size; i++) {
            size_t cur_element_size = *reinterpret_cast<size_t*>(src_ptr);
            src_ptr += sizeof(size_t);

            memcpy(dest_ptr, src_ptr, cur_element_size);
            src_ptr += cur_element_size;

            output.emplace_back(dest_ptr, cur_element_size);
            dest_ptr += cur_element_size;
        }

        // sort in merge instead of serialize_to_column, got the same result but more easy to implement
        pdqsort(output.begin() + first_size, output.end());
        // TODO: optimize it with SIMD bitonic merge
        std::inplace_merge(output.begin(), output.begin() + first_size, output.end());

        this->data(state).rate = rate;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t items_size = this->data(state).items.size();

        // should serialize: rate, vector_size, [[element0_size, element0_data],[element1_size, element1_data]...]
        size_t elements_total_size = 0;
        for (size_t i = 0; i < items_size; i++) {
            elements_total_size += (sizeof(size_t) + this->data(state).items[i].get_size());
        }
        size_t new_size = old_size + sizeof(double) + sizeof(size_t) + elements_total_size;
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).rate), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double), &items_size, sizeof(size_t));

        uint8_t* cur = bytes.data() + old_size + sizeof(double) + sizeof(size_t);
        for (size_t i = 0; i < items_size; i++) {
            size_t cur_element_size = this->data(state).items[i].get_size();
            memcpy(cur, &cur_element_size, sizeof(size_t));
            cur += sizeof(size_t);

            memcpy(cur, this->data(state).items[i].get_data(), cur_element_size);
            cur += cur_element_size;
        }

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (chunk_size <= 0) {
            return;
        }
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        double rate = ColumnHelper::get_const_value<TYPE_DOUBLE>(src[1]);

        auto src_column = *down_cast<const BinaryColumn*>(src[0].get());
        Slice* src_data = src_column.get_data().data();
        for (auto i = 0; i < chunk_size; ++i) {
            size_t old_size = bytes.size();
            // [rate, 1, element ith size, element ith data]
            bytes.resize(old_size + sizeof(double) + sizeof(size_t) + sizeof(size_t) + src_data[i].get_size());
            memcpy(bytes.data() + old_size, &rate, sizeof(double));
            *reinterpret_cast<size_t*>(bytes.data() + old_size + sizeof(double)) = 1UL;
            *reinterpret_cast<size_t*>(bytes.data() + old_size + sizeof(double) + sizeof(size_t)) =
                    src_data[i].get_size();
            memcpy(bytes.data() + old_size + sizeof(double) + sizeof(size_t) + sizeof(size_t), src_data[i].get_data(),
                   src_data[i].get_size());
            dst_column->get_offset().push_back(bytes.size());
        }
    }
};

template <LogicalType LT>
class PercentileContAggregateFunction final : public PercentileContDiscAggregateFunction<LT> {
    using InputCppType = RunTimeCppType<LT>;
    using InputColumnType = RunTimeColumnType<LT>;
    static constexpr auto ResultLT = PercentileResultLT<LT, true>;
    using ResultType = RunTimeCppType<ResultLT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        using CppType = RunTimeCppType<LT>;
        std::vector<CppType> new_vector = std::move(this->data(state).items);
        pdqsort(new_vector.begin(), new_vector.end());
        const double& rate = this->data(state).rate;

        auto* column = down_cast<ResultColumnType*>(to);
        DCHECK(!new_vector.empty());
        if (new_vector.size() == 1 || rate == 1) {
            column->append(new_vector.back());
            return;
        }

        double u = (new_vector.size() - 1) * rate;
        int index = (int)u;

        [[maybe_unused]] ResultType result;
        if constexpr (lt_is_datetime<LT>) {
            result.from_unix_second(
                    new_vector[index].to_unix_second() +
                    (u - (float)index) * (new_vector[index + 1].to_unix_second() - new_vector[index].to_unix_second()));
        } else if constexpr (lt_is_date<LT>) {
            result._julian = new_vector[index]._julian +
                             (u - (double)index) * (new_vector[index + 1]._julian - new_vector[index]._julian);
        } else if constexpr (lt_is_arithmetic<LT>) {
            result = new_vector[index] + (u - (double)index) * (new_vector[index + 1] - new_vector[index]);
        } else {
            // won't go there if percentile_cont is registered correctly
            throw std::runtime_error("Invalid PrimitiveTypes for percentile_cont function");
        }

        column->append(result);
    }

    std::string get_name() const override { return "percentile_cont"; }
};

template <LogicalType LT>
class PercentileDiscAggregateFunction final : public PercentileContDiscAggregateFunction<LT> {
    using InputCppType = RunTimeCppType<LT>;
    using InputColumnType = RunTimeColumnType<LT>;
    static constexpr auto ResultLT = PercentileResultLT<LT, false>;
    using ResultType = RunTimeCppType<ResultLT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        using CppType = RunTimeCppType<LT>;
        std::vector<CppType> new_vector = std::move(this->data(state).items);
        pdqsort(new_vector.begin(), new_vector.end());
        const double& rate = this->data(state).rate;

        auto* column = down_cast<ResultColumnType*>(to);
        DCHECK(!new_vector.empty());
        if (new_vector.size() == 1 || rate == 1) {
            column->append(new_vector.back());
            return;
        }

        // choose the uppper one
        int index = ceil((new_vector.size() - 1) * rate);

        [[maybe_unused]] ResultType result;
        if constexpr (lt_is_datetime<LT>) {
            result.from_unix_second(new_vector[index].to_unix_second());
        } else if constexpr (lt_is_date<LT>) {
            result._julian = new_vector[index]._julian;
        } else if constexpr (lt_is_arithmetic<LT> || lt_is_string<LT> || lt_is_decimal_of_any_version<LT>) {
            result = new_vector[index];
        } else {
            // won't go there if percentile_disc is registered correctly
            throw std::runtime_error("Invalid PrimitiveTypes for percentile_disc function");
        }

        column->append(result);
    }

    std::string get_name() const override { return "percentile_disc"; }
};

} // namespace starrocks
