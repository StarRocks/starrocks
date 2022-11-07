// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {

template <PrimitiveType PT>
struct ArrayAggAggregateState {
    using CppType = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;

    ArrayAggAggregateState() {
        data_column = ColumnType::create();
        null_count = 0;
    }

    void update(const ColumnType& column, size_t offset, size_t count) { data_column->append(column, offset, count); }

    void append_null() { null_count++; }

    typename ColumnType::Ptr data_column;
    size_t null_count;
};

template <PrimitiveType PT>
class ArrayAggAggregateFunction
        : public AggregateFunctionBatchHelper<ArrayAggAggregateState<PT>, ArrayAggAggregateFunction<PT>> {
public:
    using InputCppType = RunTimeCppType<PT>;
    using InputColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).update(column, row_num, 1);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).append_null();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto datum_array = input_column->get(row_num).get_array();
        auto& element_column = down_cast<const InputColumnType&>(input_column->elements());
        auto offset_size = input_column->get_element_offset_size(row_num);

        this->data(state).update(element_column, offset_size.first, offset_size.second);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        const auto& data = state_impl.data_column;
        auto* column = down_cast<ArrayColumn*>(to);
        column->append_array_element(*data, state_impl.null_count);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        return serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<ArrayColumn*>(dst->get());
        column->append(*src[0], 0, chunk_size);
    }

    std::string get_name() const override { return "array_agg"; }
};

} // namespace starrocks::vectorized
