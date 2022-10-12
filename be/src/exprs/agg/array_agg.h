// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "runtime/mem_pool.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, typename = guard::Guard>
struct ArrayAggAggregateState {};

template <PrimitiveType PT>
struct ArrayAggAggregateState<PT, FixedLengthPTGuard<PT>> {
    using CppType = RunTimeCppType<PT>;

    void update(MemPool* mem_pool, CppType key) { items.emplace_back(key); }

    std::vector<CppType> items;
    size_t null_count = 0;
};

template <PrimitiveType PT>
struct ArrayAggAggregateState<PT, BinaryPTGuard<PT>> {
    using CppType = RunTimeCppType<PT>;

    void update(MemPool* mem_pool, Slice key) {
        uint8_t* pos = mem_pool->allocate(key.size);
        memcpy(pos, key.data, key.size);
        items.emplace_back(Slice(pos, key.size));
    }

    std::vector<CppType> items;
    size_t null_count = 0;
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
        if constexpr (IsSlice<InputCppType>) {
            this->data(state).update(ctx->impl()->mem_pool(), column.get_slice(row_num));
        } else {
            this->data(state).update(ctx->impl()->mem_pool(), column.get_data()[row_num]);
        }
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).null_count++;
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto datum_array = input_column->get(row_num).get_array();

        for (size_t i = 0; i < datum_array.size(); i++) {
            if (datum_array[i].is_null()) {
                this->data(state).null_count++;
            } else {
                this->data(state).update(ctx->impl()->mem_pool(), datum_array[i].get<InputCppType>());
            }
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& data = this->data(state).items;
        const size_t null_count = this->data(state).null_count;

        auto* column = down_cast<ArrayColumn*>(to);
        auto& offsets = column->offsets_column()->get_data();
        auto& elements_column = column->elements_column();

        if (null_count > 0) {
            elements_column->append_nulls(null_count);
        }
        for (size_t i = 0; i < data.size(); i++) {
            elements_column->append_datum(data[i]);
        }
        offsets.emplace_back(offsets.back() + data.size() + null_count);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        return serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<ArrayColumn*>(dst->get());
        auto& offsets = column->offsets_column()->get_data();
        auto& elements_column = column->elements_column();

        for (size_t i = 0; i < chunk_size; i++) {
            elements_column->append_datum(src[0]->get(i));
            offsets.emplace_back(offsets.back() + 1);
        }
    }

    std::string get_name() const override { return "array_agg"; }
};

} // namespace starrocks::vectorized
