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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType LT>
struct ArrayAggAggregateState {
    using ColumnType = RunTimeColumnType<LT>;

    void update(const ColumnType& column, size_t offset, size_t count) { data_column.append(column, offset, count); }

    void append_null() { null_count++; }
    void append_null(size_t count) { null_count += count; }

    ColumnType data_column; // Aggregated elements for array_agg
    size_t null_count = 0;
};

template <LogicalType LT>
class ArrayAggAggregateFunction
        : public AggregateFunctionBatchHelper<ArrayAggAggregateState<LT>, ArrayAggAggregateFunction<LT>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        // TODO: update is random access, so we could not pre-reserve memory for State, which is the bottleneck
        this->data(state).update(column, row_num, 1);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).append_null();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        // Array element is nullable, so we need to extract the data from nullable column first
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto offset_size = input_column->get_element_offset_size(row_num);
        auto& array_element = down_cast<const NullableColumn&>(input_column->elements());
        auto* element_data_column = down_cast<const InputColumnType*>(ColumnHelper::get_data_column(&array_element));
        size_t element_null_count = array_element.null_count(offset_size.first, offset_size.second);
        DCHECK_LE(element_null_count, offset_size.second);

        this->data(state).update(*element_data_column, offset_size.first, offset_size.second - element_null_count);
        this->data(state).append_null(element_null_count);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto* column = down_cast<ArrayColumn*>(to);
        column->append_array_element(state_impl.data_column, state_impl.null_count);
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

} // namespace starrocks
