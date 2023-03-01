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
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exec/sorting/sorting.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

// input columns result in intermediate result: struct{array[col0], array[col1], array[col2]... array[coln]}
struct ArrayAggAggregateState {
    void update(const Column& column, size_t index, size_t offset, size_t count) {
        if (index >= data_columns.size()) {
            DCHECK(index == data_columns.size());
            data_columns.push_back(column.clone_empty());
        }
        data_columns[index]->append(column, offset, count);
    }
    Columns data_columns; // TODO: how to construct columns firstly?
};

class ArrayAggAggregateFunction
        : public AggregateFunctionBatchHelper<ArrayAggAggregateState, ArrayAggAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        for (auto i = 0; i < ctx->get_num_args(); ++i) {
            // TODO: update is random access, so we could not pre-reserve memory for State, which is the bottleneck
            this->data(state).update(*columns[i], i, row_num, 1);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& input_columns = down_cast<const StructColumn*>(ColumnHelper::get_data_column(column))->fields();
        for (auto i = 0; i < input_columns.size(); ++i) {
            auto array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input_columns[i].get()));
            auto& offsets = array_column->offsets().get_data();
            this->data(state).update(array_column->elements(), i, offsets[row_num],
                                     offsets[row_num + 1] - offsets[row_num]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto& columns = down_cast<StructColumn*>(ColumnHelper::get_data_column(to))->fields_column();
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        for (auto i = 0; i < columns.size(); ++i) {
            auto elem_size = state_impl.data_columns[i]->size();
            auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(columns[i].get()));
            if (columns[i]->is_nullable()) {
                down_cast<NullableColumn*>(columns[i].get())->null_column_data().emplace_back(0);
            }
            array_col->elements_column()->append(*state_impl.data_columns[i], 0, elem_size);
            auto& offsets = array_col->offsets_column()->get_data();
            offsets.push_back(offsets.back() + elem_size);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_array());
        auto& state_impl = this->data(state);
        auto elem_size = state_impl.data_columns[0]->size();
        auto res = state_impl.data_columns[0].get();
        auto tmp = state_impl.data_columns[0]->clone_empty();
        if (state_impl.data_columns.size() > 1) {
            Permutation perm;
            Columns sort_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            sort_by_columns.assign(state_impl.data_columns.begin() + 1, state_impl.data_columns.end());
            Status st = sort_and_tie_columns(false, sort_by_columns, sort_desc, &perm);
            CHECK(st.ok());
            materialize_column_by_permutation(tmp.get(), {state_impl.data_columns[0]}, perm);
            res = tmp.get();
        }
        auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        array_col->elements_column()->append(*res, 0, elem_size);
        auto& offsets = array_col->offsets_column()->get_data();
        offsets.push_back(offsets.back() + elem_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto columns = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->get()))->fields_column();
        if (dst->get()->is_nullable()) {
            down_cast<NullableColumn*>(dst->get())->null_column_data().emplace_back(0);
        }
        for (auto j = 0; j < columns.size(); ++j) {
            auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(columns[j].get()));
            if (columns[j].get()->is_nullable()) {
                down_cast<NullableColumn*>(columns[j].get())->null_column_data().emplace_back(0);
            }
            auto element_column = array_col->elements_column();
            auto offsets = array_col->offsets_column()->get_data();
            for (size_t i = 0; i < chunk_size; i++) {
                element_column->append_datum(src[j]->get(i));
                offsets.emplace_back(offsets.back() + 1);
            }
        }
    }
    std::string get_name() const override { return "array_agg"; }
};

} // namespace starrocks
