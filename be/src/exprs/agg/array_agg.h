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
#include "runtime/runtime_state.h"
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

// input columns result in intermediate result: struct{array[col0], array[col1], array[col2]... array[coln]}
// return ordered array[col0']
struct ArrayAggAggregateStateV2 {
    void update(FunctionContext* ctx, const Column& column, size_t index, size_t offset, size_t count) {
        (*data_columns)[index]->append(column, offset, count);
    }
    void update_nulls(FunctionContext* ctx, size_t index, size_t count) {
        DCHECK((*data_columns)[index]->is_nullable());
        (*data_columns)[index]->append_nulls(count);
    }

    // release the trailing N-1 order-by columns
    void release_order_by_columns() const {
        DCHECK(data_columns != nullptr);
        for (auto i = 1; i < data_columns->size(); ++i) {
            data_columns->at(i).reset();
        }
        data_columns->resize(1);
    }

    ~ArrayAggAggregateStateV2() {
        if (data_columns != nullptr) {
            for (auto& col : *data_columns) {
                col.reset();
            }
            data_columns->clear();
            delete data_columns;
            data_columns = nullptr;
        }
    }
    // using pointer rather than vector to avoid variadic size
    // array_agg(a order by b, c, d), the a,b,c,d are put into data_columns in order.
    Columns* data_columns = nullptr;
};

class ArrayAggAggregateFunctionV2
        : public AggregateFunctionBatchHelper<ArrayAggAggregateStateV2, ArrayAggAggregateFunctionV2> {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        auto num = ctx->get_num_args();
        auto* state = new (ptr) ArrayAggAggregateStateV2;
        state->data_columns = new Columns;
        for (auto i = 0; i < num; ++i) {
            state->data_columns->emplace_back(ctx->create_column(*ctx->get_arg_type(i), true));
        }
        DCHECK(ctx->get_is_asc_order().size() == ctx->get_nulls_first().size());
        DCHECK(state->data_columns->size() == ctx->get_is_asc_order().size() + 1);
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& state_impl = this->data(state);
        if (state_impl.data_columns != nullptr) {
            for (auto& col : *state_impl.data_columns) {
                col->resize(0);
            }
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK_EQ(ctx->get_num_args(), this->data(state).data_columns->size());
        for (auto i = 0; i < ctx->get_num_args(); ++i) {
            DCHECK(columns[i]->size() > row_num);
            // TODO: update is random access, so we could not pre-reserve memory for State, which is the bottleneck
            if ((columns[i]->is_nullable() && columns[i]->is_null(row_num)) || columns[i]->only_null()) {
                this->data(state).update_nulls(ctx, i, 1);
                continue;
            }
            auto* data_col = columns[i];
            auto tmp_row_num = row_num;
            if (columns[i]->is_constant()) {
                // just copy the first const value.
                data_col = down_cast<const ConstColumn*>(columns[i])->data_column().get();
                tmp_row_num = 0;
            }
            this->data(state).update(ctx, *data_col, i, tmp_row_num, 1);
        }
    }

    // struct and array elements aren't be null, as they consist from several columns
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& input_columns = down_cast<const StructColumn*>(ColumnHelper::get_data_column(column))->fields();
        for (auto i = 0; i < input_columns.size(); ++i) {
            auto array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input_columns[i].get()));
            auto& offsets = array_column->offsets().get_data();
            this->data(state).update(ctx, array_column->elements(), i, offsets[row_num],
                                     offsets[row_num + 1] - offsets[row_num]);
        }
    }

    // serialize each state->column to a [nullable] array in a [nullable] struct
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto& columns = down_cast<StructColumn*>(ColumnHelper::get_data_column(to))->fields_column();
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        for (auto i = 0; i < columns.size(); ++i) {
            auto elem_size = (*state_impl.data_columns)[i]->size();
            auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(columns[i].get()));
            if (columns[i]->is_nullable()) {
                down_cast<NullableColumn*>(columns[i].get())->null_column_data().emplace_back(0);
            }
            if ((*state_impl.data_columns)[i]->only_null()) {
                array_col->elements_column()->append_nulls(elem_size);
            } else {
                array_col->elements_column()->append(
                        *ColumnHelper::unpack_and_duplicate_const_column(elem_size, (*state_impl.data_columns)[i]), 0,
                        elem_size);
            }
            auto& offsets = array_col->offsets_column()->get_data();
            offsets.push_back(offsets.back() + elem_size);
        }
    }

    // finalize each state->column to a [nullable] array
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_nullable() || to->is_array());
        auto& state_impl = this->data(state);
        auto elem_size = (*state_impl.data_columns)[0]->size();
        auto res = (*state_impl.data_columns)[0];
        auto tmp = (*state_impl.data_columns)[0]->clone_empty();
        if (state_impl.data_columns->size() > 1) {
            Permutation perm;
            Columns order_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            order_by_columns.assign(state_impl.data_columns->begin() + 1, state_impl.data_columns->end());
            Status st = sort_and_tie_columns(ctx->state()->cancelled_ref(), order_by_columns, sort_desc, &perm);
            // release order-by columns early
            order_by_columns.clear();
            state_impl.release_order_by_columns();
            DCHECK(ctx->state()->cancelled_ref() || st.ok());
            materialize_column_by_permutation(tmp.get(), {(*state_impl.data_columns)[0]}, perm);
            res = ColumnPtr(std::move(tmp));
        }
        auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        if (res->only_null()) {
            array_col->elements_column()->append_nulls(elem_size);
        } else {
            array_col->elements_column()->append(*ColumnHelper::unpack_and_duplicate_const_column(elem_size, res), 0,
                                                 elem_size);
        }
        auto& offsets = array_col->offsets_column()->get_data();
        offsets.push_back(offsets.back() + elem_size);
    }

    // convert each cell of a row to a [nullable] array in a struct
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto columns = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->get()))->fields_column();
        if (dst->get()->is_nullable()) {
            for (size_t i = 0; i < chunk_size; i++) {
                down_cast<NullableColumn*>(dst->get())->null_column_data().emplace_back(0);
            }
        }
        for (auto j = 0; j < columns.size(); ++j) {
            auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(columns[j].get()));
            if (columns[j].get()->is_nullable()) {
                for (size_t i = 0; i < chunk_size; i++) {
                    down_cast<NullableColumn*>(columns[j].get())->null_column_data().emplace_back(0);
                }
            }
            auto& element_column = array_col->elements_column();
            auto& offsets = array_col->offsets_column()->get_data();
            for (size_t i = 0; i < chunk_size; i++) {
                element_column->append_datum(src[j]->get(i));
                offsets.emplace_back(offsets.back() + 1);
            }
        }
    }
    // V2 support order by
    std::string get_name() const override { return "array_agg2"; }
};

} // namespace starrocks
