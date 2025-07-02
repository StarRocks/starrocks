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

#include <cmath>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exec/sorting/sorting.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/utf8.h"

namespace starrocks {
template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType GroupConcatResultLT = TYPE_VARCHAR;

struct GroupConcatAggregateState {
    // intermediate_string.
    // concat with sep_length first.
    std::string intermediate_string{};
    // is initial
    bool initial{};
};

template <LogicalType LT, typename T = RunTimeCppType<LT>, LogicalType ResultLT = GroupConcatResultLT<LT>,
          typename TResult = RunTimeCppType<ResultLT>>
class GroupConcatAggregateFunction final
        : public AggregateFunctionBatchHelper<GroupConcatAggregateState,
                                              GroupConcatAggregateFunction<LT, T, ResultLT, TResult>> {
public:
    using InputColumnType = RunTimeColumnType<ResultLT>;
    using ResultColumnType = InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).intermediate_string = {};
        this->data(state).initial = false;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_binary());
        if (ctx->get_num_args() > 1) {
            if (!ctx->is_notnull_constant_column(1)) {
                const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
                const auto* column_sep = down_cast<const InputColumnType*>(columns[1]);

                std::string& result = this->data(state).intermediate_string;

                Slice val = column_val->get_slice(row_num);
                Slice sep = column_sep->get_slice(row_num);
                if (!this->data(state).initial) {
                    this->data(state).initial = true;

                    // separator's length;
                    uint32_t size = sep.get_size();
                    result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                            .append(sep.get_data(), sep.get_size())
                            .append(val.get_data(), val.get_size());
                } else {
                    result.append(sep.get_data(), sep.get_size()).append(val.get_data(), val.get_size());
                }
            } else {
                auto const_column_sep = ctx->get_constant_column(1);
                const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
                std::string& result = this->data(state).intermediate_string;

                Slice val = column_val->get_slice(row_num);
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_column_sep);

                if (!this->data(state).initial) {
                    this->data(state).initial = true;

                    // separator's length;
                    uint32_t size = sep.get_size();
                    result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                            .append(sep.get_data(), sep.get_size())
                            .append(val.get_data(), val.get_size());
                } else {
                    result.append(sep.get_data(), sep.get_size()).append(val.get_data(), val.get_size());
                }
            }
        } else {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            std::string& result = this->data(state).intermediate_string;

            Slice val = column_val->get_slice(row_num);
            //DEFAULT sep_length.
            if (!this->data(state).initial) {
                this->data(state).initial = true;

                // separator's length;
                uint32_t size = 2;
                result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                        .append(", ")
                        .append(val.get_data(), val.get_size());
            } else {
                result.append(", ").append(val.get_data(), val.get_size());
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        if (ctx->get_num_args() > 1) {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            if (!ctx->is_notnull_constant_column(1)) {
                const auto* column_sep = down_cast<const InputColumnType*>(columns[1]);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              column_sep->get_bytes().size());
            } else {
                auto const_column_sep = ctx->get_constant_column(1);
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_column_sep);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              sep.get_size() * chunk_size);
            }
        } else {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            this->data(state).intermediate_string.reserve(column_val->get_bytes().size() + 2 * chunk_size);
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        Slice slice = column->get(row_num).get_slice();
        char* data = slice.data;
        uint32_t size_value = *reinterpret_cast<uint32_t*>(data);
        data += sizeof(uint32_t);

        if (!this->data(state).initial) {
            this->data(state).initial = true;
            this->data(state).intermediate_string.append(data, size_value);
        } else {
            data += sizeof(uint32_t);

            this->data(state).intermediate_string.append(data, size_value - sizeof(uint32_t));
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());

        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        const std::string& value = this->data(state).intermediate_string;

        size_t old_size = bytes.size();
        size_t new_size = old_size + sizeof(uint32_t) + value.size();
        bytes.resize(new_size);

        uint32_t size_value = value.size();
        memcpy(bytes.data() + old_size, &size_value, sizeof(uint32_t));
        memcpy(bytes.data() + old_size + sizeof(uint32_t), value.data(), size_value);

        column->get_offset().emplace_back(new_size);
    }

    size_t serialize_sep_and_value(Bytes& bytes, size_t old_size, uint32_t size_value, uint32_t size_sep,
                                   const char* sep, const char* data) const {
        uint32_t size_total = sizeof(uint32_t) + size_value + size_sep;
        memcpy(bytes.data() + old_size, &size_total, sizeof(uint32_t));
        old_size += sizeof(uint32_t);

        memcpy(bytes.data() + old_size, &size_sep, sizeof(uint32_t));
        old_size += sizeof(uint32_t);

        memcpy(bytes.data() + old_size, sep, size_sep);
        old_size += size_sep;

        memcpy(bytes.data() + old_size, data, size_value);
        old_size += size_value;

        return old_size;
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (src.size() > 1) {
            auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
            Bytes& bytes = dst_column->get_bytes();
            const auto* column_value = down_cast<const BinaryColumn*>(src[0].get());
            if (!src[1]->is_constant()) {
                const auto* column_sep = down_cast<const BinaryColumn*>(src[1].get());
                if (chunk_size > 0) {
                    size_t old_size = bytes.size();
                    CHECK_EQ(old_size, 0);
                    size_t new_size = 2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() +
                                      column_sep->get_bytes().size();
                    bytes.resize(new_size);
                    dst_column->get_offset().resize(chunk_size + 1);

                    for (size_t i = 0; i < chunk_size; ++i) {
                        auto value = column_value->get_slice(i);
                        auto sep = column_sep->get_slice(i);

                        uint32_t size_value = value.get_size();
                        uint32_t size_sep = sep.get_size();

                        old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep.get_data(),
                                                           value.get_data());
                        dst_column->get_offset()[i + 1] = old_size;
                    }
                }
            } else {
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(src[1]);
                if (chunk_size > 0) {
                    size_t old_size = bytes.size();
                    CHECK_EQ(old_size, 0);
                    size_t new_size = 2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() +
                                      chunk_size * sep.size;
                    bytes.resize(new_size);
                    dst_column->get_offset().resize(chunk_size + 1);

                    for (size_t i = 0; i < chunk_size; ++i) {
                        auto value = column_value->get_slice(i);

                        uint32_t size_value = value.get_size();
                        uint32_t size_sep = sep.size;

                        old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep.get_data(),
                                                           value.get_data());
                        dst_column->get_offset()[i + 1] = old_size;
                    }
                }
            }
        } else { //", "
            auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
            Bytes& bytes = dst_column->get_bytes();
            const auto* column_value = down_cast<const BinaryColumn*>(src[0].get());

            if (chunk_size > 0) {
                const char* sep = ", ";
                const uint32_t size_sep = 2;

                size_t old_size = bytes.size();
                CHECK_EQ(old_size, 0);
                size_t new_size =
                        2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() + size_sep * chunk_size;
                bytes.resize(new_size);
                dst_column->get_offset().resize(chunk_size + 1);

                for (size_t i = 0; i < chunk_size; ++i) {
                    auto value = column_value->get_slice(i);
                    uint32_t size_value = value.get_size();

                    old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep, value.get_data());
                    dst_column->get_offset()[i + 1] = old_size;
                }
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const std::string& value = this->data(state).intermediate_string;
        if (value.empty()) {
            return;
        }
        // Remove first sep_length.
        const char* data = value.data();
        uint32_t size = value.size();

        uint32_t sep_size = *reinterpret_cast<const uint32_t*>(data);
        uint32_t offset = sizeof(uint32_t) + sep_size;

        data = data + offset;
        size = size - offset;
        down_cast<ResultColumnType*>(to)->append(Slice(data, size));
    }

    std::string get_name() const override { return "group concat"; }
};

// input columns result in intermediate result: struct{array[col0], array[col1], array[col2]... array[coln]}
// return ordered string("col0col1...colnSEPcol0col1...coln...")
struct GroupConcatAggregateStateV2 {
    // update without null elements
    void update(FunctionContext* ctx, const Column& column, size_t index, size_t offset, size_t count) {
        (*data_columns)[index]->append(column, offset, count);
    }

    // order-by items may be null
    void update_nulls(FunctionContext* ctx, size_t index, size_t count) { (*data_columns)[index]->append_nulls(count); }

    // release the trailing order-by columns
    void release_order_by_columns() const {
        if (data_columns == nullptr) {
            return;
        }
        for (auto i = output_col_num + 1; i < data_columns->size(); ++i) { // after the separator column
            data_columns->at(i).reset();
        }
        data_columns->resize(output_col_num + 1);
    }

    // using pointer rather than vector to avoid variadic size
    // group_concat(a, b order by c, d), the a,b,',',c,d are put into data_columns in order, and reject null for
    // output columns a and b.
    std::unique_ptr<Columns> data_columns = nullptr;
    int output_col_num = 0;
};

// group_concat concatenates non-null values from a group, and output null if the group is empty.
// TODO(fzh) we can further optimize group_concat in following 3 ways:
// 1. reuse columns for order-by clause, group_concat(a order by 1) can avoid replacing '1' with a in plan, just keep a;
// 2. convert output columns to string in finalized phase, instead of add cast functions in plan, which leads to
// redundancy columns in intermediate results. For example, group_concat(a,b order by 1,2) is rewritten to
// group_concat(cast(a to string), cast(b to string) order by a, b), resulting to keeping 4 columns, but it only needs
// keep 2 columns in intermediate results.
// 3. refactor order-by and distinct function to a combinator to clean the code.
class GroupConcatAggregateFunctionV2 final
        : public AggregateFunctionBatchHelper<GroupConcatAggregateStateV2, GroupConcatAggregateFunctionV2> {
public:
    // group_concat(a, b order by c, d), the arguments are a,b,',',c,d
    void create_impl(FunctionContext* ctx, GroupConcatAggregateStateV2& state) const {
        auto num = ctx->get_num_args();
        state.data_columns = std::make_unique<Columns>();
        auto order_by_num = ctx->get_nulls_first().size();
        state.output_col_num = num - order_by_num - 1; // excluding separator column
        if (UNLIKELY(state.output_col_num <= 0)) {
            ctx->set_error("group_concat output column should not be empty", false);
            return;
        }
        for (auto i = 0; i < state.output_col_num; ++i) {
            if (UNLIKELY(!is_string_type(ctx->get_arg_type(i)->type))) {
                ctx->set_error(std::string(std::to_string(i) + "-th input of group_concat is not string type, but is " +
                                           type_to_string(ctx->get_arg_type(i)->type))
                                       .c_str(),
                               false);
                return;
            }
        }
        for (auto i = 0; i < num; ++i) {
            state.data_columns->emplace_back(ctx->create_column(*ctx->get_arg_type(i), true));
        }
        DCHECK(ctx->get_is_asc_order().size() == ctx->get_nulls_first().size());
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& state_impl = this->data(state);
        if (state_impl.data_columns != nullptr) {
            for (auto& col : *state_impl.data_columns) {
                col->resize(0);
            }
        }
    }

    // reject null for output columns, but non-output columns may be null
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto num = ctx->get_num_args();
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            create_impl(ctx, state_impl);
        }
        for (auto i = 0; i < state_impl.output_col_num; ++i) {
            if (columns[i]->is_nullable() && columns[i]->is_null(row_num)) {
                return;
            }
        }

        for (auto i = 0; i < num; ++i) {
            // non-output columns is null
            if (i >= state_impl.output_col_num && (columns[i]->is_nullable() && columns[i]->is_null(row_num))) {
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

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            create_impl(ctx, state_impl);
        }
        for (auto i = 0; i < state_impl.output_col_num; ++i) {
            if (columns[i]->only_null()) {
                return;
            }
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            create_impl(ctx, state_impl);
        }
        for (auto i = 0; i < state_impl.output_col_num; ++i) {
            if (columns[i]->only_null()) {
                return;
            }
        }
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    // input struct column, array may be null, but array->elements of output columns should not null
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        if (UNLIKELY(row_num >= column->size())) {
            ctx->set_error(std::string(get_name() + " merge() row id overflow").c_str(), false);
            return;
        }
        // input struct is null
        if (column->is_nullable() && column->is_null(row_num)) {
            return;
        }
        auto& input_columns = down_cast<const StructColumn*>(ColumnHelper::get_data_column(column))->fields();
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            create_impl(ctx, state_impl);
        }
        // output columns is null
        for (auto i = 0; i < state_impl.output_col_num; i++) {
            if (input_columns[i]->is_null(row_num)) {
                return;
            }
        }
        for (auto i = 0; i < input_columns.size(); ++i) {
            auto array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input_columns[i].get()));
            auto& offsets = array_column->offsets().get_data();
            state_impl.update(ctx, array_column->elements(), i, offsets[row_num],
                              offsets[row_num + 1] - offsets[row_num]);
        }
    }

    // TODO: if any output column is nullable, the result and intermediate result should be nullable
    // serialize each state->column to a (nullable but no null) array in a nullable struct
    // if the data_columns is empty, output null into nullable struct
    // otherwise, each state->column construct an array.
    // nullable struct {nullable array[nullable elements]...}, the struct may be null, array and array elements from
    // output columns wouldn't be null.
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        DCHECK(state_impl.data_columns == nullptr || !state_impl.data_columns->empty());
        if (state_impl.data_columns == nullptr || (*state_impl.data_columns)[0]->size() == 0) {
            to->append_default();
            return;
        }
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
            array_col->elements_column()->append(
                    *ColumnHelper::unpack_and_duplicate_const_column(elem_size, (*state_impl.data_columns)[i]), 0,
                    elem_size);
            auto& offsets = array_col->offsets_column()->get_data();
            offsets.push_back(offsets.back() + elem_size);
            (*state_impl.data_columns)[i].reset(); // early release memory
        }
        state_impl.data_columns->clear();
    }

    // convert each cell of a row to a [nullable] array in a nullable struct, keep the same of chunk_size
    // if i-th output row is null, set i-th output column in the struct is null whether the i-th struct is null.
    // nullable struct {nullable array[nullable elements]...}, the struct and array may be null, array elements from
    // output columns wouldn't be null.
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto columns = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst->get()))->fields_column();
        if (UNLIKELY(src.size() != columns.size())) {
            ctx->set_error(std::string(get_name() + " to-serialized column num " + std::to_string(src.size()) +
                                       " != expected " + std::to_string(columns.size()))
                                   .c_str(),
                           false);
            return;
        }
        // get null info from output columns
        auto output_col_num = ctx->get_num_args() - ctx->get_nulls_first().size() - 1;
        NullColumn::MutablePtr nulls = NullColumn::create(chunk_size, false);
        auto& null_data = nulls->get_data();
        for (int j = 0; j < output_col_num; ++j) {
            if (src[j]->only_null()) {
                for (int i = 0; i < chunk_size; ++i) {
                    null_data[i] = true;
                }
                break;
            }
            if (src[j]->is_constant()) {
                continue;
            }
            if (src[j]->is_nullable()) {
                auto null_col = down_cast<const NullableColumn*>(src[j].get())->null_column_data();
                for (int i = 0; i < chunk_size; ++i) {
                    null_data[i] |= null_col[i];
                }
            }
        }
        if (dst->get()->is_nullable()) {
            auto nullable_col = down_cast<NullableColumn*>(dst->get());
            for (size_t i = 0; i < chunk_size; i++) {
                nullable_col->null_column_data().emplace_back(null_data[i]);
            }
            nullable_col->update_has_null();
        }
        // if i-th row is null, set nullable_array[x][i] = null, otherwise, set array[x][i]=src[x][i]
        std::vector<ArrayColumn*> arrays(columns.size());
        std::vector<NullData*> array_nulls(columns.size());
        std::vector<Buffer<uint32_t>*> array_offsets(columns.size());
        std::vector<NullableColumn*> nullable_arrays(columns.size());
        auto old_size = columns[0]->size();
        for (auto j = 0; j < columns.size(); ++j) {
            nullable_arrays[j] = down_cast<NullableColumn*>(columns[j].get());
            arrays[j] = down_cast<ArrayColumn*>(nullable_arrays[j]->data_column().get());
            arrays[j]->reserve(old_size + chunk_size);
            array_nulls[j] = &(nullable_arrays[j]->null_column_data());
            array_nulls[j]->resize(old_size + chunk_size);
            array_offsets[j] = &(arrays[j]->offsets_column()->get_data());
        }
        for (auto i = 0; i < chunk_size; i++) {
            if (null_data[i]) {
                for (auto j = 0; j < columns.size(); ++j) {
                    (*array_nulls[j])[i + old_size] = 1;
                    array_offsets[j]->push_back(array_offsets[j]->back());
                }
            } else {
                for (auto j = 0; j < columns.size(); ++j) {
                    (*array_nulls[j])[i + old_size] = 0;
                    arrays[j]->elements_column()->append_datum(src[j]->get(i));
                    array_offsets[j]->push_back(array_offsets[j]->back() + 1);
                }
            }
        }
        for (auto j = 0; j < columns.size(); ++j) {
            nullable_arrays[j]->update_has_null();
        }
    }

    // group_concat(a, b order by c, d), output a,b,',' by c and d, but ignore the last separator ','
    // empty state return null, other output row by row.
    // note as output columns and order-by columns are put in group-by clause if specify DISTINCT, so here need to do
    // distinct further after order by data columns.
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto defer = DeferOp([&]() {
            if (ctx->has_error() && to != nullptr) {
                to->append_default();
            }
        });
        if (UNLIKELY(!(ColumnHelper::get_data_column(to)->is_binary()))) {
            ctx->set_error(std::string("The output column of " + get_name() +
                                       " finalize_to_column() is not string, but is " + to->get_name())
                                   .c_str(),
                           false);
        }
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            to->append_default();
            return;
        }
        DCHECK(!state_impl.data_columns->empty());

        auto elem_size = (*state_impl.data_columns)[0]->size();
        if (elem_size == 0) {
            to->append_default();
            return;
        }
        auto output_col_num = state_impl.output_col_num + 1; // include sep
        Columns outputs(output_col_num);
        for (auto i = 0; i < output_col_num; ++i) {
            outputs[i] = (*state_impl.data_columns)[i];
            DCHECK(!outputs[i]->is_constant()); // as they are appended one by one.
        }
        // order by
        Permutation perm;
        if (!ctx->get_is_asc_order().empty()) {
            Columns order_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            order_by_columns.assign(state_impl.data_columns->begin() + output_col_num, state_impl.data_columns->end());
            Status st = sort_and_tie_columns(ctx->state()->cancelled_ref(), order_by_columns, sort_desc, &perm);
            // release order-by columns early
            order_by_columns.clear();
            state_impl.release_order_by_columns();
            if (UNLIKELY(ctx->state()->cancelled_ref())) {
                ctx->set_error("group_concat detects cancelled.", false);
                return;
            }
            if (UNLIKELY(!st.ok())) {
                ctx->set_error(st.to_string().c_str(), false);
                return;
            }
        }
        // further remove duplicated values, pick the last unique one to identify the last sep and don't output it.
        // TODO(fzh) optimize it later, as distinct is often rewritten to group by.
        Buffer<bool> duplicated(outputs[0]->size(), false);
        if (ctx->get_is_distinct()) {
            for (auto row_id = 0; row_id < elem_size; row_id++) {
                bool is_duplicated = false;
                for (auto next_id = row_id + 1; next_id < elem_size; next_id++) {
                    bool tmp_duplicated = true;
                    for (auto col_id = 0; col_id < output_col_num - 1; col_id++) { // exclude sep
                        if (!outputs[col_id]->equals(next_id, *outputs[col_id], row_id)) {
                            tmp_duplicated = false;
                            break;
                        }
                    }
                    if (tmp_duplicated) {
                        is_duplicated = true;
                        break;
                    }
                }
                duplicated[row_id] = is_duplicated;
            }
        }
        // copy col_0, col_1 ... col_n row by row
        auto* string = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        Bytes& bytes = string->get_bytes();
        size_t offset = bytes.size();
        size_t length = 0;
        std::vector<BinaryColumn*> binary_cols(output_col_num);
        for (auto i = 0; i < output_col_num; ++i) {
            auto tmp = ColumnHelper::get_data_column(outputs[i].get());
            binary_cols[i] = down_cast<BinaryColumn*>(tmp);
            length += binary_cols[i]->get_bytes().size();
        }

        bytes.resize(offset + length);
        bool overflow = false;
        size_t limit = ctx->get_group_concat_max_len() + offset;
        auto last_unique_row_id = elem_size - 1;
        for (auto i = elem_size - 1; i >= 0; i--) {
            auto idx = i;
            if (!perm.empty()) {
                idx = perm[i].index_in_chunk;
            }
            if (!duplicated[idx]) {
                last_unique_row_id = i;
                break;
            }
        }

        DCHECK(perm.empty() || elem_size == perm.size());
        for (auto j = 0; j <= last_unique_row_id && !overflow; ++j) {
            auto idx = j;
            if (!perm.empty()) {
                idx = perm[j].index_in_chunk;
            }
            if (duplicated[idx]) {
                continue;
            }
            for (auto i = 0; i < output_col_num && !overflow; ++i) {
                if (j == last_unique_row_id && i + 1 == output_col_num) { // ignore the last separator
                    continue;
                }
                if (UNLIKELY(i + 1 < output_col_num && binary_cols[i]->is_null(idx))) {
                    ctx->set_error("group_concat mustn't output null", false);
                    return;
                }
                auto str = binary_cols[i]->get_slice(idx);
                if (offset + str.get_size() <= limit) {
                    memcpy(bytes.data() + offset, str.get_data(), str.get_size());
                    offset += str.get_size();
                    overflow = offset == limit;
                } else { // make the last utf8 character valid
                    std::vector<size_t> index;
                    get_utf8_index(str, &index);
                    size_t end = 0;
                    for (auto id : index) {
                        if (offset + id > limit) {
                            break;
                        }
                        end = id;
                    }
                    memcpy(bytes.data() + offset, str.get_data(), end);
                    offset += end;
                    overflow = true;
                }
            }
        }
        state_impl.data_columns->clear(); // early release memory
        bytes.resize(offset);
        string->get_offset().emplace_back(offset);
    }

    std::string get_name() const override { return "group_concat2"; }
};

} // namespace starrocks
