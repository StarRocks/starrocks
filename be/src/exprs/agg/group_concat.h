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
class GroupConcatAggregateFunction
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
                const InputColumnType* column_val = down_cast<const InputColumnType*>(columns[0]);
                const InputColumnType* column_sep = down_cast<const InputColumnType*>(columns[1]);

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
                const InputColumnType* column_val = down_cast<const InputColumnType*>(columns[0]);
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
            const InputColumnType* column_val = down_cast<const InputColumnType*>(columns[0]);
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
            const InputColumnType* column_val = down_cast<const InputColumnType*>(columns[0]);
            if (!ctx->is_notnull_constant_column(1)) {
                const InputColumnType* column_sep = down_cast<const InputColumnType*>(columns[1]);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              column_sep->get_bytes().size());
            } else {
                auto const_column_sep = ctx->get_constant_column(1);
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_column_sep);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              sep.get_size() * chunk_size);
            }
        } else {
            const InputColumnType* column_val = down_cast<const InputColumnType*>(columns[0]);
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
            const auto* column_value = down_cast<BinaryColumn*>(src[0].get());
            if (!src[1]->is_constant()) {
                const auto* column_sep = down_cast<BinaryColumn*>(src[1].get());
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
            const auto* column_value = down_cast<BinaryColumn*>(src[0].get());

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
    void update(FunctionContext* ctx, const Column& column, size_t index, size_t offset, size_t count) {
        auto notnull = ColumnHelper::get_data_column(&column);
        (*data_columns)[index]->append(*notnull, offset, count);
    }

    // release the trailing N-1 order-by columns
    void release_order_by_columns(int output_col_num) const {
        DCHECK(data_columns != nullptr);
        for (auto i = output_col_num; i < data_columns->size(); ++i) {
            data_columns->at(i).reset();
        }
        data_columns->resize(output_col_num);
    }

    ~GroupConcatAggregateStateV2() {
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
    // group_concat(a, b order by c, d), the a,b,c,d are put into data_columns in order.
    Columns* data_columns = nullptr; // not null
};

class GroupConcatAggregateFunctionV2
        : public AggregateFunctionBatchHelper<GroupConcatAggregateStateV2, GroupConcatAggregateFunctionV2> {
public:
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
        auto num = ctx->get_num_args();
        GroupConcatAggregateStateV2& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) { // init
            state_impl.data_columns = new Columns;
            for (auto i = 0; i < num; ++i) {
                state_impl.data_columns->emplace_back(ctx->create_column(*ctx->get_arg_type(i), false));
            }
            DCHECK(ctx->get_is_asc_order().size() == ctx->get_nulls_first().size());
        }
        for (auto i = 0; i < num; ++i) {
            auto* data_col = columns[i];
            auto tmp_row_num = row_num;
            if (columns[i]->is_constant()) {
                // just copy the first const value.
                data_col = down_cast<const ConstColumn*>(columns[i])->data_column().get();
                tmp_row_num = 0;
            }
            this->data(state).update(ctx, *data_col, i, tmp_row_num, 1);
            std::cout << fmt::format("after update {} -th col = {} id = {}, result {}", i, columns[i]->debug_string(),
                                     row_num, (*state_impl.data_columns)[i]->debug_string())
                      << std::endl;
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& state_impl = this->data(state);
        for (auto& col : *state_impl.data_columns) {
            col->resize(col->size() + chunk_size);
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
        if (row_num >= column->size()) {
            std::cout << fmt::format("row num {} >= size {}", row_num, column->size()) << std::endl;
            throw std::runtime_error("merge error");
            return;
        }
        std::cout << fmt::format("merge {}", column->debug_string()) << std::endl;
        auto& input_columns = down_cast<const StructColumn*>(ColumnHelper::get_data_column(column))->fields();
        auto& state_impl = this->data(state);
        if (state_impl.data_columns == nullptr) {
            auto num = ctx->get_num_args();
            state_impl.data_columns = new Columns;
            for (auto i = 0; i < num; ++i) {
                state_impl.data_columns->emplace_back(ctx->create_column(*ctx->get_arg_type(i), false));
            }
            DCHECK(ctx->get_is_asc_order().size() == ctx->get_nulls_first().size());
        }
        for (auto i = 0; i < input_columns.size(); ++i) {
            auto array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input_columns[i].get()));
            auto& offsets = array_column->offsets().get_data();
            state_impl.update(ctx, array_column->elements(), i, offsets[row_num],
                              offsets[row_num + 1] - offsets[row_num]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        std::cout << fmt::format("ser to name = {}, val = {}", to->get_name(), to->debug_string()) << std::endl;
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

            array_col->elements_column()->append(
                    *ColumnHelper::unpack_and_duplicate_const_column(elem_size, (*state_impl.data_columns)[i]), 0,
                    elem_size);

            std::cout << fmt::format("after ser {} res {}", (*state_impl.data_columns)[i]->debug_string(),
                                     array_col->elements_column()->debug_string())
                      << std::endl;
            auto& offsets = array_col->offsets_column()->get_data();
            offsets.push_back(offsets.back() + elem_size);
        }
        std::cout << fmt::format("ser to name = {}, val = {}", to->get_name(), to->debug_string()) << std::endl;
    }

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
            std::cout << fmt::format("conv ser src {} to {}", src[j]->debug_string(), array_col->debug_string())
                      << std::endl;
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto elem_size = (*state_impl.data_columns)[0]->size();
        auto output_col_num = state_impl.data_columns->size() - ctx->get_is_asc_order().size();
        Columns outputs;
        outputs.resize(output_col_num);
        for (auto i = 0; i < output_col_num; ++i) {
            outputs[i] = (*state_impl.data_columns)[i];
            std::cout << fmt::format("finalize input i = {}, output = {}", i, outputs[i]->debug_string()) << std::endl;
        }
        if (!ctx->get_is_asc_order().empty()) {
            for (auto i = 0; i < output_col_num; ++i) {
                outputs[i] = (*state_impl.data_columns)[i]->clone_empty();
            }
            Permutation perm;
            Columns order_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            order_by_columns.assign(state_impl.data_columns->begin() + output_col_num, state_impl.data_columns->end());
            Status st = sort_and_tie_columns(ctx->state()->cancelled_ref(), order_by_columns, sort_desc, &perm);
            // release order-by columns early
            order_by_columns.clear();
            state_impl.release_order_by_columns(output_col_num);
            DCHECK(ctx->state()->cancelled_ref() || st.ok());
            for (auto i = 0; i < output_col_num; ++i) {
                materialize_column_by_permutation(outputs[i].get(), {(*state_impl.data_columns)[i]}, perm);
            }
        }
        std::cout << fmt::format("finalize to name = {}, val = {}", to->get_name(), to->debug_string()) << std::endl;

        auto* string = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        /// TODO(fzh) just consider string type
        Bytes& bytes = string->get_bytes();
        size_t offset = bytes.size();
        size_t length = 0;
        for (auto i = 0; i < output_col_num; ++i) {
            std::cout << i << " th col " << outputs[i]->get_name() << std::endl;
            if (outputs[i]->is_binary()) {
                length += down_cast<BinaryColumn*>(outputs[i].get())->get_bytes().size();
            }
        }

        bytes.resize(offset + length);
        Slice bstr(bytes.data(), offset);
        for (auto j = 0; j < elem_size; ++j) {
            for (auto i = 0; i < output_col_num; ++i) {
                if (outputs[i]->is_binary()) {
                    auto str = down_cast<BinaryColumn*>(outputs[i].get())->get_slice(j);
                    memcpy(bytes.data() + offset, str.get_data(), str.get_size());
                    offset += str.get_size();
                }
            }
        }
        string->get_offset().emplace_back(offset);
        std::cout << fmt::format("from {} to {}", bstr.to_string(), string->debug_string()) << std::endl;
    }

    std::string get_name() const override { return "group concat2"; }
};

} // namespace starrocks
