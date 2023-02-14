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

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "util/raw_container.h"

namespace starrocks {

template <LogicalType LT, typename = guard::Guard>
struct MaxByAggregateData {};

template <LogicalType LT>
struct MaxByAggregateData<LT, AggregateComplexLTGuard<LT>> {
    using T = AggDataValueType<LT>;
    raw::RawVector<uint8_t> buffer_result;
    T max = RunTimeTypeLimits<LT>::min_value();

    void reset() {
        buffer_result.clear();
        max = RunTimeTypeLimits<LT>::min_value();
    }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MaxByElement {
    using T = RunTimeCppType<LT>;
    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (right > state.max) {
            state.max = right;
            state.buffer_result.resize(col->serialize_size(row_num));
            col->serialize(row_num, state.buffer_result.data());
        }
    }
    void operator()(State& state, const char* buffer, size_t size, const T& right) const {
        if (right > state.max) {
            state.max = right;
            state.buffer_result.resize(size);
            memcpy(state.buffer_result.data(), buffer, size);
        }
    }
};

template <LogicalType LT, typename State>
struct MaxByElement<LT, State, JsonGuard<LT>> {
    using T = RunTimeCppType<LT>;

    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (*right > state.max) {
            AggDataTypeTraits<LT>::assign_value(state.max, right);
            state.buffer_result.resize(col->serialize_size(row_num));
            col->serialize(row_num, state.buffer_result.data());
        }
    }
    void operator()(State& state, const char* buffer, size_t size, const T& right) const {
        if (*right > state.max) {
            AggDataTypeTraits<LT>::assign_value(state.max, right);
            state.buffer_result.resize(size);
            memcpy(state.buffer_result.data(), buffer, size);
        }
    }
};

template <LogicalType LT>
struct MaxByAggregateData<LT, StringLTGuard<LT>> {
    raw::RawVector<uint8_t> buffer_result;
    raw::RawVector<uint8_t> buffer_max;
    int32_t size = -1;
    bool has_value() const { return size > -1; }
    Slice slice_max() const { return {buffer_max.data(), buffer_max.size()}; }
    void reset() {
        buffer_result.clear();
        buffer_max.clear();
        size = -1;
    }
};

template <LogicalType LT, typename State>
struct MaxByElement<LT, State, StringLTGuard<LT>> {
    void operator()(State& state, Column* col, size_t row_num, const Slice& right) const {
        if (!state.has_value() || state.slice_max().compare(right) < 0) {
            state.buffer_result.resize(col->serialize_size(row_num));
            col->serialize(row_num, state.buffer_result.data());
            state.buffer_max.resize(right.size);
            memcpy(state.buffer_max.data(), right.data, right.size);
            state.size = right.size;
        }
    }

    void operator()(State& state, const char* buffer, size_t size, const Slice& right) const {
        if (!state.has_value() || state.slice_max().compare(right) < 0) {
            state.buffer_result.resize(size);
            memcpy(state.buffer_result.data(), buffer, size);
            state.buffer_max.resize(right.size);
            memcpy(state.buffer_max.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <LogicalType LT, typename State, class OP, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class MaxByAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxByAggregateFunction<LT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        T column1_value;
        if (columns[1]->is_nullable()) {
            if (columns[1]->is_null(row_num)) {
                return;
            }
            column1_value = down_cast<const NullableColumn*>(columns[1])->data_column()->get(row_num).get<T>();
        } else {
            column1_value = down_cast<const InputColumnType*>(columns[1])->get_data()[row_num];
        }
        OP()(this->data(state), (Column*)columns[0], row_num, column1_value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        Slice src;
        if (column->is_nullable()) {
            if (column->is_null(row_num)) {
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(column);
            src = nullable_column->data_column()->get(row_num).get_slice();
        } else {
            const auto* binary_column = down_cast<const BinaryColumn*>(column);
            src = binary_column->get_slice(row_num);
        }

        if constexpr (LT != TYPE_JSON) {
            T max;
            memcpy(&max, src.data, sizeof(T));
            OP()(this->data(state), src.data + sizeof(T), src.size - sizeof(T), max);
        } else {
            JsonValue max(src);
            size_t value_size = max.serialize_size();
            OP()(this->data(state), src.data + value_size, src.size - value_size, &max);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        raw::RawVector<uint8_t> buffer;
        if constexpr (LT != TYPE_JSON) {
            size_t value_size = sizeof(T);
            buffer.resize(this->data(state).buffer_result.size() + value_size);
            memcpy(buffer.data(), &(this->data(state).max), value_size);
            memcpy(buffer.data() + value_size, this->data(state).buffer_result.data(),
                   this->data(state).buffer_result.size());
        } else {
            size_t value_size = this->data(state).max.serialize_size();
            buffer.resize(this->data(state).buffer_result.size() + value_size);
            this->data(state).max.serialize(buffer.data());
            memcpy(buffer.data() + value_size, this->data(state).buffer_result.data(),
                   this->data(state).buffer_result.size());
        }
        if (to->is_nullable()) {
            auto* column = down_cast<NullableColumn*>(to);
            if (this->data(state).buffer_result.size() == 0) {
                column->append_default();
            } else {
                down_cast<BinaryColumn*>(column->data_column().get())->append(Slice(buffer.data(), buffer.size()));
                column->null_column_data().push_back(0);
            }
        } else {
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(Slice(buffer.data(), buffer.size()));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const InputColumnType* col_max = nullptr;
        if (src[1]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[1].get());
            col_max = down_cast<const InputColumnType*>(nullable_column->data_column().get());
        } else {
            col_max = down_cast<const InputColumnType*>(src[1].get());
        }

        BinaryColumn* result = nullptr;
        if ((*dst)->is_nullable()) {
            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());

            if (src[1]->is_nullable())
                dst_nullable_column->null_column_data() =
                        down_cast<const NullableColumn*>(src[1].get())->immutable_null_column_data();
            else
                dst_nullable_column->null_column_data().resize(chunk_size, 0);

        } else {
            result = down_cast<BinaryColumn*>((*dst).get());
        }

        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (src[1]->is_null(i)) {
                auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                dst_nullable_column->set_has_null(true);
                result->get_offset()[i + 1] = old_size;
            } else {
                size_t serde_size = src[0]->serialize_size(i);
                T value = col_max->get_data()[i];
                size_t new_size;
                if constexpr (LT != TYPE_JSON) {
                    new_size = old_size + sizeof(T) + serde_size;
                    bytes.resize(new_size);
                    memcpy(bytes.data() + old_size, &value, sizeof(T));
                    src[0]->serialize(i, bytes.data() + old_size + sizeof(T));
                } else {
                    size_t value_size = value->serialize_size();
                    new_size = old_size + value_size + serde_size;
                    bytes.resize(new_size);
                    value->serialize(bytes.data() + old_size);
                    src[0]->serialize(i, bytes.data() + old_size + value_size);
                }
                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (this->data(state).buffer_result.empty())
            to->append_default();
        else
            to->deserialize_and_append(this->data(state).buffer_result.data());
    }

    std::string get_name() const override { return "max_by"; }
};

template <LogicalType LT, typename State, class OP>
class MaxByAggregateFunction<LT, State, OP, RunTimeCppType<LT>, StringLTGuard<LT>> final
        : public AggregateFunctionBatchHelper<State, MaxByAggregateFunction<LT, State, OP, RunTimeCppType<LT>>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        Slice column1_value;
        if (columns[1]->is_nullable()) {
            if (columns[1]->is_null(row_num)) {
                return;
            }
            column1_value = down_cast<const NullableColumn*>(columns[1])->data_column()->get(row_num).get_slice();
        } else {
            column1_value = columns[1]->get(row_num).get_slice();
        }
        OP()(this->data(state), (Column*)columns[0], row_num, column1_value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        Slice src;
        if (column->is_nullable()) {
            if (column->is_null(row_num)) {
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(column);
            src = nullable_column->data_column()->get(row_num).get_slice();
        } else {
            const auto* binary_column = down_cast<const BinaryColumn*>(column);
            src = binary_column->get_slice(row_num);
        }

        size_t size;
        const char* c = src.get_data();
        memcpy(&size, c, sizeof(size_t));
        if (size == -1) return;
        c += sizeof(size_t);
        Slice max(c, size);
        c += size;
        memcpy(&size, c, sizeof(size_t));
        c += sizeof(size_t);
        OP()(this->data(state), c, size, max);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        raw::RawVector<uint8_t> buffer;
        size_t result_size = this->data(state).buffer_result.size();
        size_t max_size = this->data(state).buffer_max.size();

        if (!this->data(state).has_value()) {
            size_t temp = -1;
            buffer.resize(sizeof(size_t));
            memcpy(buffer.data(), &temp, sizeof(size_t));
        } else {
            buffer.resize(result_size + max_size + 2 * sizeof(size_t));
            unsigned char* c = buffer.data();
            memcpy(c, &max_size, sizeof(size_t));
            c += sizeof(size_t);
            memcpy(c, this->data(state).buffer_max.data(), max_size);
            c += max_size;
            memcpy(c, &result_size, sizeof(size_t));
            c += sizeof(size_t);
            memcpy(c, this->data(state).buffer_result.data(), result_size);
        }

        if (to->is_nullable()) {
            auto* column = down_cast<NullableColumn*>(to);
            if (!this->data(state).has_value()) {
                column->append_default();
            } else {
                down_cast<BinaryColumn*>(column->data_column().get())->append(Slice(buffer.data(), buffer.size()));
                column->null_column_data().push_back(0);
            }
        } else {
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(Slice(buffer.data(), buffer.size()));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const BinaryColumn* col_max = nullptr;
        if (src[1]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[1].get());
            col_max = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        } else {
            col_max = down_cast<const BinaryColumn*>(src[1].get());
        }

        BinaryColumn* result = nullptr;
        if ((*dst)->is_nullable()) {
            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());

            if (src[1]->is_nullable())
                dst_nullable_column->null_column_data() =
                        down_cast<const NullableColumn*>(src[1].get())->immutable_null_column_data();
            else
                dst_nullable_column->null_column_data().resize(chunk_size, 0);

        } else {
            result = down_cast<BinaryColumn*>((*dst).get());
        }

        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (src[1]->is_null(i)) {
                auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                dst_nullable_column->set_has_null(true);
                result->get_offset()[i + 1] = old_size;
            } else {
                Slice value = col_max->get(i).get_slice();
                size_t max_size = value.size;
                size_t serde_size = src[0]->serialize_size(i);
                size_t new_size = old_size + 2 * sizeof(size_t) + max_size + serde_size;
                bytes.resize(new_size);
                unsigned char* c = bytes.data() + old_size;
                memcpy(c, &max_size, sizeof(size_t));
                c += sizeof(size_t);
                memcpy(c, value.data, max_size);
                c += max_size;
                memcpy(c, &serde_size, sizeof(size_t));
                c += sizeof(size_t);
                src[0]->serialize(i, c);
                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (this->data(state).buffer_result.empty())
            to->append_default();
        else
            to->deserialize_and_append(this->data(state).buffer_result.data());
    }

    std::string get_name() const override { return "max_by"; }
};

} // namespace starrocks
