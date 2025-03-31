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

template <LogicalType LT, bool not_filter_nulls, typename = guard::Guard>
struct MaxByAggregateData {};

template <LogicalType LT, bool not_filter_nulls>
struct MaxByAggregateData<LT, not_filter_nulls, AggregateComplexLTGuard<LT>> {
    static constexpr auto not_filter_nulls_flag = not_filter_nulls;
    using T = AggDataValueType<LT>;
    raw::RawVector<uint8_t> buffer_result;
    T value = RunTimeTypeLimits<LT>::min_value();
    bool null_result = true;
    void reset() {
        buffer_result.clear();
        value = RunTimeTypeLimits<LT>::min_value();
        null_result = true;
    }
};

template <LogicalType LT, bool not_filter_nulls, typename = guard::Guard>
struct MinByAggregateData {};

template <LogicalType LT, bool not_filter_nulls>
struct MinByAggregateData<LT, not_filter_nulls, AggregateComplexLTGuard<LT>> {
    static constexpr auto not_filter_nulls_flag = not_filter_nulls;
    using T = AggDataValueType<LT>;
    raw::RawVector<uint8_t> buffer_result;
    T value = RunTimeTypeLimits<LT>::max_value();
    bool null_result = true;
    void reset() {
        buffer_result.clear();
        value = RunTimeTypeLimits<LT>::max_value();
        null_result = true;
    }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MaxByElement {
    using T = RunTimeCppType<LT>;
    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (right > state.value) {
            bool is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    state.value = right;
                    state.buffer_result.clear();
                    state.null_result = true;
                }
            } else {
                state.value = right;
                auto* data_col = ColumnHelper::get_data_column(col);
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
            }
        }
    }
    void operator()(State& state, bool is_null, const char* buffer, size_t size, const T& right) const {
        if (right >= state.value) {
            if constexpr (State::not_filter_nulls_flag) {
                state.value = right;
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                state.value = right;
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, typename State>
struct MaxByElement<LT, State, JsonGuard<LT>> {
    using T = RunTimeCppType<LT>;

    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (*right >= state.value) {
            bool is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    AggDataTypeTraits<LT>::assign_value(state.value, right);
                    state.buffer_result.clear();
                    state.null_result = true;
                }
            } else {
                auto* data_col = ColumnHelper::get_data_column(col);
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
            }
        }
    }
    void operator()(State& state, bool is_null, const char* buffer, size_t size, const T& right) const {
        if (*right >= state.value) {
            if constexpr (State::not_filter_nulls_flag) {
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MinByElement {
    using T = RunTimeCppType<LT>;
    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (right <= state.value) {
            auto is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    state.value = right;
                    state.buffer_result.clear();
                    state.null_result = true;
                }
            } else {
                auto* data_col = ColumnHelper::get_data_column(col);
                state.value = right;
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
            }
        }
    }
    void operator()(State& state, bool is_null, const char* buffer, size_t size, const T& right) const {
        if (right <= state.value) {
            if constexpr (State::not_filter_nulls_flag) {
                state.value = right;
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                state.value = right;
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, typename State>
struct MinByElement<LT, State, JsonGuard<LT>> {
    using T = RunTimeCppType<LT>;

    void operator()(State& state, Column* col, size_t row_num, const T& right) const {
        if (*right <= state.value) {
            auto is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    AggDataTypeTraits<LT>::assign_value(state.value, right);
                    state.buffer_result.clear();
                    state.null_result = true;
                }
            } else {
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                auto* data_col = ColumnHelper::get_data_column(col);
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
            }
        }
    }
    void operator()(State& state, bool is_null, const char* buffer, size_t size, const T& right) const {
        if (*right <= state.value) {
            if constexpr (State::not_filter_nulls_flag) {
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                AggDataTypeTraits<LT>::assign_value(state.value, right);
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, bool not_filter_nulls>
struct MaxByAggregateData<LT, not_filter_nulls, StringLTGuard<LT>> {
    static constexpr auto not_filter_nulls_flag = not_filter_nulls;
    raw::RawVector<uint8_t> buffer_result;
    raw::RawVector<uint8_t> buffer;
    int32_t size = -1;
    bool null_result = true;
    bool has_value() const { return size > -1; }
    Slice slice_max() const { return {buffer.data(), buffer.size()}; }
    void reset() {
        buffer_result.clear();
        buffer.clear();
        size = -1;
        null_result = true;
    }
};

template <LogicalType LT, bool not_filter_nulls>
struct MinByAggregateData<LT, not_filter_nulls, StringLTGuard<LT>> {
    static constexpr auto not_filter_nulls_flag = not_filter_nulls;
    raw::RawVector<uint8_t> buffer_result;
    raw::RawVector<uint8_t> buffer;
    int32_t size = -1;
    bool null_result = true;
    bool has_value() const { return size > -1; }
    Slice slice_min() const { return {buffer.data(), buffer.size()}; }
    void reset() {
        buffer_result.clear();
        buffer.clear();
        size = -1;
        null_result = true;
    }
};

template <LogicalType LT, typename State>
struct MaxByElement<LT, State, StringLTGuard<LT>> {
    void operator()(State& state, Column* col, size_t row_num, const Slice& right) const {
        if (!state.has_value() || state.slice_max().compare(right) < 0) {
            bool is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    state.buffer.resize(right.size);
                    memcpy(state.buffer.data(), right.data, right.size);
                    state.size = right.size;
                    state.buffer_result.clear();
                    state.null_result = true;
                }
            } else {
                auto* data_col = ColumnHelper::get_data_column(col);
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
                state.buffer.resize(right.size);
                memcpy(state.buffer.data(), right.data, right.size);
                state.size = right.size;
            }
        }
    }

    void operator()(State& state, bool is_null, const char* buffer, size_t size, const Slice& right) const {
        if (!state.has_value() || state.slice_max().compare(right) < 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
            if constexpr (State::not_filter_nulls_flag) {
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, typename State>
struct MinByElement<LT, State, StringLTGuard<LT>> {
    void operator()(State& state, Column* col, size_t row_num, const Slice& right) const {
        if (!state.has_value() || state.slice_min().compare(right) > 0) {
            bool is_null = col->only_null() || col->is_null(row_num);
            if (is_null) {
                if constexpr (State::not_filter_nulls_flag) {
                    state.buffer_result.clear();
                    state.null_result = true;
                    state.buffer.resize(right.size);
                    memcpy(state.buffer.data(), right.data, right.size);
                    state.size = right.size;
                }
            } else {
                auto* data_col = ColumnHelper::get_data_column(col);
                state.buffer_result.resize(data_col->serialize_size(row_num));
                data_col->serialize(row_num, state.buffer_result.data());
                state.null_result = false;
                state.buffer.resize(right.size);
                memcpy(state.buffer.data(), right.data, right.size);
                state.size = right.size;
            }
        }
    }

    void operator()(State& state, bool is_null, const char* buffer, size_t size, const Slice& right) const {
        if (!state.has_value() || state.slice_min().compare(right) > 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
            if constexpr (State::not_filter_nulls_flag) {
                if (is_null) {
                    state.buffer_result.clear();
                    state.null_result = true;
                } else {
                    state.buffer_result.resize(size);
                    memcpy(state.buffer_result.data(), buffer, size);
                    state.null_result = false;
                }
            } else {
                state.buffer_result.resize(size);
                memcpy(state.buffer_result.data(), buffer, size);
                state.null_result = false;
            }
        }
    }
};

template <LogicalType LT, typename State, class OP, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class MaxMinByAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxMinByAggregateFunction<LT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if (columns[1]->only_null() || columns[1]->is_null(row_num)) {
            return;
        }

        auto* data_col1 = ColumnHelper::get_data_column(columns[1]);
        auto column1_value = down_cast<const InputColumnType*>(data_col1)->get_data()[row_num];
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
        if (column->only_null() || column->is_null(row_num)) {
            return;
        }
        auto* data_column = ColumnHelper::get_data_column(column);
        const auto* binary_column = down_cast<const BinaryColumn*>(data_column);
        src = binary_column->get_slice(row_num);

        if constexpr (LT != TYPE_JSON) {
            T value;
            auto p = src.data;
            memcpy(&value, p, sizeof(T));
            p += sizeof(T);
            bool null_result = false;
            if constexpr (State::not_filter_nulls_flag) {
                null_result = (*p == 1);
                p += 1;
            }
            OP()(this->data(state), null_result, p, src.size - (p - src.data), value);
        } else {
            // it seems wrong, FE has forbidden max_by(t, JSON)
            JsonValue value(src);
            size_t value_size = value.serialize_size();
            auto* p = src.data;
            p += value_size;
            bool null_result = false;
            if constexpr (State::not_filter_nulls_flag) {
                null_result = (*p == 1);
                p += 1;
            }
            OP()(this->data(state), null_result, src.data + value_size, src.size - (p - src.data), &value);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        raw::RawVector<uint8_t> buffer;
        if constexpr (LT != TYPE_JSON) {
            size_t value_size = sizeof(T);
            size_t buffer_size = this->data(state).buffer_result.size() + value_size;
            if constexpr (State::not_filter_nulls_flag) {
                buffer_size += 1;
            }
            buffer.resize(buffer_size);
            auto* p = buffer.data();
            memcpy(p, &(this->data(state).value), value_size);
            p += value_size;
            if constexpr (State::not_filter_nulls_flag) {
                *p = (this->data(state).null_result ? 1 : 0);
                p += 1;
            }
            memcpy(p, this->data(state).buffer_result.data(), this->data(state).buffer_result.size());
        } else {
            size_t value_size = this->data(state).value.serialize_size();
            size_t buffer_size = this->data(state).buffer_result.size() + value_size;
            if constexpr (State::not_filter_nulls_flag) {
                buffer_size += 1;
            }
            buffer.resize(buffer_size);
            auto* p = buffer.data();
            this->data(state).value.serialize(p);
            p += value_size;
            if constexpr (State::not_filter_nulls_flag) {
                *p = (this->data(state).null_result ? 1 : 0);
                p += 1;
            }
            memcpy(p, this->data(state).buffer_result.data(), this->data(state).buffer_result.size());
        }
        if (to->is_nullable()) {
            auto* column = down_cast<NullableColumn*>(to);
            if (this->data(state).buffer_result.size() == 0 && !State::not_filter_nulls_flag) {
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
        if (src[1]->only_null()) {
            DCHECK((*dst)->is_nullable());
            (*dst)->append_default(chunk_size);
            return;
        }

        const auto* col_maxmin = down_cast<const InputColumnType*>(ColumnHelper::get_data_column(src[1].get()));
        BinaryColumn* result = nullptr;
        if ((*dst)->is_nullable()) {
            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());

            if (src[1]->is_nullable()) {
                dst_nullable_column->null_column_data() =
                        down_cast<const NullableColumn*>(src[1].get())->immutable_null_column_data();
            } else {
                dst_nullable_column->null_column_data().resize(chunk_size, 0);
            }
        } else {
            result = down_cast<BinaryColumn*>((*dst).get());
        }

        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        size_t new_size = old_size;
        for (size_t i = 0; i < chunk_size; ++i) {
            if (src[1]->is_null(i)) {
                result->get_offset()[i + 1] = old_size;
                DCHECK((*dst)->is_nullable());
                down_cast<NullableColumn*>((*dst).get())->set_has_null(true);
            } else {
                auto is_null = src[0]->only_null() || src[0]->is_null(i);
                T value = col_maxmin->get_data()[i];
                if (is_null) {
                    if constexpr (State::not_filter_nulls_flag) {
                        new_size = old_size + sizeof(T) + 1;
                        bytes.resize(new_size);
                        auto* p = bytes.data() + old_size;
                        memcpy(p, &value, sizeof(T));
                        p += sizeof(T);
                        *p = 1;
                    } else {
                        auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                        auto& dst_nulls = dst_nullable_column->null_column_data();
                        dst_nulls[i] = DATUM_NULL;
                        dst_nullable_column->set_has_null(true);
                    }
                } else {
                    auto* data_column = ColumnHelper::get_data_column(src[0].get());
                    size_t serde_size = data_column->serialize_size(i);
                    if constexpr (LT != TYPE_JSON) {
                        new_size = old_size + sizeof(T) + serde_size;
                        if constexpr (State::not_filter_nulls_flag) {
                            new_size += 1;
                        }
                        bytes.resize(new_size);
                        auto* p = bytes.data() + old_size;
                        memcpy(p, &value, sizeof(T));
                        p += sizeof(T);
                        if constexpr (State::not_filter_nulls_flag) {
                            *p = 0;
                            p += 1;
                        }
                        data_column->serialize(i, p);
                    } else {
                        size_t value_size = value->serialize_size();
                        new_size = old_size + value_size + serde_size;
                        if constexpr (State::not_filter_nulls_flag) {
                            new_size += 1;
                        }
                        bytes.resize(new_size);
                        auto* p = bytes.data() + old_size;
                        value->serialize(p);
                        p += value_size;
                        if constexpr (State::not_filter_nulls_flag) {
                            *p = 0;
                            p += 1;
                        }
                        data_column->serialize(i, p);
                    }
                }
                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if constexpr (State::not_filter_nulls_flag) {
            if (this->data(state).null_result) {
                DCHECK(to->is_nullable());
                to->append_default();
            } else {
                if (to->is_nullable()) {
                    down_cast<NullableColumn*>(to)->null_column()->append(DATUM_NOT_NULL);
                }
                ColumnHelper::get_data_column(to)->deserialize_and_append(this->data(state).buffer_result.data());
            }
        } else {
            if (this->data(state).buffer_result.empty()) {
                to->append_default();
            } else {
                if (to->is_nullable()) {
                    down_cast<NullableColumn*>(to)->null_column()->append(DATUM_NOT_NULL);
                }
                ColumnHelper::get_data_column(to)->deserialize_and_append(this->data(state).buffer_result.data());
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        if constexpr (State::not_filter_nulls_flag) {
            if (this->data(state).null_result) {
                DCHECK(dst->is_nullable());
                for (size_t i = start; i < end; ++i) {
                    dst->append_default();
                }
            } else {
                if (dst->is_nullable()) {
                    for (size_t i = start; i < end; ++i) {
                        down_cast<NullableColumn*>(dst)->null_column()->append(DATUM_NOT_NULL);
                    }
                }
                for (size_t i = start; i < end; ++i) {
                    ColumnHelper::get_data_column(dst)->deserialize_and_append(this->data(state).buffer_result.data());
                }
            }
        } else {
            if (this->data(state).buffer_result.empty()) {
                for (size_t i = start; i < end; ++i) {
                    dst->append_default();
                }
            } else {
                if (dst->is_nullable()) {
                    for (size_t i = start; i < end; ++i) {
                        down_cast<NullableColumn*>(dst)->null_column()->append(DATUM_NOT_NULL);
                    }
                }
                for (size_t i = start; i < end; ++i) {
                    ColumnHelper::get_data_column(dst)->deserialize_and_append(this->data(state).buffer_result.data());
                }
            }
        }
    }

    std::string get_name() const override { return "maxmin_by"; }
};

template <LogicalType LT, typename State, class OP>
class MaxMinByAggregateFunction<LT, State, OP, RunTimeCppType<LT>, StringLTGuard<LT>> final
        : public AggregateFunctionBatchHelper<State, MaxMinByAggregateFunction<LT, State, OP, RunTimeCppType<LT>>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if (columns[1]->only_null() || columns[1]->is_null(row_num)) {
            return;
        }
        Slice column1_value = ColumnHelper::get_data_column(columns[1])->get(row_num).get_slice();
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
        if (column->only_null() || column->is_null(row_num)) {
            return;
        }
        auto* data_column = ColumnHelper::get_data_column(column);
        const auto* binary_column = down_cast<const BinaryColumn*>(data_column);
        Slice src = binary_column->get_slice(row_num);

        size_t value_size;
        const char* p = src.get_data();
        memcpy(&value_size, p, sizeof(size_t));
        if (value_size == -1) return;
        p += sizeof(size_t);
        Slice value(p, value_size);
        p += value_size;
        bool null_result = false;
        if constexpr (State::not_filter_nulls_flag) {
            null_result = (*p == 1);
            p += 1;
        }
        size_t state_size;
        memcpy(&state_size, p, sizeof(size_t));
        p += sizeof(size_t);
        OP()(this->data(state), null_result, p, state_size, value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        raw::RawVector<uint8_t> buffer;
        size_t result_size = this->data(state).buffer_result.size();
        size_t value_size = this->data(state).buffer.size();

        if (!this->data(state).has_value()) {
            size_t temp = -1;
            buffer.resize(sizeof(size_t));
            memcpy(buffer.data(), &temp, sizeof(size_t));
        } else {
            auto buffer_size = result_size + value_size + 2 * sizeof(size_t);
            if constexpr (State::not_filter_nulls_flag) {
                buffer_size += 1;
            }
            buffer.resize(buffer_size);
            unsigned char* p = buffer.data();
            memcpy(p, &value_size, sizeof(size_t));
            p += sizeof(size_t);
            memcpy(p, this->data(state).buffer.data(), value_size);
            p += value_size;
            if constexpr (State::not_filter_nulls_flag) {
                *p = (this->data(state).null_result ? 1 : 0);
                p += 1;
            }
            memcpy(p, &result_size, sizeof(size_t));
            p += sizeof(size_t);
            memcpy(p, this->data(state).buffer_result.data(), result_size);
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
        if (src[1]->only_null()) {
            DCHECK((*dst)->is_nullable());
            (*dst)->append_default(chunk_size);
            return;
        }
        const BinaryColumn* col_maxmin = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(src[1].get()));

        BinaryColumn* result = nullptr;
        if ((*dst)->is_nullable()) {
            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());

            if (src[1]->is_nullable()) {
                dst_nullable_column->null_column_data() =
                        down_cast<const NullableColumn*>(src[1].get())->immutable_null_column_data();
            } else {
                dst_nullable_column->null_column_data().resize(chunk_size, 0);
            }
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
                size_t new_size = old_size;
                auto is_null = src[0]->only_null() || src[0]->is_null(i);
                if (is_null) {
                    if constexpr (State::not_filter_nulls_flag) {
                        Slice value = col_maxmin->get(i).get_slice();
                        size_t value_size = value.size;
                        new_size = old_size + sizeof(size_t) + value_size + 1;
                        bytes.resize(new_size);
                        auto* p = bytes.data() + old_size;
                        memcpy(p, &value_size, sizeof(size_t));
                        p += sizeof(size_t);
                        memcpy(p, value.get_data(), value_size);
                        p += value_size;
                        *p = 1;
                    } else {
                        auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                        auto& dst_nulls = dst_nullable_column->null_column_data();
                        dst_nulls[i] = DATUM_NULL;
                        dst_nullable_column->set_has_null(true);
                    }
                } else {
                    Slice value = col_maxmin->get(i).get_slice();
                    size_t value_size = value.size;
                    auto* data_column = ColumnHelper::get_data_column(src[0].get());
                    size_t serde_size = data_column->serialize_size(i);
                    new_size = old_size + 2 * sizeof(size_t) + value_size + serde_size;
                    if constexpr (State::not_filter_nulls_flag) {
                        new_size += 1;
                    }
                    bytes.resize(new_size);
                    unsigned char* p = bytes.data() + old_size;
                    memcpy(p, &value_size, sizeof(size_t));
                    p += sizeof(size_t);
                    memcpy(p, value.data, value_size);
                    p += value_size;
                    if constexpr (State::not_filter_nulls_flag) {
                        *p = 0;
                        p += 1;
                    }
                    memcpy(p, &serde_size, sizeof(size_t));
                    p += sizeof(size_t);
                    data_column->serialize(i, p);
                }
                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if constexpr (State::not_filter_nulls_flag) {
            if (this->data(state).null_result) {
                DCHECK(to->is_nullable());
                to->append_default();
            } else {
                if (to->is_nullable()) {
                    down_cast<NullableColumn*>(to)->null_column()->append(DATUM_NOT_NULL);
                }
                ColumnHelper::get_data_column(to)->deserialize_and_append(this->data(state).buffer_result.data());
            }
        } else {
            if (this->data(state).buffer_result.empty()) {
                to->append_default();
            } else {
                if (to->is_nullable()) {
                    down_cast<NullableColumn*>(to)->null_column()->append(DATUM_NOT_NULL);
                }
                ColumnHelper::get_data_column(to)->deserialize_and_append(this->data(state).buffer_result.data());
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        if constexpr (State::not_filter_nulls_flag) {
            if (this->data(state).null_result) {
                DCHECK(dst->is_nullable());
                for (size_t i = start; i < end; ++i) {
                    dst->append_default();
                }
            } else {
                if (dst->is_nullable()) {
                    for (size_t i = start; i < end; ++i) {
                        down_cast<NullableColumn*>(dst)->null_column()->append(DATUM_NOT_NULL);
                    }
                }
                for (size_t i = start; i < end; ++i) {
                    ColumnHelper::get_data_column(dst)->deserialize_and_append(this->data(state).buffer_result.data());
                }
            }
        } else {
            if (this->data(state).buffer_result.empty()) {
                for (size_t i = start; i < end; ++i) {
                    dst->append_default();
                }
            } else {
                if (dst->is_nullable()) {
                    for (size_t i = start; i < end; ++i) {
                        down_cast<NullableColumn*>(dst)->null_column()->append(DATUM_NOT_NULL);
                    }
                }
                for (size_t i = start; i < end; ++i) {
                    ColumnHelper::get_data_column(dst)->deserialize_and_append(this->data(state).buffer_result.data());
                }
            }
        }
    }

    std::string get_name() const override { return "maxmin_by"; }
};

} // namespace starrocks
