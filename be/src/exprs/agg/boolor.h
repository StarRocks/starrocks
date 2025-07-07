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
#include "util/raw_container.h"

namespace starrocks {

struct BoolOrAggregateData {
    bool result = false;

    void reset() { result = false; }
};

struct BoolOrElement {
    void operator()(BoolOrAggregateData& state, const bool& value) const { state.result = state.result || value; }

    static bool is_sync(BoolOrAggregateData& state, const bool& right) { return !state.result && right; }

    static bool equals(const BoolOrAggregateData& state, const bool& right) { return state.result == right; }
};

class BoolOrAggregateFunction final
        : public AggregateFunctionBatchHelper<BoolOrAggregateData, BoolOrAggregateFunction> {
public:
    using InputColumnType = BooleanColumn;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void empty_result(FunctionContext* ctx, Column* to) const {
        if (to->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(to);
            auto& data_column = nullable->data_column();
            auto* output = down_cast<BooleanColumn*>(data_column.get());
            output->append(false);
            nullable->null_column_data().push_back(1);
        } else {
            auto* output = down_cast<BooleanColumn*>(to);
            output->append(false);
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        if (columns[0]->is_nullable()) {
            const auto& nullable_column = down_cast<const NullableColumn&>(*columns[0]);
            if (nullable_column.is_null(row_num)) {
                return;
            }
            const auto& data_column = nullable_column.data_column();
            const auto& column = down_cast<const InputColumnType&>(*data_column);
            bool value = column.immutable_data()[row_num];
            BoolOrElement()(this->data(state), value);
        } else {
            const auto& column = down_cast<const InputColumnType&>(*columns[0]);
            bool value = column.immutable_data()[row_num];
            BoolOrElement()(this->data(state), value);
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        if (this->data(state).result) {
            return;
        }

        if (columns[0]->is_nullable()) {
            const auto& nullable_column = down_cast<const NullableColumn&>(*columns[0]);
            const auto& data_column = nullable_column.data_column();

            for (size_t i = 0; i < chunk_size; ++i) {
                if (!nullable_column.is_null(i)) {
                    const auto& column = down_cast<const InputColumnType&>(*data_column);
                    bool value = column.immutable_data()[i];
                    if (value) {
                        this->data(state).result = true;
                        break;
                    }
                }
            }
        } else {
            const auto& column = down_cast<const InputColumnType&>(*columns[0]);

            for (size_t i = 0; i < chunk_size; ++i) {
                bool value = column.immutable_data()[i];
                if (value) {
                    this->data(state).result = true;
                    break;
                }
            }
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (this->data(state).result) {
            return;
        }

        if (columns[0]->is_nullable()) {
            const auto& nullable_column = down_cast<const NullableColumn&>(*columns[0]);
            const auto& data_column = nullable_column.data_column();
            const auto& column = down_cast<const InputColumnType&>(*data_column);

            for (size_t i = frame_start; i < frame_end; ++i) {
                if (!nullable_column.is_null(i)) {
                    bool value = column.immutable_data()[i];
                    if (value) {
                        this->data(state).result = true;
                        break;
                    }
                }
            }
        } else {
            const auto& column = down_cast<const InputColumnType&>(*columns[0]);

            for (size_t i = frame_start; i < frame_end; ++i) {
                bool value = column.immutable_data()[i];
                if (value) {
                    this->data(state).result = true;
                    break;
                }
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        if (column->is_nullable()) {
            const auto& nullable_column = down_cast<const NullableColumn&>(*column);
            if (nullable_column.is_null(row_num)) {
                return;
            }
            const auto& data_column = nullable_column.data_column();
            const auto* input_column = down_cast<const InputColumnType*>(data_column.get());
            bool value = input_column->immutable_data()[row_num];
            BoolOrElement()(this->data(state), value);
        } else {
            const auto* input_column = down_cast<const InputColumnType*>(column);
            bool value = input_column->immutable_data()[row_num];
            BoolOrElement()(this->data(state), value);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(to);
            auto& data_column = nullable->data_column();
            auto* output = down_cast<BooleanColumn*>(data_column.get());
            output->append(this->data(state).result);
            nullable->null_column_data().push_back(0);
        } else {
            auto* output = down_cast<BooleanColumn*>(to);
            output->append(this->data(state).result);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(to);
            auto& data_column = nullable->data_column();
            auto* output = down_cast<BooleanColumn*>(data_column.get());
            output->append(this->data(state).result);
            nullable->null_column_data().push_back(0);
        } else {
            auto* output = down_cast<BooleanColumn*>(to);
            output->append(this->data(state).result);
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        for (size_t i = start; i < end; ++i) {
            if (dst->is_nullable()) {
                auto* nullable = down_cast<NullableColumn*>(dst);
                auto& data_column = nullable->data_column();
                auto* output = down_cast<BooleanColumn*>(data_column.get());
                output->get_data()[i] = this->data(state).result;
            } else {
                auto* output = down_cast<BooleanColumn*>(dst);
                output->get_data()[i] = this->data(state).result;
            }
        }
    }

    std::string get_name() const override { return "bool_or"; }
};

} // namespace starrocks