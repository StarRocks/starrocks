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

template <LogicalType LT>
struct AnyValueAggregateData {
    using T = AggDataValueType<LT>;

    T result;
    bool has_value = false;

    void reset() {
        result = T{};
        has_value = false;
    }
};

template <LogicalType LT, typename State>
struct AnyValueElement {
    using RefType = AggDataRefType<LT>;

    void operator()(State& state, RefType right) const {
        if (UNLIKELY(!state.has_value)) {
            AggDataTypeTraits<LT>::assign_value(state.result, right);
            state.has_value = true;
        }
    }
};

template <LogicalType LT, typename State, class OP, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class AnyValueAggregateFunction final
        : public AggregateFunctionBatchHelper<State, AnyValueAggregateFunction<LT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        OP()(this->data(state), AggDataTypeTraits<LT>::get_row_ref(column, row_num));
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        update(ctx, columns, state, 0);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable());
        const auto& input_column = down_cast<const InputColumnType&>(*column);
        OP()(this->data(state), AggDataTypeTraits<LT>::get_row_ref(input_column, row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        AggDataTypeTraits<LT>::append_value(down_cast<InputColumnType*>(to), this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        AggDataTypeTraits<LT>::append_value(down_cast<InputColumnType*>(to), this->data(state).result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        InputColumnType* column = down_cast<InputColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            AggDataTypeTraits<LT>::append_value(column, this->data(state).result);
        }
    }

    std::string get_name() const override { return "any_value"; }
};

} // namespace starrocks
