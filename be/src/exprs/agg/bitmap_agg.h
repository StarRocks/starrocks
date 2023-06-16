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

#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"

namespace starrocks {

template <LogicalType LT>
class BitmapAggAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapAggAggregateFunction<LT>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using InputCppType = RunTimeCppType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const auto* col = down_cast<const InputColumnType*>(columns[0]);
        auto value = col->get_data()[row_num];
        if (value >= 0 && value <= std::numeric_limits<uint64_t>::max()) {
            this->data(state).add(value);
        }
    }

    bool check_valid(const std::vector<InputCppType>& values, size_t count) const {
        for (size_t i = 0; i < count; i++) {
            auto value = values[i];
            if (!(value >= 0 && value <= std::numeric_limits<uint64_t>::max())) {
                return false;
            }
        }
        return true;
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto& col = down_cast<const InputColumnType&>(*columns[0]);
        const auto& values = col.get_data();
        if constexpr (LT == TYPE_INT) {
            if (check_valid(values, chunk_size)) {
                // All the values is unsigned, can be safely converted to unsigned int.
                this->data(state).add_many(chunk_size, reinterpret_cast<const uint32_t*>(values.data()));
                return;
            }
        }
        for (size_t i = 0; i < chunk_size; i++) {
            auto value = values[i];
            if (value >= 0 && value <= std::numeric_limits<uint64_t>::max()) {
                this->data(state).add(value);
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto& col = down_cast<const BitmapColumn&>(*column);
        this->data(state) |= *(col.get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* col = down_cast<BitmapColumn*>(to);
        auto& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto& src_column = down_cast<InputColumnType&>(*src[0].get());
        auto* dest_column = down_cast<BitmapColumn*>(dst->get());
        for (size_t i = 0; i < chunk_size; i++) {
            BitmapValue bitmap;
            auto v = src_column.get_data()[i];
            if (v >= 0 && v <= std::numeric_limits<uint64_t>::max()) {
                bitmap.add(v);
            }
            dest_column->append(std::move(bitmap));
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* col = down_cast<BitmapColumn*>(to);
        auto& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    std::string get_name() const override { return "bitmap_agg"; }
};
} // namespace starrocks