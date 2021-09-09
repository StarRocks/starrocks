// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/agg/bitmap_union_int.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/bitmap_value.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, typename T = RunTimeCppType<PT>>
class BitmapUnionIntAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapUnionIntAggregateFunction<PT, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        DCHECK((*columns[0]).is_numeric());
        if constexpr (std::is_integral_v<T>) {
            const auto& column = static_cast<const InputColumnType&>(*columns[0]);
            this->data(state).add(column.get_data()[row_num]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(column->is_object());
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        this->data(state) |= *(col->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        col->append(&(this->data(state)));
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        if constexpr (std::is_integral_v<T>) {
            BitmapColumn* dst_column = down_cast<BitmapColumn*>((*dst).get());
            const auto* src_column = static_cast<const InputColumnType*>(src[0].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                BitmapValue bitmap(src_column->get_data()[i]);
                dst_column->append(&bitmap);
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).cardinality());
    }

    std::string get_name() const override { return "bitmap_union_int"; }
};

} // namespace starrocks::vectorized
