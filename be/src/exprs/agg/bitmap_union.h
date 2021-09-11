// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/agg/bitmap_union.h

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

class BitmapUnionAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapUnionAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(columns[0]);
        this->data(state) |= *(col->get_object(row_num));
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        DCHECK(col->is_object());
        this->data(state) |= *(col->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        BitmapValue& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        *dst = std::move(src[0]);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        BitmapValue& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    std::string get_name() const override { return "bitmap_union"; }
};

} // namespace starrocks::vectorized
