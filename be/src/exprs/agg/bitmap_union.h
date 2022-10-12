// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"

namespace starrocks::vectorized {

class BitmapUnionAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapUnionAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(columns[0]);
        this->data(state) |= *(col->get_object(row_num));
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        DCHECK(col->is_object());
        this->data(state) |= *(col->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        BitmapValue& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        BitmapValue& bitmap = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(bitmap));
    }

    std::string get_name() const override { return "bitmap_union"; }
};

} // namespace starrocks::vectorized
