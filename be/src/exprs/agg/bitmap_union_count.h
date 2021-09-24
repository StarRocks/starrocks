// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/bitmap_value.h"

namespace starrocks::vectorized {

class BitmapUnionCountAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapUnionCountAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(columns[0]);
        this->data(state) |= *(col->get_object(row_num));
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        this->data(state) |= *(col->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        auto& value = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(value));
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).cardinality());
    }

    std::string get_name() const override { return "bitmap_union_count"; }
};

} // namespace starrocks::vectorized
