// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"

namespace starrocks::vectorized {

class BitmapUnionCountAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapValue, BitmapUnionCountAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(columns[0]);
        this->data(state) |= *(col->get_object(row_num));
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        this->data(state) |= *(col->get_object(row_num));
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const BitmapColumn* col = down_cast<const BitmapColumn*>(columns[0]);
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state) |= *(col->get_object(i));
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        Int64Column* column = down_cast<Int64Column*>(dst);
        auto& value = const_cast<BitmapValue&>(this->data(state));
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = value.cardinality();
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        auto& value = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(value));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).cardinality());
    }

    std::string get_name() const override { return "bitmap_union_count"; }
};

} // namespace starrocks::vectorized
