// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"

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

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_object());
        const BitmapColumn* col = down_cast<const BitmapColumn*>(column);
        this->data(state) |= *(col->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        BitmapColumn* col = down_cast<BitmapColumn*>(to);
        auto& value = const_cast<BitmapValue&>(this->data(state));
        col->append(std::move(value));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if constexpr (std::is_integral_v<T>) {
            BitmapColumn* dst_column = down_cast<BitmapColumn*>((*dst).get());
            const auto* src_column = static_cast<const InputColumnType*>(src[0].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                BitmapValue bitmap(src_column->get_data()[i]);
                dst_column->append(std::move(bitmap));
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).cardinality());
    }

    std::string get_name() const override { return "bitmap_union_int"; }
};

} // namespace starrocks::vectorized
