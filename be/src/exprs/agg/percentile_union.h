// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {
class PercentileUnionAggregateFunction final
        : public AggregateFunctionBatchHelper<PercentileValue, PercentileUnionAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const auto* column = down_cast<const PercentileColumn*>(columns[0]);
        this->data(state).merge(column->get_object(row_num));
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const PercentileColumn* column = down_cast<const PercentileColumn*>(columns[0]);
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).merge(column->get_object(i));
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_object());

        const PercentileColumn* percentile_column = down_cast<const PercentileColumn*>(column);
        this->data(state).merge(percentile_column->get_object(row_num));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_object());
        auto* column = down_cast<PercentileColumn*>(to);
        auto& percentile_value = const_cast<PercentileValue&>(this->data(state));
        column->append(std::move(percentile_value));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_object());
        auto* column = down_cast<PercentileColumn*>(to);
        auto& percentile_value = const_cast<PercentileValue&>(this->data(state));
        column->append(std::move(percentile_value));
    }

    std::string get_name() const override { return "percentile_union"; }
};
} // namespace starrocks::vectorized
