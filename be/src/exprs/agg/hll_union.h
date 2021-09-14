// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

/**
 * RETURN_TYPE: TYPE_HLL
 * ARGS_TYPE: TYPE_HLL
 * SERIALIZED_TYPE: TYPE_HLL
 */
class HllUnionAggregateFunction final : public AggregateFunctionBatchHelper<HyperLogLog, HllUnionAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const HyperLogLogColumn* column = down_cast<const HyperLogLogColumn*>(columns[0]);
        this->data(state).merge(*(column->get_object(row_num)));
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        const HyperLogLogColumn* column = down_cast<const HyperLogLogColumn*>(columns[0]);
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).merge(*(column->get_object(i)));
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(column->is_object());

        const HyperLogLogColumn* hll_column = down_cast<const HyperLogLogColumn*>(column);
        this->data(state).merge(*(hll_column->get_object(row_num)));
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr state, Column* dst, size_t start, size_t end) const override {
        DCHECK_GT(end, start);
        DCHECK(dst->is_object());
        auto* column = down_cast<HyperLogLogColumn*>(dst);

        for (size_t i = start; i < end; ++i) {
            column->append(&this->data(state));
        }
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr state,
                             Column* to) const override {
        DCHECK(to->is_object());
        auto* column = down_cast<HyperLogLogColumn*>(to);

        column->append(&this->data(state));
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        *dst = std::move(src[0]);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr state,
                            Column* to) const override {
        DCHECK(to->is_object());
        auto* column = down_cast<HyperLogLogColumn*>(to);

        column->append(&this->data(state));
    }

    std::string get_name() const override { return "hll_union"; }
};

} // namespace starrocks::vectorized
