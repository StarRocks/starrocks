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

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "data_sketch/ds_theta.h"
#include "ds_theta_count_distinct.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {

// Aggregate function that unions serialized Apache DataSketches compact theta
// sketches across rows and emits a single serialized compact theta sketch.
//
// Input: VARBINARY column of compact theta sketches (any source — built via
//        ds_theta_accumulate, produced by scalar set ops, or loaded from
//        external Parquet/Iceberg by another Apache DataSketches consumer).
// Output: VARBINARY containing the unioned compact theta sketch.
//
// Differs from the agg-state combinator ds_theta_count_distinct_union in that
// it operates on plain VARBINARY rather than the state<varbinary> type, so it
// roundtrips with externally produced sketches and with the pairwise scalar
// set ops (ds_theta_union / intersect / a_not_b).
class ThetaSketchCombineAggregateFunction final
        : public AggregateFunctionBatchHelper<ThetaSketchState, ThetaSketchCombineAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        if (this->data(state).theta_sketch != nullptr) {
            ctx->add_mem_usage(-this->data(state).theta_sketch->mem_usage());
            this->data(state).theta_sketch->clear();
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        _merge_serialized(ctx, columns[0], state, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _merge_serialized(ctx, column, state, row_num);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _emit(state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // Input is already a compact theta serialization; pass through as-is.
        const BinaryColumn* in = down_cast<const BinaryColumn*>(src[0].get());
        auto* out = down_cast<BinaryColumn*>(dst.get());
        for (size_t i = 0; i < chunk_size; ++i) {
            out->append(in->get_slice(i));
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _emit(state, to);
    }

    std::string get_name() const override { return "ds_theta_combine"; }

private:
    void _init_if_needed(AggDataPtr __restrict state) const {
        if (UNLIKELY(this->data(state).theta_sketch == nullptr)) {
            this->data(state).theta_sketch = std::make_unique<DataSketchesTheta>(&(this->data(state).memory_usage));
        }
    }

    void _merge_serialized(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
                           size_t row_num) const {
        _init_if_needed(state);
        DCHECK(column->is_binary());
        const BinaryColumn* binary = down_cast<const BinaryColumn*>(column);
        auto slice = binary->get_slice(row_num);
        if (slice.size == 0) {
            return;
        }
        auto* mem_usage = &(this->data(state).memory_usage);
        int64_t prev_memory = *mem_usage;
        DataSketchesTheta theta(slice, mem_usage);
        this->data(state).theta_sketch->merge(theta);
        ctx->add_mem_usage(*mem_usage - prev_memory);
    }

    void _emit(ConstAggDataPtr __restrict state, Column* to) const {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        if (UNLIKELY(this->data(state).theta_sketch == nullptr)) {
            column->append_default();
            return;
        }
        size_t size = this->data(state).theta_sketch->serialize_size();
        std::vector<uint8_t> result(size);
        size = this->data(state).theta_sketch->serialize(result.data());
        column->append(Slice(result.data(), size));
    }
};

} // namespace starrocks
