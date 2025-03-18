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
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "data_sketch/ds_theta.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {

struct ThetaSketchState {
    std::unique_ptr<DataSketchesTheta> theta_sketch = nullptr;
    int64_t memory_usage = 0;
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class ThetaSketchAggregateFunction final
        : public AggregateFunctionBatchHelper<ThetaSketchState, ThetaSketchAggregateFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        if (this->data(state).theta_sketch != nullptr) {
            ctx->add_mem_usage(-this->data(state).theta_sketch->mem_usage());
            this->data(state).theta_sketch->clear();
        }
    }

    void update_state(FunctionContext* ctx, AggDataPtr state, uint64_t value) const {
        int64_t prev_memory = this->data(state).theta_sketch->mem_usage();
        this->data(state).theta_sketch->update(value);
        ctx->add_mem_usage(this->data(state).theta_sketch->mem_usage() - prev_memory);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // init state if needed
        _init_if_needed(state);

        uint64_t value = 0;
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);

        if constexpr (lt_is_string<LT>) {
            Slice s = column->get_slice(row_num);
            value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
        } else {
            const auto& v = column->get_data();
            value = HashUtil::murmur_hash64A(&v[row_num], sizeof(v[row_num]), HashUtil::MURMUR_SEED);
        }
        update_state(ctx, state, value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // init state if needed
        _init_if_needed(state);
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        if constexpr (lt_is_string<LT>) {
            uint64_t value = 0;
            for (size_t i = frame_start; i < frame_end; ++i) {
                Slice s = column->get_slice(i);
                value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
                if (value != 0) {
                    update_state(ctx, state, value);
                }
            }
        } else {
            uint64_t value = 0;
            const auto& v = column->get_data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                value = HashUtil::murmur_hash64A(&v[i], sizeof(v[i]), HashUtil::MURMUR_SEED);

                if (value != 0) {
                    update_state(ctx, state, value);
                }
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _init_if_needed(state);

        auto* mem_usage = &(this->data(state).memory_usage);
        int64_t prev_memory = *mem_usage;

        DCHECK(column->is_binary());
        const BinaryColumn* theta_column = down_cast<const BinaryColumn*>(column);
        auto slice = theta_column->get_slice(row_num);
        DataSketchesTheta theta(slice, mem_usage);
        this->data(state).theta_sketch->merge(theta);

        ctx->add_mem_usage(*mem_usage - prev_memory);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        Int64Column* column = down_cast<Int64Column*>(dst);
        int64_t result = 0L;
        if (LIKELY(this->data(state).theta_sketch != nullptr)) {
            result = this->data(state).theta_sketch->estimate_cardinality();
        }
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        if (UNLIKELY(this->data(state).theta_sketch == nullptr)) {
            column->append_default();
        } else {
            size_t size = this->data(state).theta_sketch->serialize_size();
            uint8_t result[size];
            size = this->data(state).theta_sketch->serialize(result);
            column->append(Slice(result, size));
        }
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const ColumnType* input = down_cast<const ColumnType*>(src[0].get());
        auto* result = down_cast<BinaryColumn*>((*dst).get());

        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        uint64_t value = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (lt_is_string<LT>) {
                Slice s = input->get_slice(i);
                value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
            } else {
                auto v = input->get_data()[i];
                value = HashUtil::murmur_hash64A(&v, sizeof(v), HashUtil::MURMUR_SEED);
            }

            int64_t memory_usage = 0;
            DataSketchesTheta theta{&memory_usage};
            theta.update(value);

            size_t new_size = old_size + theta.serialize_size();
            bytes.resize(new_size);
            theta.serialize(bytes.data() + old_size);

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        DCHECK(to->is_numeric());

        auto* column = down_cast<Int64Column*>(to);
        if (UNLIKELY(this->data(state).theta_sketch == nullptr)) {
            column->append(0L);
        } else {
            column->append(this->data(state).theta_sketch->estimate_cardinality());
        }
    }

    std::string get_name() const override { return "ds_theta_count_distinct"; }

private:
    // init theta sketch if needed
    void _init_if_needed(AggDataPtr __restrict state) const {
        if (UNLIKELY(this->data(state).theta_sketch == nullptr)) {
            this->data(state).theta_sketch = std::make_unique<DataSketchesTheta>(&(this->data(state).memory_usage));
        }
    }
};

} // namespace starrocks
