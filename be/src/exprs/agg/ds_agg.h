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

#include "exprs/agg/ds_frequent_state.h"
#include "exprs/agg/ds_hll_state.h"
#include "exprs/agg/ds_state.h"

namespace starrocks {

template <LogicalType LT, SketchType ST, typename StateType = DSSketchState<LT, ST>, typename T = RunTimeCppType<LT>>
class DataSketchesAggregateFunction final
        : public AggregateFunctionBatchHelper<StateType, DataSketchesAggregateFunction<LT, ST, StateType, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        if (this->data(state).is_inited()) {
            ctx->add_mem_usage(-this->data(state).memory_usage);
            this->data(state).ds_sketch_wrapper->clear();
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // init state if needed
        _init_if_needed(ctx, state);
        int64_t prev_memory = this->data(state).memory_usage;
        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        this->data(state).update(data_column, row_num);
        ctx->add_mem_usage(this->data(state).memory_usage - prev_memory);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // init state if needed
        _init_if_needed(ctx, state);
        int64_t prev_memory = this->data(state).memory_usage;
        const Column* data_column = ColumnHelper::get_data_column(columns[0]);
        this->data(state).update_batch_single_state_with_frame(data_column, frame_start, frame_end);
        ctx->add_mem_usage(this->data(state).memory_usage - prev_memory);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const BinaryColumn* sketch_data_column = down_cast<const BinaryColumn*>(column);
        int64_t prev_memory = this->data(state).memory_usage;
        this->data(state).merge(sketch_data_column, row_num);
        ctx->add_mem_usage(this->data(state).memory_usage - prev_memory);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        this->data(state).get_values(dst, start, end);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        if (UNLIKELY(!this->data(state).is_inited())) {
            column->append_default();
        } else {
            size_t serialized_size = this->data(state).serialize_size();
            std::vector<uint8_t> result(serialized_size);
            size_t actual_size = this->data(state).serialize(result.data());
            column->append(Slice(result.data(), actual_size));
        }
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* result = down_cast<BinaryColumn*>((*dst).get());

        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 10);
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        // convert to const Column*
        const auto* data_column = ColumnHelper::get_data_column(src[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            StateType state;
            state.init(ctx);
            state.update(data_column, i);
            size_t new_size = old_size + state.serialize_size();
            bytes.resize(new_size);
            state.serialize(bytes.data() + old_size);
            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        // this->data(state).finalize_to_column(to);
        this->data(state).get_values(to, 0, 1);
    }

    std::string get_name() const override { return StateType::getFunName(); }

private:
    // init hll sketch if needed
    void _init_if_needed(FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (UNLIKELY(!this->data(state).is_inited())) {
            this->data(state).init(ctx);
        }
    }
};

} // namespace starrocks
