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
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/hll_sketch.h"

namespace starrocks {

struct HLLSketchState {
    std::unique_ptr<DataSketchesHll> hll_sketch = nullptr;
    int64_t memory_usage = 0;
};

/**
 * RETURN_TYPE: TYPE_BIGINT
 * ARGS_TYPE: ALL TYPE
 * SERIALIZED_TYPE: TYPE_VARCHAR
 */
template <LogicalType LT, typename T = RunTimeCppType<LT>>
class HllSketchAggregateFunction final
        : public AggregateFunctionBatchHelper<HLLSketchState, HllSketchAggregateFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        if (this->data(state).hll_sketch != nullptr) {
            ctx->add_mem_usage(-this->data(state).hll_sketch->mem_usage());
            this->data(state).hll_sketch->clear();
        }
    }

    void update_state(FunctionContext* ctx, AggDataPtr state, uint64_t value) const {
        int64_t prev_memory = this->data(state).hll_sketch->mem_usage();
        this->data(state).hll_sketch->update(value);
        ctx->add_mem_usage(this->data(state).hll_sketch->mem_usage() - prev_memory);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // init state if needed
        _init_if_needed(ctx, columns, state);

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
        _init_if_needed(ctx, columns, state);
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
        DCHECK(column->is_binary());
        const BinaryColumn* hll_column = down_cast<const BinaryColumn*>(column);
        DataSketchesHll hll(hll_column->get(row_num).get_slice(), &(this->data(state).memory_usage));
        if (UNLIKELY(this->data(state).hll_sketch == nullptr)) {
            this->data(state).hll_sketch = std::make_unique<DataSketchesHll>(
                    hll.get_lg_config_k(), hll.get_target_type(), &(this->data(state).memory_usage));
        }
        int64_t prev_memory = this->data(state).hll_sketch->mem_usage();
        this->data(state).hll_sketch->merge(hll);
        ctx->add_mem_usage(this->data(state).hll_sketch->mem_usage() - prev_memory);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        Int64Column* column = down_cast<Int64Column*>(dst);
        int64_t result = 0L;
        if (LIKELY(this->data(state).hll_sketch != nullptr)) {
            result = this->data(state).hll_sketch->estimate_cardinality();
        }
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        if (UNLIKELY(this->data(state).hll_sketch == nullptr)) {
            column->append_default();
        } else {
            size_t size = this->data(state).hll_sketch->serialize_size();
            uint8_t result[size];
            size = this->data(state).hll_sketch->serialize(result);
            column->append(Slice(result, size));
        }
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const ColumnType* column = down_cast<const ColumnType*>(src[0].get());
        auto* result = down_cast<BinaryColumn*>((*dst).get());

        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 10);
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        uint64_t value = 0;
        uint8_t log_k;
        datasketches::target_hll_type tgt_type;
        // convert to const Column*
        std::vector<const Column*> src_datas;
        src_datas.reserve(src.size());
        std::transform(src.begin(), src.end(), std::back_inserter(src_datas),
                       [](const ColumnPtr& col) { return col.get(); });
        const Column** src_datas_ptr = src_datas.data();
        std::tie(log_k, tgt_type) = _parse_hll_sketch_args(ctx, src_datas_ptr);
        for (size_t i = 0; i < chunk_size; ++i) {
            int64_t memory_usage = 0;
            DataSketchesHll hll{log_k, tgt_type, &memory_usage};
            if constexpr (lt_is_string<LT>) {
                Slice s = column->get_slice(i);
                value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
            } else {
                auto v = column->get_data()[i];
                value = HashUtil::murmur_hash64A(&v, sizeof(v), HashUtil::MURMUR_SEED);
            }
            if (value != 0) {
                hll.update(value);
            }

            size_t new_size = old_size + hll.serialize_size();
            bytes.resize(new_size);
            hll.serialize(bytes.data() + old_size);

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        DCHECK(to->is_numeric());

        auto* column = down_cast<Int64Column*>(to);
        if (UNLIKELY(this->data(state).hll_sketch == nullptr)) {
            column->append(0L);
        } else {
            column->append(this->data(state).hll_sketch->estimate_cardinality());
        }
    }

    std::string get_name() const override { return "ds_hll_count_distinct"; }

private:
    // init hll sketch if needed
    void _init_if_needed(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state) const {
        if (UNLIKELY(this->data(state).hll_sketch == nullptr)) {
            uint8_t log_k;
            datasketches::target_hll_type tgt_type;
            std::tie(log_k, tgt_type) = _parse_hll_sketch_args(ctx, columns);
            this->data(state).hll_sketch = _init_hll_sketch(log_k, tgt_type, &(this->data(state).memory_usage));
        }
    }

    // parse log_k and target type from args
    std::tuple<uint8_t, datasketches::target_hll_type> _parse_hll_sketch_args(FunctionContext* ctx,
                                                                              const Column** columns) const {
        uint8_t log_k = DEFAULT_HLL_LOG_K;
        datasketches::target_hll_type tgt_type = datasketches::HLL_6;
        if (ctx->get_num_args() == 2) {
            log_k = (uint8_t)(columns[1]->get(0).get_int32());
        } else if (ctx->get_num_args() == 3) {
            log_k = (uint8_t)(columns[1]->get(0).get_int32());
            std::string tgt_type_str = columns[2]->get(0).get_slice().to_string();
            std::transform(tgt_type_str.begin(), tgt_type_str.end(), tgt_type_str.begin(), ::toupper);
            if (tgt_type_str == "HLL_4") {
                tgt_type = datasketches::HLL_4;
            } else if (tgt_type_str == "HLL_8") {
                tgt_type = datasketches::HLL_8;
            } else {
                tgt_type = datasketches::HLL_6;
            }
        }
        return {log_k, tgt_type};
    }

    // init hll sketch with default log_k and target type
    std::unique_ptr<DataSketchesHll> _init_hll_sketch(uint8_t log_k, datasketches::target_hll_type tgt_type,
                                                      int64_t* memory_usage) const {
        return std::make_unique<DataSketchesHll>(log_k, tgt_type, memory_usage);
    }
};

} // namespace starrocks
