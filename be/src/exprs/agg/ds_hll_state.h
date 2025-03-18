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

#include "column/column_helper.h"
#include "exprs/agg/ds_state.h"
#include "types/hll_sketch.h"

namespace starrocks {
template <LogicalType LT>
struct DSSketchState<LT, HLL> {
    using ColumnType = RunTimeColumnType<LT>;
    std::unique_ptr<DataSketchesHll> ds_sketch_wrapper = nullptr;
    int64_t memory_usage = 0;

    void init(FunctionContext* ctx) {
        uint8_t log_k;
        datasketches::target_hll_type tgt_type;
        std::tie(log_k, tgt_type) = _parse_hll_sketch_args(ctx);
        ds_sketch_wrapper = std::make_unique<DataSketchesHll>(log_k, tgt_type, &memory_usage);
    }

    bool is_inited() const { return ds_sketch_wrapper != nullptr; }

    void merge(const BinaryColumn* sketch_data_column, size_t row_num) {
        DSSketchState<LT, HLL> other_state;
        other_state.deserialize(sketch_data_column->get(row_num).get_slice(), &memory_usage);
        if (UNLIKELY(!is_inited())) {
            ds_sketch_wrapper =
                    std::make_unique<DataSketchesHll>(other_state.ds_sketch_wrapper->get_lg_config_k(),
                                                      other_state.ds_sketch_wrapper->get_target_type(), &memory_usage);
        }
        ds_sketch_wrapper->merge(*other_state.ds_sketch_wrapper);
    }

    void update(const Column* data_column, size_t row_num) const {
        uint64_t value = 0;
        const ColumnType* column = down_cast<const ColumnType*>(data_column);

        if constexpr (lt_is_string<LT>) {
            Slice s = column->get_slice(row_num);
            value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
        } else {
            const auto& v = column->get_data();
            value = HashUtil::murmur_hash64A(&v[row_num], sizeof(v[row_num]), HashUtil::MURMUR_SEED);
        }
        ds_sketch_wrapper->update(value);
    }

    void update_batch_single_state_with_frame(const Column* data_column, int64_t frame_start, int64_t frame_end) const {
        const ColumnType* column = down_cast<const ColumnType*>(data_column);
        if constexpr (lt_is_string<LT>) {
            uint64_t value = 0;
            for (size_t i = frame_start; i < frame_end; ++i) {
                Slice s = column->get_slice(i);
                value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);

                if (value != 0) {
                    ds_sketch_wrapper->update(value);
                }
            }
        } else {
            uint64_t value = 0;
            const auto& v = column->get_data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                value = HashUtil::murmur_hash64A(&v[i], sizeof(v[i]), HashUtil::MURMUR_SEED);

                if (value != 0) {
                    ds_sketch_wrapper->update(value);
                }
            }
        }
    }

    size_t serialize(uint8_t* dst) const { return ds_sketch_wrapper->serialize(dst); }

    size_t serialize_size() const { return ds_sketch_wrapper->serialize_size(); }

    void deserialize(const Slice& slice, int64_t* memory_usage) {
        ds_sketch_wrapper = std::make_unique<DataSketchesHll>(slice, memory_usage);
    }

    void get_values(Column* dst, size_t start, size_t end) const {
        Int64Column* column = down_cast<Int64Column*>(dst);
        int64_t result = 0L;
        if (LIKELY(ds_sketch_wrapper != nullptr)) {
            result = ds_sketch_wrapper->estimate_cardinality();
        }
        for (size_t i = start; i < end; ++i) {
            column->append(result);
        }
    }

    static std::string getFunName() { return "ds_hll_count_distinct"; }

private:
    // parse log_k and target type from args
    static std::tuple<uint8_t, datasketches::target_hll_type> _parse_hll_sketch_args(FunctionContext* ctx) {
        uint8_t log_k = DEFAULT_HLL_LOG_K;
        datasketches::target_hll_type tgt_type = datasketches::HLL_6;
        if (ctx->get_num_constant_columns() == 2) {
            log_k = (uint8_t)ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        } else if (ctx->get_num_constant_columns() == 3) {
            log_k = (uint8_t)ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
            Column* tgt_type_column = ColumnHelper::get_data_column(ctx->get_constant_column(2).get());
            std::string tgt_type_str = tgt_type_column->get(0).get_slice().to_string();
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
};

} // namespace starrocks