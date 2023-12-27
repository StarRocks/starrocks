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
#include "types/hll.h"

namespace starrocks {

/**
 * RETURN_TYPE: TYPE_BIGINT
 * ARGS_TYPE: ALL TYPE
 * SERIALIZED_TYPE: TYPE_VARCHAR
 */
template <LogicalType LT, bool IsOutputHLL, typename T = RunTimeCppType<LT>>
class HllNdvAggregateFunction final
        : public AggregateFunctionBatchHelper<HyperLogLog, HllNdvAggregateFunction<LT, IsOutputHLL, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        uint64_t value = 0;
        const auto* column = down_cast<const ColumnType*>(columns[0]);

        if constexpr (lt_is_string<LT>) {
            Slice s = column->get_slice(row_num);
            value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
        } else {
            const auto& v = column->get_data();
            value = HashUtil::murmur_hash64A(&v[row_num], sizeof(v[row_num]), HashUtil::MURMUR_SEED);
        }

        if (value != 0) {
            this->data(state).update(value);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const ColumnType*>(columns[0]);

        if constexpr (lt_is_string<LT>) {
            uint64_t value = 0;
            for (size_t i = frame_start; i < frame_end; ++i) {
                Slice s = column->get_slice(i);
                value = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);

                if (value != 0) {
                    this->data(state).update(value);
                }
            }
        } else {
            uint64_t value = 0;
            const auto& v = column->get_data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                value = HashUtil::murmur_hash64A(&v[i], sizeof(v[i]), HashUtil::MURMUR_SEED);

                if (value != 0) {
                    this->data(state).update(value);
                }
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        const auto* hll_column = down_cast<const BinaryColumn*>(column);
        HyperLogLog hll(hll_column->get(row_num).get_slice());
        this->data(state).merge(hll);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        int64_t result = this->data(state).estimate_cardinality();

        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_binary());

        auto* column = down_cast<BinaryColumn*>(to);
        size_t size = this->data(state).max_serialized_size();
        uint8_t result[size];

        size = this->data(state).serialize(result);
        column->append(Slice(result, size));
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const auto* column = down_cast<const ColumnType*>(src[0].get());
        auto* result = down_cast<BinaryColumn*>((*dst).get());

        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 10);
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        uint64_t value = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            HyperLogLog hll;
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

            size_t new_size = old_size + hll.max_serialized_size();
            bytes.resize(new_size);
            hll.serialize(bytes.data() + old_size);

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        if constexpr (IsOutputHLL) {
            DCHECK(to->is_object());
            auto* column = down_cast<HyperLogLogColumn*>(to);
            auto& hll_value = const_cast<HyperLogLog&>(this->data(state));
            column->append(std::move(hll_value));
        } else {
            DCHECK(to->is_numeric());

            auto* column = down_cast<Int64Column*>(to);
            column->append(this->data(state).estimate_cardinality());
        }
    }

    std::string get_name() const override {
        if constexpr (IsOutputHLL) {
            return "hll_raw";
        } else {
            return "ndv";
        }
    }
};

} // namespace starrocks
