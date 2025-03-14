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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "exprs/agg/ds_state.h"
#include "types/ds_theta_sketch.h"

namespace starrocks {
template <LogicalType LT>
struct DSSketchState<LT, THETA> {
    using CppType = typename SpecialCppType<LT>::CppType;
    using ColumnType = RunTimeColumnType<LT>;
    using SketchWarapperType = DataSketchesTheta;

    std::unique_ptr<SketchWarapperType> ds_sketch_wrapper = nullptr;
    int64_t memory_usage = 0;

    void init(FunctionContext* ctx) { ds_sketch_wrapper = std::make_unique<SketchWarapperType>(&memory_usage); }

    bool is_inited() const { return ds_sketch_wrapper != nullptr; }

    void merge(const BinaryColumn* sketch_data_column, size_t row_num) {
        DSSketchState<LT, THETA> other_state;
        other_state.deserialize(sketch_data_column->get(row_num).get_slice(), &memory_usage);
        if (UNLIKELY(!is_inited())) {
            ds_sketch_wrapper = std::make_unique<SketchWarapperType>(&memory_usage);
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
        ds_sketch_wrapper = std::make_unique<SketchWarapperType>(slice, memory_usage);
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

    static std::string getFunName() { return "ds_theta"; }
};

} // namespace starrocks