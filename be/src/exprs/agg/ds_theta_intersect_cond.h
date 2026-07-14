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

#include <cstring>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "data_sketch/ds_theta.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

#pragma push_macro("IS_BIG_ENDIAN")
#undef IS_BIG_ENDIAN
#include "datasketches/theta_intersection.hpp"
#pragma pop_macro("IS_BIG_ENDIAN")

namespace starrocks {

// State for ds_theta_intersect_cond_agg: two independent union sketches routed
// by the is_anchor flag. At finalize, the two unions are intersected and the
// cardinality estimate is returned.
struct ThetaSketchIntersectCondState {
    std::unique_ptr<DataSketchesTheta> anchor_sketch = nullptr;
    std::unique_ptr<DataSketchesTheta> window_sketch = nullptr;
    int64_t anchor_mem = 0;
    int64_t window_mem = 0;
};

// Aggregate function: ds_theta_intersect_cond_agg(VARBINARY sketch, INT is_anchor) → DOUBLE
//
// Maintains two running theta-union sketches — anchor (is_anchor=1) and window
// (is_anchor=0). At finalize, intersects the two unions and returns the
// cardinality estimate of the intersection.
//
// Serialization format for distributed merge:
//   [4-byte uint32 anchor_size][anchor_bytes...][window_bytes...]
// where anchor_bytes and window_bytes are compact theta sketch serializations.
//
// This is the native equivalent of the Java ThetaSketchIntersectCondAgg UDAF,
// but operates directly on VARBINARY (no hex encoding) and returns DOUBLE.
class ThetaSketchIntersectCondAggregateFunction final
        : public AggregateFunctionBatchHelper<ThetaSketchIntersectCondState,
                                              ThetaSketchIntersectCondAggregateFunction> {
public:
    using alloc_type = DataSketchesTheta::alloc_type;
    using theta_intersection_type = datasketches::theta_intersection_alloc<alloc_type>;
    using wrapped_compact_theta_sketch = DataSketchesTheta::wrapped_compact_theta_sketch;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        auto& s = this->data(state);
        if (s.anchor_sketch != nullptr) {
            ctx->add_mem_usage(-s.anchor_mem);
            s.anchor_sketch.reset();
            s.anchor_mem = 0;
        }
        if (s.window_sketch != nullptr) {
            ctx->add_mem_usage(-s.window_mem);
            s.window_sketch.reset();
            s.window_mem = 0;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // columns[0] = VARBINARY compact theta sketch
        // columns[1] = INT is_anchor flag (1 = anchor, 0 = window)
        const Column* raw0 = ColumnHelper::get_data_column(columns[0]);
        const BinaryColumn* sketch_col = down_cast<const BinaryColumn*>(raw0);
        auto slice = sketch_col->get_slice(row_num);
        if (slice.size == 0) return;

        const Column* raw1 = ColumnHelper::get_data_column(columns[1]);
        int32_t is_anchor = down_cast<const FixedLengthColumn<int32_t>*>(raw1)->get_data()[row_num];

        if (is_anchor != 0) {
            _init_anchor_if_needed(state);
            auto* mem = &(this->data(state).anchor_mem);
            int64_t prev = *mem;
            DataSketchesTheta tmp(slice, mem);
            this->data(state).anchor_sketch->merge(tmp);
            ctx->add_mem_usage(*mem - prev);
        } else {
            _init_window_if_needed(state);
            auto* mem = &(this->data(state).window_mem);
            int64_t prev = *mem;
            DataSketchesTheta tmp(slice, mem);
            this->data(state).window_sketch->merge(tmp);
            ctx->add_mem_usage(*mem - prev);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const BinaryColumn* binary = down_cast<const BinaryColumn*>(column);
        auto slice = binary->get_slice(row_num);
        if (slice.size < 4) return;

        uint32_t anchor_size;
        memcpy(&anchor_size, slice.data, 4);
        if (4 + anchor_size > static_cast<uint32_t>(slice.size)) return;

        Slice anchor_slice(slice.data + 4, anchor_size);
        Slice window_slice(slice.data + 4 + anchor_size, slice.size - 4 - anchor_size);

        if (anchor_slice.size > 0) {
            _init_anchor_if_needed(state);
            auto* mem = &(this->data(state).anchor_mem);
            int64_t prev = *mem;
            DataSketchesTheta tmp(anchor_slice, mem);
            this->data(state).anchor_sketch->merge(tmp);
            ctx->add_mem_usage(*mem - prev);
        }
        if (window_slice.size > 0) {
            _init_window_if_needed(state);
            auto* mem = &(this->data(state).window_mem);
            int64_t prev = *mem;
            DataSketchesTheta tmp(window_slice, mem);
            this->data(state).window_sketch->merge(tmp);
            ctx->add_mem_usage(*mem - prev);
        }
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _emit_serialized(state, to);
    }

    // Single input row → partial serialized state for streaming pre-aggregation.
    // If is_anchor=1: writes [sketch_size][sketch_bytes] (anchor=sketch, window=empty).
    // If is_anchor=0: writes [0][sketch_bytes]           (anchor=empty, window=sketch).
    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        const Column* raw0 = ColumnHelper::get_data_column(src[0].get());
        const BinaryColumn* sketch_col = down_cast<const BinaryColumn*>(raw0);
        const Column* raw1 = ColumnHelper::get_data_column(src[1].get());
        const auto* flag_col = down_cast<const FixedLengthColumn<int32_t>*>(raw1);
        auto* out = down_cast<BinaryColumn*>(dst.get());

        for (size_t i = 0; i < chunk_size; ++i) {
            auto sketch = sketch_col->get_slice(i);
            int32_t is_anchor = flag_col->get_data()[i];
            uint32_t anchor_size = (is_anchor != 0) ? static_cast<uint32_t>(sketch.size) : 0;
            std::string row(4 + sketch.size, '\0');
            char* ptr = row.data();
            memcpy(ptr, &anchor_size, 4);
            if (sketch.size > 0) {
                memcpy(ptr + 4, sketch.data, sketch.size);
            }
            out->append(Slice(row.data(), row.size()));
        }
    }

    void finalize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                            Column* to) const override {
        const auto& s = this->data(state);
        auto* out = down_cast<DoubleColumn*>(to);

        if (s.anchor_sketch == nullptr || s.window_sketch == nullptr) {
            out->append(0.0);
            return;
        }

        std::vector<uint8_t> a_bytes(s.anchor_sketch->serialize_size());
        size_t a_sz = s.anchor_sketch->serialize(a_bytes.data());

        std::vector<uint8_t> w_bytes(s.window_sketch->serialize_size());
        size_t w_sz = s.window_sketch->serialize(w_bytes.data());

        if (a_sz == 0 || w_sz == 0) {
            out->append(0.0);
            return;
        }

        try {
            int64_t mem = 0;
            theta_intersection_type inter(datasketches::DEFAULT_SEED, alloc_type(&mem));
            inter.update(wrapped_compact_theta_sketch::wrap(reinterpret_cast<const char*>(a_bytes.data()), a_sz));
            inter.update(wrapped_compact_theta_sketch::wrap(reinterpret_cast<const char*>(w_bytes.data()), w_sz));
            out->append(inter.get_result().get_estimate());
        } catch (const std::exception&) {
            out->append(0.0);
        }
    }

    std::string get_name() const override { return "ds_theta_intersect_cond_agg"; }

private:
    void _init_anchor_if_needed(AggDataPtr __restrict state) const {
        if (UNLIKELY(this->data(state).anchor_sketch == nullptr)) {
            this->data(state).anchor_sketch = std::make_unique<DataSketchesTheta>(&(this->data(state).anchor_mem));
        }
    }

    void _init_window_if_needed(AggDataPtr __restrict state) const {
        if (UNLIKELY(this->data(state).window_sketch == nullptr)) {
            this->data(state).window_sketch = std::make_unique<DataSketchesTheta>(&(this->data(state).window_mem));
        }
    }

    void _emit_serialized(ConstAggDataPtr __restrict state, Column* to) const {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        const auto& s = this->data(state);

        std::vector<uint8_t> a_bytes;
        if (s.anchor_sketch != nullptr) {
            a_bytes.resize(s.anchor_sketch->serialize_size());
            size_t sz = s.anchor_sketch->serialize(a_bytes.data());
            a_bytes.resize(sz);
        }

        std::vector<uint8_t> w_bytes;
        if (s.window_sketch != nullptr) {
            w_bytes.resize(s.window_sketch->serialize_size());
            size_t sz = s.window_sketch->serialize(w_bytes.data());
            w_bytes.resize(sz);
        }

        uint32_t a_size = static_cast<uint32_t>(a_bytes.size());
        std::string result(4 + a_bytes.size() + w_bytes.size(), '\0');
        char* ptr = result.data();
        memcpy(ptr, &a_size, 4);
        ptr += 4;
        if (!a_bytes.empty()) {
            memcpy(ptr, a_bytes.data(), a_bytes.size());
            ptr += a_bytes.size();
        }
        if (!w_bytes.empty()) {
            memcpy(ptr, w_bytes.data(), w_bytes.size());
        }
        column->append(Slice(result.data(), result.size()));
    }
};

} // namespace starrocks
