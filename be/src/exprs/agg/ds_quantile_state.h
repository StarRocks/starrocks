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
#include "types/ds_quantile_sketch.cpp"

namespace starrocks {
template <LogicalType LT>
struct DSSketchState<LT, QUANTILE> {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using SketchWarapperType = DataSketchesQuantile<CppType>;
    uint32_t ranks_size;
    std::unique_ptr<double[]> ranks = nullptr;
    std::unique_ptr<SketchWarapperType> ds_sketch_wrapper = nullptr;
    int64_t memory_usage = 0;

    void init(FunctionContext* ctx) {
        DatumArray datum_array;
        uint16_t k;
        std::tie(k, datum_array) = _parse_sketch_args(ctx);
        if (datum_array.size() < 1) {
            ranks_size = 1;
            ranks = std::make_unique<double[]>(ranks_size);
            *ranks.get() = 0.5;
        } else {
            ranks_size = datum_array.size();
            ranks = std::make_unique<double[]>(ranks_size);
            double* ranks_prt = ranks.get();
            for (Datum rank : datum_array) {
                *ranks_prt = rank.get_double();
                ranks_prt++;
            }
        }
        if (ranks_size == 0) {
            ranks_size = 0;
        }
        ds_sketch_wrapper = std::make_unique<SketchWarapperType>(k, &memory_usage);
    }

    bool is_inited() const { return ds_sketch_wrapper != nullptr; }

    void update(const Column* data_column, size_t row_num) const {
        const ColumnType* column = down_cast<const ColumnType*>(data_column);
        const auto& values = column->get_data();
        ds_sketch_wrapper->update(values[row_num]);
    }

    void update_batch_single_state_with_frame(const Column* data_column, int64_t frame_start, int64_t frame_end) const {
        const ColumnType* column = down_cast<const ColumnType*>(data_column);
        const auto& values = column->get_data();
        for (size_t i = frame_start; i < frame_end; ++i) {
            ds_sketch_wrapper->update(values[i]);
        }
    }

    void merge(const BinaryColumn* sketch_data_column, size_t row_num) {
        DSSketchState<LT, QUANTILE> other_state;
        other_state.deserialize(sketch_data_column->get(row_num).get_slice(), &memory_usage);
        if (UNLIKELY(!is_inited())) {
            ranks_size = other_state.ranks_size;
            ranks = std::make_unique<double[]>(ranks_size);
            double* ranks_prt = ranks.get();
            for (int i = 0; i < ranks_size; i++) {
                *ranks_prt = other_state.ranks.get()[i];
                ranks_prt++;
            }
            ds_sketch_wrapper =
                    std::make_unique<SketchWarapperType>(other_state.ds_sketch_wrapper->get_k(), &memory_usage);
        }
        ds_sketch_wrapper->merge(*other_state.ds_sketch_wrapper);
    }

    size_t serialize(uint8_t* dst) const {
        size_t offset = 0;
        memcpy(dst + offset, &ranks_size, sizeof(ranks_size));
        offset = offset + sizeof(uint32_t);
        memcpy(dst + offset, ranks.get(), ranks_size * sizeof(double));
        offset = offset + ranks_size * sizeof(double);
        size_t ser_sketch_size = ds_sketch_wrapper->serialize(dst + offset);
        return offset + ser_sketch_size;
    }

    size_t serialize_size() const {
        return sizeof(uint32_t) + ranks_size * sizeof(double) + ds_sketch_wrapper->serialize_size();
    }

    void deserialize(const Slice& slice, int64_t* memory_usage) {
        uint8_t* ptr = (uint8_t*)slice.get_data();
        size_t offset = 0;
        memcpy(&ranks_size, ptr + offset, sizeof(uint32_t));
        if (ranks_size == 0) {
            ranks_size = 0;
        }
        offset = offset + sizeof(uint32_t);
        ranks = std::make_unique<double[]>(ranks_size);
        memcpy(ranks.get(), ptr + offset, ranks_size * sizeof(double));
        offset = offset + ranks_size * sizeof(double);
        const Slice sketch_data_slice = Slice(slice.get_data() + offset, slice.size - offset);
        ds_sketch_wrapper = std::make_unique<SketchWarapperType>(sketch_data_slice, memory_usage);
    }

    void get_values(Column* dst, size_t start, size_t end) const {
        auto* array_column = down_cast<ArrayColumn*>(dst);
        auto& offset_column = array_column->offsets_column();
        auto& elements_column = array_column->elements_column();
        auto* nullable_column = down_cast<NullableColumn*>(elements_column.get());
        auto* result_column = down_cast<ColumnType*>(nullable_column->data_column().get());
        std::vector<CppType> result;
        if (LIKELY(ds_sketch_wrapper != nullptr)) {
            result = ds_sketch_wrapper->get_quantiles(ranks.get(), ranks_size);
        }

        uint32_t index = 0;
        for (size_t row = start; row < end; row++) {
            for (CppType result_data : result) {
                result_column->append(result_data);
                nullable_column->null_column()->append(0);
                index++;
            }
            offset_column->append(index);
        }
    }

    static std::string getFunName() { return "ds_quantile"; }

private:
    // parse k and rank_arr from args
    static std::tuple<uint16_t, DatumArray> _parse_sketch_args(FunctionContext* ctx) {
        uint16_t k = DEFAULT_QUANTILE_K;
        if (ctx->get_num_constant_columns() > 1) {
            if (ctx->get_num_constant_columns() > 2) {
                k = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));
                if (k <= 1) {
                    k = DEFAULT_QUANTILE_K;
                }
                int i = 1;
                while ((1 << i) < k) {
                    i += 1;
                }
                k = 1 << i;
            }
            Column* ranks_column = ColumnHelper::get_data_column(ctx->get_constant_column(1).get());
            if (ranks_column->is_array()) {
                DatumArray rank_arr = ranks_column->get(0).get_array();
                return {k, rank_arr};
            } else {
                DatumArray rank_arr;
                double rank_value = ranks_column->get(0).get_double();
                rank_arr.push_back(rank_value);
                return {k, rank_arr};
            }
        }
        DatumArray rank_arr;
        return {k, rank_arr};
    }
};

} // namespace starrocks
