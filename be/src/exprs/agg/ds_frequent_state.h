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
#include "types/ds_frequent_sketch.cpp"

namespace starrocks {

template <LogicalType LT>
struct DSSketchState<LT, FREQUENT> {
    using OriginalCppType = RunTimeCppType<LT>;
    using CppType = typename SpecialCppType<LT>::CppType;
    using ColumnType = RunTimeColumnType<LT>;
    using SketchWarapperType = DataSketchesFrequent<CppType>;
    uint64_t counter_num;
    uint8_t lg_max_map_size;
    uint8_t lg_start_map_size;
    std::unique_ptr<SketchWarapperType> ds_sketch_wrapper = nullptr;
    int64_t memory_usage = 0;

    void init(FunctionContext* ctx) {
        std::tie(counter_num, lg_max_map_size, lg_start_map_size) = _parse_sketch_args(ctx);
        ds_sketch_wrapper = std::make_unique<SketchWarapperType>(lg_max_map_size, lg_start_map_size, &memory_usage);
    }

    bool is_inited() const { return ds_sketch_wrapper != nullptr; }

    void update(const Column* data_column, size_t row_num) const {
        if constexpr (!IsSlice<OriginalCppType>) {
            const ColumnType* column = down_cast<const ColumnType*>(data_column);
            const auto& values = column->get_data();
            ds_sketch_wrapper->update(values[row_num]);
        } else {
            const BinaryColumn* column = down_cast<const BinaryColumn*>(data_column);
            const Slice data = column->get_slice(row_num);
            ds_sketch_wrapper->update(std::string(data.get_data(), data.size));
        }
    }

    void update_batch_single_state_with_frame(const Column* data_column, int64_t frame_start, int64_t frame_end) const {
        if constexpr (!IsSlice<OriginalCppType>) {
            const ColumnType* column = down_cast<const ColumnType*>(data_column);
            const auto& values = column->get_data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                ds_sketch_wrapper->update(values[i]);
            }
        } else {
            const BinaryColumn* column = down_cast<const BinaryColumn*>(data_column);
            for (size_t i = frame_start; i < frame_end; ++i) {
                const Slice data = column->get_slice(i);
                ds_sketch_wrapper->update(std::string(data.get_data(), data.size));
            }
        }
    }

    void merge(const BinaryColumn* sketch_data_column, size_t row_num) {
        DSSketchState<LT, FREQUENT> other_state;
        other_state.deserialize(sketch_data_column->get(row_num).get_slice(), &memory_usage);
        if (UNLIKELY(!is_inited())) {
            counter_num = other_state.counter_num;
            lg_max_map_size = other_state.lg_max_map_size;
            lg_start_map_size = other_state.lg_start_map_size;
            ds_sketch_wrapper = std::make_unique<SketchWarapperType>(lg_max_map_size, lg_max_map_size, &memory_usage);
        }
        ds_sketch_wrapper->merge(*other_state.ds_sketch_wrapper);
    }

    size_t serialize(uint8_t* dst) const {
        size_t offset = 0;
        memcpy(dst + offset, &counter_num, sizeof(uint64_t));
        offset = offset + sizeof(uint64_t);
        memcpy(dst + offset, &lg_max_map_size, sizeof(uint8_t));
        offset = offset + sizeof(uint8_t);
        memcpy(dst + offset, &lg_start_map_size, sizeof(uint8_t));
        offset = offset + sizeof(uint8_t);
        size_t ser_sketch_size = ds_sketch_wrapper->serialize(dst + offset);
        return offset + ser_sketch_size;
    }

    size_t serialize_size() const {
        return sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint8_t) + ds_sketch_wrapper->serialize_size();
    }

    void deserialize(const Slice& slice, int64_t* memory_usage) {
        uint8_t* ptr = (uint8_t*)slice.get_data();
        size_t offset = 0;
        memcpy(&counter_num, ptr + offset, sizeof(uint64_t));
        offset = offset + sizeof(uint64_t);
        memcpy(&lg_max_map_size, ptr + offset, sizeof(uint8_t));
        offset = offset + sizeof(uint8_t);
        memcpy(&lg_start_map_size, ptr + offset, sizeof(uint8_t));
        offset = offset + sizeof(uint8_t);
        const Slice sketch_data_slice = Slice(slice.get_data() + offset, slice.size - offset);
        ds_sketch_wrapper = std::make_unique<SketchWarapperType>(sketch_data_slice, lg_max_map_size, lg_start_map_size,
                                                                 memory_usage);
    }

    void get_values(Column* dst, size_t start, size_t end) const {
        auto* array_column = down_cast<ArrayColumn*>(dst);
        auto& offset_column = array_column->offsets_column();
        auto& elements_column = array_column->elements_column();

        auto* nullable_struct_column = down_cast<NullableColumn*>(elements_column.get());
        auto* struct_column = down_cast<StructColumn*>(nullable_struct_column->data_column().get());
        auto* value_column = down_cast<NullableColumn*>(struct_column->fields_column()[0].get());
        auto* count_column = down_cast<NullableColumn*>(struct_column->fields_column()[1].get());
        auto* lower_bound_column = down_cast<NullableColumn*>(struct_column->fields_column()[2].get());
        auto* upper_bound_column = down_cast<NullableColumn*>(struct_column->fields_column()[3].get());

        std::vector<FrequentRow<CppType>> result;
        if (LIKELY(ds_sketch_wrapper != nullptr)) {
            result = ds_sketch_wrapper->get_frequent_items(0);
        }
        uint32_t index = 0;
        for (size_t row = start; row < end; row++) {
            uint32_t counter_num_index = 0;
            for (FrequentRow<CppType> frequentRow : result) {
                if (counter_num_index >= counter_num) {
                    break;
                }
                if constexpr (!IsSlice<OriginalCppType>) {
                    value_column->append_datum(frequentRow.value);
                } else {
                    std::string value = frequentRow.value;
                    uint8_t value_data[value.length() + 1];
                    std::memcpy(value_data, value.data(), value.length());
                    value_data[value.length()] = '\0';
                    value_column->append_datum(Slice(value_data, value.length() + 1));
                }
                count_column->append_datum(frequentRow.count);
                lower_bound_column->append_datum(frequentRow.lower_bound);
                upper_bound_column->append_datum(frequentRow.upper_bound);
                nullable_struct_column->null_column()->append(0);
                index++;
                counter_num_index++;
            }
            offset_column->append(index);
        }
    }

    static std::string getFunName() { return "ds_frequent"; }

private:
    // parse threshold lg_max_map_size and lg_start_map_size from args
    static std::tuple<uint64_t, uint8_t, uint8_t> _parse_sketch_args(FunctionContext* ctx) {
        uint64_t counter_num = DEFAULT_COUNTER_NUM;
        uint8_t lg_max_map_size = DEFAULT_FREQUENT_LG_MAX_SIZE;
        uint8_t lg_start_map_size = DEFAULT_FREQUENT_LG_MIn_SIZE;
        if (ctx->get_num_constant_columns() > 1) {
            counter_num = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(1));
            if (ctx->get_num_constant_columns() > 2) {
                lg_max_map_size = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));
                if (ctx->get_num_constant_columns() > 3) {
                    lg_start_map_size = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(3));
                }
            }
        }
        if (lg_max_map_size <= lg_start_map_size) {
            lg_max_map_size = lg_start_map_size;
        }
        return {counter_num, lg_max_map_size, lg_start_map_size};
    }
};

} // namespace starrocks
