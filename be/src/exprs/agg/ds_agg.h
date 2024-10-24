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
#include "types/ds_sketch.h"

namespace starrocks {

enum SketchType {
    HLL = 0,
    QUANTILE = 1,
    FREQUENT = 2,
    THETA = 3,
};

template <LogicalType LT, SketchType ST>
struct DSSketchState {};

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
        if (ctx->get_num_args() == 2) {
            log_k = (uint8_t)ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        } else if (ctx->get_num_args() == 3) {
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
        if (ctx->get_num_args() > 1) {
            if (ctx->get_num_args() > 2) {
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

template <LogicalType LT>
struct SpecialCppType {
    using CppType = RunTimeCppType<LT>;
};
template <>
struct SpecialCppType<TYPE_BINARY> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_VARBINARY> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_CHAR> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_VARCHAR> {
    using CppType = std::string;
};

template <LogicalType LT>
struct DSSketchState<LT, FREQUENT> {
    using OriginalCppType = RunTimeCppType<LT>;
    using CppType = SpecialCppType<LT>::CppType;
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
        if (ctx->get_num_args() > 1) {
            counter_num = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(1));
            if (ctx->get_num_args() > 2) {
                lg_max_map_size = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));
                if (ctx->get_num_args() > 3) {
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

template <LogicalType LT>
struct DSSketchState<LT, THETA> {
    using CppType = SpecialCppType<LT>::CppType;
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
            size_t size = this->data(state).serialize_size();
            uint8_t result[size];
            size = this->data(state).serialize(result);
            column->append(Slice(result, size));
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
