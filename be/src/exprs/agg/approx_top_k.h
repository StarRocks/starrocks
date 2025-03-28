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

#include <type_traits>

#include "column/array_column.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/aggregate_traits.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"
#include "util/phmap/phmap.h"

namespace starrocks {

template <LogicalType LT>
struct ApproxTopKState {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;

    struct Counter {
        CppType value = {};
        int64_t count = 0;

        // The index of this counter in the <counters> array
        const size_t _index;
        explicit Counter(const size_t index) : _index(index) {}
    };

    inline static std::function<bool(const Counter&, const Counter&)> cmp = [](const Counter& c1, const Counter& c2) {
        return c1.count < c2.count;
    };

    int32_t k = 0;
    int32_t counter_num = 0;
    int32_t unused_idx = 0;
    mutable VectorWithAggStateAllocator<Counter> counters;
    mutable Counter null_counter{0};
    using EQ = std::conditional_t<IsSlice<CppType>, SliceEqual, phmap::priv::hash_default_eq<CppType>>;
    phmap::flat_hash_map<CppType, Counter*, PhmapDefaultHashFunc<LT, PhmapSeed1>, EQ> table;
    bool is_init = false;

    void reset(int32_t k, int32_t counter_num) {
        this->k = k;
        this->counter_num = counter_num;
        this->unused_idx = 0;
        this->counters.reserve(counter_num);
        for (size_t i = 0; i < counter_num; i++) {
            this->counters.emplace_back(i);
        }
        null_counter.count = 0;
        this->table.clear();
    }

    void merge(MemPool* mem_pool, const std::vector<Counter> other_counters) {
        for (auto& other_counter : other_counters) {
            process<false>(mem_pool, other_counter.value, other_counter.count, true);
        }
    }

    // Here's a problem, we can't know if its update operation or merge operation
    // So we always increase the null_counter
    void process_null(const int64_t count) { null_counter.count += count; }

    template <bool IsDeepCopy>
    void process(MemPool* mem_pool, const CppType& value, const int64_t count, bool is_merge) {
        auto it = table.find(value);
        if (it != table.end()) {
            auto* counter = it->second;
            counter->count += count;
            _maintain_ordering(counter->_index);
        } else if (unused_idx < counter_num) {
            auto& empty_counter = counters[unused_idx];
            empty_counter.value = _copy<IsDeepCopy>(mem_pool, value);
            empty_counter.count = count;
            table[empty_counter.value] = &empty_counter;
            unused_idx++;
            _maintain_ordering(unused_idx - 1);
        } else {
            size_t min_idx = _min_index();
            auto& min_counter = counters[min_idx];
            if (!is_merge) {
                table.erase(min_counter.value);
                min_counter.value = _copy<IsDeepCopy>(mem_pool, value);
                // This is by design, space space algorithm requires increasing it instead of setting to <count>
                min_counter.count += count;
                table[min_counter.value] = &min_counter;
                _maintain_ordering(min_idx);
            } else if (count > min_counter.count) {
                table.erase(min_counter.value);
                min_counter.value = _copy<IsDeepCopy>(mem_pool, value);
                min_counter.count = count;
                table[min_counter.value] = &min_counter;
                _maintain_ordering(min_idx);
            }
        }
    }

    void get_values(Column* dst, size_t start, size_t end) const {
        auto* array_column = down_cast<ArrayColumn*>(dst);
        auto& offset_column = array_column->offsets_column();
        auto& elements_column = array_column->elements_column();

        // Array's fields must be nullable
        auto* nullable_struct_column = down_cast<NullableColumn*>(elements_column.get());
        auto* struct_column = down_cast<StructColumn*>(nullable_struct_column->data_column().get());
        // Struct's fields must be nullable
        auto* value_column = down_cast<NullableColumn*>(struct_column->fields_column()[0].get());
        auto* order_column = down_cast<NullableColumn*>(struct_column->fields_column()[1].get());

        for (size_t row = start; row < end; row++) {
            bool has_null = null_counter.count > 0;
            if (has_null) {
                const int32_t cnt = std::min(k, unused_idx + 1);
                // array_elements
                int32_t counter_i = unused_idx - 1;
                bool is_null_processed = false;
                for (int32_t i = 0; i < cnt; ++i) {
                    if (counter_i >= 0 && (is_null_processed || counters[counter_i].count > null_counter.count)) {
                        value_column->append_datum(counters[counter_i].value);
                        order_column->append_datum(counters[counter_i].count);
                        nullable_struct_column->null_column()->append(0);
                        counter_i--;
                    } else {
                        value_column->append_nulls(1);
                        order_column->append_datum(null_counter.count);
                        nullable_struct_column->null_column()->append(0);
                        is_null_processed = true;
                    }
                }

                // array_offsets
                offset_column->append(offset_column->get_data().back() + cnt);
            } else {
                const int32_t cnt = std::min(k, unused_idx);
                // array_elements
                for (int32_t i = unused_idx - 1; i >= (unused_idx - cnt); i--) {
                    value_column->append_datum(counters[i].value);
                    order_column->append_datum(counters[i].count);
                    nullable_struct_column->null_column()->append(0);
                }

                // array_offsets
                offset_column->append(offset_column->get_data().back() + cnt);
            }
        }
    }

private:
    // All counters are not empty, so the first counter must has the smallest count
    // But here, we need to find the rightest index that has the same count number as the first counter
    // to reduce the number of swap operations when maintaing the ordering property
    size_t _min_index() {
        DCHECK_EQ(counter_num, unused_idx);
        const auto& min_counter = counters[0];

        // Using std::upper_bound to find the first element greater than min_count
        auto it = std::upper_bound(counters.begin(), counters.begin() + unused_idx, min_counter, cmp);

        size_t boundary = std::distance(counters.begin(), it);
        DCHECK_GT(boundary, 0);
        return boundary - 1;
    }

    // Maintain the ordering of the counters, counters with smaller count are put in the front
    // This is used to speed up the lookup of the min counter
    void _maintain_ordering(const size_t idx) {
        DCHECK_LT(idx, unused_idx);
        size_t start = idx, end = idx;
        if (start > 0 && counters[start].count < counters[start - 1].count) {
            while (start > 0 && counters[start].count < counters[start - 1].count) {
                std::swap(counters[start].value, counters[start - 1].value);
                std::swap(counters[start].count, counters[start - 1].count);
                start--;
            }
        } else if (end < unused_idx - 1 && counters[end].count > counters[end + 1].count) {
            while (end < unused_idx - 1 && counters[end].count > counters[end + 1].count) {
                std::swap(counters[end].value, counters[end + 1].value);
                std::swap(counters[end].count, counters[end + 1].count);
                end++;
            }
        }
        if (start < end) {
            for (size_t i = start; i <= end; i++) {
                table.erase(counters[i].value);
            }
            for (size_t i = start; i <= end; i++) {
                table[counters[i].value] = &counters[i];
            }
        }
        DCHECK(std::is_sorted(counters.begin(), counters.begin() + unused_idx, cmp));
        [[maybe_unused]] auto check = [this]() -> bool {
            // Check index
            for (size_t i = 0; i < counter_num; i++) {
                if (i != counters[i]._index) {
                    return false;
                }
            }
            // Check table
            static EQ eq;
            for (auto& [k, v] : table) {
                if (!eq(k, v->value)) {
                    return false;
                }
            }
            return true;
        };
        DCHECK(check());
    }

    template <bool IsDeepCopy>
    CppType _copy(MemPool* mem_pool, const CppType& value) {
        if constexpr (IsDeepCopy && IsSlice<CppType>) {
            uint8_t* pos = mem_pool->allocate(value.size);
            std::memcpy(pos, value.data, value.size);
            return Slice{pos, value.size};
        } else {
            return value;
        }
    }
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class ApproxTopKAggregateFunction final
        : public AggregateFunctionBatchHelper<ApproxTopKState<LT>, ApproxTopKAggregateFunction<LT, T>> {
public:
    using CppType = RunTimeCppType<LT>;
    using InputColumnType = RunTimeColumnType<LT>;

    static constexpr int32_t DEFAULT_K = 5;
    static constexpr int32_t MAX_COUNTER_NUM = 100000;

    std::pair<int32_t, int32_t> get_k_and_counter_num(FunctionContext* ctx) const {
        int32_t k = DEFAULT_K;
        if (ctx->get_num_args() > 1) {
            k = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        }
        int32_t counter_num;
        if (ctx->get_num_args() > 2) {
            counter_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));
        } else {
            counter_num = std::min(std::max(2 * k, 100), MAX_COUNTER_NUM);
        }
        DCHECK_GT(k, 0);
        DCHECK_GT(counter_num, 0);
        DCHECK_LE(k, MAX_COUNTER_NUM);
        DCHECK_LE(counter_num, MAX_COUNTER_NUM);
        DCHECK_GE(counter_num, k);
        return std::make_pair(k, counter_num);
    }

    void init_state_if_necessary(FunctionContext* ctx, AggDataPtr __restrict state) const {
        if (this->data(state).is_init) {
            return;
        }
        this->data(state).is_init = true;
        const auto kv = get_k_and_counter_num(ctx);
        this->data(state).reset(kv.first, kv.second);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const {
        init_state_if_necessary(ctx, state);
        this->data(state).process_null(1);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice bytes = column->get(row_num).get_slice();

        size_t start = 0;
        int64_t null_count;
        int32_t effective_counter_num;
        // null count
        std::memcpy(&null_count, bytes.data + start, sizeof(int64_t));
        start += sizeof(int64_t);
        // effective counter number
        std::memcpy(&effective_counter_num, bytes.data + start, sizeof(int32_t));
        start += sizeof(int32_t);

        std::vector<typename ApproxTopKState<LT>::Counter> descrialize_counters;
        descrialize_counters.reserve(effective_counter_num);
        for (size_t i = 0; i < effective_counter_num; i++) {
            descrialize_counters.emplace_back(i);
        }

        for (size_t i = 0; i < effective_counter_num; i++) {
            // counter::value
            if constexpr (IsSlice<CppType>) {
                int64_t slice_size;
                std::memcpy(&slice_size, bytes.data + start, sizeof(int64_t));
                start += sizeof(int64_t);
                uint8_t* slice_data = ctx->mem_pool()->allocate(slice_size);
                std::memcpy(slice_data, bytes.data + start, slice_size);
                start += slice_size;
                descrialize_counters[i].value = Slice{slice_data, static_cast<size_t>(slice_size)};
            } else {
                std::memcpy(&descrialize_counters[i].value, bytes.data + start, sizeof(CppType));
                start += sizeof(CppType);
            }
            // counter::count
            std::memcpy(&descrialize_counters[i].count, bytes.data + start, sizeof(int64_t));
            start += sizeof(int64_t);
        }

        init_state_if_necessary(ctx, state);
        this->data(state).process_null(null_count);
        this->data(state).merge(ctx->mem_pool(), descrialize_counters);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        init_state_if_necessary(ctx, state);
        const auto* column = down_cast<const InputColumnType*>(ColumnHelper::get_data_column(columns[0]));
        const auto& value = AggDataTypeTraits<LT>::get_row_ref(*column, row_num);
        this->data(state).template process<true>(ctx->mem_pool(), value, 1, false);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        serialize_state(this->data(state), down_cast<BinaryColumn*>(to));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const auto kv = get_k_and_counter_num(ctx);
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());

        if (src[0]->is_nullable()) {
            auto* src_nullable_column = down_cast<const NullableColumn*>(src[0].get());
            auto* src_column = down_cast<const InputColumnType*>(src_nullable_column->data_column().get());

            ApproxTopKState<LT> state;
            for (size_t i = 0; i < src_nullable_column->size(); ++i) {
                state.reset(kv.first, kv.second);
                if (src_nullable_column->is_null(i)) {
                    state.process_null(1);
                } else {
                    state.template process<false>(ctx->mem_pool(), src_column->get_data()[i], 1, false);
                }
                serialize_state(state, dst_column);
            }
        } else {
            auto* src_column = down_cast<const InputColumnType*>(src[0].get());

            ApproxTopKState<LT> state;
            for (auto& value : src_column->get_data()) {
                state.reset(kv.first, kv.second);
                state.template process<false>(ctx->mem_pool(), value, 1, false);
                serialize_state(state, dst_column);
            }
        }
    }

    void serialize_state(const ApproxTopKState<LT>& state, BinaryColumn* dst) const {
        Bytes& bytes = dst->get_bytes();

        const size_t old_size = bytes.size();
        int64_t total_size = 0;
        int32_t effective_counter_num = 0;
        // null counter
        total_size += sizeof(int64_t);
        // effective counter number
        total_size += sizeof(int32_t);
        for (auto& counter : state.counters) {
            if (counter.count == 0) {
                continue;
            }
            effective_counter_num++;
            // counter::value
            if constexpr (IsSlice<CppType>) {
                total_size += sizeof(int64_t);
                total_size += counter.value.get_size();
            } else {
                total_size += sizeof(CppType);
            }
            // counter::count
            total_size += sizeof(int64_t);
        }
        const size_t new_size = old_size + total_size;
        bytes.resize(new_size);

        size_t start = old_size;
        // null counter
        std::memcpy(bytes.data() + start, &state.null_counter.count, sizeof(int64_t));
        start += sizeof(int64_t);
        // effective counter number
        std::memcpy(bytes.data() + start, &effective_counter_num, sizeof(int32_t));
        start += sizeof(int32_t);

        for (auto& counter : state.counters) {
            if (counter.count == 0) {
                continue;
            }
            // counter::value
            if constexpr (IsSlice<CppType>) {
                int64_t slice_size = counter.value.get_size();
                std::memcpy(bytes.data() + start, &slice_size, sizeof(int64_t));
                start += sizeof(int64_t);
                std::memcpy(bytes.data() + start, counter.value.get_data(), slice_size);
                start += slice_size;
            } else {
                std::memcpy(bytes.data() + start, &counter.value, sizeof(CppType));
                start += sizeof(CppType);
            }
            // counter::count
            std::memcpy(bytes.data() + start, &counter.count, sizeof(int64_t));
            start += sizeof(int64_t);
        }
        dst->get_offset().emplace_back(new_size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).get_values(to, 0, 1);
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        const auto kv = get_k_and_counter_num(ctx);
        this->data(state).reset(kv.first, kv.second);
    }

    void update_single_state_null(FunctionContext* ctx, AggDataPtr __restrict state, int64_t peer_group_start,
                                  int64_t peer_group_end) const {
        this->data(state).process_null(1);
    }

    // For approx_top_k, only support unbounded window
    // peer_group_start = partition_start
    // peer_group_end = partition_end
    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        for (size_t i = frame_start; i < frame_end; i++) {
            const auto& value = AggDataTypeTraits<LT>::get_row_ref(*column, i);
            this->data(state).template process<true>(ctx->mem_pool(), value, 1, false);
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        this->data(state).get_values(dst, start, end);
    }

    std::string get_name() const override { return "approx_top_k"; }
};
} // namespace starrocks
