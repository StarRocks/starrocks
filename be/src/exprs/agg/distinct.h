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

#include <algorithm>
#include <cstring>
#include <string>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregator_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/sum.h"
#include "exprs/function_context.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks {

enum AggDistinctType { COUNT = 0, SUM = 1 };

static const size_t MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA = 24;

template <LogicalType LT, LogicalType SumLT, typename = guard::Guard>
struct DistinctAggregateState {};

template <LogicalType LT, LogicalType SumLT>
struct DistinctAggregateState<LT, SumLT, FixedLengthLTGuard<LT>> {
    using T = RunTimeCppType<LT>;
    using SumType = RunTimeCppType<SumLT>;
    using MyHashSet = HashSetWithAggStateAllocator<T>;

    void update(T key) { set.insert(key); }

    void update_with_hash([[maybe_unused]] MemPool* mempool, T key, size_t hash) { set.emplace_with_hash(hash, key); }

    void prefetch(T key) { set.prefetch(key); }

    int64_t distinct_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = set.dump_bound();
        DCHECK(size >= MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA);
        return size;
    }

    void serialize(uint8_t* dst) const {
        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst));
        [[maybe_unused]] auto err = set.dump(output);
        DCHECK(output.length() == set.dump_bound());
    }

    void deserialize_and_merge(const uint8_t* src, size_t len) {
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src));
        auto old_size = set.size();
        if (old_size == 0) {
            [[maybe_unused]] auto err = set.load(input);
        } else {
            MyHashSet set_src;
            [[maybe_unused]] auto err = set_src.load(input);
            set.merge(set_src);
        }
    }

    SumType sum_distinct() const {
        SumType sum{};
        // Sum distinct doesn't support timestamp and date type
        if constexpr (IsDateTime<SumType>) {
            return sum;
        }

        for (auto& key : set) {
            sum += key;
        }
        return sum;
    }

    MyHashSet set;
};

struct AdaptiveSliceHashSet {
    using KeyType = typename SliceHashSet::key_type;

    AdaptiveSliceHashSet() { set = std::make_shared<SliceHashSetWithAggStateAllocator>(); }

    void clear() {
        set = std::make_shared<SliceHashSetWithAggStateAllocator>();
        two_level_set.reset();
        distinct_size = 0;
    }

    void try_convert_to_two_level(MemPool* mem_pool) {
        if (distinct_size % 65536 == 0 && mem_pool->total_allocated_bytes() >= agg::two_level_memory_threshold) {
            two_level_set = std::make_shared<SliceTwoLevelHashSetWithAggStateAllocator>();
            two_level_set->reserve(set->capacity());
            two_level_set->insert(set->begin(), set->end());
            set.reset();
        }
    }

    void emplace(MemPool* mem_pool, Slice raw_key) {
        KeyType key(raw_key);
        if (set != nullptr) {
#if defined(__clang__) && (__clang_major__ >= 16)
            set->lazy_emplace(key, [&](const auto& ctor) {
#else
            set->template lazy_emplace(key, [&](const auto& ctor) {
#endif
                uint8_t* pos = mem_pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                assert(pos != nullptr);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                distinct_size++;
                try_convert_to_two_level(mem_pool);
            });
        } else {
#if defined(__clang__) && (__clang_major__ >= 16)
            two_level_set->lazy_emplace(key, [&](const auto& ctor) {
#else
            two_level_set->template lazy_emplace(key, [&](const auto& ctor) {
#endif
                uint8_t* pos = mem_pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                assert(pos != nullptr);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                distinct_size++;
            });
        }
    }

    void lazy_emplace_with_hash(MemPool* mem_pool, Slice raw_key, size_t hash) {
        KeyType key(reinterpret_cast<uint8_t*>(raw_key.data), raw_key.size, hash);
        if (set != nullptr) {
#if defined(__clang__) && (__clang_major__ >= 16)
            set->lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
#else
            set->template lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
#endif
                uint8_t* pos = mem_pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                assert(pos != nullptr);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                distinct_size++;
                try_convert_to_two_level(mem_pool);
            });
        } else {
#if defined(__clang__) && (__clang_major__ >= 16)
            two_level_set->lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
#else
            two_level_set->template lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
#endif
                uint8_t* pos = mem_pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                assert(pos != nullptr);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                distinct_size++;
            });
        }
    }

    void prefetch_hash(size_t hash_value) {
        if (set != nullptr) {
            set->prefetch_hash(hash_value);
        } else {
            two_level_set->prefetch_hash(hash_value);
        }
    }

    int64_t serialize_size() const {
        size_t size = 0;
        if (set != nullptr) {
            for (auto& key : *set) {
                size += key.size + sizeof(uint32_t);
            }
        } else {
            for (auto& key : *two_level_set) {
                size += key.size + sizeof(uint32_t);
            }
        }
        return size;
    }

    void serialize(uint8_t* dst) const {
        if (set != nullptr) {
            for (auto& key : *set) {
                auto size = (uint32_t)key.size;
                memcpy(dst, &size, sizeof(uint32_t));
                dst += sizeof(uint32_t);
                memcpy(dst, key.data, key.size);
                dst += key.size;
            }
        } else {
            for (auto& key : *two_level_set) {
                auto size = (uint32_t)key.size;
                memcpy(dst, &size, sizeof(uint32_t));
                dst += sizeof(uint32_t);
                memcpy(dst, key.data, key.size);
                dst += key.size;
            }
        }
    }

    void fill_vector(std::vector<std::string>& values) const {
        if (set != nullptr) {
            for (const auto& v : *set) {
                values.emplace_back(v.data, v.size);
            }
        } else {
            for (const auto& v : *two_level_set) {
                values.emplace_back(v.data, v.size);
            }
        }
    }

    int64_t size() const { return distinct_size; }

    std::shared_ptr<SliceHashSetWithAggStateAllocator> set;
    std::shared_ptr<SliceTwoLevelHashSetWithAggStateAllocator> two_level_set;
    int64_t distinct_size = 0;

    HashOnSliceWithHash hash_function() const { return HashOnSliceWithHash(); }
};

template <LogicalType LT, LogicalType SumLT>
struct DistinctAggregateState<LT, SumLT, StringOrBinaryGuard<LT>> {
    DistinctAggregateState() = default;
    using KeyType = typename SliceHashSet::key_type;

    void reset() { set.clear(); }
    void update(MemPool* mem_pool, Slice raw_key) { set.emplace(mem_pool, raw_key); }

    void update_with_hash(MemPool* mem_pool, Slice raw_key, size_t hash) {
        set.lazy_emplace_with_hash(mem_pool, raw_key, hash);
    }

    int64_t distinct_count() const { return set.distinct_size; }

    size_t serialize_size() const { return set.serialize_size(); }

    // TODO(kks): If we put all string key to one continue memory,
    // then we could only one memcpy.
    void serialize(uint8_t* dst) const { return set.serialize(dst); }

    void deserialize_and_merge(MemPool* mem_pool, const uint8_t* src, size_t len) {
        const uint8_t* end = src + len;
        while (src < end) {
            uint32_t size = 0;
            memcpy(&size, src, sizeof(uint32_t));
            src += sizeof(uint32_t);
            Slice raw_key(src, size);
            // we only memcpy when the key is new
            set.emplace(mem_pool, raw_key);
            src += size;
        }
        DCHECK(src == end);
    }

    AdaptiveSliceHashSet set;
};

// use a different way to do serialization to gain performance.
template <LogicalType LT, LogicalType SumLT, bool compute_sum = false, typename = guard::Guard>
struct DistinctAggregateStateV2Base {};

template <LogicalType LT, LogicalType SumLT, bool compute_sum>
struct DistinctAggregateStateV2Base<LT, SumLT, compute_sum, FixedLengthLTGuard<LT>> {
    using T = RunTimeCppType<LT>;
    using SumType = RunTimeCppType<SumLT>;
    using MyHashSet = HashSetWithAggStateAllocator<T>;

    void reset() {
        set.clear();
        sum = SumType{};
    }

    void update(T key) {
        [[maybe_unused]] const auto result = set.insert(key);
        if constexpr (compute_sum) {
            if (result.second) {
                sum += key;
            }
        }
    }

    void update_with_hash([[maybe_unused]] MemPool* mempool, T key, size_t hash) {
        [[maybe_unused]] const auto result = set.emplace_with_hash(hash, key);
        if constexpr (compute_sum) {
            if (result.second) {
                sum += key;
            }
        }
    }

    void prefetch(T key) { set.prefetch(key); }

    int64_t distinct_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = set.size() * sizeof(T) + sizeof(size_t);
        size = std::max(size, MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA);
        return size;
    }

    void serialize(uint8_t* dst) const {
        size_t size = set.size();
        memcpy(dst, &size, sizeof(size));
        dst += sizeof(size);
        for (auto& key : set) {
            memcpy(dst, &key, sizeof(key));
            dst += sizeof(T);
        }
    }

    void deserialize_and_merge(const uint8_t* src, size_t len) {
        size_t size = 0;
        memcpy(&size, src, sizeof(size));
        set.rehash(set.size() + size);

        src += sizeof(size);
        for (size_t i = 0; i < size; i++) {
            T key;
            memcpy(&key, src, sizeof(T));
            set.insert(key);
            src += sizeof(T);
        }
    }

    SumType sum_distinct() const {
        SumType sum{};
        // Sum distinct doesn't support timestamp and date type
        if constexpr (IsDateTime<SumType>) {
            return sum;
        }

        for (auto& key : set) {
            sum += key;
        }
        return sum;
    }

    // NOLINTBEGIN
    ~DistinctAggregateStateV2Base() {
#ifdef PHMAP_USE_CUSTOM_INFO_HANDLE
        const auto& info = set.infoz();
        VLOG_FILE << "HashSet Info: # of insert = " << info.insert_number
                  << ", insert probe length = " << info.insert_probe_length << ", # of rehash = " << info.rehash_number;
#endif
    }
    // NOLINTEND

    MyHashSet set;
    SumType sum{};
};

template <LogicalType LT, LogicalType SumLT>
struct DistinctAggregateStateV2Base<LT, SumLT, false, StringOrBinaryGuard<LT>>
        : public DistinctAggregateState<LT, SumLT> {};

template <LogicalType LT, LogicalType SumLT, typename = guard::Guard>
struct DistinctAggregateStateV2 : public DistinctAggregateStateV2Base<LT, SumLT, false> {};

template <LogicalType LT, LogicalType SumLT, bool compute_sum, typename = guard::Guard>
struct FusedMultiDistinctAggregateState : public DistinctAggregateStateV2Base<LT, SumLT, compute_sum> {
    using CppType = RunTimeCppType<LT>;
    using Base = DistinctAggregateStateV2Base<LT, SumLT, compute_sum>;
    using Base::reset;
    using Base::update;
    MemPool mem_pool;

    void update(CppType key) {
        if constexpr (IsSlice<CppType>) {
            this->Base::update(&mem_pool, key);
        } else {
            this->Base::update(key);
        }
    }

    void reset() {
        this->Base::reset();
        mem_pool.clear();
    }
};

template <LogicalType LT, LogicalType SumLT,
          template <LogicalType X, LogicalType Y, typename = guard::Guard> class TDistinctAggState,
          AggDistinctType DistinctType, typename T = RunTimeCppType<LT>>
class TDistinctAggregateFunction final
        : public AggregateFunctionBatchHelper<
                  TDistinctAggState<LT, SumLT>,
                  TDistinctAggregateFunction<LT, SumLT, TDistinctAggState, DistinctType, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const auto* column = down_cast<const ColumnType*>(columns[0]);
        if constexpr (IsSlice<T>) {
            this->data(state).update(ctx->mem_pool(), column->get_slice(row_num));
        } else {
            this->data(state).update(column->get_data()[row_num]);
        }
    }

    // The following two functions are specialized because of performance issue.
    // We have found out that by precomputing and prefetching hash values, we can boost peformance of hash table by a lot.
    // And this is a quite useful pattern for phmap::flat_hash_table.
    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto* column = down_cast<const ColumnType*>(columns[0]);
        auto& agg_state = this->data(state);

        struct CacheEntry {
            size_t hash_value;
        };

        std::vector<CacheEntry> cache(chunk_size);
        const auto& container_data = column->get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            size_t hash_value = agg_state.set.hash_function()(container_data[i]);
            cache[i] = CacheEntry{hash_value};
        }
        // This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
        size_t prefetch_index = 16;

        MemPool* mem_pool = ctx->mem_pool();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (prefetch_index < chunk_size) {
                agg_state.set.prefetch_hash(cache[prefetch_index].hash_value);
                prefetch_index++;
            }
            agg_state.update_with_hash(mem_pool, container_data[i], cache[i].hash_value);
        }
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        const auto* column = down_cast<const ColumnType*>(columns[0]);

        // We find that agg_states are scatterd in `states`, we can collect them together with hash value,
        // so there will be good cache locality. We can also collect column data into this `CacheEntry` to
        // exploit cache locality further, but I don't see much steady performance gain by doing that.
        struct CacheEntry {
            TDistinctAggState<LT, SumLT>* agg_state;
            size_t hash_value;
        };

        std::vector<CacheEntry> cache(chunk_size);
        const auto& container_data = column->get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            AggDataPtr state = states[i] + state_offset;
            auto& agg_state = this->data(state);
            size_t hash_value = agg_state.set.hash_function()(container_data[i]);
            cache[i] = CacheEntry{&agg_state, hash_value};
        }
        // This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
        size_t prefetch_index = 16;

        MemPool* mem_pool = ctx->mem_pool();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (prefetch_index < chunk_size) {
                cache[prefetch_index].agg_state->set.prefetch_hash(cache[prefetch_index].hash_value);
                prefetch_index++;
            }
            cache[i].agg_state->update_with_hash(mem_pool, container_data[i], cache[i].hash_value);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);
        if constexpr (IsSlice<T>) {
            this->data(state).deserialize_and_merge(ctx->mem_pool(), (const uint8_t*)slice.data, slice.size);
        } else {
            // slice size larger than `MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA`, means which is a hash set
            // that's said, size of hash set serialization data should be larger than `MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA`
            // otherwise this slice could be reinterpreted as a single value going be to inserted into hashset.
            if (slice.size >= MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA) {
                this->data(state).deserialize_and_merge((const uint8_t*)slice.data, slice.size);
            } else {
                T key;
                memcpy(&key, slice.data, sizeof(T));
                this->data(state).update(key);
            }
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        size_t old_size = column->get_bytes().size();
        size_t new_size = old_size + this->data(state).serialize_size();
        column->get_bytes().resize(new_size);
        this->data(state).serialize(column->get_bytes().data() + old_size);
        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();

        const auto* src_column = down_cast<const ColumnType*>(src[0].get());
        if constexpr (IsSlice<T>) {
            bytes.reserve(chunk_size * (sizeof(uint32_t) + src_column->get_slice(0).size));
        } else {
            bytes.reserve(chunk_size * sizeof(T));
        }
        dst_column->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (IsSlice<T>) {
                Slice key = src_column->get_slice(i);
                size_t new_size = old_size + key.size + sizeof(uint32_t);
                bytes.resize(new_size);

                auto size = (uint32_t)key.size;
                memcpy(bytes.data() + old_size, &size, sizeof(uint32_t));
                old_size += sizeof(uint32_t);
                memcpy(bytes.data() + old_size, key.data, key.size);
                old_size += key.size;
                dst_column->get_offset()[i + 1] = new_size;
            } else {
                T key = src_column->get_data()[i];

                size_t new_size = old_size + sizeof(T);
                bytes.resize(new_size);
                memcpy(bytes.data() + old_size, &key, sizeof(T));

                dst_column->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        if constexpr (DistinctType == AggDistinctType::COUNT) {
            down_cast<Int64Column*>(to)->append(this->data(state).distinct_count());
        } else if constexpr (DistinctType == AggDistinctType::SUM && is_starrocks_arithmetic<T>::value) {
            to->append_datum(Datum(this->data(state).sum_distinct()));
        }
    }

    std::string get_name() const override {
        if constexpr (DistinctType == AggDistinctType::COUNT) {
            return "count-distinct";
        } else {
            return "sum-distinct";
        }
    }
};

template <LogicalType LT, AggDistinctType DistinctType, typename T = RunTimeCppType<LT>>
using DistinctAggregateFunction =
        TDistinctAggregateFunction<LT, SumResultLT<LT>, DistinctAggregateState, DistinctType, T>;

template <LogicalType LT, AggDistinctType DistinctType, typename T = RunTimeCppType<LT>>
using DistinctAggregateFunctionV2 =
        TDistinctAggregateFunction<LT, SumResultLT<LT>, DistinctAggregateStateV2, DistinctType, T>;

template <LogicalType LT, AggDistinctType DistinctType, typename T = RunTimeCppType<LT>>
using DecimalDistinctAggregateFunction =
        TDistinctAggregateFunction<LT, lt_is_decimal256<LT> ? TYPE_DECIMAL256 : TYPE_DECIMAL128,
                                   DistinctAggregateStateV2, DistinctType, T>;

// now we only support String
struct DictMergeState : DistinctAggregateStateV2<TYPE_VARCHAR, SumResultLT<TYPE_VARCHAR>> {
    DictMergeState() = default;

    void update_over_limit() { over_limit = set.size() > dict_threshold; }

    bool over_limit = false;
    int dict_threshold = 255;
};

class DictMergeAggregateFunction final
        : public AggregateFunctionBatchHelper<DictMergeState, DictMergeAggregateFunction> {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        AggregateFunctionBatchHelper::create(ctx, ptr);
        if (ctx->get_num_constant_columns() > 1) {
            this->data(ptr).dict_threshold = ctx->get_constant_column(1)->get(0).get_int32();
        }
    }

    DictMergeState fake_dict_state(FunctionContext* ctx, int dict_threshold) const {
        DictMergeState fake_state;
        for (int i = 0; i < dict_threshold + 1; ++i) {
            fake_state.update(ctx->mem_pool(), std::to_string(i));
        }
        return fake_state;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& agg_state = this->data(state);
        MemPool* mem_pool = ctx->mem_pool();

        // if dict size greater than threshold. we return a FAKE dictionary
        if (agg_state.over_limit || columns[0]->is_null(row_num)) {
            return;
        }

        auto* data_column = ColumnHelper::get_data_column(columns[0]);

        if (data_column->is_array()) {
            const auto* array_column = down_cast<const ArrayColumn*>(data_column);
            const auto* column = array_column->elements_column().get();
            const auto& off = array_column->offsets().get_data();
            const auto* binary_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
            for (auto i = off[row_num]; i < off[row_num + 1]; i++) {
                if (!column->is_null(i)) {
                    agg_state.update(mem_pool, binary_column->get_slice(i));
                }
            }
        } else {
            const auto& binary_column = down_cast<const BinaryColumn&>(*data_column);
            agg_state.update(mem_pool, binary_column.get_slice(row_num));
        }

        agg_state.update_over_limit();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& agg_state = this->data(state);

        if (agg_state.over_limit || column->is_null(row_num)) {
            return;
        }

        const auto* input_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        Slice slice = input_column->get_slice(row_num);

        this->data(state).deserialize_and_merge(ctx->mem_pool(), (const uint8_t*)slice.data, slice.size);

        agg_state.update_over_limit();
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& agg_state = this->data(state);

        auto serialize = [=](const DictMergeState& dict_state) {
            auto* column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
            if (to->is_nullable()) {
                down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
            }
            size_t old_size = column->get_bytes().size();
            size_t new_size = old_size + dict_state.serialize_size();
            column->get_bytes().resize(new_size);
            dict_state.serialize(column->get_bytes().data() + old_size);
            column->get_offset().emplace_back(new_size);
        };

        if (agg_state.over_limit) {
            serialize(fake_dict_state(ctx, agg_state.dict_threshold));
        } else {
            serialize(this->data(state));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK(false) << "this method shouldn't be called";
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& agg_state = this->data(state);

        auto finalize = [](const DictMergeState& agg_state, Column* to) {
            if (agg_state.set.size() == 0) {
                to->append_default();
                return;
            }
            std::vector<int32_t> dict_ids;
            dict_ids.resize(agg_state.set.size());

            auto* binary_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
            if (to->is_nullable()) {
                down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
            }

            // set dict_ids as [1...n]
            for (int i = 0; i < dict_ids.size(); ++i) {
                dict_ids[i] = i + 1;
            }

            TGlobalDict tglobal_dict;
            tglobal_dict.__isset.ids = true;
            tglobal_dict.ids = std::move(dict_ids);
            tglobal_dict.__isset.strings = true;
            tglobal_dict.strings.reserve(dict_ids.size());

            agg_state.set.fill_vector(tglobal_dict.strings);

            // Since the id in global dictionary may be used for sorting,
            // we also need to ensure that the dictionary is ordered when we build it

            Slice::Comparator comparator;
            std::sort(tglobal_dict.strings.begin(), tglobal_dict.strings.end(), comparator);

            std::string result_value = apache::thrift::ThriftJSONString(tglobal_dict);

            size_t old_size = binary_column->get_bytes().size();
            size_t new_size = old_size + result_value.size();

            auto& data = binary_column->get_bytes();
            data.resize(old_size + new_size);

            memcpy(data.data() + old_size, result_value.data(), result_value.size());

            binary_column->get_offset().emplace_back(new_size);
        };

        if (agg_state.over_limit) {
            finalize(fake_dict_state(ctx, agg_state.dict_threshold), to);
        } else {
            finalize(agg_state, to);
        }
    }

    std::string get_name() const override { return "dict_merge"; }
};

static constexpr int COMPUTE_SUM_BIT = 0x1;
static constexpr int COMPUTE_AVG_BIT = 0x2;

static constexpr int compute_bits(const bool compute_sum, const bool compute_avg) {
    return (COMPUTE_SUM_BIT * compute_sum) | (COMPUTE_AVG_BIT * compute_avg);
}

static constexpr bool enable_bit_sum(const int bits) {
    return (bits & COMPUTE_SUM_BIT) != 0;
}

static constexpr bool enable_bit_avg(const int bits) {
    return (bits & COMPUTE_AVG_BIT) != 0;
}

template <LogicalType LT, LogicalType SumLT, int compute_bits,
          template <LogicalType X, LogicalType Y, bool, typename = guard::Guard> class TDistinctAggState,
          typename T = RunTimeCppType<LT>>
struct TFusedMultiDistinctFunction final
        : public AggregateFunctionBatchHelper<
                  TDistinctAggState<LT, SumLT, lt_is_numeric<LT> && compute_bits != 0>,
                  TFusedMultiDistinctFunction<LT, SumLT, compute_bits, TDistinctAggState, T>> {
    using State = TDistinctAggState<LT, SumLT, lt_is_numeric<LT> && compute_bits != 0>;
    using InputColumn = RunTimeColumnType<LT>;
    using SumColumn = RunTimeColumnType<SumLT>;
    static constexpr auto AvgLT = AvgResultLT<LT>;
    using AvgColumn = RunTimeColumnType<AvgLT>;
    using AvgCppType = RunTimeCppType<AvgLT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        auto& state_impl = this->data(const_cast<AggDataPtr>(state));
        auto* struct_column = down_cast<StructColumn*>(dst);
        // compute count
        {
            auto* count_column = struct_column->field_column("count").get();
            auto* count_data_column = down_cast<Int64Column*>(ColumnHelper::get_data_column(count_column));
            const auto count = state_impl.distinct_count();
            for (auto i = start; i < end; ++i) {
                count_data_column->get_data()[i] = count;
            }
        }

        // compute sum
        if constexpr (lt_is_numeric<LT> && enable_bit_sum(compute_bits)) {
            auto* sum_column = struct_column->field_column("sum").get();
            auto* sum_data_column = down_cast<SumColumn*>(ColumnHelper::get_data_column(sum_column));
            const auto sum = state_impl.sum;
            for (auto i = start; i < end; ++i) {
                sum_data_column->get_data()[i] = sum;
            }
        }

        // compute avg
        if constexpr (lt_is_numeric<LT> && enable_bit_avg(compute_bits)) {
            auto* avg_column = struct_column->field_column("avg").get();
            auto* avg_data_column = down_cast<AvgColumn*>(ColumnHelper::get_data_column(avg_column));
            const auto sum = state_impl.sum;
            const auto count = state_impl.distinct_count();

            if (count == 0) {
                DCHECK(avg_column->is_nullable());
                auto& null_data = down_cast<NullableColumn*>(avg_column)->null_column()->get_data();
                for (auto i = start; i < end; ++i) {
                    null_data[i] = DATUM_NULL;
                }
                return;
            }

            AvgCppType avg;
            if constexpr (lt_is_arithmetic<LT>) {
                avg = static_cast<AvgCppType>(static_cast<double>(sum) / static_cast<double>(count));
            } else if constexpr (lt_is_decimal<LT>) {
                static_assert(lt_is_decimal128<AvgLT> || lt_is_decimal256<AvgLT>,
                              "Result type of avg on decimal32/64/128/256 is decimal 128 or 256");
                avg = decimal_div_integer<AvgCppType>(AvgCppType(sum), AvgCppType(count), ctx->get_arg_type(0)->scale);
            } else {
                DCHECK(false) << "Invalid LogicalTypes for avg function";
            }
            for (auto i = start; i < end; ++i) {
                avg_data_column->get_data()[i] = avg;
            }
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const auto* column = down_cast<const InputColumn*>(columns[0]);
        if constexpr (IsSlice<T>) {
            this->data(state).update(column->get_slice(row_num));
        } else {
            this->data(state).update(column->get_data()[row_num]);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const InputColumn*>(columns[0]);
        if constexpr (IsSlice<T>) {
            for (auto i = frame_start; i < frame_end; ++i) {
                this->data(state).update(column->get_slice(i));
            }
        } else {
            const auto* data = column->get_data().data();
            for (size_t i = frame_start; i < frame_end; ++i) {
                this->data(state).update(data[i]);
            }
        }
    }

    std::string get_name() const override { return "fused_multi_distinct"; }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK(false) << "Shouldn't call this method for window function!";
    }
};

template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType DistinctSumLT = SumResultLT<LT>;

template <LogicalType LT>
inline constexpr LogicalType DistinctSumLT<LT, Decimal256LTGuard<LT>> = TYPE_DECIMAL256;

template <LogicalType LT>
inline constexpr LogicalType DistinctSumLT<LT, Decimal128LTGuard<LT>> = TYPE_DECIMAL128;

template <LogicalType LT>
inline constexpr LogicalType DistinctSumLT<LT, Decimal64LTGuard<LT>> = TYPE_DECIMAL128;

template <LogicalType LT>
inline constexpr LogicalType DistinctSumLT<LT, Decimal32LTGuard<LT>> = TYPE_DECIMAL128;

template <LogicalType LT, int compute_bits, typename T = RunTimeCppType<LT>>
using FusedMultiDistinctFunction =
        TFusedMultiDistinctFunction<LT, DistinctSumLT<LT>, compute_bits, FusedMultiDistinctAggregateState, T>;

} // namespace starrocks
