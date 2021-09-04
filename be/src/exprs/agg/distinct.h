// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/agg/distinct.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/sum.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "util/bitmap_value.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {
class DistinctBitmap {
public:
    static const size_t BITMAP_MAX_VALUE = 2 * 1024 * 1024;
    static const size_t BITMAP_MAX_SIZE = BITMAP_MAX_VALUE / 8;
    static_assert(BITMAP_MAX_VALUE % 64 == 0);
    static_assert(BITMAP_MAX_SIZE >= (1 << 16)); // at least hold uint16_t.
    static const size_t MAX_GROUP_BY_NUMBER = 16;
    static const size_t BITMAP_TOTAL_SIZE = MAX_GROUP_BY_NUMBER * BITMAP_MAX_SIZE;

    DistinctBitmap() {}
    ~DistinctBitmap() {
        free(_data);
        _data = nullptr;
    }

    uint8_t* alloc() {
        if (_data == nullptr) {
            _data = reinterpret_cast<uint8_t*>(malloc(BITMAP_TOTAL_SIZE));
            _bits = (1 << MAX_GROUP_BY_NUMBER) - 1;
        }
        if (_bits == 0) return nullptr;
        int x = __builtin_ctz(_bits);
        _bits ^= (1 << x);
        return _data + (x * BITMAP_MAX_SIZE);
        return nullptr;
    }

    void free(uint8_t* ptr) {
        if (ptr == nullptr) return;
        int idx = (ptr - _data) / BITMAP_MAX_SIZE;
        _bits |= (1 << idx);
    }

private:
    uint32_t _bits = (1 << MAX_GROUP_BY_NUMBER) - 1;
    uint8_t* _data = nullptr;
};
}; // namespace starrocks

namespace starrocks::vectorized {

enum AggDistinctType { COUNT = 0, SUM = 1 };

template <PrimitiveType PT, typename = guard::Guard>
struct DistinctAggregateState {};

// 0 original version
// 1 raw bitmap version
// 2 two-level hash set.
// 3 raw bitmap to shield more request.
#define DISTINCT_AGG_IMPL 3

#if DISTINCT_AGG_IMPL == 0
template <PrimitiveType PT>
struct DistinctAggregateState<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumResultPT<PT>>;

    bool update(T key) {
        auto pair = set.insert(key);
        return pair.second;
    }

    size_t value_size() const { return phmap::item_serialize_size<HashSet<T>>::value; }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const { return set.dump_bound(); }

    void serialize(uint8_t* dst) const {
        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst));
        set.dump(output);
        DCHECK(output.length() == set.dump_bound());
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src));
        auto old_size = set.size();
        if (old_size == 0) {
            set.load(input);
            return set.size() * phmap::item_serialize_size<HashSet<T>>::value;
        } else {
            HashSet<T> set_src;
            set_src.load(input);
            set.merge(set_src);
            return (set.size() - old_size) * phmap::item_serialize_size<HashSet<T>>::value;
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

    HashSet<T> set;
};

#elif DISTINCT_AGG_IMPL == 1

// NOTE(yan): use bitmap to hold small values.
template <PrimitiveType PT>
struct DistinctAggregateState<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumResultPT<PT>>;

    DistinctAggregateState() { bitmap.assign(BITMAP_MAX_SIZE, 0); }

    bool update(T key) {
        if constexpr (sizeof(T) == 1) {
            uint8_t value = *reinterpret_cast<uint8_t*>(&key);
            update_bitmap(value);
            return false;
        }

        if constexpr (sizeof(T) == 2) {
            uint16_t value = *reinterpret_cast<uint16_t*>(&key);
            update_bitmap(value);
            return false;
        }

        if constexpr (sizeof(T) == 4) {
            uint32_t value = *reinterpret_cast<uint32_t*>(&key);
            if (value < BITMAP_MAX_VALUE) {
                update_bitmap(value);
                return false;
            }
        }
        if constexpr (sizeof(T) == 8) {
            uint64_t value = *reinterpret_cast<uint64_t*>(&key);
            if (value < BITMAP_MAX_VALUE) {
                update_bitmap(value);
                return false;
            }
        }

        auto pair = set.insert(key);
        return pair.second;
    }

    size_t value_size() const { return phmap::item_serialize_size<HashSet<T>>::value; }

    void update_bitmap(uint64_t value) {
        static const uint8_t shift_values[8] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
        uint8_t* data = bitmap.data();
        data[value / 8] |= shift_values[value % 8];
    }

    size_t bitmap_count() const {
        const uint8_t* data = bitmap.data();
        size_t ans = 0;
        for (size_t i = 0; i < BITMAP_MAX_SIZE; i += 4) {
            const int32_t value = *reinterpret_cast<const int32_t*>(data + i);
            ans += __builtin_popcount(value);
        }
        return ans;
    }

    int64_t disctint_count() const { return set.size() + bitmap_count(); }

    size_t serialize_size() const { return sizeof(size_t) + set.dump_bound() + bitmap.size(); }

    void serialize(uint8_t* dst) const {
        size_t set_size = set.dump_bound();

        size_t offset = 0;
        memcpy(dst + offset, &set_size, sizeof(set_size));
        offset += sizeof(set_size);

        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst + offset));
        set.dump(output);
        // DCHECK(output.length() == set.dump_bound());
        offset += set_size;

        // todo(yan): no need to serialize bitmap if empty.
        memcpy(dst + offset, bitmap.data(), bitmap.size());
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        size_t set_size = 0;
        size_t offset = 0;
        memcpy(&set_size, src + offset, sizeof(set_size));
        offset += sizeof(set_size);

        size_t total_size = 0;
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src + offset));
        auto old_size = set.size();
        if (old_size == 0) {
            set.load(input);
            total_size = set.size() * phmap::item_serialize_size<HashSet<T>>::value;
        } else {
            HashSet<T> set_src;
            set_src.load(input);
            set.merge(set_src);
            total_size = (set.size() - old_size) * phmap::item_serialize_size<HashSet<T>>::value;
        }
        offset += set_size;

        // todo(yan): sse.
        uint8_t* data = bitmap.data();
        const uint8_t* data2 = src + offset;
        for (size_t i = 0; i < BITMAP_MAX_SIZE; i += 8) {
            const uint64_t value = *reinterpret_cast<const uint64_t*>(data + i);
            const uint64_t value2 = *reinterpret_cast<const uint64_t*>(data2 + i);
            *reinterpret_cast<uint64_t*>(data + i) = value | value2;
        }
        return total_size;
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
        // todo(yan): cast bitmap index to value.
        return sum;
    }
    static const size_t BITMAP_MAX_VALUE = 256 * 1024;
    static const size_t BITMAP_MAX_SIZE = BITMAP_MAX_VALUE / 8;
    static_assert(BITMAP_MAX_VALUE % 64 == 0);
    std::vector<uint8_t> bitmap;

    HashSet<T> set;
};

#elif DISTINCT_AGG_IMPL == 2

// NOTE(yan): use two-level hash set.
template <PrimitiveType PT>
struct DistinctAggregateState<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumResultPT<PT>>;
    using TwoLevelHashSet = phmap::parallel_flat_hash_set<T, StdHash<T>>;

    DistinctAggregateState() {}

    bool update(T key) {
        auto pair = set.insert(key);
        return pair.second;
    }

    size_t value_size() const { return phmap::item_serialize_size<TwoLevelHashSet>::value; }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const { return set.dump_bound(); }

    void serialize(uint8_t* dst) const {
        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst));
        set.dump(output);
        // DCHECK(output.length() == set.dump_bound());
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        size_t total_size = 0;
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src));
        auto old_size = set.size();
        if (old_size == 0) {
            set.load(input);
            total_size = set.size() * phmap::item_serialize_size<TwoLevelHashSet>::value;
        } else {
            TwoLevelHashSet set_src;
            set_src.load(input);
            set.merge(set_src);
            total_size = (set.size() - old_size) * phmap::item_serialize_size<TwoLevelHashSet>::value;
        }
        return total_size;
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
        // todo(yan): cast bitmap index to value.
        return sum;
    }

    TwoLevelHashSet set;
};

#elif DISTINCT_AGG_IMPL == 3

// NOTE(yan): use bitmap to shed request to hashset.
template <PrimitiveType PT>
struct DistinctAggregateState<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumResultPT<PT>>;

    DistinctAggregateState() {}
    ~DistinctAggregateState() {
        if (state != nullptr) {
            state->distinct_bitmap()->free(bitmap);
        }
    }

    size_t value_size() const { return phmap::item_serialize_size<HashSet<T>>::value; }

    size_t update(RuntimeState* state, T key) {
        bool ok = update_ok(state, key);
        return ok * phmap::item_serialize_size<HashSet<T>>::value;
    }

    bool update_ok(RuntimeState* state, T key) {
        // try to acquire a bitmap from runtime state at the first time.
        if (this->state == nullptr) {
            this->state = state;
            bitmap = state->distinct_bitmap()->alloc();
            if (bitmap != nullptr) {
                memset(bitmap, 0, DistinctBitmap::BITMAP_MAX_SIZE);
            }
        }
        // use bitmap to shed request onto hashset.
        if (bitmap != nullptr && !try_update_bitmap(key)) {
            return false;
        }

        auto pair = set.insert(key);
        return pair.second;
    }

    bool try_update_bitmap(T& key) {
        if constexpr (sizeof(T) == 1) {
            uint8_t value = *reinterpret_cast<uint8_t*>(&key);
            if (!update_bitmap(value)) {
                return false;
            }
        }

        if constexpr (sizeof(T) == 2) {
            uint16_t value = *reinterpret_cast<uint16_t*>(&key);
            if (!update_bitmap(value)) {
                return false;
            }
        }

        if constexpr (sizeof(T) == 4 || sizeof(T) == 8) {
            int64_t value = 0;
            if constexpr (sizeof(T) == 4) {
                value = *reinterpret_cast<int32_t*>(&key);
            } else {
                value = *reinterpret_cast<int64_t*>(&key);
            }

            if (sample_index > 0) {
                samples[sample_index - 1] = value;
                sample_index -= 1;
                if (sample_index == 0) {
                    std::sort(samples, samples + SAMPLE_SIZE);
                }
                return true;
            }

            // use median value as center value.
            int64_t shift = value - samples[SAMPLE_SIZE / 2];
            // zigzag to wrap negative value.
            uint64_t index = (shift >> 63) ^ (shift << 1);

            if (index < DistinctBitmap::BITMAP_MAX_VALUE) {
                if (!update_bitmap(index)) {
                    return false;
                }
            }
        }
        return true;
    }

    bool update_bitmap(uint64_t value) {
        static const uint8_t shift_values[8] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
        uint8_t mask = shift_values[value % 8];
        uint8_t old = bitmap[value / 8];
        if (old & mask) {
            return false;
        }
        bitmap[value / 8] = (old | mask);
        return true;
    }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const { return set.dump_bound(); }

    void serialize(uint8_t* dst) const {
        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst));
        set.dump(output);
        // DCHECK(output.length() == set.dump_bound());
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        size_t total_size = 0;
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src));
        auto old_size = set.size();
        if (old_size == 0) {
            set.load(input);
            total_size = set.size() * value_size();
        } else {
            HashSet<T> set_src;
            set_src.load(input);
            set.merge(set_src);
            total_size = (set.size() - old_size) * value_size();
        }
        return total_size;
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

    static const int SAMPLE_SIZE = 5;
    int64_t samples[SAMPLE_SIZE];
    int64_t sample_index = SAMPLE_SIZE;
    RuntimeState* state = nullptr;
    uint8_t* bitmap = nullptr;
    HashSet<T> set;
};

#endif

template <PrimitiveType PT>
struct DistinctAggregateState<PT, BinaryPTGuard<PT>> {
    DistinctAggregateState() = default;
    using KeyType = typename SliceHashSet::key_type;

    size_t update(MemPool* mem_pool, Slice raw_key) {
        size_t ret = 0;
        KeyType key(raw_key);
        set.template lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = mem_pool->allocate(key.size);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
            ret = phmap::item_serialize_size<SliceHashSet>::value;
        });
        return ret;
    }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = 0;
        for (auto& key : set) {
            size += key.size + sizeof(uint32_t);
        }
        return size;
    }

    // TODO(kks): If we put all string key to one continue memory,
    // then we could only one memcpy.
    void serialize(uint8_t* dst) const {
        for (auto& key : set) {
            uint32_t size = (uint32_t)key.size;
            memcpy(dst, &size, sizeof(uint32_t));
            dst += sizeof(uint32_t);
            memcpy(dst, key.data, key.size);
            dst += key.size;
        }
    }

    size_t deserialize_and_merge(MemPool* mem_pool, const uint8_t* src, size_t len) {
        size_t mem_usage = 0;
        const uint8_t* end = src + len;
        while (src < end) {
            uint32_t size = 0;
            memcpy(&size, src, sizeof(uint32_t));
            src += sizeof(uint32_t);
            Slice raw_key(src, size);
            KeyType key(raw_key);
            // we only memcpy when the key is new
            set.template lazy_emplace(key, [&](const auto& ctor) {
                uint8_t* pos = mem_pool->allocate(key.size);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                mem_usage += phmap::item_serialize_size<SliceHashSet>::value;
            });
            src += size;
        }
        DCHECK(src == end);
        return mem_usage;
    }

    SliceHashSet set;
};

template <PrimitiveType PT, AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class DistinctAggregateFunction final
        : public AggregateFunctionBatchHelper<DistinctAggregateState<PT>,
                                              DistinctAggregateFunction<PT, DistinctType, T>> {
public:
    using ColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        size_t mem_usage;
        if constexpr (IsSlice<T>) {
            mem_usage = this->data(state).update(ctx->impl()->mem_pool(), column->get_slice(row_num));
        } else {
            mem_usage = this->data(state).update(ctx->impl()->state(), column->get_data()[row_num]);
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);
        size_t mem_usage = 0;
        if constexpr (IsSlice<T>) {
            mem_usage += this->data(state).deserialize_and_merge(ctx->impl()->mem_pool(), (const uint8_t*)slice.data,
                                                                 slice.size);
        } else {
            // slice size larger than 16, means which is a hash set
            if (slice.size > 16) {
                mem_usage += this->data(state).deserialize_and_merge((const uint8_t*)slice.data, slice.size);
            } else {
                T key = *reinterpret_cast<T*>(slice.data);
                mem_usage += this->data(state).update(ctx->impl()->state(), key);
            }
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        size_t old_size = column->get_bytes().size();
        size_t new_size = old_size + this->data(state).serialize_size();
        column->get_bytes().resize(new_size);
        this->data(state).serialize(column->get_bytes().data() + old_size);
        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();

        const ColumnType* src_column = down_cast<const ColumnType*>(src[0].get());
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

                uint32_t size = (uint32_t)key.size;
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

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        DCHECK(!to->is_nullable());
        if constexpr (DistinctType == AggDistinctType::COUNT) {
            down_cast<Int64Column*>(to)->append(this->data(state).disctint_count());
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

} // namespace starrocks::vectorized
