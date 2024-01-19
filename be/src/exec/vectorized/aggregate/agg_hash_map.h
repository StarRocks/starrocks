// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <type_traits>

#include "column/column.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "exec/vectorized/aggregate/agg_hash_set.h"
#include "exec/vectorized/aggregate/agg_profile.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_pool.h"
#include "util/fixed_hash_map.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks::vectorized {

const constexpr int32_t prefetch_threhold = 8192;

using AggDataPtr = uint8_t*;

// =====================
// one level agg hash map
template <PhmapSeed seed>
using Int8AggHashMap = SmallFixedSizeHashMap<int8_t, AggDataPtr, seed>;
template <PhmapSeed seed>
using Int16AggHashMap = phmap::flat_hash_map<int16_t, AggDataPtr, StdHashWithSeed<int16_t, seed>>;
template <PhmapSeed seed>
using Int32AggHashMap = phmap::flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashMap = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashMap = phmap::flat_hash_map<int128_t, AggDataPtr, Hash128WithSeed<seed>>;
template <PhmapSeed seed>
using DateAggHashMap = phmap::flat_hash_map<DateValue, AggDataPtr, StdHashWithSeed<DateValue, seed>>;
template <PhmapSeed seed>
using TimeStampAggHashMap = phmap::flat_hash_map<TimestampValue, AggDataPtr, StdHashWithSeed<TimestampValue, seed>>;
template <PhmapSeed seed>
using SliceAggHashMap = phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed<seed>, SliceEqual>;

// ==================
// one level fixed size slice hash map
template <PhmapSeed seed>
using FixedSize4SliceAggHashMap = phmap::flat_hash_map<SliceKey4, AggDataPtr, FixedSizeSliceKeyHash<SliceKey4, seed>>;
template <PhmapSeed seed>
using FixedSize8SliceAggHashMap = phmap::flat_hash_map<SliceKey8, AggDataPtr, FixedSizeSliceKeyHash<SliceKey8, seed>>;
template <PhmapSeed seed>
using FixedSize16SliceAggHashMap =
        phmap::flat_hash_map<SliceKey16, AggDataPtr, FixedSizeSliceKeyHash<SliceKey16, seed>>;

// =====================
// two level agg hash map
template <PhmapSeed seed>
using Int32AggTwoLevelHashMap = phmap::parallel_flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;

// The SliceAggTwoLevelHashMap will have 2 ^ 4 = 16 sub map,
// The 16 is same as PartitionedAggregationNode::PARTITION_FANOUT
static constexpr uint8_t PHMAPN = 4;
template <PhmapSeed seed>
using SliceAggTwoLevelHashMap =
        phmap::parallel_flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed<seed>, SliceEqual,
                                      phmap::priv::Allocator<phmap::priv::Pair<const Slice, AggDataPtr>>, PHMAPN>;

// This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
static constexpr size_t AGG_HASH_MAP_DEFAULT_PREFETCH_DIST = 16;

static_assert(sizeof(AggDataPtr) == sizeof(size_t));
#define AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, prefetch_dist)              \
    size_t const column_size = column->size();                                  \
    size_t* hash_values = reinterpret_cast<size_t*>(agg_states->data());        \
    {                                                                           \
        const auto& container_data = column->get_data();                        \
        for (size_t i = 0; i < column_size; i++) {                              \
            size_t hashval = this->hash_map.hash_function()(container_data[i]); \
            hash_values[i] = hashval;                                           \
        }                                                                       \
    }                                                                           \
    size_t __prefetch_index = prefetch_dist;

#define AGG_HASH_MAP_PREFETCH_HASH_VALUE()                             \
    if (__prefetch_index < column_size) {                              \
        this->hash_map.prefetch_hash(hash_values[__prefetch_index++]); \
    }

template <typename HashMap>
struct AggHashMapWithKey {
    AggHashMapWithKey(int chunk_size, AggStatistics* agg_stat_) : agg_stat(agg_stat_) {}
    using HashMapType = HashMap;
    HashMap hash_map;
    AggStatistics* agg_stat;
};

// ==============================================================
// TODO(kks): Remove redundant code for compute_agg_states method
// handle one number hash key
template <PrimitiveType primitive_type, typename HashMap>
struct AggHashMapWithOneNumberKey : public AggHashMapWithKey<HashMap> {
    using Base = AggHashMapWithKey<HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ColumnType = RunTimeColumnType<primitive_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<primitive_type>;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash map key size needs to be larger than the actual element");

    template <class... Args>
    AggHashMapWithOneNumberKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return nullptr; }

    // prefetch branch better performance in case with larger hash tables
    template <typename Func>
    void compute_agg_prefetch(ColumnType* column, Buffer<AggDataPtr>* agg_states, Func&& allocate_func) {
        AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, AGG_HASH_MAP_DEFAULT_PREFETCH_DIST);
        for (size_t i = 0; i < column_size; i++) {
            AGG_HASH_MAP_PREFETCH_HASH_VALUE();

            FieldType key = column->get_data()[i];
            auto iter = this->hash_map.lazy_emplace_with_hash(key, hash_values[i], [&](const auto& ctor) {
                AggDataPtr pv = allocate_func(key);
                ctor(key, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    // prefetch branch better performance in case with small hash tables
    template <typename Func>
    void compute_agg_noprefetch(ColumnType* column, Buffer<AggDataPtr>* agg_states, Func&& allocate_func) {
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; i++) {
            FieldType key = column->get_data()[i];
            auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) { ctor(key, allocate_func(key)); });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(!key_columns[0]->is_nullable());
        auto column = down_cast<ColumnType*>(key_columns[0].get());

        size_t bucket_count = this->hash_map.bucket_count();

        if (bucket_count < prefetch_threhold) {
            compute_agg_noprefetch(column, agg_states, allocate_func);
        } else {
            compute_agg_prefetch(column, agg_states, allocate_func);
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        DCHECK(!key_columns[0]->is_nullable());
        (*not_founds).assign(chunk_size, 0);
        auto column = down_cast<ColumnType*>(key_columns[0].get());

        for (size_t i = 0; i < chunk_size; i++) {
            FieldType key = column->get_data()[i];
            if (auto iter = this->hash_map.find(key); iter != this->hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    void insert_keys_to_columns(const ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
    }

    static constexpr bool has_single_null_key = false;

    ResultVector results;
};

template <PrimitiveType primitive_type, typename HashMap>
struct AggHashMapWithOneNullableNumberKey : public AggHashMapWithOneNumberKey<primitive_type, HashMap> {
    using Base = AggHashMapWithOneNumberKey<primitive_type, HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ColumnType = RunTimeColumnType<primitive_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<primitive_type>;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash map key size needs to be larger than the actual element");

    template <class... Args>
    AggHashMapWithOneNullableNumberKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return null_key_data; }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<ColumnType*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();

            if (!nullable_column->has_null()) {
                if (this->hash_map.bucket_count() < prefetch_threhold) {
                    Base::compute_agg_noprefetch(data_column, agg_states, allocate_func);
                } else {
                    Base::compute_agg_prefetch(data_column, agg_states, allocate_func);
                }
                return;
            }

            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (UNLIKELY(null_key_data == nullptr)) {
                        null_key_data = allocate_func(nullptr);
                    }
                    (*agg_states)[i] = null_key_data;
                } else {
                    _handle_data_key_column(data_column, i, std::move(allocate_func), agg_states);
                }
            }
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        not_founds->assign(chunk_size, 0);

        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            // TODO: performance optimization
            //  if key is find in hash table, null key will be filtered,
            //  if all keys is not found in hash table, null key will not be filtered
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(key_columns[0]);
            auto* data_column = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column());
            const auto& null_data = nullable_column->null_column_data();

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; i++) {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
                return;
            }

            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (UNLIKELY(null_key_data == nullptr)) {
                        null_key_data = allocate_func(nullptr);
                    }
                    (*agg_states)[i] = null_key_data;
                } else {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
            }
        }
    }

    template <typename Func>
    void _handle_data_key_column(ColumnType* data_column, size_t row, Func&& allocate_func,
                                 Buffer<AggDataPtr>* agg_states) {
        auto key = data_column->get_data()[row];
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        (*agg_states)[row] = iter->second;
    }

    void _handle_data_key_column(ColumnType* data_column, size_t row, Buffer<AggDataPtr>* agg_states,
                                 std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_data()[row];
        if (auto iter = this->hash_map.find(key); iter != this->hash_map.end()) {
            (*agg_states)[row] = iter->second;
        } else {
            (*not_founds)[row] = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->mutable_data_column());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    AggDataPtr null_key_data = nullptr;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithOneStringKey : public AggHashMapWithKey<HashMap> {
    using Base = AggHashMapWithKey<HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;

    template <class... Args>
    AggHashMapWithOneStringKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return nullptr; }

    template <typename Func>
    void compute_agg_prefetch(BinaryColumn* column, Buffer<AggDataPtr>* agg_states, MemPool* pool,
                              Func&& allocate_func) {
        AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, AGG_HASH_MAP_DEFAULT_PREFETCH_DIST);
        for (size_t i = 0; i < column_size; i++) {
            AGG_HASH_MAP_PREFETCH_HASH_VALUE();
            auto key = column->get_slice(i);
            auto iter = this->hash_map.lazy_emplace_with_hash(key, hash_values[i], [&](const auto& ctor) {
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func(pk);
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_noprefetch(BinaryColumn* column, Buffer<AggDataPtr>* agg_states, MemPool* pool,
                                Func&& allocate_func) {
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; i++) {
            auto key = column->get_slice(i);
            auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func(pk);
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(key_columns[0]->is_binary());
        auto column = down_cast<BinaryColumn*>(key_columns[0].get());

        if (this->hash_map.bucket_count() < prefetch_threhold) {
            compute_agg_noprefetch(column, agg_states, pool, allocate_func);
        } else {
            compute_agg_prefetch(column, agg_states, pool, allocate_func);
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        DCHECK(key_columns[0]->is_binary());
        auto* column = ColumnHelper::as_raw_column<BinaryColumn>(key_columns[0]);
        not_founds->assign(chunk_size, 0);

        for (size_t i = 0; i < chunk_size; i++) {
            auto key = column->get_slice(i);
            if (auto iter = this->hash_map.find(key); iter != this->hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        keys.resize(chunk_size);
        column->append_strings(keys);
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithOneNullableStringKey : public AggHashMapWithOneStringKey<HashMap> {
    using Base = AggHashMapWithOneStringKey<HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;

    template <class... Args>
    AggHashMapWithOneNullableStringKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return null_key_data; }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();
            DCHECK(data_column->is_binary());

            if (!nullable_column->has_null()) {
                if (this->hash_map.bucket_count() < prefetch_threhold) {
                    Base::compute_agg_noprefetch(data_column, agg_states, pool, allocate_func);
                } else {
                    Base::compute_agg_prefetch(data_column, agg_states, pool, allocate_func);
                }
                return;
            }

            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (UNLIKELY(null_key_data == nullptr)) {
                        null_key_data = allocate_func(nullptr);
                    }
                    (*agg_states)[i] = null_key_data;
                } else {
                    _handle_data_key_column(data_column, i, pool, std::move(allocate_func), agg_states);
                }
            }
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        not_founds->assign(chunk_size, 0);
        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(key_columns[0]);
            auto* data_column = ColumnHelper::as_raw_column<BinaryColumn>(nullable_column->data_column());
            const auto& null_data = nullable_column->null_column_data();
            DCHECK(data_column->is_binary());

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < data_column->size(); i++) {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
                return;
            }

            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (UNLIKELY(null_key_data == nullptr)) {
                        null_key_data = allocate_func(nullptr);
                    }
                    (*agg_states)[i] = null_key_data;
                } else {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
            }
        }
    }

    template <typename Func>
    void _handle_data_key_column(BinaryColumn* data_column, size_t row, MemPool* pool, Func&& allocate_func,
                                 Buffer<AggDataPtr>* agg_states) {
        auto key = data_column->get_slice(row);
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate(key.size);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func(pk);
            ctor(pk, pv);
        });
        (*agg_states)[row] = iter->second;
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, Buffer<AggDataPtr>* agg_states,
                                 std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_slice(row);
        if (auto iter = this->hash_map.find(key); iter != this->hash_map.end()) {
            (*agg_states)[row] = iter->second;
        } else {
            (*not_founds)[row] = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        DCHECK(key_columns[0]->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<BinaryColumn*>(nullable_column->mutable_data_column());
        keys.resize(chunk_size);
        column->append_strings(keys);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    AggDataPtr null_key_data = nullptr;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithSerializedKey : public AggHashMapWithKey<HashMap> {
    using Base = AggHashMapWithKey<HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;

    template <class... Args>
    AggHashMapWithSerializedKey(int chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              mem_pool(std::make_unique<MemPool>()),
              buffer(mem_pool->allocate(max_one_row_size * chunk_size)),
              _chunk_size(chunk_size) {}

    AggDataPtr get_null_key_data() { return nullptr; }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        slice_sizes.assign(_chunk_size, 0);
        // Assign not_founds vector when needs compute not founds

        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            size_t batch_allocate_size = (size_t)cur_max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING;
            // too large, process by rows
            if(batch_allocate_size > std::numeric_limits<int32_t>::max()) {
                max_one_row_size = 0;
                mem_pool->clear();
                buffer = mem_pool->allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
                return compute_agg_states_by_rows<Func>(
                        chunk_size, key_columns, pool, std::move(allocate_func), agg_states, nullptr,
                        cur_max_one_row_size);
            }
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(batch_allocate_size);
        }
        // process by cols
        return compute_agg_states_by_cols<Func>(
                chunk_size, key_columns, pool, std::move(allocate_func), agg_states, nullptr, cur_max_one_row_size);
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        slice_sizes.assign(_chunk_size, 0);
        // Assign not_founds vector when needs compute not founds

        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            size_t batch_allocate_size = (size_t)cur_max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING;
            // too large, process by rows
            if(batch_allocate_size > std::numeric_limits<int32_t>::max()) {
                max_one_row_size = 0;
                mem_pool->clear();
                buffer = mem_pool->allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
                return compute_agg_states_by_rows<Func>(
                        chunk_size, key_columns, mem_pool.get(), std::move(allocate_func), agg_states, not_founds,
                        cur_max_one_row_size);
            }
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(batch_allocate_size);
        }
        // process by cols
        return compute_agg_states_by_cols<Func>(
                chunk_size, key_columns, mem_pool.get(), std::move(allocate_func), agg_states, not_founds, cur_max_one_row_size);
    }

    template <typename Func> // true false  bool allocate_and_compute_state, bool compute_not_founds
    void compute_agg_states_by_rows(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                    Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                    std::vector<uint8_t>* not_founds, size_t max_serialize_each_row) {
        for(size_t i = 0; i < chunk_size; ++i) {
            auto serialize_cursor = buffer;
            for (const auto& key_column : key_columns) {
                serialize_cursor += key_column->serialize(i, serialize_cursor);
            }
            DCHECK(serialize_cursor <= buffer + max_serialize_each_row);
            size_t serialize_size = serialize_cursor - buffer;
            Slice key = {buffer, serialize_size};
            auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func(pk);
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_states_by_cols(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                    Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                    std::vector<uint8_t>* not_founds, size_t max_serialize_each_row) {
        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        for(size_t i = 0; i < chunk_size; ++i) {
            Slice key = {buffer + i * max_one_row_size, slice_sizes[i]};
            auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func(pk);
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    uint32_t get_max_serialize_size(const Columns& key_columns) {
        uint32_t max_size = 0;
        for (const auto& key_column : key_columns) {
            max_size += key_column->max_one_element_serialize_size();
        }
        return max_size;
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t chunk_size) {
        // When GroupBy has multiple columns, the memory is serialized by row.
        // If the length of a row is relatively long and there are multiple columns,
        // deserialization by column will cause the memory locality to deteriorate,
        // resulting in poor performance
        if (keys.size() > 0 && keys[0].size > 64) {
            // deserialize by row
            for (size_t i = 0; i < chunk_size; i++) {
                for (const auto& key_column : key_columns) {
                    keys[i].data =
                            (char*)(key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(keys[i].data)));
                }
            }
        } else {
            // deserialize by column
            for (const auto& key_column : key_columns) {
                key_column->deserialize_and_append_batch(keys, chunk_size);
            }
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    uint32_t max_one_row_size = 8;

    std::unique_ptr<MemPool> mem_pool;
    uint8_t* buffer;
    ResultVector results;

    int32_t _chunk_size;
};

template <typename HashMap>
struct AggHashMapWithSerializedKeyFixedSize : public AggHashMapWithKey<HashMap> {
    using Base = AggHashMapWithKey<HashMap>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using FixedSizeSliceKey = typename HashMap::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;

    // TODO: make has_null_column as a constexpr
    bool has_null_column = false;
    int fixed_byte_size = -1; // unset state
    struct CacheEntry {
        FixedSizeSliceKey key;
        size_t hashval;
    };

    static constexpr size_t max_fixed_size = sizeof(CacheEntry);

    std::vector<CacheEntry> caches;

    template <class... Args>
    AggHashMapWithSerializedKeyFixedSize(int chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              mem_pool(std::make_unique<MemPool>()),
              _chunk_size(chunk_size) {
        caches.reserve(chunk_size);
        uint8_t* buffer = reinterpret_cast<uint8_t*>(caches.data());
        memset(buffer, 0x0, max_fixed_size * _chunk_size);
    }

    AggDataPtr get_null_key_data() { return nullptr; }

    template <typename Func>
    void compute_agg_prefetch(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* agg_states,
                              Func&& allocate_func) {
        uint8_t* buffer = reinterpret_cast<uint8_t*>(caches.data());
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }
        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                caches[i].key.u.size = slice_sizes[i];
            }
        }
        for (size_t i = 0; i < chunk_size; i++) {
            caches[i].hashval = this->hash_map.hash_function()(caches[i].key);
        }

        size_t __prefetch_index = AGG_HASH_MAP_DEFAULT_PREFETCH_DIST;

        for (size_t i = 0; i < chunk_size; ++i) {
            if (__prefetch_index < chunk_size) {
                this->hash_map.prefetch_hash(caches[__prefetch_index++].hashval);
            }
            FixedSizeSliceKey& key = caches[i].key;
            auto iter = this->hash_map.lazy_emplace_with_hash(key, caches[i].hashval, [&](const auto& ctor) {
                AggDataPtr pv = allocate_func(key);
                ctor(key, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_noprefetch(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* agg_states,
                                Func&& allocate_func) {
        constexpr int key_size = sizeof(FixedSizeSliceKey);
        uint8_t* buffer = reinterpret_cast<uint8_t*>(caches.data());
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, key_size);
        }
        FixedSizeSliceKey* key = reinterpret_cast<FixedSizeSliceKey*>(caches.data());
        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                key[i].u.size = slice_sizes[i];
            }
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            auto iter =
                    this->hash_map.lazy_emplace(key[i], [&](const auto& ctor) { ctor(key[i], allocate_func(key[i])); });
            (*agg_states)[i] = iter->second;
        }
    }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        uint8_t* buffer = reinterpret_cast<uint8_t*>(caches.data());
        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        if (this->hash_map.bucket_count() < prefetch_threhold) {
            compute_agg_noprefetch(chunk_size, key_columns, agg_states, allocate_func);
        } else {
            compute_agg_prefetch(chunk_size, key_columns, agg_states, allocate_func);
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        uint8_t* buffer = reinterpret_cast<uint8_t*>(caches.data());
        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        not_founds->assign(chunk_size, 0);

        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                caches[i].key.u.size = slice_sizes[i];
            }
        }
        for (size_t i = 0; i < chunk_size; i++) {
            caches[i].hashval = this->hash_map.hash_function()(caches[i].key);
        }

        size_t __prefetch_index = AGG_HASH_MAP_DEFAULT_PREFETCH_DIST;

        for (size_t i = 0; i < chunk_size; ++i) {
            if (__prefetch_index < chunk_size) {
                this->hash_map.prefetch_hash(caches[__prefetch_index++].hashval);
            }
            FixedSizeSliceKey& key = caches[i].key;
            if (auto iter = this->hash_map.find(key, caches[i].hashval); iter != this->hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t chunk_size) {
        DCHECK(fixed_byte_size != -1);
        tmp_slices.reserve(chunk_size);

        if (!has_null_column) {
            for (int i = 0; i < chunk_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = fixed_byte_size;
            }
        } else {
            for (int i = 0; i < chunk_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = key.u.size;
            }
        }

        // deserialize by column
        for (const auto& key_column : key_columns) {
            key_column->deserialize_and_append_batch(tmp_slices, chunk_size);
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemPool> mem_pool;
    ResultVector results;
    std::vector<Slice> tmp_slices;

    int32_t _chunk_size;
};

} // namespace starrocks::vectorized
