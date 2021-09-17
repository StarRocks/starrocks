// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/column.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"
namespace starrocks::vectorized {

using AggDataPtr = uint8_t*;

// =====================
// one level agg hash map
template <PhmapSeed seed>
using Int8AggHashMap = phmap::flat_hash_map<int8_t, AggDataPtr, StdHashWithSeed<int8_t, seed>>;
template <PhmapSeed seed>
using Int16AggHashMap = phmap::flat_hash_map<int16_t, AggDataPtr, StdHashWithSeed<int16_t, seed>>;
template <PhmapSeed seed>
using Int32AggHashMap = phmap::flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashMap = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashMap = phmap::flat_hash_map<int128_t, AggDataPtr, StdHashWithSeed<int128_t, seed>>;
template <PhmapSeed seed>
using DateAggHashMap = phmap::flat_hash_map<DateValue, AggDataPtr, StdHashWithSeed<DateValue, seed>>;
template <PhmapSeed seed>
using TimeStampAggHashMap = phmap::flat_hash_map<TimestampValue, AggDataPtr, StdHashWithSeed<TimestampValue, seed>>;
template <PhmapSeed seed>
using SliceAggHashMap = phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed<seed>, SliceEqual>;

// ==================
// one level fixed size slice hash map
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

// ==============================================================
// TODO(kks): Remove redundant code for compute_agg_states method
// handle one number hash key
template <typename FieldType, typename HashMap>
struct AggHashMapWithOneNumberKey {
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<FieldType>;
    HashMap hash_map;

    using ColumnType = type_select_t<cond<IsDate<FieldType>, DateColumn>, cond<IsTimestamp<FieldType>, TimestampColumn>,
                                     cond<std::is_integral_v<FieldType>, FixedLengthColumn<FieldType>>>;

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(!key_columns[0]->is_nullable());
        auto column = down_cast<ColumnType*>(key_columns[0].get());

        for (size_t i = 0; i < column->size(); i++) {
            FieldType key = column->get_data()[i];
            auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) { ctor(key, allocate_func()); });
            (*agg_states)[i] = iter->second;
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
            if (auto iter = hash_map.find(key); iter != hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    void insert_keys_to_columns(const ResultVector& keys, const Columns& key_columns, size_t batch_size) {
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + batch_size);
    }

    static constexpr bool has_single_null_key = false;

    ResultVector results;
};

template <typename FieldType, typename HashMap>
struct AggHashMapWithOneNullableNumberKey {
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<FieldType>;
    HashMap hash_map;

    using ColumnType = type_select_t<cond<IsDate<FieldType>, DateColumn>, cond<IsTimestamp<FieldType>, TimestampColumn>,
                                     cond<std::is_integral_v<FieldType>, FixedLengthColumn<FieldType>>>;

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func();
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else if (key_columns[0]->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<ColumnType*>(nullable_column->data_column().get());

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < data_column->size(); i++) {
                    _handle_data_key_column(data_column, i, std::move(allocate_func), agg_states);
                }
                return;
            }

            for (size_t i = 0; i < data_column->size(); i++) {
                if (key_columns[0]->is_null(i)) {
                    if (null_key_data == nullptr) {
                        null_key_data = allocate_func();
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
                null_key_data = allocate_func();
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

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < data_column->size(); i++) {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
                return;
            }

            for (size_t i = 0; i < data_column->size(); i++) {
                if (key_columns[0]->is_null(i)) {
                    if (null_key_data == nullptr) {
                        null_key_data = allocate_func();
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
        auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) { ctor(key, allocate_func()); });
        (*agg_states)[row] = iter->second;
    }

    void _handle_data_key_column(ColumnType* data_column, size_t row, Buffer<AggDataPtr>* agg_states,
                                 std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_data()[row];
        if (auto iter = hash_map.find(key); iter != hash_map.end()) {
            (*agg_states)[row] = iter->second;
        } else {
            (*not_founds)[row] = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t batch_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->mutable_data_column());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + batch_size);
        nullable_column->null_column_data().resize(batch_size);
    }

    static constexpr bool has_single_null_key = true;
    AggDataPtr null_key_data = nullptr;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithOneStringKey {
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;
    HashMap hash_map;

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(key_columns[0]->is_binary());
        auto column = down_cast<BinaryColumn*>(key_columns[0].get());

        for (size_t i = 0; i < column->size(); i++) {
            auto key = column->get_slice(i);
            auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func();
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
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
            if (auto iter = hash_map.find(key); iter != hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t batch_size) {
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        keys.resize(batch_size);
        column->append_strings(keys);
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithOneNullableStringKey {
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;
    HashMap hash_map;

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        if (key_columns[0]->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func();
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else if (key_columns[0]->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
            DCHECK(data_column->is_binary());

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < data_column->size(); i++) {
                    _handle_data_key_column(data_column, i, pool, std::move(allocate_func), agg_states);
                }
                return;
            }

            for (size_t i = 0; i < data_column->size(); i++) {
                if (key_columns[0]->is_null(i)) {
                    if (null_key_data == nullptr) {
                        null_key_data = allocate_func();
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
                null_key_data = allocate_func();
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(key_columns[0]);
            auto* data_column = ColumnHelper::as_raw_column<BinaryColumn>(nullable_column->data_column());
            DCHECK(data_column->is_binary());

            if (!nullable_column->has_null()) {
                for (size_t i = 0; i < data_column->size(); i++) {
                    _handle_data_key_column(data_column, i, agg_states, not_founds);
                }
                return;
            }

            for (size_t i = 0; i < data_column->size(); i++) {
                if (nullable_column->is_null(i)) {
                    if (null_key_data == nullptr) {
                        null_key_data = allocate_func();
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
        auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate(key.size);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func();
            ctor(pk, pv);
        });
        (*agg_states)[row] = iter->second;
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, Buffer<AggDataPtr>* agg_states,
                                 std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_slice(row);
        if (auto iter = hash_map.find(key); iter != hash_map.end()) {
            (*agg_states)[row] = iter->second;
        } else {
            (*not_founds)[row] = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t batch_size) {
        DCHECK(key_columns[0]->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<BinaryColumn*>(nullable_column->mutable_data_column());
        keys.resize(batch_size);
        column->append_strings(keys);
        nullable_column->null_column_data().resize(batch_size);
    }

    static constexpr bool has_single_null_key = true;
    AggDataPtr null_key_data = nullptr;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithSerializedKey {
    using Iterator = typename HashMap::iterator;
    using ResultVector = typename std::vector<Slice>;
    HashMap hash_map;

    AggHashMapWithSerializedKey()
            : tracker(std::make_unique<MemTracker>()),
              mem_pool(std::make_unique<MemPool>(tracker.get())),
              buffer(mem_pool->allocate(max_one_row_size * config::vector_chunk_size)) {}

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        slice_sizes.assign(config::vector_chunk_size, 0);

        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(max_one_row_size * config::vector_chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            Slice key = {buffer + i * max_one_row_size, slice_sizes[i]};
            auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                strings::memcpy_inlined(pos, key.data, key.size);
                Slice pk{pos, key.size};
                AggDataPtr pv = allocate_func();
                ctor(pk, pv);
            });
            (*agg_states)[i] = iter->second;
        }
    }

    // Elements queried in HashMap will be added to HashMap,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, std::vector<uint8_t>* not_founds) {
        slice_sizes.assign(config::vector_chunk_size, 0);

        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            buffer = mem_pool->allocate(max_one_row_size * config::vector_chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        not_founds->assign(chunk_size, 0);
        for (size_t i = 0; i < chunk_size; ++i) {
            Slice key = {buffer + i * max_one_row_size, slice_sizes[i]};
            if (auto iter = hash_map.find(key); iter != hash_map.end()) {
                (*agg_states)[i] = iter->second;
            } else {
                (*not_founds)[i] = 1;
            }
        }
    }

    uint32_t get_max_serialize_size(const Columns& key_columns) {
        uint32_t max_size = 0;
        for (const auto& key_column : key_columns) {
            max_size += key_column->max_one_element_serialize_size();
        }
        return max_size;
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t batch_size) {
        // When GroupBy has multiple columns, the memory is serialized by row.
        // If the length of a row is relatively long and there are multiple columns,
        // deserialization by column will cause the memory locality to deteriorate,
        // resulting in poor performance
        if (keys.size() > 0 && keys[0].size > 64) {
            // deserialize by row
            for (size_t i = 0; i < batch_size; i++) {
                for (const auto& key_column : key_columns) {
                    keys[i].data =
                            (char*)(key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(keys[i].data)));
                }
            }
        } else {
            // deserialize by column
            for (const auto& key_column : key_columns) {
                key_column->deserialize_and_append_batch(keys, batch_size);
            }
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    uint32_t max_one_row_size = 8;

    std::unique_ptr<MemTracker> tracker;
    std::unique_ptr<MemPool> mem_pool;
    uint8_t* buffer;
    ResultVector results;
};

template <typename HashMap>
struct AggHashMapWithSerializedKeyFixedSize {
    using Iterator = typename HashMap::iterator;
    using FixedSizeSliceKey = typename HashMap::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;
    HashMap hash_map;

    bool has_null_column = false;
    int fixed_byte_size = -1; // unset state
    static constexpr size_t max_fixed_size = sizeof(FixedSizeSliceKey);

    AggHashMapWithSerializedKeyFixedSize()
            : tracker(std::make_unique<MemTracker>()),
              mem_pool(std::make_unique<MemPool>(tracker.get())),
              buffer(mem_pool->allocate(max_fixed_size * config::vector_chunk_size)) {
        memset(buffer, 0x0, max_fixed_size * config::vector_chunk_size);
    }

    template <typename Func>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        FixedSizeSliceKey key;

        if (!has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
                    AggDataPtr pv = allocate_func();
                    ctor(key, pv);
                });
                (*agg_states)[i] = iter->second;
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                key.u.size = slice_sizes[i];
                auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
                    AggDataPtr pv = allocate_func();
                    ctor(key, pv);
                });
                (*agg_states)[i] = iter->second;
            }
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

        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        not_founds->assign(chunk_size, 0);

        FixedSizeSliceKey key;

        if (!has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                if (auto iter = hash_map.find(key); iter != hash_map.end()) {
                    (*agg_states)[i] = iter->second;
                } else {
                    (*not_founds)[i] = 1;
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                key.u.size = slice_sizes[i];
                if (auto iter = hash_map.find(key); iter != hash_map.end()) {
                    (*agg_states)[i] = iter->second;
                } else {
                    (*not_founds)[i] = 1;
                }
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t batch_size) {
        DCHECK(fixed_byte_size != -1);
        tmp_slices.reserve(batch_size);

        if (!has_null_column) {
            for (int i = 0; i < batch_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = fixed_byte_size;
            }
        } else {
            for (int i = 0; i < batch_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = key.u.size;
            }
        }

        // deserialize by column
        for (const auto& key_column : key_columns) {
            key_column->deserialize_and_append_batch(tmp_slices, batch_size);
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemTracker> tracker;
    std::unique_ptr<MemPool> mem_pool;
    uint8_t* buffer;
    ResultVector results;
    std::vector<Slice> tmp_slices;
};

} // namespace starrocks::vectorized
