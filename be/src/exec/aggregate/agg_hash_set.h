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

#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "util/fixed_hash_map.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"

namespace starrocks {

// =====================
// one level agg hash set
template <PhmapSeed seed>
using Int8AggHashSet = SmallFixedSizeHashSet<int8_t, seed>;
template <PhmapSeed seed>
using Int16AggHashSet = phmap::flat_hash_set<int16_t, StdHashWithSeed<int16_t, seed>>;
template <PhmapSeed seed>
using Int32AggHashSet = phmap::flat_hash_set<int32_t, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashSet = phmap::flat_hash_set<int64_t, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashSet = phmap::flat_hash_set<int128_t, Hash128WithSeed<seed>>;
template <PhmapSeed seed>
using DateAggHashSet = phmap::flat_hash_set<DateValue, StdHashWithSeed<DateValue, seed>>;
template <PhmapSeed seed>
using TimeStampAggHashSet = phmap::flat_hash_set<TimestampValue, StdHashWithSeed<TimestampValue, seed>>;
template <PhmapSeed seed>
using SliceAggHashSet =
        phmap::flat_hash_set<TSliceWithHash<seed>, THashOnSliceWithHash<seed>, TEqualOnSliceWithHash<seed>>;

// ==================
// one level fixed size slice hash set
template <PhmapSeed seed>
using FixedSize4SliceAggHashSet = phmap::flat_hash_set<SliceKey4, FixedSizeSliceKeyHash<SliceKey4, seed>>;
template <PhmapSeed seed>
using FixedSize8SliceAggHashSet = phmap::flat_hash_set<SliceKey8, FixedSizeSliceKeyHash<SliceKey8, seed>>;
template <PhmapSeed seed>
using FixedSize16SliceAggHashSet = phmap::flat_hash_set<SliceKey16, FixedSizeSliceKeyHash<SliceKey16, seed>>;

// =====================
// two level agg hash set
template <PhmapSeed seed>
using Int32AggTwoLevelHashSet = phmap::parallel_flat_hash_set<int32_t, StdHashWithSeed<int32_t, seed>>;

template <PhmapSeed seed>
using SliceAggTwoLevelHashSet =
        phmap::parallel_flat_hash_set<TSliceWithHash<seed>, THashOnSliceWithHash<seed>, TEqualOnSliceWithHash<seed>,
                                      phmap::priv::Allocator<Slice>, 4>;

// ==============================================================

template <typename HashSet, typename Impl>
struct AggHashSet {
    AggHashSet() = default;
    using HHashSetType = HashSet;
    HashSet hash_set;

    ////// Common Methods ////////
    void build_hash_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        return static_cast<Impl*>(this)->template build_set<true>(chunk_size, key_columns, pool, nullptr);
    }

    void build_hash_set_with_selection(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                       std::vector<uint8_t>* not_founds) {
        return static_cast<Impl*>(this)->template build_set<false>(chunk_size, key_columns, pool, not_founds);
    }
};

// handle one number hash key
template <LogicalType logical_type, typename HashSet>
struct AggHashSetOfOneNumberKey : public AggHashSet<HashSet, AggHashSetOfOneNumberKey<logical_type, HashSet>> {
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<logical_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<logical_type>;
    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    AggHashSetOfOneNumberKey(int32_t chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        DCHECK(!key_columns[0]->is_nullable());
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        const size_t row_num = column->size();
        auto& keys = column->get_data();
        for (size_t i = 0; i < row_num; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace(keys[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(keys[i]);
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

template <LogicalType logical_type, typename HashSet>
struct AggHashSetOfOneNullableNumberKey
        : public AggHashSet<HashSet, AggHashSetOfOneNullableNumberKey<logical_type, HashSet>> {
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<logical_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<logical_type>;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    AggHashSetOfOneNullableNumberKey(int32_t chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<ColumnType*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();
            auto& keys = data_column->get_data();

            size_t row_num = nullable_column->size();
            if (nullable_column->has_null()) {
                for (size_t i = 0; i < row_num; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        if constexpr (compute_and_allocate) {
                            this->hash_set.emplace(keys[i]);
                        } else {
                            (*not_founds)[i] = !this->hash_set.contains(keys[i]);
                        }
                    }
                }
            } else {
                for (size_t i = 0; i < row_num; ++i) {
                    if constexpr (compute_and_allocate) {
                        this->hash_set.emplace(keys[i]);
                    } else {
                        (*not_founds)[i] = !this->hash_set.contains(keys[i]);
                    }
                }
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->mutable_data_column());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
};

template <typename HashSet>
struct AggHashSetOfOneStringKey : public AggHashSet<HashSet, AggHashSetOfOneStringKey<HashSet>> {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = typename std::vector<Slice>;

    AggHashSetOfOneStringKey(int32_t chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        DCHECK(key_columns[0]->is_binary());
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        size_t row_num = column->size();
        for (size_t i = 0; i < row_num; ++i) {
            auto tmp = column->get_slice(i);
            if constexpr (compute_and_allocate) {
                KeyType key(tmp);
                this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate(key.size);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = !this->hash_set.contains(tmp);
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

template <typename HashSet>
struct AggHashSetOfOneNullableStringKey : public AggHashSet<HashSet, AggHashSetOfOneNullableStringKey<HashSet>> {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = typename std::vector<Slice>;

    AggHashSetOfOneNullableStringKey(int32_t chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else if (key_columns[0]->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();

            size_t row_num = nullable_column->size();
            if (nullable_column->has_null()) {
                for (size_t i = 0; i < row_num; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        if constexpr (compute_and_allocate) {
                            _handle_data_key_column(data_column, i, pool, not_founds);
                        } else {
                            _handle_data_key_column(data_column, i, not_founds);
                        }
                    }
                }
            } else {
                for (size_t i = 0; i < row_num; ++i) {
                    if constexpr (compute_and_allocate) {
                        _handle_data_key_column(data_column, i, pool, not_founds);
                    } else {
                        _handle_data_key_column(data_column, i, not_founds);
                    }
                }
            }
        }
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, MemPool* pool,
                                 std::vector<uint8_t>* not_founds) {
        auto tmp = data_column->get_slice(row);
        KeyType key(tmp);

        this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate(key.size);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
        });
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_slice(row);
        (*not_founds)[row] = !this->hash_set.contains(key);
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
    bool has_null_key = false;
    ResultVector results;
};

template <typename HashSet>
struct AggHashSetOfSerializedKey : public AggHashSet<HashSet, AggHashSetOfSerializedKey<HashSet>> {
    using Iterator = typename HashSet::iterator;
    using ResultVector = typename std::vector<Slice>;
    using KeyType = typename HashSet::key_type;

    AggHashSetOfSerializedKey(int32_t chunk_size)
            : _mem_pool(std::make_unique<MemPool>()),
              _buffer(_mem_pool->allocate(max_one_row_size * chunk_size)),
              _chunk_size(chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        slice_sizes.assign(_chunk_size, 0);
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        size_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            _mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            _buffer = _mem_pool->allocate(max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(_buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            Slice tmp = {_buffer + i * max_one_row_size, slice_sizes[i]};
            if constexpr (compute_and_allocate) {
                KeyType key(tmp);
                this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate(key.size);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = !this->hash_set.contains(tmp);
            }
        }
    }

    size_t get_max_serialize_size(const Columns& key_columns) {
        size_t max_size = 0;
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
    size_t max_one_row_size = 8;

    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* _buffer;
    ResultVector results;

    int32_t _chunk_size;
};

template <typename HashSet>
struct AggHashSetOfSerializedKeyFixedSize : public AggHashSet<HashSet, AggHashSetOfSerializedKeyFixedSize<HashSet>> {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using FixedSizeSliceKey = typename HashSet::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;

    bool has_null_column = false;
    int fixed_byte_size = -1; // unset state
    static constexpr size_t max_fixed_size = sizeof(FixedSizeSliceKey);

    AggHashSetOfSerializedKeyFixedSize(int32_t chunk_size)
            : _mem_pool(std::make_unique<MemPool>()),
              buffer(_mem_pool->allocate(max_fixed_size * chunk_size)),
              _chunk_size(chunk_size) {
        memset(buffer, 0x0, max_fixed_size * _chunk_size);
    }

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, std::vector<uint8_t>* not_founds) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        auto* key = reinterpret_cast<FixedSizeSliceKey*>(buffer);

        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                key[i].u.size = slice_sizes[i];
            }
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.insert(key[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(key[i]);
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
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* buffer;
    ResultVector results;
    std::vector<Slice> tmp_slices;

    int32_t _chunk_size;
};

} // namespace starrocks
