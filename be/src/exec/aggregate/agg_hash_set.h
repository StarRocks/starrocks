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
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_profile.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "util/fixed_hash_map.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

namespace starrocks {

const constexpr int32_t prefetch_threhold = 8192;
// This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
static constexpr size_t AGG_HASH_MAP_DEFAULT_PREFETCH_DIST = 16;

#define AGG_HASH_SET_PRECOMPUTE_HASH_VALS()                  \
    hashes.reserve(chunk_size);                              \
    for (size_t i = 0; i < chunk_size; i++) {                \
        hashes[i] = this->hash_set.hash_function()(keys[i]); \
    }

#define AGG_HASH_SET_PREFETCH_HASH_VAL()                                              \
    if (i + AGG_HASH_MAP_DEFAULT_PREFETCH_DIST < chunk_size) {                        \
        this->hash_set.prefetch_hash(hashes[i + AGG_HASH_MAP_DEFAULT_PREFETCH_DIST]); \
    }

#define AGG_STRING_HASH_SET_PREFETCH_HASH_VAL()                                           \
    if (i + AGG_HASH_MAP_DEFAULT_PREFETCH_DIST < chunk_size) {                            \
        this->hash_set.prefetch_hash(cache[i + AGG_HASH_MAP_DEFAULT_PREFETCH_DIST].hash); \
    }

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
    AggHashSet(size_t chunk_size, AggStatistics* agg_stat_) : agg_stat(agg_stat_) {}
    using HHashSetType = HashSet;
    HashSet hash_set;
    AggStatistics* agg_stat;

    ////// Common Methods ////////
    void build_hash_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        return static_cast<Impl*>(this)->template build_set<true>(chunk_size, key_columns, pool, nullptr);
    }

    void build_hash_set_with_selection(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                       Filter* not_founds) {
        return static_cast<Impl*>(this)->template build_set<false>(chunk_size, key_columns, pool, not_founds);
    }
};

template <typename T>
struct no_prefetch_set : std::false_type {};
template <PhmapSeed seed>
struct no_prefetch_set<Int8AggHashSet<seed>> : std::true_type {};

template <class T>
constexpr bool is_no_prefetch_set = no_prefetch_set<T>::value;

// handle one number hash key
template <LogicalType logical_type, typename HashSet>
struct AggHashSetOfOneNumberKey : public AggHashSet<HashSet, AggHashSetOfOneNumberKey<logical_type, HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfOneNumberKey<logical_type, HashSet>>;
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<logical_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<logical_type>;
    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    template <class... Args>
    AggHashSetOfOneNumberKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        DCHECK(!key_columns[0]->is_nullable());
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        if constexpr (is_no_prefetch_set<HashSet>) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        } else {
            if (this->hash_set.bucket_count() < prefetch_threhold) {
                this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
            } else {
                this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        const auto* column = down_cast<const ColumnType*>(key_columns[0].get());
        const auto& keys = column->get_data();

        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace(keys[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(keys[i]);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* column = down_cast<const ColumnType*>(key_columns[0].get());
        const auto& keys = column->get_data();

        AGG_HASH_SET_PRECOMPUTE_HASH_VALS();
        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_HASH_SET_PREFETCH_HASH_VAL();
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace_with_hash(this->hashes[i], keys[i]);
            } else {
                (*not_founds)[i] = this->hash_set.find(keys[i], this->hashes[i]) == this->hash_set.end();
            }
        }
    }

    void insert_keys_to_columns(const ResultVector& keys, Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
    std::vector<size_t> hashes;
};

template <LogicalType logical_type, typename HashSet>
struct AggHashSetOfOneNullableNumberKey
        : public AggHashSet<HashSet, AggHashSetOfOneNullableNumberKey<logical_type, HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfOneNullableNumberKey<logical_type, HashSet>>;
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<logical_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<logical_type>;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    template <class... Args>
    AggHashSetOfOneNullableNumberKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        DCHECK(key_columns[0]->is_nullable());

        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            if constexpr (is_no_prefetch_set<HashSet>) {
                this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
            } else {
                if (key_columns[0]->has_null() || this->hash_set.bucket_count() < prefetch_threhold) {
                    this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool,
                                                                              not_founds);
                } else {
                    this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
                }
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        const auto* nullable_column = down_cast<const NullableColumn*>(key_columns[0].get());
        const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
        const auto& null_data = nullable_column->null_column_data();
        const auto& keys = data_column->get_data();

        if (nullable_column->has_null()) {
            for (size_t i = 0; i < chunk_size; ++i) {
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
            for (size_t i = 0; i < chunk_size; ++i) {
                if constexpr (compute_and_allocate) {
                    this->hash_set.emplace(keys[i]);
                } else {
                    (*not_founds)[i] = !this->hash_set.contains(keys[i]);
                }
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* nullable_column = down_cast<const NullableColumn*>(key_columns[0].get());
        const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
        const auto& keys = data_column->get_data();

        AGG_HASH_SET_PRECOMPUTE_HASH_VALS();
        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_HASH_SET_PREFETCH_HASH_VAL();
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace_with_hash(this->hashes[i], keys[i]);
            } else {
                (*not_founds)[i] = this->hash_set.find(keys[i], hashes[i]) == this->hash_set.end();
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, Columns& key_columns, size_t chunk_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->mutable_data_column());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
    std::vector<size_t> hashes;
};

template <typename HashSet>
struct AggHashSetOfOneStringKey : public AggHashSet<HashSet, AggHashSetOfOneStringKey<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfOneStringKey<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = Buffer<Slice>;

    template <class... Args>
    AggHashSetOfOneStringKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        DCHECK(key_columns[0]->is_binary());
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        if (this->hash_set.bucket_count() < prefetch_threhold) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        auto* column = down_cast<const BinaryColumn*>(key_columns[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            auto tmp = column->get_slice(i);
            if constexpr (compute_and_allocate) {
                KeyType key(tmp);
                this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = !this->hash_set.contains(tmp);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* column = down_cast<const BinaryColumn*>(key_columns[0].get());
        cache.reserve(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            cache[i] = KeyType(column->get_slice(i));
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_STRING_HASH_SET_PREFETCH_HASH_VAL();
            const auto& key = cache[i];
            if constexpr (compute_and_allocate) {
                this->hash_set.lazy_emplace_with_hash(key, key.hash, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = this->hash_set.find(key, key.hash) == this->hash_set.end();
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        keys.resize(chunk_size);
        column->append_strings(keys.data(), keys.size());
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
    std::vector<KeyType> cache;
};

template <typename HashSet>
struct AggHashSetOfOneNullableStringKey : public AggHashSet<HashSet, AggHashSetOfOneNullableStringKey<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfOneNullableStringKey<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = Buffer<Slice>;

    template <class... Args>
    AggHashSetOfOneNullableStringKey(Args&&... args) : Base(std::forward<Args>(args)...) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        DCHECK(key_columns[0]->is_nullable());
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            if (key_columns[0]->has_null() || this->hash_set.bucket_count() < prefetch_threhold) {
                this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
            } else {
                this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        const auto* nullable_column = down_cast<const NullableColumn*>(key_columns[0].get());
        const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        const auto& null_data = nullable_column->null_column_data();

        if (nullable_column->has_null()) {
            for (size_t i = 0; i < chunk_size; ++i) {
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
            for (size_t i = 0; i < chunk_size; ++i) {
                if constexpr (compute_and_allocate) {
                    _handle_data_key_column(data_column, i, pool, not_founds);
                } else {
                    _handle_data_key_column(data_column, i, not_founds);
                }
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* nullable_column = down_cast<const NullableColumn*>(key_columns[0].get());
        const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());

        cache.reserve(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            cache[i] = KeyType(data_column->get_slice(i));
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_STRING_HASH_SET_PREFETCH_HASH_VAL();
            const auto& key = cache[i];
            if constexpr (compute_and_allocate) {
                this->hash_set.lazy_emplace_with_hash(key, key.hash, [&](const auto& ctor) {
                    uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = this->hash_set.find(key, key.hash) == this->hash_set.end();
            }
        }
    }

    void _handle_data_key_column(const BinaryColumn* data_column, size_t row, MemPool* pool, Filter* not_founds) {
        const auto tmp = data_column->get_slice(row);
        KeyType key(tmp);

        this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
        });
    }

    void _handle_data_key_column(const BinaryColumn* data_column, size_t row, Filter* not_founds) {
        const auto key = data_column->get_slice(row);
        (*not_founds)[row] = !this->hash_set.contains(key);
    }

    void insert_keys_to_columns(ResultVector& keys, Columns& key_columns, size_t chunk_size) {
        DCHECK(key_columns[0]->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<BinaryColumn*>(nullable_column->mutable_data_column());
        keys.resize(chunk_size);
        column->append_strings(keys.data(), keys.size());
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
    std::vector<KeyType> cache;
};

template <typename HashSet>
struct AggHashSetOfSerializedKey : public AggHashSet<HashSet, AggHashSetOfSerializedKey<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfSerializedKey<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using ResultVector = Buffer<Slice>;
    using KeyType = typename HashSet::key_type;

    template <class... Args>
    AggHashSetOfSerializedKey(int32_t chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              _mem_pool(std::make_unique<MemPool>()),
              _buffer(_mem_pool->allocate(max_one_row_size * chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING)),
              _chunk_size(chunk_size) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
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

        if (this->hash_set.bucket_count() < prefetch_threhold) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        for (size_t i = 0; i < chunk_size; ++i) {
            Slice tmp = {_buffer + i * max_one_row_size, slice_sizes[i]};
            if constexpr (compute_and_allocate) {
                KeyType key(tmp);
                this->hash_set.lazy_emplace(key, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = !this->hash_set.contains(tmp);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        cache.reserve(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            cache[i] = KeyType(Slice(_buffer + i * max_one_row_size, slice_sizes[i]));
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_STRING_HASH_SET_PREFETCH_HASH_VAL();
            const auto& key = cache[i];
            if constexpr (compute_and_allocate) {
                this->hash_set.lazy_emplace_with_hash(key, key.hash, [&](const auto& ctor) {
                    // we must persist the slice before insert
                    uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                    memcpy(pos, key.data, key.size);
                    ctor(pos, key.size, key.hash);
                });
            } else {
                (*not_founds)[i] = this->hash_set.find(key, key.hash) == this->hash_set.end();
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

    void insert_keys_to_columns(ResultVector& keys, Columns& key_columns, int32_t chunk_size) {
        // When GroupBy has multiple columns, the memory is serialized by row.
        // If the length of a row is relatively long and there are multiple columns,
        // deserialization by column will cause the memory locality to deteriorate,
        // resulting in poor performance
        if (keys.size() > 0 && keys[0].size > 64) {
            // deserialize by row
            for (size_t i = 0; i < chunk_size; i++) {
                for (auto& key_column : key_columns) {
                    keys[i].data =
                            (char*)(key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(keys[i].data)));
                }
            }
        } else {
            // deserialize by column
            for (auto& key_column : key_columns) {
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
    std::vector<KeyType> cache;
};

template <typename HashSet>
struct AggHashSetOfSerializedKeyFixedSize : public AggHashSet<HashSet, AggHashSetOfSerializedKeyFixedSize<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfSerializedKeyFixedSize<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using FixedSizeSliceKey = typename HashSet::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;

    bool has_null_column = false;
    int fixed_byte_size = -1; // unset state
    static constexpr size_t max_fixed_size = sizeof(FixedSizeSliceKey);

    template <class... Args>
    AggHashSetOfSerializedKeyFixedSize(int32_t chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              _mem_pool(std::make_unique<MemPool>()),
              buffer(_mem_pool->allocate(max_fixed_size * chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING)),
              _chunk_size(chunk_size) {
        memset(buffer, 0x0, max_fixed_size * _chunk_size);
    }

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
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

        if (this->hash_set.bucket_count() < prefetch_threhold) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        auto* key = reinterpret_cast<FixedSizeSliceKey*>(buffer);

        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.insert(key[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(key[i]);
            }
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        auto* keys = reinterpret_cast<FixedSizeSliceKey*>(buffer);
        AGG_HASH_SET_PRECOMPUTE_HASH_VALS();

        for (size_t i = 0; i < chunk_size; ++i) {
            AGG_HASH_SET_PREFETCH_HASH_VAL();
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace_with_hash(hashes[i], keys[i]);
            } else {
                (*not_founds)[i] = this->hash_set.find(keys[i], hashes[i]) == this->hash_set.end();
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, Columns& key_columns, int32_t chunk_size) {
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
        for (auto& key_column : key_columns) {
            key_column->deserialize_and_append_batch(tmp_slices, chunk_size);
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* buffer;
    ResultVector results;
    Buffer<Slice> tmp_slices;
    // std::vector<Slice> tmp_slices;

    int32_t _chunk_size;
    std::vector<size_t> hashes;
};

} // namespace starrocks
