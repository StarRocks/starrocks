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

#include <any>
#include <limits>

#include "base/container/fixed_hash_map.h"
#include "base/failpoint/fail_point.h"
#include "base/phmap/phmap.h"
#include "base/utility/defer_op.h"
#include "column/column_hash.h"
#include "column/hash_set.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/system/cpu_info.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregate/serialized_key_buffer.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_pool.h"

namespace starrocks {
DECLARE_FAIL_POINT(agg_hash_set_bad_alloc);

// Software prefetch only pays off once the bucket array spills L2. Below that
// the table is L2-resident and the prefetch is a net loss of 4-9% (measured by
// agg_prefetch_dist_bench across number/string keys, map and set; the crossover
// sits right at the L2 size). So the prefetch path is gated on the resident
// footprint vs L2 instead of the old fixed bucket_count >= 8192 threshold.
inline size_t agg_prefetch_min_bytes() {
    static const long l2 = []() {
        const long v = CpuInfo::get_l2_cache_size();
        return v > 0 ? v : 1L * 1024 * 1024;
    }();
    // Ratio read live (mutable knob); L2 size is constant so it is cached.
    return static_cast<size_t>(static_cast<double>(l2) * config::agg_prefetch_l2_ratio);
}

// resident footprint = bucket_count * slot_bytes (+1 control byte per slot).
template <typename Table>
inline bool agg_should_prefetch_table(const Table& table) {
    return table.bucket_count() * (sizeof(typename Table::value_type) + 1) >= agg_prefetch_min_bytes();
}

// Read once per chunk from config::agg_hash_map_prefetch_dist (default 16).
// The macros below capture it into __prefetch_dist so each inner-loop
// iteration is a register read, not an atomic load.
inline size_t agg_hash_map_default_prefetch_dist() {
    const int32_t v = config::agg_hash_map_prefetch_dist;
    return v > 0 ? static_cast<size_t>(v) : 0;
}

#define AGG_HASH_SET_PRECOMPUTE_HASH_VALS()                  \
    hashes.reserve(chunk_size);                              \
    for (size_t i = 0; i < chunk_size; i++) {                \
        hashes[i] = this->hash_set.hash_function()(keys[i]); \
    }                                                        \
    const size_t __prefetch_dist = agg_hash_map_default_prefetch_dist();

#define AGG_HASH_SET_PREFETCH_HASH_VAL()                            \
    if (__prefetch_dist != 0 && i + __prefetch_dist < chunk_size) { \
        this->hash_set.prefetch_hash(hashes[i + __prefetch_dist]);  \
    }

// Key of the slice agg hash maps and sets. It is exactly sizeof(Slice): `size` is narrowed to 32 bits and the freed
// 4 bytes cache the key's crc, so a rehash never walks the key bytes again. The size of the map/set slot and of the
// agg state prefix (which holds a copy of the key, see AllocateState) stay unchanged, and SliceAggHashSet /
// SliceAggTwoLevelHashSet shrink from 24-byte TSliceWithHash slots to 16-byte SliceWithHash32 slots.
//
// A key of kEscape bytes or more stores kEscape in `size` and its real length in the kEscapeHeader bytes in front of
// `data`. Such keys are always materialized by make_agg_slice_key() or persist_agg_slice_key().
template <PhmapSeed seed>
struct SliceWithHash32 {
    static constexpr uint32_t kEscape = std::numeric_limits<uint32_t>::max();
    static constexpr size_t kEscapeHeader = sizeof(uint64_t);

    const char* data;
    uint32_t size;
    uint32_t hash;

    ALWAYS_INLINE
    SliceWithHash32(const char* d = nullptr, uint32_t s = 0, uint32_t h = 0) : data(d), size(s), hash(h) {}
    // Only for keys shorter than kEscape.
    ALWAYS_INLINE
    explicit SliceWithHash32(const Slice& s) : SliceWithHash32(s.data, static_cast<uint32_t>(s.size), hash_of(s)) {
        DCHECK_LT(s.size, kEscape);
    }

    ALWAYS_INLINE static uint32_t hash_of(const Slice& s) {
        size_t h = SliceHashWithSeed<seed>()(Slice(s.data, std::min(s.size, kMaxHashPiece)));
        for (size_t offset = kMaxHashPiece; UNLIKELY(offset < s.size); offset += kMaxHashPiece) {
            h = crc_hash_64(s.data + offset, static_cast<int32_t>(std::min(kMaxHashPiece, s.size - offset)), h);
        }
        return static_cast<uint32_t>(h);
    }

    ALWAYS_INLINE size_t real_size() const {
        return LIKELY(size != kEscape) ? size : unaligned_load<uint64_t>(data - kEscapeHeader);
    }
    ALWAYS_INLINE Slice slice() const { return {data, real_size()}; }
    // convert_hash_map_to_chunk copies the key into a Buffer<Slice>.
    ALWAYS_INLINE operator Slice() const { return slice(); }

private:
    // SliceHashWithSeed takes an int32 length.
    static constexpr size_t kMaxHashPiece = std::numeric_limits<int32_t>::max();
};
static_assert(sizeof(SliceWithHash32<PhmapSeed1>) == sizeof(Slice));
static_assert(alignof(SliceWithHash32<PhmapSeed1>) == alignof(Slice));

template <PhmapSeed seed>
struct SliceWithHash32Hash {
    // The crc has 32 bits of entropy, the mix spreads them over 64 bits so H1 keeps its high bits.
    ALWAYS_INLINE size_t operator()(const SliceWithHash32<seed>& s) const {
        return phmap_mix_with_seed<8, seed>()(s.hash);
    }
};

template <PhmapSeed seed>
struct SliceWithHash32Equal {
    ALWAYS_INLINE bool operator()(const SliceWithHash32<seed>& x, const SliceWithHash32<seed>& y) const {
        if (x.hash != y.hash) {
            return false;
        }
        if (LIKELY(x.size == y.size && x.size != SliceWithHash32<seed>::kEscape)) {
            return memequal(x.data, x.size, y.data, x.size);
        }
        return memequal(x.data, x.real_size(), y.data, y.real_size());
    }
};

// Builds the key of `data[0, size)`. A key of kEscape bytes or more is copied into `scratch` behind its length
// header, because the bytes in front of `data` are not ours to write.
template <typename Key>
ALWAYS_INLINE Key make_agg_slice_key(const char* data, size_t size, uint32_t hash, MemPool* scratch) {
    if (LIKELY(size < Key::kEscape)) {
        return {data, static_cast<uint32_t>(size), hash};
    }
    uint8_t* pos = scratch->allocate_with_reserve(Key::kEscapeHeader + size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
    unaligned_store<uint64_t>(pos, size);
    strings::memcpy_inlined(pos + Key::kEscapeHeader, data, size);
    return {reinterpret_cast<const char*>(pos + Key::kEscapeHeader), Key::kEscape, hash};
}

template <typename Key>
ALWAYS_INLINE Key make_agg_slice_key(const Slice& s, MemPool* scratch) {
    return LIKELY(s.size < Key::kEscape) ? Key(s) : make_agg_slice_key<Key>(s.data, s.size, Key::hash_of(s), scratch);
}

// Copies the key bytes into `pool` so the key can live in the hash map or set.
template <typename Key>
ALWAYS_INLINE Key persist_agg_slice_key(const Key& key, MemPool* pool) {
    const Slice s = key;
    const size_t header = UNLIKELY(key.size == Key::kEscape) ? Key::kEscapeHeader : 0;
    const size_t total = header + s.size;
    uint8_t* pos = pool->allocate_with_reserve(total, SLICE_MEMEQUAL_OVERFLOW_PADDING);
    strings::memcpy_inlined(pos, s.data - header, total);
    return {reinterpret_cast<const char*>(pos + header), key.size, key.hash};
}

// Frees the escaped probe keys once the chunk is probed: a single one holds 4 GiB or more, which must not outlive the
// chunk that needed it.
ALWAYS_INLINE inline void reset_agg_escape_pool(MemPool* pool) {
    if (UNLIKELY(pool->total_reserved_bytes() > 0)) {
        pool->free_all();
    }
}

// =====================
// one level agg hash set
template <PhmapSeed seed>
using Int8AggHashSet = SmallFixedSizeHashSet<int8_t, seed>;
template <PhmapSeed seed>
using Int16AggHashSet = SmallFixedSizeHashSet<int16_t, seed>;
template <PhmapSeed seed>
using Int32AggHashSet = phmap::flat_hash_set<int32_t, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashSet = phmap::flat_hash_set<int64_t, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashSet = phmap::flat_hash_set<int128_t, Hash128WithSeed<seed>>;
template <PhmapSeed seed>
using Int256AggHashSet = phmap::flat_hash_set<int256_t, Hash256WithSeed<seed>>;
template <PhmapSeed seed>
using DateAggHashSet = phmap::flat_hash_set<DateValue, StdHashWithSeed<DateValue, seed>>;
template <PhmapSeed seed>
using TimeStampAggHashSet = phmap::flat_hash_set<TimestampValue, StdHashWithSeed<TimestampValue, seed>>;
template <PhmapSeed seed>
using SliceAggHashSet =
        phmap::flat_hash_set<SliceWithHash32<seed>, SliceWithHash32Hash<seed>, SliceWithHash32Equal<seed>>;

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
        phmap::parallel_flat_hash_set<SliceWithHash32<seed>, SliceWithHash32Hash<seed>, SliceWithHash32Equal<seed>,
                                      phmap::priv::Allocator<SliceWithHash32<seed>>, 4>;

// ==============================================================

template <typename HashSet, typename Impl>
struct AggHashSet {
    AggHashSet(size_t chunk_size, AggStatistics* agg_stat_) : agg_stat(agg_stat_) {}
    using HHashSetType = HashSet;
    HashSet hash_set;
    AggStatistics* agg_stat;

    ////// Common Methods ////////
    void build_hash_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        // auto cleanup hash set if any exception raised during build_set
        CancelableDefer defer = [this]() { hash_set.clear(); };
        static_cast<Impl*>(this)->template build_set<true>(chunk_size, key_columns, pool, nullptr);
        defer.cancel();
    }

    void build_hash_set_with_selection(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                       Filter* not_founds) {
        CancelableDefer defer = [this]() { hash_set.clear(); };
        static_cast<Impl*>(this)->template build_set<false>(chunk_size, key_columns, pool, not_founds);
        defer.cancel();
    }

    template <bool compute_and_allocate>
    ALWAYS_INLINE void probe_or_insert_slice_key(const typename HashSet::key_type& key, MemPool* pool,
                                                 Filter* not_founds, size_t i) {
        if constexpr (compute_and_allocate) {
            this->hash_set.lazy_emplace(key, [&](const auto& ctor) { ctor(persist_agg_slice_key(key, pool)); });
        } else {
            (*not_founds)[i] = !this->hash_set.contains(key);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_INLINE void probe_or_insert_slice_key_with_hash(const typename HashSet::key_type& key, size_t hashval,
                                                           MemPool* pool, Filter* not_founds, size_t i) {
        if constexpr (compute_and_allocate) {
            this->hash_set.lazy_emplace_with_hash(key, hashval,
                                                  [&](const auto& ctor) { ctor(persist_agg_slice_key(key, pool)); });
        } else {
            (*not_founds)[i] = this->hash_set.find(key, hashval) == this->hash_set.end();
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_INLINE void probe_slice_cache_prefetch(size_t chunk_size,
                                                  const typename HashSet::key_type* __restrict cache_ptr, MemPool* pool,
                                                  Filter* not_founds) {
        constexpr size_t kRingCap = 64;
        constexpr size_t kRingMask = kRingCap - 1;
        const size_t dist = std::min<size_t>(agg_hash_map_default_prefetch_dist(), kRingCap - 1);
        size_t hash_ring[kRingCap];
        const size_t preload = std::min(dist, chunk_size);
        for (size_t j = 0; j < preload; ++j) {
            hash_ring[j] = this->hash_set.hash_function()(cache_ptr[j]);
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            if (i + dist < chunk_size) {
                const size_t next_hash = this->hash_set.hash_function()(cache_ptr[i + dist]);
                hash_ring[(i + dist) & kRingMask] = next_hash;
                if (LIKELY(dist != 0)) {
                    this->hash_set.prefetch_hash(next_hash);
                }
            }
            const size_t hashval = hash_ring[i & kRingMask];
            this->template probe_or_insert_slice_key_with_hash<compute_and_allocate>(cache_ptr[i], hashval, pool,
                                                                                     not_founds, i);
        }
    }
};

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
        } else if (!agg_should_prefetch_table(this->hash_set)) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        const auto* column = down_cast<const ColumnType*>(key_columns[0].get());
        const auto keys = column->immutable_data();

        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.emplace(keys[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(keys[i]);
            }
            FAIL_POINT_TRIGGER_EXECUTE(agg_hash_set_bad_alloc, {
                if (i > 0) throw std::bad_alloc();
            });
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* column = down_cast<const ColumnType*>(key_columns[0].get());
        const auto keys = column->immutable_data();

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

    void insert_keys_to_columns(const ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
    }

    static constexpr bool has_single_null_key = false;
    bool has_null_key = false;
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
                if (key_columns[0]->has_null() || !agg_should_prefetch_table(this->hash_set)) {
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
        const auto keys = data_column->immutable_data();

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
        const auto keys = data_column->immutable_data();

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

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->data_column_raw_ptr());
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

        if (!agg_should_prefetch_table(this->hash_set)) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, key_columns, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Filter* not_founds) {
        const auto* column = down_cast<const BinaryColumn*>(key_columns[0].get());
        const uint8_t* base = column->raw_bytes();
        column->get_offset().visit_storage([&](const auto& offsets) {
            const auto* off = offsets.data();
            for (size_t i = 0; i < chunk_size; ++i) {
                const Slice s(base + off[i], off[i + 1] - off[i]);
                this->template probe_or_insert_slice_key<compute_and_allocate>(
                        make_agg_slice_key<KeyType>(s, &_escape_pool), pool, not_founds, i);
            }
        });
        reset_agg_escape_pool(&_escape_pool);
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* column = down_cast<const BinaryColumn*>(key_columns[0].get());
        const uint8_t* base = column->raw_bytes();
        cache.resize(chunk_size);
        KeyType* __restrict cache_ptr = cache.data();
        column->get_offset().visit_storage([&](const auto& offsets) {
            const auto* off = offsets.data();
            for (size_t i = 0; i < chunk_size; ++i) {
                const Slice s(base + off[i], off[i + 1] - off[i]);
                cache_ptr[i] = make_agg_slice_key<KeyType>(s, &_escape_pool);
            }
        });
        this->template probe_slice_cache_prefetch<compute_and_allocate>(chunk_size, cache_ptr, pool, not_founds);
        reset_agg_escape_pool(&_escape_pool);
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        keys.resize(chunk_size);
        column->append_strings(keys.data(), keys.size());
    }

    static constexpr bool has_single_null_key = false;
    bool has_null_key = false;
    ResultVector results;
    std::vector<KeyType> cache;
    MemPool _escape_pool;
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
            if (key_columns[0]->has_null() || !agg_should_prefetch_table(this->hash_set)) {
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
        const uint8_t* base = data_column->raw_bytes();

        data_column->get_offset().visit_storage([&](const auto& offsets) {
            const auto* off = offsets.data();
            if (nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        const Slice s(base + off[i], off[i + 1] - off[i]);
                        this->template probe_or_insert_slice_key<compute_and_allocate>(
                                make_agg_slice_key<KeyType>(s, &_escape_pool), pool, not_founds, i);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; ++i) {
                    const Slice s(base + off[i], off[i + 1] - off[i]);
                    this->template probe_or_insert_slice_key<compute_and_allocate>(
                            make_agg_slice_key<KeyType>(s, &_escape_pool), pool, not_founds, i);
                }
            }
        });
        reset_agg_escape_pool(&_escape_pool);
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                            Filter* not_founds) {
        const auto* nullable_column = down_cast<const NullableColumn*>(key_columns[0].get());
        const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        const uint8_t* base = data_column->raw_bytes();

        cache.resize(chunk_size);
        KeyType* __restrict cache_ptr = cache.data();
        data_column->get_offset().visit_storage([&](const auto& offsets) {
            const auto* off = offsets.data();
            for (size_t i = 0; i < chunk_size; ++i) {
                const Slice s(base + off[i], off[i + 1] - off[i]);
                cache_ptr[i] = make_agg_slice_key<KeyType>(s, &_escape_pool);
            }
        });
        this->template probe_slice_cache_prefetch<compute_and_allocate>(chunk_size, cache_ptr, pool, not_founds);
        reset_agg_escape_pool(&_escape_pool);
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        DCHECK(key_columns[0]->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<BinaryColumn*>(nullable_column->data_column_raw_ptr());
        keys.resize(chunk_size);
        column->append_strings(keys.data(), keys.size());
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
    std::vector<KeyType> cache;
    MemPool _escape_pool;
};

template <typename HashSet>
struct AggHashSetOfSerializedKey : public AggHashSet<HashSet, AggHashSetOfSerializedKey<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetOfSerializedKey<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using ResultVector = Buffer<Slice>;
    using KeyType = typename HashSet::key_type;

    template <class... Args>
    AggHashSetOfSerializedKey(int32_t chunk_size, Args&&... args) : Base(chunk_size, std::forward<Args>(args)...) {}

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        if (UNLIKELY(!_key_buffer.serialize(key_columns, chunk_size))) {
            build_set_by_rows<compute_and_allocate>(chunk_size, key_columns, pool, not_founds,
                                                    _key_buffer.max_row_size());
            return;
        }

        if (!agg_should_prefetch_table(this->hash_set)) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        for (size_t i = 0; i < chunk_size; ++i) {
            _probe_or_insert<compute_and_allocate>(i, _key_buffer.key(i), pool, not_founds);
        }
        reset_agg_escape_pool(&_escape_pool);
    }

    // A chunk whose keys do not fit one batch (see SerializedKeyBuffer::serialize()): serialize
    // and probe one row at a time through a buffer sized for the largest row.
    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_by_rows(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                           Filter* not_founds, size_t max_one_row_size) {
        uint8_t* buffer = _key_buffer.row_buffer(max_one_row_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            uint8_t* cursor = buffer;
            for (const auto& key_column : key_columns) {
                cursor += key_column->serialize_compact(i, cursor);
            }
            DCHECK(cursor <= buffer + max_one_row_size);
            _probe_or_insert<compute_and_allocate>(i, Slice(buffer, cursor - buffer), pool, not_founds);
        }
        reset_agg_escape_pool(&_escape_pool);
    }

    template <bool compute_and_allocate>
    ALWAYS_INLINE void _probe_or_insert(size_t i, const Slice& tmp, MemPool* pool, Filter* not_founds) {
        this->template probe_or_insert_slice_key<compute_and_allocate>(make_agg_slice_key<KeyType>(tmp, &_escape_pool),
                                                                       pool, not_founds, i);
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        cache.resize(chunk_size);
        KeyType* __restrict cache_ptr = cache.data();
        for (size_t i = 0; i < chunk_size; ++i) {
            cache_ptr[i] = make_agg_slice_key<KeyType>(_key_buffer.key(i), &_escape_pool);
        }
        this->template probe_slice_cache_prefetch<compute_and_allocate>(chunk_size, cache_ptr, pool, not_founds);
        reset_agg_escape_pool(&_escape_pool);
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, int32_t chunk_size) {
        // When GroupBy has multiple columns, the memory is serialized by row.
        // If the length of a row is relatively long and there are multiple columns,
        // deserialization by column will cause the memory locality to deteriorate,
        // resulting in poor performance
        if (keys.size() > 0 && keys[0].size > 64) {
            // deserialize by row
            for (size_t i = 0; i < chunk_size; i++) {
                for (auto& key_column : key_columns) {
                    keys[i].data = (char*)(key_column->deserialize_compact_and_append(
                            reinterpret_cast<const uint8_t*>(keys[i].data)));
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
    bool has_null_key = false;

    SerializedKeyBuffer _key_buffer;
    ResultVector results;

    int32_t _chunk_size;
    std::vector<KeyType> cache;
    MemPool _escape_pool;
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

        if (!agg_should_prefetch_table(this->hash_set)) {
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

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, int32_t chunk_size) {
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
    bool has_null_key = false;

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* buffer;
    ResultVector results;
    Buffer<Slice> tmp_slices;

    int32_t _chunk_size;
    std::vector<size_t> hashes;
};

template <typename HashSet>
struct AggHashSetCompressedFixedSize : public AggHashSet<HashSet, AggHashSetCompressedFixedSize<HashSet>> {
    using Base = AggHashSet<HashSet, AggHashSetCompressedFixedSize<HashSet>>;
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using FixedSizeSliceKey = typename HashSet::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;

    bool has_null_column = false;
    static constexpr size_t max_fixed_size = sizeof(FixedSizeSliceKey);

    template <class... Args>
    AggHashSetCompressedFixedSize(int32_t chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...), _chunk_size(chunk_size) {
        fixed_keys.reserve(chunk_size);
    }

    // When compute_and_allocate=false:
    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    template <bool compute_and_allocate>
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool, Filter* not_founds) {
        if constexpr (!compute_and_allocate) {
            DCHECK(not_founds);
            not_founds->assign(chunk_size, 0);
        }

        auto* buffer = reinterpret_cast<uint8_t*>(fixed_keys.data());
        memset(buffer, 0x0, sizeof(FixedSizeSliceKey) * chunk_size);
        bitcompress_serialize(key_columns, bases, offsets, chunk_size, sizeof(FixedSizeSliceKey), fixed_keys.data());

        if constexpr (is_no_prefetch_set<HashSet>) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        } else if (!agg_should_prefetch_table(this->hash_set)) {
            this->template build_set_noprefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        } else {
            this->template build_set_prefetch<compute_and_allocate>(chunk_size, pool, not_founds);
        }
    }

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_prefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        auto* keys = reinterpret_cast<FixedSizeSliceKey*>(fixed_keys.data());
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

    template <bool compute_and_allocate>
    ALWAYS_NOINLINE void build_set_noprefetch(size_t chunk_size, MemPool* pool, Filter* not_founds) {
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (compute_and_allocate) {
                this->hash_set.insert(fixed_keys[i]);
            } else {
                (*not_founds)[i] = !this->hash_set.contains(fixed_keys[i]);
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, int32_t chunk_size) {
        bitcompress_deserialize(key_columns, bases, offsets, used_bits, chunk_size, sizeof(FixedSizeSliceKey),
                                keys.data());
    }

    static constexpr bool has_single_null_key = false;
    bool has_null_key = false;

    std::vector<int> used_bits;
    std::vector<int> offsets;
    std::vector<std::any> bases;
    std::vector<FixedSizeSliceKey> fixed_keys;
    std::vector<size_t> hashes;
    ResultVector results;

    int32_t _chunk_size;
};

} // namespace starrocks
