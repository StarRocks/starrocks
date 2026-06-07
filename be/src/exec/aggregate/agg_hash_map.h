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
#include <cstdint>
#include <limits>
#include <utility>

#include "base/container/fixed_hash_map.h"
#include "base/failpoint/fail_point.h"
#include "base/phmap/phmap.h"
#include "base/utility/defer_op.h"
#include "column/column.h"
#include "column/column_hash.h"
#include "column/hash_set.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_inline_accumulator.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregate/compress_serializer.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_pool.h"

namespace starrocks {

DECLARE_FAIL_POINT(aggregate_build_hash_map_bad_alloc);

template <typename T>
concept HasKeyType = requires {
    typename T::KeyType;
};

template <typename T, typename HashMapWithKey>
concept AllocFunc = HasKeyType<HashMapWithKey>&& requires(T t, const typename HashMapWithKey::KeyType& key,
                                                          std::nullptr_t null) {
    { t(key) }
    ->std::same_as<AggDataPtr>;
    { t(null) }
    ->std::same_as<AggDataPtr>;
};

// =====================
// one level agg hash map
template <PhmapSeed seed>
using Int8AggHashMap = SmallFixedSizeHashMap<int8_t, AggDataPtr, seed>;
template <PhmapSeed seed>
using Int16AggHashMap = SmallFixedSizeHashMap<int16_t, AggDataPtr, seed>;
template <PhmapSeed seed>
using Int32AggHashMap = phmap::flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashMap = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashMap = phmap::flat_hash_map<int128_t, AggDataPtr, Hash128WithSeed<seed>>;
template <PhmapSeed seed>
using Int256AggHashMap = phmap::flat_hash_map<int256_t, AggDataPtr, Hash256WithSeed<seed>>;
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

template <typename T>
auto get_immutable_data(T* obj) {
    return obj->immutable_data();
}

static_assert(sizeof(AggDataPtr) == sizeof(size_t));
// The `prefetch_dist` argument is read once and stashed into a const local
// so the inner loop sees a register read.  We also capture it as
// `__prefetch_dist` separately so AGG_HASH_MAP_PREFETCH_HASH_VALUE can
// short-circuit when the operator set the knob to 0 (disable).
#define AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, prefetch_dist)              \
    size_t const column_size = column->size();                                  \
    size_t* hash_values = reinterpret_cast<size_t*>(agg_states->data());        \
    {                                                                           \
        const auto container_data = get_immutable_data(column);                 \
        for (size_t i = 0; i < column_size; i++) {                              \
            size_t hashval = this->hash_map.hash_function()(container_data[i]); \
            hash_values[i] = hashval;                                           \
        }                                                                       \
    }                                                                           \
    const size_t __prefetch_dist = prefetch_dist;                               \
    size_t __prefetch_index = __prefetch_dist;

#define AGG_HASH_MAP_PREFETCH_HASH_VALUE()                             \
    if (__prefetch_dist != 0 && __prefetch_index < column_size) {      \
        this->hash_map.prefetch_hash(hash_values[__prefetch_index++]); \
    }

template <typename HashMap, typename Impl>
struct AggHashMapWithKey {
    AggHashMapWithKey(int chunk_size, AggStatistics* agg_stat_) : agg_stat(agg_stat_) {}
    using HashMapType = HashMap;
    HashMap hash_map;
    AggStatistics* agg_stat;

    ////// Common Methods ////////
    template <AllocFunc<Impl> Func>
    void build_hash_map(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                        Buffer<AggDataPtr>* agg_states) {
        CancelableDefer defer = [this]() { hash_map.clear(); };
        ExtraAggParam extra;
        static_cast<Impl*>(this)->template compute_agg_states<Func, HTBuildOp<true, false, false>>(
                chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, &extra);
        defer.cancel();
    }

    template <AllocFunc<Impl> Func>
    void build_hash_map_with_selection(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                       Func&& allocate_func, Buffer<AggDataPtr>* agg_states, Filter* not_founds) {
        CancelableDefer defer = [this]() { hash_map.clear(); };
        // Assign not_founds vector when needs compute not founds.
        ExtraAggParam extra;
        extra.not_founds = not_founds;
        DCHECK(not_founds);
        (*not_founds).assign(chunk_size, 0);
        static_cast<Impl*>(this)->template compute_agg_states<Func, HTBuildOp<false, true, false>>(
                chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, &extra);
        defer.cancel();
    }

    template <AllocFunc<Impl> Func>
    void build_hash_map_with_limit(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                                   Buffer<AggDataPtr>* agg_states, Filter* not_founds, size_t limit) {
        CancelableDefer defer = [this]() { hash_map.clear(); };
        // Assign not_founds vector when needs compute not founds.
        ExtraAggParam extra;
        extra.not_founds = not_founds;
        extra.limits = limit;
        DCHECK(not_founds);
        (*not_founds).assign(chunk_size, 0);
        static_cast<Impl*>(this)->template compute_agg_states<Func, HTBuildOp<false, true, true>>(
                chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, &extra);
        defer.cancel();
    }

    template <AllocFunc<Impl> Func>
    void build_hash_map_with_selection_and_allocation(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                                      Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                      Filter* not_founds) {
        CancelableDefer defer = [this]() { hash_map.clear(); };
        // Assign not_founds vector when needs compute not founds.
        ExtraAggParam extra;
        extra.not_founds = not_founds;
        DCHECK(not_founds);
        (*not_founds).assign(chunk_size, 0);
        static_cast<Impl*>(this)->template compute_agg_states<Func, HTBuildOp<true, true, false>>(
                chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, &extra);
        defer.cancel();
    }
};

// ==============================================================
// handle one number hash key
template <LogicalType logical_type, typename HashMap, bool is_nullable>
struct AggHashMapWithOneNumberKeyWithNullable
        : public AggHashMapWithKey<HashMap, AggHashMapWithOneNumberKeyWithNullable<logical_type, HashMap, is_nullable>>,
          public InlineAggMapMixin<AggHashMapWithOneNumberKeyWithNullable<logical_type, HashMap, is_nullable>> {
    using Self = AggHashMapWithOneNumberKeyWithNullable<logical_type, HashMap, is_nullable>;
    using Base = AggHashMapWithKey<HashMap, Self>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ColumnType = RunTimeColumnType<logical_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<logical_type>;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash map key size needs to be larger than the actual element");

    template <class... Args>
    AggHashMapWithOneNumberKeyWithNullable(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return null_key_data; }
    // Whether the NULL-key group exists. On the general path the sentinel is the arena state
    // pointer; in inline mode every NULL-group touch sets the explicit existence bit, because
    // a per-row-delta op (count(col) over all-NULL values) can legally leave the accumulator
    // at 0 -- "slot != 0" stopped being a usable sentinel. Every consumer (variant size(),
    // classify, finalize) goes through this predicate.
    bool has_null_key() const { return null_key_exists || null_key_data != nullptr; }
    // First inline touch of the NULL-group accumulator: store the op's identity image (all-zero
    // for the additive family -- a no-op over the zero-initialized field -- but min/max start
    // from their typed limit) and mark existence.
    template <typename Op>
    ALWAYS_INLINE void _touch_null_acc() {
        if (!null_key_exists) {
            null_key_data = Op::identity_slot();
            null_key_exists = true;
        }
    }

    void set_null_key_data(AggDataPtr data) { null_key_data = data; }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        auto key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            return this->template compute_agg_states_nullable<Func, HTBuildOp>(
                    chunk_size, key_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
        } else {
            return this->template compute_agg_states_non_nullable<Func, HTBuildOp>(
                    chunk_size, key_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
        }
    }

    // Non Nullble
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_non_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                         Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                         ExtraAggParam* extra) {
        DCHECK(!key_column->is_nullable());
        const auto column = down_cast<const ColumnType*>(key_column);

        if constexpr (is_no_prefetch_map<HashMap>) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(column, agg_states,
                                                                   std::forward<Func>(allocate_func), extra);
        } else if (!agg_should_prefetch_table(this->hash_map)) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(column, agg_states,
                                                                   std::forward<Func>(allocate_func), extra);
        } else {
            this->template compute_agg_prefetch<Func, HTBuildOp>(column, agg_states, std::forward<Func>(allocate_func),
                                                                 extra);
        }
    }

    // Nullable
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                     Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                     ExtraAggParam* extra) {
        if (key_column->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_column->is_nullable());
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());

            // Shortcut: if nullable column has no nulls.
            if (!nullable_column->has_null()) {
                this->template compute_agg_states_non_nullable<Func, HTBuildOp>(
                        chunk_size, data_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
            } else {
                this->template compute_agg_through_null_data<Func, HTBuildOp>(chunk_size, nullable_column, agg_states,
                                                                              std::forward<Func>(allocate_func), extra);
            }
        }
    }

    // prefetch branch better performance in case with larger hash tables
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_prefetch(const ColumnType* column, Buffer<AggDataPtr>* agg_states,
                                              Func&& allocate_func, ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, agg_hash_map_default_prefetch_dist());
        for (size_t i = 0; i < column_size; i++) {
            AGG_HASH_MAP_PREFETCH_HASH_VALUE();

            FieldType key = column->immutable_data()[i];

            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key_with_hash(key, hash_values[i], (*agg_states)[i], allocate_func,
                                           [&] { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key, hash_values[i]);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key_with_hash(key, hash_values[i], (*agg_states)[i], allocate_func,
                                       FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                DCHECK(not_founds);
                _find_key((*agg_states)[i], (*not_founds)[i], key, hash_values[i]);
            }
        }
    }

    // prefetch branch better performance in case with small hash tables
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_noprefetch(const ColumnType* column, Buffer<AggDataPtr>* agg_states,
                                                Func&& allocate_func, ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        size_t num_rows = column->size();
        auto container = column->immutable_data();

        for (size_t i = 0; i < num_rows; i++) {
            FieldType key = container[i];
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key(key, (*agg_states)[i], allocate_func, [&] { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key(key, (*agg_states)[i], allocate_func,
                             FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                DCHECK(not_founds);
                _find_key((*agg_states)[i], (*not_founds)[i], key);
            }
            FAIL_POINT_TRIGGER_EXECUTE(aggregate_build_hash_map_bad_alloc, {
                if (i > 0) throw std::bad_alloc();
            });
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_through_null_data(size_t chunk_size, const NullableColumn* nullable_column,
                                                       Buffer<AggDataPtr>* agg_states, Func&& allocate_func,
                                                       ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
        const auto container = data_column->immutable_data();
        const auto& null_data = nullable_column->null_column_data();
        for (size_t i = 0; i < chunk_size; i++) {
            const auto key = container[i];
            if (null_data[i]) {
                if (UNLIKELY(null_key_data == nullptr)) {
                    null_key_data = allocate_func(nullptr);
                }
                (*agg_states)[i] = null_key_data;
            } else {
                if constexpr (HTBuildOp::process_limit) {
                    if (hash_table_size < extra->limits) {
                        this->template _emplace_key<Func>(key, (*agg_states)[i], std::forward<Func>(allocate_func),
                                                          [&]() { hash_table_size++; });
                    } else {
                        _find_key((*agg_states)[i], (*not_founds)[i], key);
                    }
                } else if constexpr (HTBuildOp::allocate) {
                    this->template _emplace_key<Func>(key, (*agg_states)[i], std::forward<Func>(allocate_func),
                                                      FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
                } else if constexpr (HTBuildOp::fill_not_found) {
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            }
        }
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key(KeyType key, AggDataPtr& target_state, Func&& allocate_func,
                                    EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key_with_hash(KeyType key, size_t hash, AggDataPtr& target_state, Func&& allocate_func,
                                              EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <typename... Args>
    ALWAYS_INLINE void _find_key(AggDataPtr& target_state, uint8_t& not_found, Args&&... args) {
        if (auto iter = this->hash_map.find(std::forward<Args>(args)...); iter != this->hash_map.end()) {
            target_state = iter->second;
        } else {
            not_found = 1;
        }
    }

    // ====================== inline-agg fast path ==========================
    // The hash-map value slot holds the op accumulator directly (no arena
    // state, no key dup, no update_batch). NULL here is about the group-by KEY:
    // for the nullable wrapper the NULL-key group accumulates into
    // null_key_data (reused as a slot). NULL input VALUES are a different axis
    // entirely -- the gate rejects nullable inputs (the contract: an existing
    // group never yields NULL), count(col)
    // being the one op that reads its input null mask, as deltas. Only reached
    // for supported numeric wrappers (gated by agg_inline_supported<> at the
    // visit site); compiles for all so the std::variant visit stays
    // well-formed. `agg_states` is reused only as the hash-precompute scratch
    // (never as state pointers here).
    // `delta` is what an emplaced/found row adds: 1 on a fused count update;
    // the op identity (no-op combine) on a merge chunk routed through a
    // creating build (group-by limit / allocate), which then only
    // classifies-and-creates -- the partials are folded by selection in
    // compute.
    template <typename Op, typename HTBuildOp>
    ALWAYS_NOINLINE void build_inline_agg(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                          Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra,
                                          typename Op::DeltaType delta) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            // NULL rows count into null_key_data (an int64 reused like a slot); non-null rows go
            // through the hash map. only_null / no-null are shortcuts over the general case.
            if (key_column->only_null()) {
                if constexpr (HTBuildOp::fill_not_found && !HTBuildOp::allocate && !HTBuildOp::process_limit) {
                    // Selective classify-only: the NULL group is a hit if it already exists (commit
                    // counts it later), otherwise a miss to be streamed -- never created here.
                    if (!has_null_key()) {
                        std::fill_n(extra->not_founds->begin(), chunk_size, 1);
                    }
                } else {
                    // Allocate or group-by-limit build: the NULL group always counts. The general path
                    // never streams an all-null chunk, even under a limit, so neither do we. The
                    // creating build folds the op-neutral delta per row (1 fused count, identity
                    // otherwise) -- combine is not assumed additive.
                    _touch_null_acc<Op>();
                    for (size_t i = 0; i < chunk_size; i++) {
                        Op::combine(null_key_data, delta);
                    }
                }
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                this->template inline_agg_build_dense<Op, HTBuildOp>(data_column, chunk_size, agg_states, extra, delta);
            } else {
                inline_agg_through_null<Op, HTBuildOp>(chunk_size, nullable_column, agg_states, extra, delta);
            }
        } else {
            this->template inline_agg_build_dense<Op, HTBuildOp>(down_cast<const ColumnType*>(key_column), chunk_size,
                                                                 agg_states, extra, delta);
        }
    }

    // Key-preparation hook for the shared inline loops: a single numeric key is a zero-copy
    // view over the column's data; when the loop prefetches, the hashes are precomputed into
    // the caller-provided scratch (the dead-in-inline-mode agg_states buffer, the same reuse
    // the general prefetch build does).
    InlineKeyView<FieldType> prepare_inline_keys(const ColumnType* column, size_t n, bool want_hashes,
                                                 Buffer<AggDataPtr>* scratch) {
        InlineKeyView<FieldType> view;
        const auto data = column->immutable_data();
        DCHECK_EQ(data.size(), n);
        view.key_base = reinterpret_cast<const uint8_t*>(data.data());
        view.key_stride = sizeof(FieldType);
        if (want_hashes) {
            static_assert(sizeof(AggDataPtr) == sizeof(size_t));
            DCHECK_GE(scratch->size(), data.size());
            size_t* hashes = reinterpret_cast<size_t*>(scratch->data());
            const size_t n = data.size();
            for (size_t i = 0; i < n; ++i) {
                hashes[i] = this->hash_map.hash_function()(data[i]);
            }
            view.hash_base = reinterpret_cast<const uint8_t*>(hashes);
            view.hash_stride = sizeof(size_t);
        }
        return view;
    }

    // Nullable build with mixed null/non-null rows.
    //  - Pure selective build (fill_not_found, no allocate, no limit): classify-only. An existing
    //    group (incl. the NULL group) is a hit committed later; a new key is a miss to be streamed,
    //    neither created nor counted here.
    //  - Group-by-limit build (process_limit): the NULL group always counts (the general path never
    //    streams nulls); a non-null key counts in place if it exists or there is room, else it is a
    //    miss. On a merge chunk (delta 0) the same walk only classifies-and-creates.
    //  - Allocate (fused) build: count everything in place.
    template <typename Op, typename HTBuildOp>
    ALWAYS_NOINLINE void inline_agg_through_null(size_t chunk_size, const NullableColumn* nullable_column,
                                                 Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra,
                                                 typename Op::DeltaType delta) {
        const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
        const auto container = data_column->immutable_data();
        const auto& null_data = nullable_column->null_column_data();
        if constexpr (HTBuildOp::fill_not_found && !HTBuildOp::allocate && !HTBuildOp::process_limit) {
            auto* __restrict not_founds = extra->not_founds;
            const bool null_group_exists = has_null_key();
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (!null_group_exists) (*not_founds)[i] = 1;
                } else {
                    inline_agg_probe(this->hash_map, container[i], (*not_founds)[i]);
                }
            }
        } else if constexpr (HTBuildOp::process_limit) {
            [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
            auto* __restrict not_founds = extra->not_founds;
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, delta);
                } else if (hash_table_size < extra->limits) {
                    inline_agg_emplace<Op>(
                            this->hash_map, container[i], [&] { hash_table_size++; }, delta);
                } else {
                    inline_agg_find<Op>(this->hash_map, container[i], (*not_founds)[i], delta);
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, delta);
                } else {
                    inline_agg_emplace<Op>(
                            this->hash_map, container[i], [] {}, delta);
                }
            }
        }
    }

    // Merge fold: slot += partials[i] for each kept row, in one emplace pass. `selection` is the
    // spill preaggregation miss mask; rows with selection[i] != 0 are streamed unchanged and must
    // not be folded into the local hash table. A null selection folds every row (non-selective merge).
    template <typename Op>
    void build_inline_agg_fold(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                               Buffer<AggDataPtr>* agg_states, const typename Op::DeltaType* partials,
                               const Filter* selection = nullptr) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            if (key_column->only_null()) {
                _touch_null_acc<Op>();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection == nullptr || (*selection)[i] == 0) {
                        Op::combine(null_key_data, partials[i]);
                    }
                }
                return;
            }
            DCHECK(key_column->is_nullable());
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
            if (nullable_column->has_null()) {
                const auto container = data_column->immutable_data();
                const auto& null_data = nullable_column->null_column_data();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection != nullptr && (*selection)[i] != 0) continue;
                    if (null_data[i]) {
                        _touch_null_acc<Op>();
                        Op::combine(null_key_data, partials[i]);
                    } else {
                        inline_agg_emplace<Op>(
                                this->hash_map, container[i], [] {}, partials[i]);
                    }
                }
                return;
            }
            this->template inline_agg_fold_dense<Op>(data_column, chunk_size, agg_states, partials, selection);
        } else {
            DCHECK(!key_column->is_nullable());
            this->template inline_agg_fold_dense<Op>(down_cast<const ColumnType*>(key_column), chunk_size, agg_states,
                                                     partials, selection);
        }
    }

    // Commit pass for the selective path: re-probe each non-streamed row (selection[i] == 0) and add
    // +1 to its slot. Re-looking-up the key instead of replaying a stashed slot address keeps the
    // count valid even if the hash table was rehashed/spilled after the classifying build. `selection`
    // may be null when the caller wants every row committed (the all-hit branch).
    template <typename Op>
    void commit_inline_agg(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* scratch,
                           const Filter* selection) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            if (key_column->only_null()) {
                _touch_null_acc<Op>();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection == nullptr || (*selection)[i] == 0) {
                        Op::combine(null_key_data, 1);
                    }
                }
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const ColumnType*>(nullable_column->data_column().get());
            const auto container = data_column->immutable_data();
            const auto& null_data = nullable_column->null_column_data();
            uint8_t ignore_not_found = 0;
            for (size_t i = 0; i < chunk_size; i++) {
                if (selection != nullptr && (*selection)[i] != 0) continue;
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, 1);
                } else {
                    inline_agg_find<Op>(this->hash_map, container[i], ignore_not_found, 1);
                }
            }
        } else {
            this->template inline_agg_commit_dense<Op>(down_cast<const ColumnType*>(key_column), chunk_size, selection);
        }
    }

    void insert_keys_to_columns(const ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        if constexpr (is_nullable) {
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* column = down_cast<ColumnType*>(nullable_column->data_column_raw_ptr());
            column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
            nullable_column->null_column_data().resize(chunk_size);
        } else {
            DCHECK(!null_key_data);
            auto* column = down_cast<ColumnType*>(key_columns[0].get());
            column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
        }
    }

    static constexpr bool has_single_null_key = is_nullable;
    AggDataPtr null_key_data = nullptr;
    // Inline-mode existence bit for the NULL group (see has_null_key()).
    bool null_key_exists = false;
    ResultVector results;
};

template <LogicalType logical_type, typename HashMap>
using AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKeyWithNullable<logical_type, HashMap, false>;
template <LogicalType logical_type, typename HashMap>
using AggHashMapWithOneNullableNumberKey = AggHashMapWithOneNumberKeyWithNullable<logical_type, HashMap, true>;

template <typename HashMap>
struct AggHashMapWithSerializedKeyFixedSize;
template <typename HashMap>
struct AggHashMapWithCompressedKeyFixedSize;
template <typename HashMap, bool is_nullable>
struct AggHashMapWithOneStringKeyWithNullable;

// Selects, at the variant visit site, which key types run the inline-agg path. The qualifier is
// that the value slot is a raw AggDataPtr the int64 counter can occupy directly.
template <typename T>
struct AggInlineSupported : std::false_type {};
// Single numeric key, nullable or not. The no-prefetch SmallFixedSizeHashMap (int8/int16 keys)
// is excluded -- its value slot is a direct-array pointer sentinel, not a per-group counter cell.
// For the nullable wrapper the NULL group's counter rides in the null_key_data field (reused as an
// int64 the same way a slot is), so it needs no hash-map cell.
template <LogicalType lt, typename HashMap, bool is_nullable>
struct AggInlineSupported<AggHashMapWithOneNumberKeyWithNullable<lt, HashMap, is_nullable>>
        : std::bool_constant<!is_no_prefetch_map<HashMap>> {};
// Multi-column fixed-size keys (serialized and compressed) also keep a raw AggDataPtr slot. A wide
// key is backed by a phmap, but a key that compresses to a single byte (cx1) is backed by the
// direct-array SmallFixedSizeHashMap, whose value cell is a pointer sentinel and which the inline
// build/finalize cannot iterate -- so, like the single-number key, the no-prefetch map is excluded
// and that key falls back to the general aggregation path.
template <typename HashMap>
struct AggInlineSupported<AggHashMapWithSerializedKeyFixedSize<HashMap>>
        : std::bool_constant<!is_no_prefetch_map<HashMap>> {};
template <typename HashMap>
struct AggInlineSupported<AggHashMapWithCompressedKeyFixedSize<HashMap>>
        : std::bool_constant<!is_no_prefetch_map<HashMap>> {};
// Single string key, nullable or not, on both the flat and the two-level phmap backings (one
// class covers both, so one specialization does too). The two-level conversion stays legal
// while the inline path is active: it migrates the {key, slot} pairs by value, and the only
// build that can span the conversion point -- the classify -> fold pair -- re-probes keys
// instead of replaying cell addresses.
template <typename HashMap, bool is_nullable>
struct AggInlineSupported<AggHashMapWithOneStringKeyWithNullable<HashMap, is_nullable>> : std::true_type {};
template <typename T>
inline constexpr bool agg_inline_supported = AggInlineSupported<std::remove_reference_t<T>>::value;

template <typename HashMap, bool is_nullable>
struct AggHashMapWithOneStringKeyWithNullable
        : public AggHashMapWithKey<HashMap, AggHashMapWithOneStringKeyWithNullable<HashMap, is_nullable>> {
    using Self = AggHashMapWithOneStringKeyWithNullable<HashMap, is_nullable>;
    using Base = AggHashMapWithKey<HashMap, Self>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = Buffer<Slice>;

    template <class... Args>
    AggHashMapWithOneStringKeyWithNullable(Args&&... args) : Base(std::forward<Args>(args)...) {}

    AggDataPtr get_null_key_data() { return null_key_data; }
    // Whether the NULL-key group exists. On the general path the sentinel is the arena state
    // pointer; in inline mode every NULL-group touch sets the explicit existence bit, because
    // a per-row-delta op (count(col) over all-NULL values) can legally leave the accumulator
    // at 0 -- "slot != 0" is not a usable sentinel. Every consumer (variant size(), classify,
    // finalize) goes through this predicate.
    bool has_null_key() const { return null_key_exists || null_key_data != nullptr; }

    void set_null_key_data(AggDataPtr data) { null_key_data = data; }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        const auto* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            return this->template compute_agg_states_nullable<Func, HTBuildOp>(
                    chunk_size, key_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
        } else {
            return this->template compute_agg_states_non_nullable<Func, HTBuildOp>(
                    chunk_size, key_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
        }
    }

    // Non Nullable
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_non_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                         Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                         ExtraAggParam* extra) {
        DCHECK(key_column->is_binary());
        const auto* column = down_cast<const BinaryColumn*>(key_column);
        if (!agg_should_prefetch_table(this->hash_map)) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(column, agg_states, pool,
                                                                   std::forward<Func>(allocate_func), extra);
        } else {
            this->template compute_agg_prefetch<Func, HTBuildOp>(column, agg_states, pool,
                                                                 std::forward<Func>(allocate_func), extra);
        }
    }

    // Nullable
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_nullable(size_t chunk_size, const Column* key_column, MemPool* pool,
                                                     Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                     ExtraAggParam* extra) {
        if (key_column->only_null()) {
            if (null_key_data == nullptr) {
                null_key_data = allocate_func(nullptr);
            }
            for (size_t i = 0; i < chunk_size; i++) {
                (*agg_states)[i] = null_key_data;
            }
        } else {
            DCHECK(key_column->is_nullable());
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
            DCHECK(data_column->is_binary());

            if (!nullable_column->has_null()) {
                this->template compute_agg_states_non_nullable<Func, HTBuildOp>(
                        chunk_size, data_column, pool, std::forward<Func>(allocate_func), agg_states, extra);
            } else {
                this->template compute_agg_through_null_data<Func, HTBuildOp>(
                        chunk_size, nullable_column, agg_states, pool, std::forward<Func>(allocate_func), extra);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_prefetch(const BinaryColumn* column, Buffer<AggDataPtr>* agg_states, MemPool* pool,
                                              Func&& allocate_func, ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        AGG_HASH_MAP_PRECOMPUTE_HASH_VALUES(column, agg_hash_map_default_prefetch_dist());
        for (size_t i = 0; i < column_size; i++) {
            AGG_HASH_MAP_PREFETCH_HASH_VALUE();
            auto key = column->get_slice(i);
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    this->template _emplace_key_with_hash<Func>(key, hash_values[i], pool,
                                                                std::forward<Func>(allocate_func), (*agg_states)[i],
                                                                [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key, hash_values[i]);
                }
            } else if constexpr (HTBuildOp::allocate) {
                this->template _emplace_key_with_hash<Func>(key, hash_values[i], pool,
                                                            std::forward<Func>(allocate_func), (*agg_states)[i],
                                                            FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key, hash_values[i]);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_noprefetch(const BinaryColumn* column, Buffer<AggDataPtr>* agg_states,
                                                MemPool* pool, Func&& allocate_func, ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        size_t num_rows = column->size();

        for (size_t i = 0; i < num_rows; i++) {
            auto key = column->get_slice(i);
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    this->template _emplace_key<Func>(key, pool, std::forward<Func>(allocate_func), (*agg_states)[i],
                                                      [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            } else if constexpr (HTBuildOp::allocate) {
                this->template _emplace_key<Func>(key, pool, std::forward<Func>(allocate_func), (*agg_states)[i],
                                                  FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_through_null_data(size_t chunk_size, const NullableColumn* nullable_column,
                                                       Buffer<AggDataPtr>* agg_states, MemPool* pool,
                                                       Func&& allocate_func, ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        const auto& null_data = nullable_column->null_column_data();

        for (size_t i = 0; i < chunk_size; i++) {
            if (null_data[i]) {
                if (UNLIKELY(null_key_data == nullptr)) {
                    null_key_data = allocate_func(nullptr);
                }
                (*agg_states)[i] = null_key_data;
            } else {
                const auto key = data_column->get_slice(i);
                if constexpr (HTBuildOp::process_limit) {
                    if (hash_table_size < extra->limits) {
                        this->template _emplace_key<Func>(key, pool, std::forward<Func>(allocate_func),
                                                          (*agg_states)[i], [&]() { hash_table_size++; });
                    } else {
                        _find_key((*agg_states)[i], (*not_founds)[i], key);
                    }
                } else if constexpr (HTBuildOp::allocate) {
                    this->template _emplace_key<Func>(key, pool, std::forward<Func>(allocate_func), (*agg_states)[i],
                                                      FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
                } else if constexpr (HTBuildOp::fill_not_found) {
                    DCHECK(not_founds);
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            }
        }
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    void _emplace_key_with_hash(const KeyType& key, size_t hash_val, MemPool* pool, Func&& allocate_func,
                                AggDataPtr& target_state, EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash_val, [&](const auto& ctor) {
            callback();
            uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func(pk);
            ctor(pk, pv);
        });
        target_state = iter->second;
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    void _emplace_key(const KeyType& key, MemPool* pool, Func&& allocate_func, AggDataPtr& target_state,
                      EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            callback();
            uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func(pk);
            ctor(pk, pv);
        });
        target_state = iter->second;
    }

    template <typename... Args>
    ALWAYS_INLINE void _find_key(AggDataPtr& target_state, uint8_t& not_found, Args&&... args) {
        if (auto iter = this->hash_map.find(std::forward<Args>(args)...); iter != this->hash_map.end()) {
            target_state = iter->second;
        } else {
            not_found = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, size_t chunk_size) {
        if constexpr (is_nullable) {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* column = down_cast<BinaryColumn*>(nullable_column->data_column_raw_ptr());
            keys.resize(chunk_size);
            column->append_strings(keys.data(), keys.size());
            nullable_column->null_column_data().resize(chunk_size);
        } else {
            DCHECK(!null_key_data);
            auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
            keys.resize(chunk_size);
            column->append_strings(keys.data(), keys.size());
        }
    }

    // ====================== inline-agg fast path ==========================
    // String flavor of the inline path: the hash-map value slot holds the op accumulator
    // directly (no arena state). The key handling stays the general path's: an inserted
    // key is duplicated into the pool (the slot saves the STATE indirection, never the key
    // copy). This flavor does not ride the shared fixed-size loops; its own passes differ
    // on purpose:
    //   - the chunk's hashes are ALWAYS precomputed in a dense pass first. The string hash
    //     branches on the key length, and on mixed-length keys that branch mispredicts about
    //     once per two rows; inside the dependent emplace chain such a mispredict stalls the
    //     probe pipeline, while in a dense pass it overlaps with the neighboring rows.
    //   - the Slice keys are formed per row from the binary column (no key view: a slice is
    //     two loads from the column, not a strided buffer).
    // NULL here is about the group-by KEY (null_key_data is reused as the NULL group's
    // slot, existence tracked by null_key_exists). NULL input VALUES are a different axis:
    // the gate rejects nullable inputs, count(col) reads its input null mask as deltas.
    template <typename Op>
    ALWAYS_INLINE void _touch_null_acc() {
        if (!null_key_exists) {
            null_key_data = Op::identity_slot();
            null_key_exists = true;
        }
    }

    // Dup-and-init emplace: a new group copies the key into the pool (with the memequal
    // overflow reserve, like the general emplace) and starts its slot from the op identity.
    template <typename Op, typename Callback>
    ALWAYS_INLINE void _inline_emplace_str(const Slice& key, size_t hash, MemPool* pool, Callback&& cb,
                                           typename Op::DeltaType delta) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
            cb();
            uint8_t* pos = pool->allocate_with_reserve(key.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            strings::memcpy_inlined(pos, key.data, key.size);
            ctor(Slice{pos, key.size}, Op::identity_slot());
        });
        Op::combine(iter->second, delta);
    }

    // Dense hash precompute into the caller's scratch (the dead-in-inline-mode agg_states
    // buffer, 8 bytes per row -- same reuse as the numeric flavor).
    size_t* _inline_prehash_str(const BinaryColumn* column, size_t n, Buffer<AggDataPtr>* scratch) {
        static_assert(sizeof(AggDataPtr) == sizeof(size_t));
        DCHECK_GE(scratch->size(), n);
        size_t* hashes = reinterpret_cast<size_t*>(scratch->data());
        const auto hasher = this->hash_map.hash_function();
        for (size_t i = 0; i < n; ++i) {
            hashes[i] = hasher(column->get_slice(i));
        }
        return hashes;
    }

    template <typename Op, typename HTBuildOp>
    ALWAYS_NOINLINE void _inline_agg_str_build(const BinaryColumn* column, size_t n, MemPool* pool,
                                               Buffer<AggDataPtr>* scratch, ExtraAggParam* extra,
                                               typename Op::DeltaType delta) {
        size_t* hashes = _inline_prehash_str(column, n, scratch);
        const size_t dist = agg_hash_map_default_prefetch_dist();
        const bool do_pf = dist != 0 && agg_should_prefetch_table(this->hash_map);
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        [[maybe_unused]] size_t prefetch_index = dist;
        for (size_t i = 0; i < n; ++i) {
            if (do_pf && prefetch_index < n) {
                this->hash_map.prefetch_hash(hashes[prefetch_index++]);
            }
            const Slice key = column->get_slice(i);
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _inline_emplace_str<Op>(
                            key, hashes[i], pool, [&] { hash_table_size++; }, delta);
                } else {
                    inline_agg_find_with_hash<Op>(this->hash_map, key, hashes[i], (*not_founds)[i], delta);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _inline_emplace_str<Op>(key, hashes[i], pool, FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i),
                                        delta);
            } else if constexpr (HTBuildOp::fill_not_found) {
                inline_agg_probe_with_hash(this->hash_map, key, hashes[i], (*not_founds)[i]);
            }
        }
    }

    template <typename Op>
    ALWAYS_NOINLINE void _inline_agg_str_fold(const BinaryColumn* column, size_t n, MemPool* pool,
                                              Buffer<AggDataPtr>* scratch, const typename Op::DeltaType* partials,
                                              const Filter* selection) {
        size_t* hashes = _inline_prehash_str(column, n, scratch);
        const size_t dist = agg_hash_map_default_prefetch_dist();
        const bool do_pf = dist != 0 && agg_should_prefetch_table(this->hash_map);
        [[maybe_unused]] size_t prefetch_index = dist;
        for (size_t i = 0; i < n; ++i) {
            if (do_pf && prefetch_index < n) {
                this->hash_map.prefetch_hash(hashes[prefetch_index++]);
            }
            if (selection != nullptr && (*selection)[i] != 0) continue;
            _inline_emplace_str<Op>(
                    column->get_slice(i), hashes[i], pool, [] {}, partials[i]);
        }
    }

    template <typename Op>
    ALWAYS_NOINLINE void _inline_agg_str_commit(const BinaryColumn* column, size_t n, Buffer<AggDataPtr>* scratch,
                                                const Filter* selection) {
        size_t* hashes = _inline_prehash_str(column, n, scratch);
        uint8_t ignore_not_found = 0;
        for (size_t i = 0; i < n; ++i) {
            if (selection != nullptr && (*selection)[i] != 0) continue;
            inline_agg_find_with_hash<Op>(this->hash_map, column->get_slice(i), hashes[i], ignore_not_found, 1);
        }
    }

    // Build entry. Mirrors the numeric flavor's NULL-key plumbing: only_null / no-null are
    // shortcuts, mixed chunks walk the null mask row by row.
    template <typename Op, typename HTBuildOp>
    ALWAYS_NOINLINE void build_inline_agg(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                          Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra,
                                          typename Op::DeltaType delta) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            if (key_column->only_null()) {
                if constexpr (HTBuildOp::fill_not_found && !HTBuildOp::allocate && !HTBuildOp::process_limit) {
                    // Selective classify-only: the NULL group is a hit if it already exists,
                    // otherwise a miss to be streamed -- never created here.
                    if (!has_null_key()) {
                        std::fill_n(extra->not_founds->begin(), chunk_size, 1);
                    }
                } else {
                    // Allocate or group-by-limit build: the NULL group always counts (the general
                    // path never streams an all-null chunk).
                    _touch_null_acc<Op>();
                    for (size_t i = 0; i < chunk_size; i++) {
                        Op::combine(null_key_data, delta);
                    }
                }
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                _inline_agg_str_build<Op, HTBuildOp>(data_column, chunk_size, pool, agg_states, extra, delta);
            } else {
                _inline_str_through_null<Op, HTBuildOp>(chunk_size, nullable_column, pool, extra, delta);
            }
        } else {
            _inline_agg_str_build<Op, HTBuildOp>(down_cast<const BinaryColumn*>(key_column), chunk_size, pool,
                                                 agg_states, extra, delta);
        }
    }

    // Mixed null/non-null build. Same classify/limit/allocate split as the numeric flavor;
    // rows go one by one (the null mask interleaves the two destinations), so no prehash here.
    template <typename Op, typename HTBuildOp>
    ALWAYS_NOINLINE void _inline_str_through_null(size_t chunk_size, const NullableColumn* nullable_column,
                                                  MemPool* pool, ExtraAggParam* extra, typename Op::DeltaType delta) {
        const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        const auto& null_data = nullable_column->null_column_data();
        if constexpr (HTBuildOp::fill_not_found && !HTBuildOp::allocate && !HTBuildOp::process_limit) {
            auto* __restrict not_founds = extra->not_founds;
            const bool null_group_exists = has_null_key();
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    if (!null_group_exists) (*not_founds)[i] = 1;
                } else {
                    inline_agg_probe(this->hash_map, data_column->get_slice(i), (*not_founds)[i]);
                }
            }
        } else if constexpr (HTBuildOp::process_limit) {
            [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
            auto* __restrict not_founds = extra->not_founds;
            const auto hasher = this->hash_map.hash_function();
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, delta);
                } else if (const Slice key = data_column->get_slice(i); hash_table_size < extra->limits) {
                    _inline_emplace_str<Op>(
                            key, hasher(key), pool, [&] { hash_table_size++; }, delta);
                } else {
                    inline_agg_find<Op>(this->hash_map, key, (*not_founds)[i], delta);
                }
            }
        } else {
            const auto hasher = this->hash_map.hash_function();
            for (size_t i = 0; i < chunk_size; i++) {
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, delta);
                } else {
                    const Slice key = data_column->get_slice(i);
                    _inline_emplace_str<Op>(
                            key, hasher(key), pool, [] {}, delta);
                }
            }
        }
    }

    // Merge fold: slot += partials[i] for each kept row (selection[i] != 0 rows are streamed
    // unchanged). Same shape as the numeric flavor's fold.
    template <typename Op>
    void build_inline_agg_fold(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                               Buffer<AggDataPtr>* agg_states, const typename Op::DeltaType* partials,
                               const Filter* selection = nullptr) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            if (key_column->only_null()) {
                _touch_null_acc<Op>();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection == nullptr || (*selection)[i] == 0) {
                        Op::combine(null_key_data, partials[i]);
                    }
                }
                return;
            }
            DCHECK(key_column->is_nullable());
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
            if (nullable_column->has_null()) {
                const auto& null_data = nullable_column->null_column_data();
                const auto hasher = this->hash_map.hash_function();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection != nullptr && (*selection)[i] != 0) continue;
                    if (null_data[i]) {
                        _touch_null_acc<Op>();
                        Op::combine(null_key_data, partials[i]);
                    } else {
                        const Slice key = data_column->get_slice(i);
                        _inline_emplace_str<Op>(
                                key, hasher(key), pool, [] {}, partials[i]);
                    }
                }
                return;
            }
            _inline_agg_str_fold<Op>(data_column, chunk_size, pool, agg_states, partials, selection);
        } else {
            DCHECK(!key_column->is_nullable());
            _inline_agg_str_fold<Op>(down_cast<const BinaryColumn*>(key_column), chunk_size, pool, agg_states, partials,
                                     selection);
        }
    }

    // Commit pass for the selective path: re-probe each kept row and add +1 to its slot
    // (re-looking-up the key keeps the count valid across a rehash or a two-level conversion).
    template <typename Op>
    void commit_inline_agg(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* scratch,
                           const Filter* selection) {
        const Column* key_column = key_columns[0].get();
        if constexpr (is_nullable) {
            if (key_column->only_null()) {
                _touch_null_acc<Op>();
                for (size_t i = 0; i < chunk_size; i++) {
                    if (selection == nullptr || (*selection)[i] == 0) {
                        Op::combine(null_key_data, 1);
                    }
                }
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(key_column);
            const auto* data_column = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();
            uint8_t ignore_not_found = 0;
            for (size_t i = 0; i < chunk_size; i++) {
                if (selection != nullptr && (*selection)[i] != 0) continue;
                if (null_data[i]) {
                    _touch_null_acc<Op>();
                    Op::combine(null_key_data, 1);
                } else {
                    inline_agg_find<Op>(this->hash_map, data_column->get_slice(i), ignore_not_found, 1);
                }
            }
        } else {
            _inline_agg_str_commit<Op>(down_cast<const BinaryColumn*>(key_column), chunk_size, scratch, selection);
        }
    }

    static constexpr bool has_single_null_key = is_nullable;

    AggDataPtr null_key_data = nullptr;
    // Inline-mode existence bit for the NULL group: a per-row-delta op (count(col) over
    // all-NULL values) can legally leave the accumulator at 0, so "slot != 0" stopped being
    // a usable sentinel (see the numeric flavor).
    bool null_key_exists = false;
    ResultVector results;
};

template <typename HashMap>
using AggHashMapWithOneStringKey = AggHashMapWithOneStringKeyWithNullable<HashMap, false>;
template <typename HashMap>
using AggHashMapWithOneNullableStringKey = AggHashMapWithOneStringKeyWithNullable<HashMap, true>;

template <typename HashMap>
struct AggHashMapWithSerializedKey : public AggHashMapWithKey<HashMap, AggHashMapWithSerializedKey<HashMap>> {
    using Self = AggHashMapWithSerializedKey<HashMap>;
    using Base = AggHashMapWithKey<HashMap, AggHashMapWithSerializedKey<HashMap>>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using ResultVector = Buffer<Slice>;

    struct CacheEntry {
        KeyType key;
        size_t hashval;
    };
    std::vector<CacheEntry> caches;

    template <class... Args>
    AggHashMapWithSerializedKey(int chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              mem_pool(std::make_unique<MemPool>()),
              buffer(mem_pool->allocate(max_one_row_size * chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING)),
              _chunk_size(chunk_size) {}

    AggDataPtr get_null_key_data() { return nullptr; }
    bool has_null_key() const { return false; }
    void set_null_key_data(AggDataPtr data) {}

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        slice_sizes.assign(_chunk_size, 0);
        size_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            size_t batch_allocate_size = (size_t)cur_max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING;
            // too large, process by rows
            if (batch_allocate_size > std::numeric_limits<int32_t>::max()) {
                max_one_row_size = 0;
                mem_pool->clear();
                buffer = mem_pool->allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
                return compute_agg_states_by_rows<Func, HTBuildOp>(chunk_size, key_columns, pool,
                                                                   std::move(allocate_func), agg_states, extra,
                                                                   cur_max_one_row_size);
            }
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(batch_allocate_size);
        }
        // process by cols
        return compute_agg_states_by_cols<Func, HTBuildOp>(chunk_size, key_columns, pool, std::move(allocate_func),
                                                           agg_states, extra, cur_max_one_row_size);
    }

    // There may be additional virtual function overhead, but the bottleneck point for this branch is serialization
    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_by_rows(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                                    Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                    ExtraAggParam* extra, size_t max_serialize_each_row) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        for (size_t i = 0; i < chunk_size; ++i) {
            auto serialize_cursor = buffer;
            for (const auto& key_column : key_columns) {
                serialize_cursor += key_column->serialize(i, serialize_cursor);
            }
            DCHECK(serialize_cursor <= buffer + max_serialize_each_row);
            size_t serialize_size = serialize_cursor - buffer;
            Slice key = {buffer, serialize_size};
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key(key, pool, allocate_func, (*agg_states)[i], [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key(key, pool, allocate_func, (*agg_states)[i],
                             FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_by_cols(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                                    Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                    ExtraAggParam* extra, size_t max_serialize_each_row) {
        uint32_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            buffer = mem_pool->allocate(max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_one_row_size);
        }
        if (!agg_should_prefetch_table(this->hash_map)) {
            this->template compute_agg_states_by_cols_non_prefetch<Func, HTBuildOp>(
                    chunk_size, key_columns, pool, std::move(allocate_func), agg_states, extra, max_serialize_each_row);
        } else {
            this->template compute_agg_states_by_cols_prefetch<Func, HTBuildOp>(
                    chunk_size, key_columns, pool, std::move(allocate_func), agg_states, extra, max_serialize_each_row);
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_by_cols_non_prefetch(size_t chunk_size, const Columns& key_columns,
                                                                 MemPool* pool, Func&& allocate_func,
                                                                 Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra,
                                                                 size_t max_serialize_each_row) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        for (size_t i = 0; i < chunk_size; ++i) {
            Slice key = {buffer + i * max_one_row_size, slice_sizes[i]};
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key(key, pool, allocate_func, (*agg_states)[i], [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key(key, pool, allocate_func, (*agg_states)[i],
                             FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));

            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_states_by_cols_prefetch(size_t chunk_size, const Columns& key_columns,
                                                             MemPool* pool, Func&& allocate_func,
                                                             Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra,
                                                             size_t max_serialize_each_row) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        caches.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            caches[i].key = KeyType(Slice(buffer + i * max_one_row_size, slice_sizes[i]));
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            caches[i].hashval = this->hash_map.hash_function()(caches[i].key);
        }

        const size_t __prefetch_dist = agg_hash_map_default_prefetch_dist();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (__prefetch_dist != 0 && i + __prefetch_dist < chunk_size) {
                this->hash_map.prefetch_hash(caches[i + __prefetch_dist].hashval);
            }

            const auto& key = caches[i].key;
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key_with_hash(key, caches[i].hashval, pool, allocate_func, (*agg_states)[i],
                                           [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key, caches[i].hashval);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key_with_hash(key, caches[i].hashval, pool, allocate_func, (*agg_states)[i],
                                       FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key, caches[i].hashval);
            }
        }
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    void _emplace_key_with_hash(const KeyType& key, size_t hash_val, MemPool* pool, Func&& allocate_func,
                                AggDataPtr& target_state, EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash_val, [&](const auto& ctor) {
            callback();
            // we must persist the slice before insert
            uint8_t* pos = pool->allocate(key.size);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func(pk);
            ctor(pk, pv);
        });
        target_state = iter->second;
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    void _emplace_key(const KeyType& key, MemPool* pool, Func&& allocate_func, AggDataPtr& target_state,
                      EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            callback();
            // we must persist the slice before insert
            uint8_t* pos = pool->allocate(key.size);
            strings::memcpy_inlined(pos, key.data, key.size);
            Slice pk{pos, key.size};
            AggDataPtr pv = allocate_func(pk);
            ctor(pk, pv);
        });
        target_state = iter->second;
    }

    template <typename... Args>
    ALWAYS_INLINE void _find_key(AggDataPtr& target_state, uint8_t& not_found, Args&&... args) {
        if (auto iter = this->hash_map.find(std::forward<Args>(args)...); iter != this->hash_map.end()) {
            target_state = iter->second;
        } else {
            not_found = 1;
        }
    }

    uint32_t get_max_serialize_size(const Columns& key_columns) {
        uint32_t max_size = 0;
        for (const auto& key_column : key_columns) {
            max_size += key_column->max_one_element_serialize_size();
        }
        return max_size;
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
    uint32_t max_one_row_size = 8;

    std::unique_ptr<MemPool> mem_pool;
    uint8_t* buffer;
    ResultVector results;

    int32_t _chunk_size;
};

template <typename HashMap>
struct AggHashMapWithSerializedKeyFixedSize
        : public AggHashMapWithKey<HashMap, AggHashMapWithSerializedKeyFixedSize<HashMap>>,
          public InlineAggMapMixin<AggHashMapWithSerializedKeyFixedSize<HashMap>> {
    using Self = AggHashMapWithSerializedKeyFixedSize<HashMap>;
    using Base = AggHashMapWithKey<HashMap, AggHashMapWithSerializedKeyFixedSize<HashMap>>;
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
        auto* buffer = reinterpret_cast<uint8_t*>(caches.data());
        memset(buffer, 0x0, max_fixed_size * _chunk_size);
    }

    AggDataPtr get_null_key_data() { return nullptr; }
    bool has_null_key() const { return false; }
    void set_null_key_data(AggDataPtr data) {}

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_prefetch(size_t chunk_size, const Columns& key_columns,
                                              Buffer<AggDataPtr>* agg_states, Func&& allocate_func,
                                              ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        auto* buffer = reinterpret_cast<uint8_t*>(caches.data());
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

        const size_t __prefetch_dist = agg_hash_map_default_prefetch_dist();
        size_t __prefetch_index = __prefetch_dist;

        for (size_t i = 0; i < chunk_size; ++i) {
            if (__prefetch_dist != 0 && __prefetch_index < chunk_size) {
                this->hash_map.prefetch_hash(caches[__prefetch_index++].hashval);
            }
            FixedSizeSliceKey& key = caches[i].key;
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key_with_hash(key, caches[i].hashval, (*agg_states)[i], allocate_func,
                                           [&]() { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key, caches[i].hashval);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key_with_hash(key, caches[i].hashval, (*agg_states)[i], allocate_func,
                                       FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key, caches[i].hashval);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_noprefetch(size_t chunk_size, const Columns& key_columns,
                                                Buffer<AggDataPtr>* agg_states, Func&& allocate_func,
                                                ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        constexpr int key_size = sizeof(FixedSizeSliceKey);
        auto* buffer = reinterpret_cast<uint8_t*>(caches.data());
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, key_size);
        }
        auto* key = reinterpret_cast<FixedSizeSliceKey*>(caches.data());
        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                key[i].u.size = slice_sizes[i];
            }
        }
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key(key[i], (*agg_states)[i], allocate_func, [&] { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], key[i]);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key(key[i], (*agg_states)[i], allocate_func,
                             FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], key[i]);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        auto* buffer = reinterpret_cast<uint8_t*>(caches.data());
        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        if (!agg_should_prefetch_table(this->hash_map)) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(chunk_size, key_columns, agg_states,
                                                                   std::forward<Func>(allocate_func), extra);
        } else {
            this->template compute_agg_prefetch<Func, HTBuildOp>(chunk_size, key_columns, agg_states,
                                                                 std::forward<Func>(allocate_func), extra);
        }
    }

    // ====================== inline-agg fast path ==========================
    // Multi-column fixed-size key: serialize the key columns the same way the general path
    // does, then keep the op accumulator in the value slot instead of an arena state.
    // Selective build (fill_not_found, no allocate) only classifies hit vs miss; the kept
    // rows are committed/folded later by re-probing the keys.
    void _inline_serialize_keys(size_t chunk_size, const Columns& key_columns, size_t stride) {
        slice_sizes.assign(chunk_size, 0);
        auto* buffer = reinterpret_cast<uint8_t*>(caches.data());
        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }
        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, stride);
        }
    }

    // Key-preparation hook for the shared inline loops: serialize the key columns into the
    // caches buffer. Without hashes the keys are packed densely; with hashes each CacheEntry
    // interleaves {key, hashval}, which the strided view expresses directly.
    InlineKeyView<FixedSizeSliceKey> prepare_inline_keys(const Columns& key_columns, size_t chunk_size,
                                                         bool want_hashes, Buffer<AggDataPtr>* /*scratch*/) {
        DCHECK(fixed_byte_size != -1);
        InlineKeyView<FixedSizeSliceKey> view;
        if (!want_hashes) {
            _inline_serialize_keys(chunk_size, key_columns, sizeof(FixedSizeSliceKey));
            auto* keys = reinterpret_cast<FixedSizeSliceKey*>(caches.data());
            if (has_null_column) {
                for (size_t i = 0; i < chunk_size; ++i) keys[i].u.size = slice_sizes[i];
            }
            view.key_base = reinterpret_cast<const uint8_t*>(keys);
            view.key_stride = sizeof(FixedSizeSliceKey);
        } else {
            _inline_serialize_keys(chunk_size, key_columns, max_fixed_size);
            if (has_null_column) {
                for (size_t i = 0; i < chunk_size; ++i) caches[i].key.u.size = slice_sizes[i];
            }
            for (size_t i = 0; i < chunk_size; ++i) caches[i].hashval = this->hash_map.hash_function()(caches[i].key);
            view.key_base = reinterpret_cast<const uint8_t*>(&caches[0].key);
            view.key_stride = sizeof(CacheEntry);
            view.hash_base = reinterpret_cast<const uint8_t*>(&caches[0].hashval);
            view.hash_stride = sizeof(CacheEntry);
        }
        return view;
    }

    // build/fold/commit come from InlineAggMapMixin over the hook above.

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key(KeyType key, AggDataPtr& target_state, Func&& allocate_func,
                                    EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key_with_hash(KeyType key, size_t hash, AggDataPtr& target_state, Func&& allocate_func,
                                              EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <typename... Args>
    ALWAYS_INLINE void _find_key(AggDataPtr& target_state, uint8_t& not_found, Args&&... args) {
        if (auto iter = this->hash_map.find(std::forward<Args>(args)...); iter != this->hash_map.end()) {
            target_state = iter->second;
        } else {
            not_found = 1;
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

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemPool> mem_pool;
    ResultVector results;
    Buffer<Slice> tmp_slices;
    int32_t _chunk_size;
};

template <typename HashMap>
struct AggHashMapWithCompressedKeyFixedSize
        : public AggHashMapWithKey<HashMap, AggHashMapWithCompressedKeyFixedSize<HashMap>>,
          public InlineAggMapMixin<AggHashMapWithCompressedKeyFixedSize<HashMap>> {
    using Self = AggHashMapWithCompressedKeyFixedSize<HashMap>;
    using Base = AggHashMapWithKey<HashMap, AggHashMapWithCompressedKeyFixedSize<HashMap>>;
    using KeyType = typename HashMap::key_type;
    using Iterator = typename HashMap::iterator;
    using FixedSizeSliceKey = typename HashMap::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;

    template <class... Args>
    AggHashMapWithCompressedKeyFixedSize(int chunk_size, Args&&... args)
            : Base(chunk_size, std::forward<Args>(args)...),
              mem_pool(std::make_unique<MemPool>()),
              _chunk_size(chunk_size) {
        fixed_keys.reserve(chunk_size);
    }

    AggDataPtr get_null_key_data() { return nullptr; }
    bool has_null_key() const { return false; }
    void set_null_key_data(AggDataPtr data) {}

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_noprefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                                Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                                ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        // serialize
        bitcompress_serialize(key_columns, bases, offsets, chunk_size, sizeof(FixedSizeSliceKey), fixed_keys.data());

        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key(fixed_keys[i], (*agg_states)[i], allocate_func, [&] { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], fixed_keys[i]);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key(fixed_keys[i], (*agg_states)[i], allocate_func,
                             FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], fixed_keys[i]);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    ALWAYS_NOINLINE void compute_agg_prefetch(size_t chunk_size, const Columns& key_columns, MemPool* pool,
                                              Func&& allocate_func, Buffer<AggDataPtr>* agg_states,
                                              ExtraAggParam* extra) {
        [[maybe_unused]] size_t hash_table_size = this->hash_map.size();
        auto* __restrict not_founds = extra->not_founds;
        // serialize
        bitcompress_serialize(key_columns, bases, offsets, chunk_size, sizeof(FixedSizeSliceKey), fixed_keys.data());

        hashs.reserve(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            hashs[i] = this->hash_map.hash_function()(fixed_keys[i]);
        }

        const size_t prefetch_dist = agg_hash_map_default_prefetch_dist();
        size_t prefetch_index = prefetch_dist;
        for (size_t i = 0; i < chunk_size; ++i) {
            if (prefetch_dist != 0 && prefetch_index < chunk_size) {
                this->hash_map.prefetch_hash(hashs[prefetch_index++]);
            }
            if constexpr (HTBuildOp::process_limit) {
                if (hash_table_size < extra->limits) {
                    _emplace_key_with_hash(fixed_keys[i], hashs[i], (*agg_states)[i], allocate_func,
                                           [&] { hash_table_size++; });
                } else {
                    _find_key((*agg_states)[i], (*not_founds)[i], fixed_keys[i]);
                }
            } else if constexpr (HTBuildOp::allocate) {
                _emplace_key_with_hash(fixed_keys[i], hashs[i], (*agg_states)[i], allocate_func,
                                       FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i));
            } else if constexpr (HTBuildOp::fill_not_found) {
                _find_key((*agg_states)[i], (*not_founds)[i], fixed_keys[i]);
            }
        }
    }

    template <AllocFunc<Self> Func, typename HTBuildOp>
    void compute_agg_states(size_t chunk_size, const Columns& key_columns, MemPool* pool, Func&& allocate_func,
                            Buffer<AggDataPtr>* agg_states, ExtraAggParam* extra) {
        auto* buffer = reinterpret_cast<uint8_t*>(fixed_keys.data());
        memset(buffer, 0x0, sizeof(FixedSizeSliceKey) * chunk_size);

        if constexpr (is_no_prefetch_map<HashMap>) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(
                    chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, extra);
        } else if (!agg_should_prefetch_table(this->hash_map)) {
            this->template compute_agg_noprefetch<Func, HTBuildOp>(
                    chunk_size, key_columns, pool, std::forward<Func>(allocate_func), agg_states, extra);
        } else {
            this->template compute_agg_prefetch<Func, HTBuildOp>(chunk_size, key_columns, pool,
                                                                 std::forward<Func>(allocate_func), agg_states, extra);
        }
    }

    // ===== inline-agg fast path (multi-column compressed fixed-size key) =====
    // Keys are bit-compressed (bitcompress_serialize) into the fixed-size value; the op
    // accumulator then lives in the value slot. Only the phmap-backed widths (cx4/cx8/cx16) reach here:
    // a key that compresses to one byte (cx1) is backed by the no-prefetch SmallFixedSizeHashMap and
    // is excluded by AggInlineSupported, so it falls back to the general path. The
    // is_no_prefetch_map guard on the with-hash branch is kept so the template stays well-formed.

    // Key-preparation hook for the shared inline loops: bit-compress the key columns into
    // fixed_keys; with hashes they are precomputed densely into hashs.
    InlineKeyView<FixedSizeSliceKey> prepare_inline_keys(const Columns& key_columns, size_t chunk_size,
                                                         bool want_hashes, Buffer<AggDataPtr>* /*scratch*/) {
        InlineKeyView<FixedSizeSliceKey> view;
        _inline_compress_keys(chunk_size, key_columns);
        view.key_base = reinterpret_cast<const uint8_t*>(fixed_keys.data());
        view.key_stride = sizeof(FixedSizeSliceKey);
        if (want_hashes) {
            hashs.resize(chunk_size);
            for (size_t i = 0; i < chunk_size; ++i) hashs[i] = this->hash_map.hash_function()(fixed_keys[i]);
            view.hash_base = reinterpret_cast<const uint8_t*>(hashs.data());
            view.hash_stride = sizeof(size_t);
        }
        return view;
    }

    // build/fold/commit come from InlineAggMapMixin over the hook above (the mixin also owns
    // the is_no_prefetch_map guard the cx1 SmallFixedSizeHashMap instantiation needs).

    void _inline_compress_keys(size_t chunk_size, const Columns& key_columns) {
        auto* buffer = reinterpret_cast<uint8_t*>(fixed_keys.data());
        memset(buffer, 0x0, sizeof(FixedSizeSliceKey) * chunk_size);
        bitcompress_serialize(key_columns, bases, offsets, chunk_size, sizeof(FixedSizeSliceKey), fixed_keys.data());
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key(KeyType key, AggDataPtr& target_state, Func&& allocate_func,
                                    EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace(key, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <AllocFunc<Self> Func, typename EmplaceCallBack>
    ALWAYS_INLINE void _emplace_key_with_hash(KeyType key, size_t hash, AggDataPtr& target_state, Func&& allocate_func,
                                              EmplaceCallBack&& callback) {
        auto iter = this->hash_map.lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
            callback();
            AggDataPtr pv = allocate_func(key);
            ctor(key, pv);
        });
        target_state = iter->second;
    }

    template <typename... Args>
    ALWAYS_INLINE void _find_key(AggDataPtr& target_state, uint8_t& not_found, Args&&... args) {
        if (auto iter = this->hash_map.find(std::forward<Args>(args)...); iter != this->hash_map.end()) {
            target_state = iter->second;
        } else {
            not_found = 1;
        }
    }

    void insert_keys_to_columns(ResultVector& keys, MutableColumns& key_columns, int32_t chunk_size) {
        bitcompress_deserialize(key_columns, bases, offsets, used_bits, chunk_size, sizeof(FixedSizeSliceKey),
                                keys.data());
    }

    static constexpr bool has_single_null_key = false;

    std::vector<int> used_bits;
    std::vector<int> offsets;
    std::vector<std::any> bases;
    std::vector<FixedSizeSliceKey> fixed_keys;
    std::vector<size_t> hashs;
    std::unique_ptr<MemPool> mem_pool;
    ResultVector results;
    int32_t _chunk_size;
};

} // namespace starrocks
