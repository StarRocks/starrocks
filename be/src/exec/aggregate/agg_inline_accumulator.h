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

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "base/container/fixed_hash_map.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exprs/agg/aggregate_traits.h"
#include "types/logical_type.h"

namespace starrocks {

using AggDataPtr = uint8_t*;

// ============================ hash-table build-op vocabulary ============================
// Shared by the general (arena-state) build paths in agg_hash_map.h and the inline
// accumulator loops below: which build flavor runs is encoded in compile-time HTBuildOp
// policies, the per-call selection/limit context travels in ExtraAggParam.

struct ExtraAggParam {
    Filter* not_founds = nullptr;
    size_t limits = 0;
};

template <bool allocate_and_compute_state, bool compute_not_founds, bool has_limit>
struct HTBuildOp {
    static auto constexpr allocate = allocate_and_compute_state;
    static auto constexpr fill_not_found = compute_not_founds;
    static auto constexpr process_limit = has_limit;
};

template <bool fill_not_found>
struct FillNotFounds {};

template <>
struct FillNotFounds<false> {
    FillNotFounds(Filter*, int idx) {}
    void operator()() {}
};

template <>
struct FillNotFounds<true> {
    FillNotFounds(Filter* filter, int idx) : _filter(filter), _idx(idx) {}
    void operator()() { (*_filter)[_idx] = 1; }

private:
    Filter* _filter;
    int _idx;
};

// ============================ inline aggregate accumulator ============================
//
// Value specialization of the group-by hash map: for a qualifying aggregate the 8-byte
// map value slot holds the accumulator directly instead of a pointer to an arena-allocated
// state. Clients: count(*) / count(col) (int64 counter), sum over the int family / over
// float+double (int64 / double accumulator), and min/max over the fixed-size whitelist
// (the value type T itself).
//
// An op can be an inline client iff (the client contract):
//   - its state fits the 8-byte slot and is trivially relocatable;
//   - the result of an EXISTING group is never NULL (there is no room for an is-null flag;
//     the nullable wrappers therefore never qualify);
//   - its update decomposes into combine(slot, delta(row)) where the delta is a pure
//     function of at most one input column;
//   - its intermediate type equals its result type (one finalize path serves both the
//     final output and the spill/intermediate drain);
//   - an identity value exists to initialize the slot when a group is created.
// Merge additionally requires that the partial stream is provably NULL-free:
// merge = combine(slot, partial). COUNT satisfies that on any plan stage (its intermediate
// slot is catalog-non-nullable, NULL partials cannot exist). sum/min/max satisfy it on the
// merge stage of a NOT NULL input outside outer-join/repeat contexts: the planner marks the
// producer stage's slots with their real nullability (see PlanFragmentBuilder
// markHonestProducerAggSlotNullability), so the BE resolves the non-nullable variant there;
// a nullable input or context keeps the "nullable ..." resolve and falls back.
//
// The contract is NOT introspectable: AggregateFunction::update_batch is opaque, so ops are
// detected by FE function name and each combine/identity is re-implemented here and proven
// equivalent per op. combine deliberately does NOT call the existing update_batch over slot
// addresses: collecting &slot pointers for a batched call resurrects the mid-chunk
// rehash -> dangling pointer hazard that the fused build removed (see the v1 design notes).
// On output (finalize/serialize) there is no rehash, so delegating to the stock batch_*
// over collected slot addresses stays a legal future option.
//
// The slot is accessed via memcpy, never via an int64_t& aliased onto the AggDataPtr lvalue:
// that is strict-aliasing UB which miscompiles at -O3 (the build uses -Wno-, not
// -fno-strict-aliasing).

template <typename T>
ALWAYS_INLINE T agg_inline_slot_load(const AggDataPtr& slot) {
    static_assert(sizeof(T) <= sizeof(AggDataPtr) && std::is_trivially_copyable_v<T>);
    T v;
    __builtin_memcpy(&v, &slot, sizeof(v));
    return v;
}
template <typename T>
ALWAYS_INLINE void agg_inline_slot_store(AggDataPtr& slot, T v) {
    static_assert(sizeof(T) <= sizeof(AggDataPtr) && std::is_trivially_copyable_v<T>);
    __builtin_memcpy(&slot, &v, sizeof(v));
}

// ================================== op policies ==================================
// One policy per inline client: the slot type, the identity a new group's slot starts
// from, and the combine that folds a delta (an update-row contribution or a merge
// partial) into the slot. combine/identity are hand-proven equivalents of the stock
// AggregateFunction's update/create for the op (see the contract above).
//
// The additive family shares one template: count's accumulator is an int64 += of 0/1
// deltas, sum's is an int64/double += of value deltas, and their merges are the same +=
// of NULL-free partials. int64 overflow wraps exactly like SumAggregateState does on the
// general path.
template <typename T>
struct InlineAddOp {
    using SlotType = T;
    using DeltaType = T;
    static constexpr SlotType identity() { return SlotType{}; }
    // The identity slot image a lazy_emplace ctor stores for a new group. For the additive
    // family this is all-zero bytes (int64 0 / double +0.0); an op with a nonzero identity
    // (min/max) materializes it here.
    ALWAYS_INLINE static AggDataPtr identity_slot() {
        AggDataPtr slot{};
        agg_inline_slot_store<SlotType>(slot, identity());
        return slot;
    }
    ALWAYS_INLINE static void combine(AggDataPtr& slot, DeltaType delta) {
        agg_inline_slot_store<SlotType>(slot, agg_inline_slot_load<SlotType>(slot) + delta);
    }
};
// The whole additive family rides InlineAddOp directly: InlineAddOp<int64_t> serves
// count(*) (delta 1), count(col) (delta !null[i]) and sum(bool/int8..64) -> BIGINT;
// InlineAddOp<double> serves sum(float/double) -> DOUBLE.

// min/max keep the value type T itself in the slot (every whitelisted T fits 8 bytes; the
// identity image writes sizeof(T) bytes over the zeroed slot). combine reuses the exact
// general-path expression -- AggDataTypeTraits<LT>::update_min/max == std::min/max<T> -- so
// the NaN/-0.0 semantics match bit-for-bit on the same row sequence. The identity is
// RunTimeTypeLimits<LT>: lowest()/max() for arithmetic (double: lowest, NOT -inf), the
// MIN/MAX_DATE/TIMESTAMP values for the date types; a group always sees at least one real
// value (the gate rejects nullable inputs), so the identity never leaks into a result.
template <LogicalType LT, bool IsMin>
struct InlineMinMaxOp {
    using T = RunTimeCppType<LT>;
    using SlotType = T;
    using DeltaType = T;
    static SlotType identity() {
        return IsMin ? RunTimeTypeLimits<LT>::max_value() : RunTimeTypeLimits<LT>::min_value();
    }
    ALWAYS_INLINE static AggDataPtr identity_slot() {
        AggDataPtr slot{};
        agg_inline_slot_store<SlotType>(slot, identity());
        return slot;
    }
    ALWAYS_INLINE static void combine(AggDataPtr& slot, DeltaType delta) {
        SlotType cur = agg_inline_slot_load<SlotType>(slot);
        if constexpr (IsMin) {
            AggDataTypeTraits<LT>::update_min(cur, delta);
        } else {
            AggDataTypeTraits<LT>::update_max(cur, delta);
        }
        agg_inline_slot_store<SlotType>(slot, cur);
    }
};

// Slot-level emplace helpers shared by every key class: lazy_emplace the key (a new group's
// slot starts at the op's identity) and combine the delta in.
// `cb` is the emplace callback (limit bookkeeping / not-found marking).
template <typename Op, typename HashMap, typename KeyType, typename Callback>
ALWAYS_INLINE void inline_agg_emplace(HashMap& hash_map, KeyType key, Callback&& cb, typename Op::DeltaType delta) {
    auto iter = hash_map.lazy_emplace(key, [&](const auto& ctor) {
        cb();
        ctor(key, Op::identity_slot());
    });
    Op::combine(iter->second, delta);
}
template <typename Op, typename HashMap, typename KeyType, typename Callback>
ALWAYS_INLINE void inline_agg_emplace_with_hash(HashMap& hash_map, KeyType key, size_t hash, Callback&& cb,
                                                typename Op::DeltaType delta) {
    auto iter = hash_map.lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
        cb();
        ctor(key, Op::identity_slot());
    });
    Op::combine(iter->second, delta);
}

// Find-or-miss over the slot: an existing group combines the delta straight into its slot
// (an identity delta makes this a pure classifying probe: combine(slot, identity) == slot
// for every op), a missing key marks not_found for streaming.
template <typename Op, typename HashMap, typename KeyT>
ALWAYS_INLINE void inline_agg_find(HashMap& hash_map, const KeyT& key, uint8_t& not_found,
                                   typename Op::DeltaType delta) {
    if (auto iter = hash_map.find(key); iter != hash_map.end()) {
        Op::combine(iter->second, delta);
    } else {
        not_found = 1;
    }
}
template <typename Op, typename HashMap, typename KeyT>
ALWAYS_INLINE void inline_agg_find_with_hash(HashMap& hash_map, const KeyT& key, size_t hash, uint8_t& not_found,
                                             typename Op::DeltaType delta) {
    if (auto iter = hash_map.find(key, hash); iter != hash_map.end()) {
        Op::combine(iter->second, delta);
    } else {
        not_found = 1;
    }
}
// Probing find (selective classify build): mark a miss, count nothing, store nothing.
template <typename HashMap, typename KeyT>
ALWAYS_INLINE void inline_agg_probe(HashMap& hash_map, const KeyT& key, uint8_t& not_found) {
    if (hash_map.find(key) == hash_map.end()) not_found = 1;
}
template <typename HashMap, typename KeyT>
ALWAYS_INLINE void inline_agg_probe_with_hash(HashMap& hash_map, const KeyT& key, size_t hash, uint8_t& not_found) {
    if (hash_map.find(key, hash) == hash_map.end()) not_found = 1;
}

// A one-shot, strided view over the chunk's prepared keys (and optionally their hashes),
// produced by each map class's key-preparation hook. Strides are in bytes: the serialized
// fixed-size key class interleaves {key, hash} cache entries, so neither array is
// necessarily dense. The view must not outlive the loop call it was prepared for: fold and
// commit passes re-prepare the keys instead of caching a view across build/compute (the
// backing storage is per-chunk scratch, and nothing pointer-like may survive a rehash).
template <typename KeyT>
struct InlineKeyView {
    const uint8_t* key_base = nullptr;
    size_t key_stride = 0;
    const uint8_t* hash_base = nullptr; // null when the loop runs without prefetch
    size_t hash_stride = 0;

    ALWAYS_INLINE const KeyT& key_at(size_t i) const {
        return *reinterpret_cast<const KeyT*>(key_base + i * key_stride);
    }
    ALWAYS_INLINE size_t hash_at(size_t i) const {
        return *reinterpret_cast<const size_t*>(hash_base + i * hash_stride);
    }
    bool has_hashes() const { return hash_base != nullptr; }
};

// ================================ shared inline loops ================================
// One definition per pass, shared by every fixed-size key class (single numeric,
// serialized fixed-size, compressed fixed-size). WithHash is a compile-time switch so the
// prefetching and plain flavors stay separate instantiations, exactly like the dedicated
// v1 loops they replace; the caller decides it from its prefetch policy and prepares the
// view accordingly. `prefetch_dist` is read once by the caller (0 = disabled by config).

// Build pass: drive one HTBuildOp flavor over the prepared keys. `delta` is what an
// emplaced/found row adds: 1 for the fused count update; the op identity (a no-op combine)
// when a creating build only classifies-and-creates and the real fold runs later in compute
// (merge partials and every per-row-delta op -- see the aggregator dispatch).
template <typename Op, typename HTBuildOp, bool WithHash, typename HashMap, typename KeyT>
ALWAYS_NOINLINE void agg_inline_build_keys(HashMap& hash_map, const InlineKeyView<KeyT>& view, size_t num_rows,
                                           ExtraAggParam* extra, typename Op::DeltaType delta, size_t prefetch_dist) {
    [[maybe_unused]] size_t hash_table_size = hash_map.size();
    auto* __restrict not_founds = extra->not_founds;
    [[maybe_unused]] size_t prefetch_index = prefetch_dist;
    for (size_t i = 0; i < num_rows; ++i) {
        if constexpr (WithHash) {
            if (prefetch_dist != 0 && prefetch_index < num_rows) {
                hash_map.prefetch_hash(view.hash_at(prefetch_index++));
            }
        }
        const KeyT& key = view.key_at(i);
        if constexpr (HTBuildOp::process_limit) {
            if (hash_table_size < extra->limits) {
                if constexpr (WithHash) {
                    inline_agg_emplace_with_hash<Op>(
                            hash_map, key, view.hash_at(i), [&] { hash_table_size++; }, delta);
                } else {
                    inline_agg_emplace<Op>(
                            hash_map, key, [&] { hash_table_size++; }, delta);
                }
            } else {
                if constexpr (WithHash) {
                    inline_agg_find_with_hash<Op>(hash_map, key, view.hash_at(i), (*not_founds)[i], delta);
                } else {
                    inline_agg_find<Op>(hash_map, key, (*not_founds)[i], delta);
                }
            }
        } else if constexpr (HTBuildOp::allocate) {
            if constexpr (WithHash) {
                inline_agg_emplace_with_hash<Op>(hash_map, key, view.hash_at(i),
                                                 FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i), delta);
            } else {
                inline_agg_emplace<Op>(hash_map, key, FillNotFounds<HTBuildOp::fill_not_found>(not_founds, i), delta);
            }
        } else if constexpr (HTBuildOp::fill_not_found) {
            // Selective build is a speculative classifier, not a commit: it only marks misses
            // (new keys) for streaming and never counts. The kept rows are committed/folded later
            // by re-probing the keys, so nothing fragile is stashed across the streaming-sink
            // decision (stream / force-preagg / spill) that follows.
            if constexpr (WithHash) {
                inline_agg_probe_with_hash(hash_map, key, view.hash_at(i), (*not_founds)[i]);
            } else {
                inline_agg_probe(hash_map, key, (*not_founds)[i]);
            }
        }
    }
}

// Merge fold pass: slot += partials[i] for each kept row, lazy-emplacing missing keys.
// `selection` is the streamed-rows mask (selection[i] != 0 rows leave unchanged and must not
// fold); null folds every row.
template <typename Op, bool WithHash, typename HashMap, typename KeyT>
ALWAYS_NOINLINE void agg_inline_fold_keys(HashMap& hash_map, const InlineKeyView<KeyT>& view, size_t num_rows,
                                          const typename Op::DeltaType* partials, const Filter* selection,
                                          size_t prefetch_dist) {
    [[maybe_unused]] size_t prefetch_index = prefetch_dist;
    for (size_t i = 0; i < num_rows; ++i) {
        if constexpr (WithHash) {
            if (prefetch_dist != 0 && prefetch_index < num_rows) {
                hash_map.prefetch_hash(view.hash_at(prefetch_index++));
            }
        }
        if (selection != nullptr && (*selection)[i] != 0) continue;
        if constexpr (WithHash) {
            inline_agg_emplace_with_hash<Op>(
                    hash_map, view.key_at(i), view.hash_at(i), [] {}, partials[i]);
        } else {
            inline_agg_emplace<Op>(
                    hash_map, view.key_at(i), [] {}, partials[i]);
        }
    }
}

// Commit pass for the selective classify build: re-probe each kept row (selection[i] == 0, or
// all rows when selection is null) and add +1 to its slot. Re-looking-up the key instead of
// replaying a stashed slot address keeps the count valid even if the hash table was rehashed
// after the classifying build.
template <typename Op, typename HashMap, typename KeyT>
ALWAYS_NOINLINE void agg_inline_commit_keys(HashMap& hash_map, const InlineKeyView<KeyT>& view, size_t num_rows,
                                            const Filter* selection) {
    uint8_t ignore_not_found = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        if (selection != nullptr && (*selection)[i] != 0) continue;
        inline_agg_find<Op>(hash_map, view.key_at(i), ignore_not_found, 1);
    }
}

// ===================== map-side integration mixin ==========================
// CRTP base the map structures inherit for the inline-agg entry points. A map
// flavor supplies exactly one hook:
//     prepare_inline_keys(keys, n, want_hashes, scratch) -> InlineKeyView<KeyT>
// where `keys` is whatever the flavor consumes (the typed column pointer for a
// single numeric key, the key-column vector for the multi-column flavors) and
// `scratch` is the caller-offered hash buffer the flavor may use or ignore.
// The mixin owns the prefetch decision and drives the shared loops above, so a
// new key flavor implements the hook and inherits build/fold/commit unchanged.
// The nullable single-key wrapper shadows the public entry points with its
// NULL plumbing and calls the *_dense cores for the non-null rows.
template <typename Derived>
struct InlineAggMapMixin {
    template <typename Op, typename HTBuildOp, typename KeysArg>
    ALWAYS_NOINLINE void inline_agg_build_dense(const KeysArg& keys, size_t n, Buffer<AggDataPtr>* scratch,
                                                ExtraAggParam* extra, typename Op::DeltaType delta) {
        auto* self = static_cast<Derived*>(this);
        auto& map = self->hash_map;
        if constexpr (is_no_prefetch_map<std::decay_t<decltype(map)>>) {
            agg_inline_build_keys<Op, HTBuildOp, false>(map, self->prepare_inline_keys(keys, n, false, scratch), n,
                                                        extra, delta, 0);
        } else if (!agg_should_prefetch_table(map)) {
            agg_inline_build_keys<Op, HTBuildOp, false>(map, self->prepare_inline_keys(keys, n, false, scratch), n,
                                                        extra, delta, 0);
        } else {
            agg_inline_build_keys<Op, HTBuildOp, true>(map, self->prepare_inline_keys(keys, n, true, scratch), n, extra,
                                                       delta, agg_hash_map_default_prefetch_dist());
        }
    }

    template <typename Op, typename KeysArg>
    ALWAYS_NOINLINE void inline_agg_fold_dense(const KeysArg& keys, size_t n, Buffer<AggDataPtr>* scratch,
                                               const typename Op::DeltaType* partials, const Filter* selection) {
        auto* self = static_cast<Derived*>(this);
        auto& map = self->hash_map;
        if constexpr (is_no_prefetch_map<std::decay_t<decltype(map)>>) {
            agg_inline_fold_keys<Op, false>(map, self->prepare_inline_keys(keys, n, false, scratch), n, partials,
                                            selection, 0);
        } else if (!agg_should_prefetch_table(map)) {
            agg_inline_fold_keys<Op, false>(map, self->prepare_inline_keys(keys, n, false, scratch), n, partials,
                                            selection, 0);
        } else {
            agg_inline_fold_keys<Op, true>(map, self->prepare_inline_keys(keys, n, true, scratch), n, partials,
                                           selection, agg_hash_map_default_prefetch_dist());
        }
    }

    template <typename Op, typename KeysArg>
    void inline_agg_commit_dense(const KeysArg& keys, size_t n, const Filter* selection) {
        auto* self = static_cast<Derived*>(this);
        agg_inline_commit_keys<Op>(self->hash_map, self->prepare_inline_keys(keys, n, false, nullptr), n, selection);
    }

    // Public entry points for the flavors whose key columns go straight to the hook (the
    // multi-column fixed-size maps). `agg_states` is forwarded as the hash scratch offer.
    template <typename Op, typename HTBuildOp>
    void build_inline_agg(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* agg_states,
                          ExtraAggParam* extra, typename Op::DeltaType delta) {
        inline_agg_build_dense<Op, HTBuildOp>(key_columns, chunk_size, agg_states, extra, delta);
    }
    template <typename Op>
    void build_inline_agg_fold(size_t chunk_size, const Columns& key_columns, Buffer<AggDataPtr>* agg_states,
                               const typename Op::DeltaType* partials, const Filter* selection = nullptr) {
        inline_agg_fold_dense<Op>(key_columns, chunk_size, agg_states, partials, selection);
    }
    template <typename Op>
    void commit_inline_agg(size_t chunk_size, const Columns& key_columns, const Filter* selection) {
        inline_agg_commit_dense<Op>(key_columns, chunk_size, selection);
    }
};

} // namespace starrocks
