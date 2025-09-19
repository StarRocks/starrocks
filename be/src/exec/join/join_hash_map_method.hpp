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

#include "exec/join/join_hash_map_helper.h"
#include "exec/join/join_hash_map_method.h"
#include "simd/gather.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// BucketChainedJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void BucketChainedJoinHashMap<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
void BucketChainedJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
                                                        const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const auto num_rows = 1 + table_items->row_count;

    if (!is_nulls.has_value()) {
        auto* __restrict next = table_items->next.data();
        for (uint32_t i = 1; i < num_rows; i++) {
            // Use `next` stores `bucket_num` temporarily.
            next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items->bucket_size,
                                                                  table_items->log_bucket_size);
        }

        auto* __restrict first = table_items->first.data();
        for (uint32_t i = 1; i < num_rows; i++) {
            const uint32_t bucket_num = next[i];
            next[i] = first[bucket_num];
            first[bucket_num] = i;
        }
    } else {
        const auto* __restrict is_nulls_data = is_nulls->data();
        auto need_calc_bucket_num = [&](const uint32_t index) {
            if constexpr (!std::is_same_v<CppType, Slice>) {
                return true;
            } else {
                return is_nulls_data[index] == 0;
            }
        };

        auto* __restrict next = table_items->next.data();
        for (uint32_t i = 0; i < num_rows; i++) {
            // Use `next` stores `bucket_num` temporarily.
            if (need_calc_bucket_num(i)) {
                next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items->bucket_size,
                                                                      table_items->log_bucket_size);
            }
        }

        auto* __restrict first = table_items->first.data();
        for (uint32_t i = 0; i < num_rows; i++) {
            if (is_nulls_data[i] == 0) {
                const uint32_t bucket_num = next[i];
                next[i] = first[bucket_num];
                first[bucket_num] = i;
            } else {
                next[i] = 0;
            }
        }
    }
}

template <LogicalType LT>
void BucketChainedJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                               const ImmBuffer<CppType>& build_keys,
                                               const ImmBuffer<CppType>& probe_keys,
                                               const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint32_t row_count = probe_state->probe_row_count;
    const auto* firsts = table_items.first.data();
    const auto* buckets = probe_state->buckets.data();
    auto* nexts = probe_state->next.data();

    if (!is_nulls.has_value()) {
        for (uint32_t i = 0; i < row_count; i++) {
            probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<CppType>(
                    probe_keys[i], table_items.bucket_size, table_items.log_bucket_size);
        }
        SIMDGather::gather(nexts, firsts, buckets, row_count);
    } else {
        const auto* is_nulls_data = is_nulls->data();
        auto need_calc_bucket_num = [&](const uint32_t index) {
            if constexpr (!std::is_same_v<CppType, Slice>) {
                return true;
            } else {
                return is_nulls_data[index] == 0;
            }
        };
        for (uint32_t i = 0; i < row_count; i++) {
            if (need_calc_bucket_num(i)) {
                probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<CppType>(
                        probe_keys[i], table_items.bucket_size, table_items.log_bucket_size);
            }
        }
        SIMDGather::gather(nexts, firsts, buckets, is_nulls_data, row_count);
    }
}

// ------------------------------------------------------------------------------------
// LinearChainedJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::build_prepare(RuntimeState* state,
                                                                   JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::construct_hash_table(
        JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
        const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    auto process = [&]<bool IsNullable>() {
        const auto num_rows = 1 + table_items->row_count;
        const uint32_t bucket_size_mask = table_items->bucket_size - 1;

        auto* __restrict next = table_items->next.data();
        auto* __restrict first = table_items->first.data();
        const uint8_t* __restrict is_nulls_data = IsNullable ? is_nulls->data() : nullptr;

        auto need_calc_bucket_num = [&](const uint32_t index) {
            // Only check `is_nulls_data[i]` for the nullable slice type. The hash calculation overhead for
            // fixed-size types is small, and thus we do not check it to allow vectorization of the hash calculation.
            if constexpr (!IsNullable || !std::is_same_v<CppType, Slice>) {
                return true;
            } else {
                return is_nulls_data[index] == 0;
            }
        };
        auto is_null = [&](const uint32_t index) {
            if constexpr (!IsNullable) {
                return false;
            } else {
                return is_nulls_data[index] != 0;
            }
        };

        for (uint32_t i = 1; i < num_rows; i++) {
            // Use `next` stores `bucket_num` temporarily.
            if (need_calc_bucket_num(i)) {
                next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items->bucket_size << FP_BITS,
                                                                      table_items->log_bucket_size + FP_BITS);
            }
        }

        for (uint32_t i = 1; i < num_rows; i++) {
            if (i + 16 < num_rows && !is_null(i + 16)) {
                __builtin_prefetch(first + _get_bucket_num_from_hash(next[i + 16]));
            }

            if (is_null(i)) {
                next[i] = 0;
                continue;
            }

            const uint32_t hash = next[i];
            const uint32_t fp = _get_fp_from_hash(hash);
            uint32_t bucket_num = _get_bucket_num_from_hash(hash);

            uint32_t probe_times = 1;
            while (true) {
                if (first[bucket_num] == 0) {
                    if constexpr (NeedBuildChained) {
                        next[i] = 0;
                    }
                    first[bucket_num] = _combine_data_fp(i, fp);
                    break;
                }

                if (fp == _extract_fp(first[bucket_num]) && keys[i] == keys[_extract_data(first[bucket_num])]) {
                    if constexpr (NeedBuildChained) {
                        next[i] = _extract_data(first[bucket_num]);
                        first[bucket_num] = _combine_data_fp(i, fp);
                    }
                    break;
                }

                bucket_num = (bucket_num + probe_times) & bucket_size_mask;
                probe_times++;
            }
        }

        if constexpr (!NeedBuildChained) {
            table_items->next.clear();
        }
    };

    if (!is_nulls.has_value()) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::lookup_init(const JoinHashTableItems& table_items,
                                                                 HashTableProbeState* probe_state,
                                                                 const ImmBuffer<CppType>& build_keys,
                                                                 const ImmBuffer<CppType>& probe_keys,
                                                                 const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    auto process = [&]<bool IsNullable>() {
        const uint32_t bucket_size_mask = table_items.bucket_size - 1;
        const uint32_t row_count = probe_state->probe_row_count;

        const auto* firsts = table_items.first.data();
        auto* hashes = probe_state->buckets.data();
        auto* nexts = probe_state->next.data();
        const uint8_t* is_nulls_data = IsNullable ? is_nulls->data() : nullptr;

        auto need_calc_bucket_num = [&](const uint32_t index) {
            if constexpr (!IsNullable || !std::is_same_v<CppType, Slice>) {
                // Only check `is_nulls_data[i]` for the nullable slice type. The hash calculation overhead for
                // fixed-size types is small, and thus we do not check it to allow vectorization of the hash calculation.
                return true;
            } else {
                return is_nulls_data[index] == 0;
            }
        };
        auto is_null = [&](const uint32_t index) {
            if constexpr (!IsNullable) {
                return false;
            } else {
                return is_nulls_data[index] != 0;
            }
        };

        for (uint32_t i = 0; i < row_count; i++) {
            if (need_calc_bucket_num(i)) {
                hashes[i] = JoinHashMapHelper::calc_bucket_num<CppType>(
                        probe_keys[i], table_items.bucket_size << FP_BITS, table_items.log_bucket_size + FP_BITS);
            }
        }

        for (uint32_t i = 0; i < row_count; i++) {
            if (i + 16 < row_count && !is_null(i + 16)) {
                __builtin_prefetch(firsts + _get_bucket_num_from_hash(hashes[i + 16]));
            }

            if (is_null(i)) {
                nexts[i] = 0;
                continue;
            }

            const uint32_t hash = hashes[i];
            const uint32_t fp = _get_fp_from_hash(hash);
            uint32_t bucket_num = _get_bucket_num_from_hash(hash);

            uint32_t probe_times = 1;
            while (true) {
                if (firsts[bucket_num] == 0) {
                    nexts[i] = 0;
                    break;
                }

                const uint32_t cur_fp = _extract_fp(firsts[bucket_num]);
                const uint32_t cur_index = _extract_data(firsts[bucket_num]);
                if (fp == cur_fp && probe_keys[i] == build_keys[cur_index]) {
                    if constexpr (NeedBuildChained) {
                        nexts[i] = cur_index;
                    } else {
                        nexts[i] = 1;
                    }
                    break;
                }

                bucket_num = (bucket_num + probe_times) & bucket_size_mask;
                probe_times++;
            }
        }
    };

    if (!is_nulls.has_value()) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

// ------------------------------------------------------------------------------------
// DirectMappingJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void DirectMappingJoinHashMap<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    static constexpr size_t BUCKET_SIZE = static_cast<int64_t>(RunTimeTypeLimits<LT>::max_value()) -
                                          static_cast<int64_t>(RunTimeTypeLimits<LT>::min_value()) + 1L;
    table_items->bucket_size = BUCKET_SIZE;
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
void DirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
                                                        const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    const auto num_rows = 1 + table_items->row_count;
    if (!is_nulls.has_value()) {
        for (uint32_t i = 1; i < num_rows; i++) {
            const size_t bucket_num = keys[i] - MIN_VALUE;
            table_items->next[i] = table_items->first[bucket_num];
            table_items->first[bucket_num] = i;
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (uint32_t i = 1; i < num_rows; i++) {
            if (is_nulls_data[i] == 0) {
                const size_t bucket_num = keys[i] - MIN_VALUE;
                table_items->next[i] = table_items->first[bucket_num];
                table_items->first[bucket_num] = i;
            }
        }
    }
}

template <LogicalType LT>
void DirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                               const ImmBuffer<CppType>& build_keys,
                                               const ImmBuffer<CppType>& probe_keys,
                                               const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();
    const size_t probe_row_count = probe_state->probe_row_count;

    if (!is_nulls.has_value()) {
        for (size_t i = 0; i < probe_row_count; i++) {
            probe_state->next[i] = table_items.first[probe_keys[i] - MIN_VALUE];
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < probe_row_count; i++) {
            if (is_nulls_data[i] == 0) {
                probe_state->next[i] = table_items.first[probe_keys[i] - MIN_VALUE];
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

// ------------------------------------------------------------------------------------
// RangeDirectMappingJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    const uint64_t value_interval = static_cast<uint64_t>(table_items->max_value) - table_items->min_value + 1L;
    table_items->bucket_size = value_interval;
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                             const ImmBuffer<CppType>& keys,
                                                             const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;
    if (!is_nulls.has_value()) {
        for (uint32_t i = 1; i < num_rows; i++) {
            const size_t bucket_num = keys[i] - min_value;
            table_items->next[i] = table_items->first[bucket_num];
            table_items->first[bucket_num] = i;
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (uint32_t i = 1; i < num_rows; i++) {
            if (is_nulls_data[i] == 0) {
                const size_t bucket_num = keys[i] - min_value;
                table_items->next[i] = table_items->first[bucket_num];
                table_items->first[bucket_num] = i;
            }
        }
    }
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                    HashTableProbeState* probe_state,
                                                    const ImmBuffer<CppType>& build_keys,
                                                    const ImmBuffer<CppType>& probe_keys,
                                                    const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;
    if (!is_nulls.has_value()) {
        for (size_t i = 0; i < num_rows; i++) {
            if ((probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t index = probe_keys[i] - min_value;
                probe_state->next[i] = table_items.first[index];
            } else {
                probe_state->next[i] = 0;
            }
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < num_rows; i++) {
            if ((is_nulls_data[i] == 0) & (probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t index = probe_keys[i] - min_value;
                probe_state->next[i] = table_items.first[index];
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

// ------------------------------------------------------------------------------------
// RangeDirectMappingJoinHashSet
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void RangeDirectMappingJoinHashSet<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    const uint64_t value_interval = static_cast<uint64_t>(table_items->max_value) - table_items->min_value + 1L;
    table_items->bucket_size = (value_interval + 7) / 8;
    table_items->key_bitset.resize(table_items->bucket_size, 0);
}

template <LogicalType LT>
void RangeDirectMappingJoinHashSet<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                             const ImmBuffer<CppType>& keys,
                                                             const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;
    if (!is_nulls.has_value()) {
        for (uint32_t i = 1; i < num_rows; i++) {
            const uint64_t bucket = keys[i] - min_value;
            const uint32_t group = bucket / 8;
            const uint32_t offset = bucket % 8;
            table_items->key_bitset[group] |= 1 << offset;
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (uint32_t i = 1; i < num_rows; i++) {
            const uint64_t bucket = keys[i] - min_value;
            const uint32_t group = bucket / 8;
            const uint32_t offset = bucket % 8;
            table_items->key_bitset[group] |= (is_nulls_data[i] == 0) << offset;
        }
    }
}

template <LogicalType LT>
void RangeDirectMappingJoinHashSet<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                    HashTableProbeState* probe_state,
                                                    const ImmBuffer<CppType>& build_keys,
                                                    const ImmBuffer<CppType>& probe_keys,
                                                    const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;
    if (!is_nulls.has_value()) {
        for (size_t i = 0; i < num_rows; i++) {
            if ((probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t index = probe_keys[i] - min_value;
                const uint32_t group = index / 8;
                const uint32_t offset = index % 8;
                probe_state->next[i] = (table_items.key_bitset[group] & (1 << offset)) != 0;
            } else {
                probe_state->next[i] = 0;
            }
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < num_rows; i++) {
            if ((is_nulls_data[i] == 0) & (probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t index = probe_keys[i] - min_value;
                const uint32_t group = index / 8;
                const uint32_t offset = index % 8;
                probe_state->next[i] = (table_items.key_bitset[group] & (1 << offset)) != 0;
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

// ------------------------------------------------------------------------------------
// DenseRangeDirectMappingJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    const uint64_t value_interval = static_cast<uint64_t>(table_items->max_value) - table_items->min_value + 1L;
    table_items->bucket_size = table_items->row_count + 1;
    table_items->dense_groups.resize((value_interval + 31) / 32);
    table_items->first.resize(table_items->row_count + 1, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                                  const ImmBuffer<CppType>& keys,
                                                                  const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;

    const uint8_t* is_nulls_data = !is_nulls.has_value() ? nullptr : is_nulls->data();
    auto is_null = [&]<bool Nullable>(const uint32_t index) {
        if constexpr (Nullable) {
            return is_nulls_data[index] != 0;
        } else {
            return false;
        }
    };

    auto process = [&]<bool Nullable>() {
        // Initialize `bitset` of each group.
        for (uint32_t i = 1; i < num_rows; i++) {
            if (!is_null.template operator()<Nullable>(i)) {
                const uint32_t bucket_num = keys[i] - min_value;
                const uint32_t group_index = bucket_num / 32;
                const uint32_t index_in_group = bucket_num % 32;
                table_items->dense_groups[group_index].bitset |= 1 << index_in_group;
            }
        }

        // Calculate `start_index` of each group.
        for (uint32_t start_index = 0; auto& group : table_items->dense_groups) {
            group.start_index = start_index;
            start_index += BitUtil::count_one_bits(group.bitset);
        }

        // Initialize `first` and `next` arrays by `bitset` and `start_index` of each group.
        for (size_t i = 1; i < num_rows; i++) {
            if (!is_null.template operator()<Nullable>(i)) {
                const uint32_t bucket_num = keys[i] - min_value;
                const uint32_t group_index = bucket_num / 32;
                const uint32_t index_in_group = bucket_num % 32;

                // Keep the low `index_in_group`-th bits of the bitset to count the number of ones from 0 to index_in_group-1.
                const uint32_t cur_bitset = table_items->dense_groups[group_index].bitset & ((1 << index_in_group) - 1);
                const uint32_t offset_in_group = BitUtil::count_one_bits(cur_bitset);
                const uint32_t index = table_items->dense_groups[group_index].start_index + offset_in_group;

                table_items->next[i] = table_items->first[index];
                table_items->first[index] = i;
            }
        }
    };

    if (!is_nulls.has_value()) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                         HashTableProbeState* probe_state,
                                                         const ImmBuffer<CppType>& build_keys,
                                                         const ImmBuffer<CppType>& probe_keys,
                                                         const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;

    auto get_dense_first = [&](const uint64_t bucket_num) -> uint32_t {
        const uint32_t group_index = bucket_num / 32;
        auto [start_index, bitset] = table_items.dense_groups[group_index];

        if (bitset == 0) {
            return 0;
        }

        const uint32_t index_in_group = bucket_num % 32;
        if ((bitset & (1 << index_in_group)) == 0) {
            return 0;
        }

        bitset &= (1 << index_in_group) - 1;
        const uint32_t offset_in_group = BitUtil::count_one_bits(bitset);
        return table_items.first[start_index + offset_in_group];
    };

    const size_t num_rows = probe_state->probe_row_count;
    if (!is_nulls.has_value()) {
        for (size_t i = 0; i < num_rows; i++) {
            if ((probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t bucket_num = probe_keys[i] - min_value;
                probe_state->next[i] = get_dense_first(bucket_num);
            } else {
                probe_state->next[i] = 0;
            }
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < num_rows; i++) {
            if ((is_nulls_data[i] == 0) & (probe_keys[i] >= min_value) & (probe_keys[i] <= max_value)) {
                const uint64_t bucket_num = probe_keys[i] - min_value;
                probe_state->next[i] = get_dense_first(bucket_num);
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

} // namespace starrocks
