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
#include "exec/join/join_hash_table_descriptor.h"
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
    if (is_asof_join(table_items->join_type)) {
        table_items->resize_asof_index_vector(table_items->row_count + 1);
    }
}

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::construct_hash_table(
        JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
        const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const auto num_rows = 1 + table_items->row_count;
    const uint32_t bucket_size_mask = table_items->bucket_size - 1;
    auto* __restrict next = table_items->next.data();
    auto* __restrict first = table_items->first.data();
    const uint8_t* __restrict equi_join_key_nulls = is_nulls.has_value() ? is_nulls->data() : nullptr;

    auto linear_probe = [&]<bool BuildChained, bool ReturnAnchor>(const ImmBuffer<CppType>& keys_ref, uint32_t i,
                                                                  auto&& is_null_pred) -> uint32_t {
        if (i + 16 < num_rows && !is_null_pred(i + 16)) {
            __builtin_prefetch(first + _get_bucket_num_from_hash(next[i + 16]));
        }

        const uint32_t hash = next[i];
        const uint32_t fp = _get_fp_from_hash(hash);
        uint32_t bucket_num = _get_bucket_num_from_hash(hash);

        uint32_t probe_times = 1;
        while (true) {
            if (first[bucket_num] == 0) {
                if constexpr (BuildChained) {
                    next[i] = 0;
                }
                first[bucket_num] = _combine_data_fp(i, fp);
                break;
            }

            if (fp == _extract_fp(first[bucket_num]) && keys_ref[i] == keys_ref[_extract_data(first[bucket_num])]) {
                if constexpr (BuildChained) {
                    next[i] = _extract_data(first[bucket_num]);
                    first[bucket_num] = _combine_data_fp(i, fp);
                }
                break;
            }

            bucket_num = (bucket_num + probe_times) & bucket_size_mask;
            probe_times++;
        }

        if constexpr (ReturnAnchor) {
            return _extract_data(first[bucket_num]);
        } else {
            return 0u;
        }
    };

    auto compute_hash_values = [&]<bool HasEquiJoinKeyNulls, bool HasAsofTemporalNulls>(
                                       const uint8_t* asof_temporal_nulls) {
        for (uint32_t i = 1; i < num_rows; i++) {
            if constexpr (HasAsofTemporalNulls) {
                if (asof_temporal_nulls[i] != 0) continue;
            }
            if constexpr (std::is_same_v<CppType, Slice> && HasEquiJoinKeyNulls) {
                if (equi_join_key_nulls[i] != 0) continue;
            }

            // Only check `is_nulls_data[i]` for the nullable slice type. The hash calculation overhead for
            // fixed-size types is small, and thus we do not check it to allow vectorization of the hash calculation.
            next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items->bucket_size << FP_BITS,
                                                                  table_items->log_bucket_size + FP_BITS);
        }
    };

    auto dispatch_hash_computation = [&](const uint8_t* asof_temporal_nulls) {
        if (equi_join_key_nulls == nullptr) {
            if (asof_temporal_nulls == nullptr) {
                compute_hash_values.template operator()<false, false>(nullptr);
            } else {
                compute_hash_values.template operator()<false, true>(asof_temporal_nulls);
            }
        } else {
            if (asof_temporal_nulls == nullptr) {
                compute_hash_values.template operator()<true, false>(nullptr);
            } else {
                compute_hash_values.template operator()<true, true>(asof_temporal_nulls);
            }
        }
    };

    auto build_hash_table_without_temporal_index = [&]<bool HasEquiJoinKeyNulls>() {
        dispatch_hash_computation(nullptr);

        auto is_null_row = [&](const uint32_t index) {
            if constexpr (!HasEquiJoinKeyNulls) {
                return false;
            } else {
                return equi_join_key_nulls[index] != 0;
            }
        };

        for (uint32_t i = 1; i < num_rows; i++) {
            if (is_null_row(i)) {
                next[i] = 0;
                continue;
            }
            (void)linear_probe.template operator()<NeedBuildChained, false>(keys, i, is_null_row);
        }

        if constexpr (!NeedBuildChained) {
            table_items->next.clear();
        }
    };

    if (!is_asof_join(table_items->join_type)) {
        if (!is_nulls.has_value()) {
            build_hash_table_without_temporal_index.template operator()<false>();
        } else {
            build_hash_table_without_temporal_index.template operator()<true>();
        }
        return;
    }

    auto build_hash_table_with_temporal_index = [&]() {
        const ColumnPtr& asof_temporal_column =
                table_items->build_chunk->get_column_by_slot_id(table_items->asof_join_condition_desc.build_slot_id);
        const NullColumn* asof_temporal_col_nulls = ColumnHelper::get_null_column(asof_temporal_column);
        const uint8_t* __restrict asof_temporal_nulls = nullptr;
        if (asof_temporal_col_nulls != nullptr) {
            auto* mutable_null_column = const_cast<NullColumn*>(asof_temporal_col_nulls);
            asof_temporal_nulls = mutable_null_column->get_data().data();
        }

        dispatch_hash_computation(asof_temporal_nulls);

        if (equi_join_key_nulls == nullptr) {
            auto equi_join_bucket_locator = [&, num_rows](JoinHashTableItems*, const ImmBuffer<CppType>& keys_ref,
                                                          uint32_t i) {
                return linear_probe.template operator()<false, true>(keys_ref, i, [](uint32_t) { return false; });
            };
            AsofJoinDispatcher::dispatch_and_process(
                    table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr, equi_join_bucket_locator);
        } else {
            auto is_null_predicate = [&](const uint32_t index) { return equi_join_key_nulls[index] != 0; };
            auto equi_join_bucket_locator = [&, num_rows, is_null_predicate](
                                                    JoinHashTableItems*, const ImmBuffer<CppType>& keys_ref, uint32_t i) {
                return linear_probe.template operator()<false, true>(keys_ref, i, is_null_predicate);
            };
            AsofJoinDispatcher::dispatch_and_process(
                    table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr, equi_join_bucket_locator);
        }
        table_items->finalize_asof_index_vector();
    };

    build_hash_table_with_temporal_index();
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
// LinearChainedAsofJoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void LinearChainedAsofJoinHashMap<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->fps.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    table_items->resize_asof_index_vector(table_items->row_count + 1);
}

template <LogicalType LT>
void LinearChainedAsofJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                            const ImmBuffer<CppType>& keys,
                                                            const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint32_t num_rows = table_items->row_count + 1;
    auto* __restrict temp_bucket_numbers = table_items->next.data();
    std::vector<uint8_t> temp_fingerprints(num_rows);

    const uint8_t* __restrict equi_join_key_nulls = is_nulls.has_value() ? is_nulls->data() : nullptr;
    const ColumnPtr& asof_temporal_column =
            table_items->build_chunk->get_column_by_slot_id(table_items->asof_join_condition_desc.build_slot_id);
    const NullColumn* asof_temporal_col_nulls = ColumnHelper::get_null_column(asof_temporal_column);
    const uint8_t* __restrict asof_temporal_nulls =
            asof_temporal_col_nulls ? const_cast<NullColumn*>(asof_temporal_col_nulls)->get_data().data() : nullptr;

    static constexpr uint32_t BATCH_SIZE = 4096;
    auto compute_batch_hash_values = [&]<bool HasEquiJoinKeyNulls, bool HasAsofTemporalNulls>() {
        for (uint32_t i = 1; i < num_rows; i += BATCH_SIZE) {
            const uint32_t batch_count = std::min<uint32_t>(BATCH_SIZE, num_rows - i);
            auto* bucket_buffer = temp_bucket_numbers + i;
            auto* fingerprint_buffer = temp_fingerprints.data() + i;
            for (uint32_t j = 0; j < batch_count; j++) {
                const uint32_t row_index = i + j;
                if constexpr (HasEquiJoinKeyNulls && std::is_same_v<CppType, Slice>) {
                    if (equi_join_key_nulls[row_index] != 0) continue;
                }
                if constexpr (HasAsofTemporalNulls) {
                    if (asof_temporal_nulls[row_index] != 0) continue;
                }
                std::tie(bucket_buffer[j], fingerprint_buffer[j]) = JoinHashMapHelper::calc_bucket_num_and_fp<CppType>(
                        keys[row_index], table_items->bucket_size, table_items->log_bucket_size);
            }
        }
    };

    if (equi_join_key_nulls == nullptr) {
        if (asof_temporal_nulls == nullptr) {
            compute_batch_hash_values.template operator()<false, false>();
        } else {
            compute_batch_hash_values.template operator()<false, true>();
        }
    } else {
        if (asof_temporal_nulls == nullptr) {
            compute_batch_hash_values.template operator()<true, false>();
        } else {
            compute_batch_hash_values.template operator()<true, true>();
        }
    }

    const uint32_t bucket_mask = table_items->bucket_size - 1;
    auto* __restrict bucket_first_indices = table_items->first.data();
    auto* __restrict bucket_fingerprints = table_items->fps.data();

    auto equi_join_bucket_locator = [&](JoinHashTableItems*, const ImmBuffer<CppType>&, uint32_t row_index) -> uint32_t {
        uint32_t bucket_number = temp_bucket_numbers[row_index];
        const uint8_t fingerprint = temp_fingerprints[row_index];
        uint32_t probe_attempts = 1;
        while (true) {
            if (bucket_fingerprints[bucket_number] == 0) {
                bucket_first_indices[bucket_number] = row_index;
                bucket_fingerprints[bucket_number] = fingerprint;
                break;
            }
            if (fingerprint == bucket_fingerprints[bucket_number] &&
                keys[row_index] == keys[bucket_first_indices[bucket_number]]) {
                break;
            }
            bucket_number = (bucket_number + probe_attempts) & bucket_mask;
            probe_attempts++;
        }
        return bucket_first_indices[bucket_number];
    };

    AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr,
                                             equi_join_bucket_locator);
    table_items->finalize_asof_index_vector();
}

template <LogicalType LT>
void LinearChainedAsofJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                   HashTableProbeState* probe_state,
                                                   const ImmBuffer<CppType>& build_keys,
                                                   const ImmBuffer<CppType>& probe_keys,
                                                   const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    auto process = [&]<bool IsNullable>() {
        const uint32_t bucket_size_mask = table_items.bucket_size - 1;
        const uint32_t row_count = probe_state->probe_row_count;

        const auto* firsts = table_items.first.data();
        const auto* fps = table_items.fps.data();
        auto* bucket_nums = probe_state->buckets.data();
        auto* nexts = probe_state->next.data();
        const uint8_t* is_nulls_data = IsNullable && is_nulls.has_value() ? is_nulls->data() : nullptr;

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
                std::tie(bucket_nums[i], nexts[i]) = JoinHashMapHelper::calc_bucket_num_and_fp<CppType>(
                        probe_keys[i], table_items.bucket_size, table_items.log_bucket_size);
            }
        }

        for (uint32_t i = 0; i < row_count; i++) {
            if (is_null(i)) {
                nexts[i] = 0;
                continue;
            }

            const uint8_t fp = nexts[i];
            uint32_t bucket_num = bucket_nums[i];

            uint32_t probe_times = 1;
            while (true) {
                if (fps[bucket_num] == 0) {
                    nexts[i] = 0;
                    break;
                }

                if (fp == fps[bucket_num] && probe_keys[i] == build_keys[firsts[bucket_num]]) {
                    nexts[i] = firsts[bucket_num];
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
    if (is_asof_join(table_items->join_type)) {
        table_items->resize_asof_index_vector(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void DirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
                                                        const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    const auto num_rows = 1 + table_items->row_count;
    if (is_asof_join(table_items->join_type)) {
        auto equi_join_bucket_locator = [](JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
                                           uint32_t row_index) -> uint32_t {
            const size_t bucket_num = keys[row_index] - MIN_VALUE;
            if (table_items->first[bucket_num] == 0) {
                table_items->first[bucket_num] = row_index;
            }
            return table_items->first[bucket_num];
        };

        AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr,
                                                 equi_join_bucket_locator);

        table_items->finalize_asof_index_vector();
    } else {
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
    if (is_asof_join(table_items->join_type)) {
        table_items->resize_asof_index_vector(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                             const ImmBuffer<CppType>& keys,
                                                             const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;

    if (is_asof_join(table_items->join_type)) {
        auto equi_join_bucket_locator = [min_value](JoinHashTableItems* ti, const ImmBuffer<CppType>& k,
                                                    uint32_t row_index) -> uint32_t {
            const uint64_t index = static_cast<uint64_t>(k[row_index] - min_value);
            if (ti->first[index] == 0) {
                ti->first[index] = row_index;
            }
            return ti->first[index];
        };

        AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr,
                                                 equi_join_bucket_locator);
        table_items->finalize_asof_index_vector();
    } else {
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
    if (is_asof_join(table_items->join_type)) {
        table_items->resize_asof_index_vector(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                                  const ImmBuffer<CppType>& keys,
                                                                  const std::optional<ImmBuffer<uint8_t>> is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const uint32_t num_rows = table_items->row_count + 1;

    const bool is_asof_join_type = is_asof_join(table_items->join_type);

    const uint8_t* equi_join_key_nulls_data = is_nulls ? is_nulls->data() : nullptr;
    const bool has_equi_join_key_nulls = (equi_join_key_nulls_data != nullptr);

    const uint8_t* asof_temporal_null_data = nullptr;
    bool has_asof_temporal_nulls = false;

    if (is_asof_join_type) {
        const ColumnPtr& asof_temporal_col =
                table_items->build_chunk->get_column_by_slot_id(table_items->asof_join_condition_desc.build_slot_id);
        const NullColumn* asof_temporal_col_nulls = ColumnHelper::get_null_column(asof_temporal_col);
        if (asof_temporal_col_nulls != nullptr) {
            has_asof_temporal_nulls = true;
            auto* mutable_null_column = const_cast<NullColumn*>(asof_temporal_col_nulls);
            asof_temporal_null_data = mutable_null_column->get_data().data();
        }
    }

    auto get_dense_slot = [min_value](JoinHashTableItems* table_items, const ImmBuffer<CppType>& keys,
                                      uint32_t row) ALWAYS_INLINE {
        const uint32_t bucket_num = keys[row] - min_value;
        const uint32_t group_index = bucket_num / 32;
        const uint32_t index_in_group = bucket_num % 32;

        // Keep the low `index_in_group`-th bits of the bitset to count the number of ones from 0 to index_in_group-1.
        const uint32_t cur_bitset = table_items->dense_groups[group_index].bitset & ((1u << index_in_group) - 1);
        const uint32_t offset_in_group = BitUtil::count_one_bits(cur_bitset);
        return table_items->dense_groups[group_index].start_index + offset_in_group;
    };

    // Initialize `bitset` of each group.
    auto init_group_bitsets = [&]<bool HasNullableKey, bool HasAsofTemporalNulls>() {
        for (uint32_t row = 1; row < num_rows; ++row) {
            if constexpr (HasNullableKey) {
                if (equi_join_key_nulls_data[row] != 0) continue;
            }
            if constexpr (HasAsofTemporalNulls) {
                if (asof_temporal_null_data[row] != 0) continue;
            }
            const uint32_t bucket_num = keys[row] - min_value;
            const uint32_t group_index = bucket_num / 32;
            const uint32_t index_in_group = bucket_num % 32;
            table_items->dense_groups[group_index].bitset |= (1u << index_in_group);
        }
    };

    auto build_hash_chains = [&]<bool HasNullableKey>() {
        // Initialize `first` and `next` arrays by `bitset` and `start_index` of each group.
        for (uint32_t row = 1; row < num_rows; ++row) {
            if constexpr (HasNullableKey) {
                if (equi_join_key_nulls_data[row] != 0) continue;
            }
            const uint32_t index = get_dense_slot(table_items, keys, row);

            table_items->next[row] = table_items->first[index];
            table_items->first[index] = row;
        }
    };

    auto dispatch_bitset_init = [&]<bool HasNullableKey>() {
        if (has_asof_temporal_nulls) {
            init_group_bitsets.template operator()<HasNullableKey, true>();
        } else {
            init_group_bitsets.template operator()<HasNullableKey, false>();
        }
    };

    if (has_equi_join_key_nulls) {
        dispatch_bitset_init.template operator()<true>();
    } else {
        dispatch_bitset_init.template operator()<false>();
    }

    // Calculate `start_index` of each group.
    for (uint32_t start_index = 0; auto& group : table_items->dense_groups) {
        group.start_index = start_index;
        start_index += BitUtil::count_one_bits(group.bitset);
    }

    if (is_asof_join_type) {
        auto equi_join_bucket_locator = [get_dense_slot](JoinHashTableItems* ti, const ImmBuffer<CppType>& k,
                                                         uint32_t row) -> uint32_t {
            const uint32_t index = get_dense_slot(ti, k, row);
            if (ti->first[index] == 0) {
                ti->first[index] = row;
            }
            return ti->first[index];
        };

        AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls.has_value() ? &is_nulls.value() : nullptr,
                                                 equi_join_bucket_locator);
        table_items->finalize_asof_index_vector();
    } else {
        if (!has_equi_join_key_nulls) {
            build_hash_chains.template operator()<false>();
        } else {
            build_hash_chains.template operator()<true>();
        }
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
