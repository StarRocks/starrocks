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

#include "join_hash_map_method.h"
#include "simd/gather.h"
#include "storage/olap_type_infra.h"

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
void BucketChainedJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                                        const Buffer<uint8_t>* is_nulls) {
    const auto num_rows = 1 + table_items->row_count;

    if (is_nulls == nullptr) {
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
                                               const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                                               const Buffer<uint8_t>* is_nulls) {
    const uint32_t row_count = probe_state->probe_row_count;
    const auto* firsts = table_items.first.data();
    const auto* buckets = probe_state->buckets.data();
    auto* nexts = probe_state->next.data();

    if (is_nulls == nullptr) {
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
    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        table_items->resize_asof_lookup_vectors(table_items->row_count + 1);
    }
}

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::construct_hash_table(JoinHashTableItems* table_items,
                                                                          const Buffer<CppType>& keys,
                                                                          const Buffer<uint8_t>* is_nulls) {
    const auto num_rows = 1 + table_items->row_count;
    const uint32_t bucket_size_mask = table_items->bucket_size - 1;
    auto* __restrict next = table_items->next.data();
    auto* __restrict first = table_items->first.data();

    // Shared linear-probing routine; is_null_pred should return true if row i is null (to skip prefetch)
    // ReturnAnchor controls whether to compute and return the anchor index (only needed for ASOF)
    auto linear_probe = [&]<bool BuildChained, bool ReturnAnchor>(const Buffer<CppType>& keys_ref, uint32_t i,
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


    auto precompute_hashes_generic = [&]<bool HasKeyNulls, bool HasAsofNulls>(const uint8_t* key_nulls,
                                                                              const uint8_t* asof_nulls) {
        for (uint32_t i = 1; i < num_rows; i++) {
            if constexpr (HasAsofNulls) {
                if (asof_nulls[i] != 0) continue;
            }
            if constexpr (std::is_same_v<CppType, Slice> && HasKeyNulls) {
                if (key_nulls[i] != 0) continue;
            }
            next[i] = JoinHashMapHelper::calc_bucket_num<CppType>(
                    keys[i], table_items->bucket_size << FP_BITS, table_items->log_bucket_size + FP_BITS);
        }
    };

    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        const uint8_t* __restrict is_nulls_data = is_nulls ? is_nulls->data() : nullptr;
        const ColumnPtr& asof_temporal_column =
                table_items->build_chunk->get_column_by_slot_id(table_items->asof_join_condition_desc.build_slot_id);
        const NullColumn* nullable_asof_column = ColumnHelper::get_null_column(asof_temporal_column);
        const uint8_t* __restrict asof_nulls_data =
                nullable_asof_column ? nullable_asof_column->get_data().data() : nullptr;

        auto is_null = [&](const uint32_t index) { return is_nulls_data != nullptr && is_nulls_data[index] != 0; };

        if (is_nulls_data == nullptr) {
            if (asof_nulls_data == nullptr) {
                precompute_hashes_generic.template operator()<false, false>(nullptr, nullptr);
            } else {
                precompute_hashes_generic.template operator()<false, true>(nullptr, asof_nulls_data);
            }
        } else {
            if (asof_nulls_data == nullptr) {
                precompute_hashes_generic.template operator()<true, false>(is_nulls_data, nullptr);
            } else {
                precompute_hashes_generic.template operator()<true, true>(is_nulls_data, asof_nulls_data);
            }
        }

        if (is_nulls_data == nullptr) {
            auto asof_anchor_index_no_null = [&, num_rows](JoinHashTableItems*, const Buffer<CppType>& keys_ref,
                                                          uint32_t i) -> uint32_t {
                if (i + 16 < num_rows) {
                    __builtin_prefetch(first + _get_bucket_num_from_hash(next[i + 16]));
                }
                return linear_probe.template operator()<false, true>(keys_ref, i, [](uint32_t) { return false; });
            };
            AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls, asof_anchor_index_no_null);
        } else {
            auto is_null_pred = [&](const uint32_t index) { return is_nulls_data[index] != 0; };
            auto asof_anchor_index_nullable = [&, num_rows, is_null_pred](JoinHashTableItems*,
                                                                         const Buffer<CppType>& keys_ref,
                                                                         uint32_t i) -> uint32_t {
                if (i + 16 < num_rows && !is_null_pred(i + 16)) {
                    __builtin_prefetch(first + _get_bucket_num_from_hash(next[i + 16]));
                }
                return linear_probe.template operator()<false, true>(keys_ref, i, is_null_pred);
            };
            AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls, asof_anchor_index_nullable);
        }
        table_items->finalize_asof_lookup_vectors();
        return;
    }

    auto process = [&]<bool IsNullable>() {
        const uint8_t* __restrict is_nulls_data = IsNullable ? is_nulls->data() : nullptr;

        auto is_null = [&](const uint32_t index) {
            if constexpr (!IsNullable) {
                return false;
            } else {
                return is_nulls_data[index] != 0;
            }
        };

        // Shared precompute for non-ASOF via generic helper
        if constexpr (IsNullable) {
            precompute_hashes_generic.template operator()<true, false>(is_nulls_data, nullptr);
        } else {
            precompute_hashes_generic.template operator()<false, false>(nullptr, nullptr);
        }

        auto is_null_pred = [&](const uint32_t index) {
            if constexpr (!IsNullable) {
                return false;
            } else {
                return is_nulls_data[index] != 0;
            }
        };

        for (uint32_t i = 1; i < num_rows; i++) {
            if (is_null(i)) {
                next[i] = 0;
                continue;
            }
            (void)linear_probe.template operator()<NeedBuildChained, false>(keys, i, is_null_pred);
        }

        if constexpr (!NeedBuildChained) {
            table_items->next.clear();
        }
    };

    if (is_nulls == nullptr) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

template <LogicalType LT, bool NeedBuildChained>
void LinearChainedJoinHashMap<LT, NeedBuildChained>::lookup_init(const JoinHashTableItems& table_items,
                                                                 HashTableProbeState* probe_state,
                                                                 const Buffer<CppType>& build_keys,
                                                                 const Buffer<CppType>& probe_keys,
                                                                 const Buffer<uint8_t>* is_nulls) {
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

    if (is_nulls == nullptr) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

// ------------------------------------------------------------------------------------
// LinearChainedJoinHashMap2
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void LinearChainedJoinHashMap2<LT>::build_prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->fps.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    // if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
    //     auto& asof_buffer = ASOF_BUFFER(table_items);
    //     asof_buffer->resize(table_items->row_count + 1);
    // }
}

template <LogicalType LT>
void LinearChainedJoinHashMap2<LT>::construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                                         const Buffer<uint8_t>* is_nulls) {
    // if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
    //     LogicalType asof_join_build_type = table_items->asof_join_condition_desc.build_logical_type;
    //     auto process_build_rows = [&]<LogicalType ASOF_LT>() {
    //         using ASOF_CppType = RunTimeCppType<ASOF_LT>;
    //
    //         auto& asof_buffer = ASOF_BUFFER(table_items);
    //         if (!table_items->build_chunk) {
    //             CHECK(false) << "ASOF JOIN: build_chunk is null";
    //             return;
    //         }
    //
    //         const ColumnPtr& asof_temporal_column = table_items->build_chunk->get_column_by_slot_id(
    //                 table_items->asof_join_condition_desc.build_slot_id);
    //
    //         if (!asof_temporal_column) {
    //             CHECK(false) << "ASOF JOIN: asof_temporal_column is null for slot_id: "
    //                          << table_items->asof_join_condition_desc.build_slot_id;
    //             return;
    //         }
    //
    //         const auto* typed_column = ColumnHelper::get_data_column_by_type<ASOF_LT>(asof_temporal_column.get());
    //
    //         if (!typed_column) {
    //             CHECK(false) << "ASOF JOIN: typed_column is null for type: " << ASOF_LT;
    //             return;
    //         }
    //
    //         const NullColumn* nullable_asof_column = ColumnHelper::get_null_column(asof_temporal_column);
    //         const ASOF_CppType* probe_temporal_values = typed_column->get_data().data();
    //
    //         if (!probe_temporal_values) {
    //             CHECK(false) << "ASOF JOIN: probe_temporal_values is null";
    //             return;
    //         }
    //
    //         const auto num_rows = 1 + table_items->row_count;
    //         auto* __restrict next = table_items->next.data();
    //         auto* __restrict first = table_items->first.data();
    //         auto* __restrict fps = table_items->fps.data();
    //         const uint8_t* __restrict is_nulls_data = is_nulls ? is_nulls->data() : nullptr;
    //
    //         const uint32_t bucket_size_mask = table_items->bucket_size - 1;
    //         auto is_null = [&](const uint32_t index) { return is_nulls_data[index] != 0; };
    //
    //         auto need_calc_bucket_num = [&](const uint32_t index) {
    //             // Only check `is_nulls_data[i]` for the nullable slice type. The hash calculation overhead for
    //             // fixed-size types is small, and thus we do not check it to allow vectorization of the hash calculation.
    //             if constexpr (!std::is_same_v<CppType, Slice>) {
    //                 return true;
    //             } else {
    //                 return is_nulls_data[index] == 0;
    //             }
    //         };
    //
    //         static constexpr uint32_t BATCH_SIZE = 4096;
    //         uint8_t buffer_fps[BATCH_SIZE];
    //         for (uint64_t i = 1; i < num_rows; i += BATCH_SIZE) {
    //             const uint32_t count = std::min<uint32_t>(BATCH_SIZE, num_rows - i);
    //
    //             auto* buffer_bucket_nums = next + i;
    //             for (uint32_t j = 0; j < count; j++) {
    //                 if (need_calc_bucket_num(i + j)) {
    //                     std::tie(buffer_bucket_nums[j], buffer_fps[j]) =
    //                             JoinHashMapHelper::calc_bucket_num_and_fp<CppType>(
    //                                     keys[i + j], table_items->bucket_size, table_items->log_bucket_size);
    //                 }
    //             }
    //
    //             for (uint32_t j = 0; j < count; j++) {
    //                 if (is_nulls_data != nullptr && is_null(i + j)) {
    //                     continue;
    //                 }
    //
    //                 if (nullable_asof_column && nullable_asof_column->get_data()[i + j] != 0) {
    //                     continue;
    //                 }
    //                 // if (j + 16 < count && !is_null(i + j + 16)) {
    //                 //     __builtin_prefetch(first + buffer_bucket_nums[j + 16]);
    //                 // }
    //
    //                 uint32_t bucket_num = buffer_bucket_nums[j];
    //                 const uint8_t fp = buffer_fps[j];
    //
    //                 uint32_t probe_times = 1;
    //                 while (true) {
    //                     if (fps[bucket_num] == 0) {
    //                         first[bucket_num] = i + j;
    //                         fps[bucket_num] = fp;
    //                         break;
    //                     }
    //
    //                     if (fp == fps[bucket_num] && keys[i + j] == keys[first[bucket_num]]) {
    //                         break;
    //                     }
    //
    //                     bucket_num = (bucket_num + probe_times) & bucket_size_mask;
    //                     probe_times++;
    //                 }
    //
    //                 uint32_t asof_lookup_index = first[bucket_num];
    //
    //                 // 直接使用提取的 buffer，避免 variant 操作
    //                 if (asof_lookup_index >= asof_buffer.size()) {
    //                     asof_buffer.resize(asof_lookup_index + 1);
    //                 }
    //                 if (!asof_buffer[asof_lookup_index]) {
    //                     CREATE_ASOF_VECTOR_BY_INDEX(asof_buffer, table_items->asof_variant_index, asof_lookup_index);
    //                 }
    //
    //                 // 直接调用，零 variant 开销
    //                 asof_buffer[asof_lookup_index]->add_row(probe_temporal_values[i + j], i + j);
    //             }
    //         }
    //     };
    //
    //     switch (asof_join_build_type) {
    //     case TYPE_BIGINT:
    //         process_build_rows.template operator()<TYPE_BIGINT>();
    //         break;
    //     case TYPE_DATE:
    //         process_build_rows.template operator()<TYPE_DATE>();
    //         break;
    //     case TYPE_DATETIME:
    //         process_build_rows.template operator()<TYPE_DATETIME>();
    //         break;
    //     default:
    //         CHECK(false) << "ASOF JOIN: Unsupported build_type: " << asof_join_build_type
    //                      << ". Only TYPE_BIGINT, TYPE_DATE, TYPE_DATETIME are supported.";
    //         __builtin_unreachable();
    //     }
    //
    //     table_items->finalize_asof_lookup_vectors();
    // }
}

template <LogicalType LT>
void LinearChainedJoinHashMap2<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                                const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                                                const Buffer<uint8_t>* is_nulls) {
    auto process = [&]<bool IsNullable>() {
        const uint32_t bucket_size_mask = table_items.bucket_size - 1;
        const uint32_t row_count = probe_state->probe_row_count;

        const auto* firsts = table_items.first.data();
        const auto* fps = table_items.fps.data();
        auto* bucket_nums = probe_state->buckets.data();
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
                std::tie(bucket_nums[i], nexts[i]) = JoinHashMapHelper::calc_bucket_num_and_fp<CppType>(
                        probe_keys[i], table_items.bucket_size, table_items.log_bucket_size);
            }
        }

        for (uint32_t i = 0; i < row_count; i++) {
            // if (i + 16 < row_count && !is_null(i + 16)) {
            //     __builtin_prefetch(firsts + bucket_nums[i + 16]);
            // }

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

    if (is_nulls == nullptr) {
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
    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        table_items->resize_asof_lookup_vectors(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void DirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                                        const Buffer<uint8_t>* is_nulls) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    const auto num_rows = 1 + table_items->row_count;

    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        auto direct_mapping_index_strategy = [](JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                                uint32_t row_index) -> uint32_t {
            const size_t bucket_num = keys[row_index] - MIN_VALUE;
            if (table_items->first[bucket_num] == 0) {
                table_items->first[bucket_num] = row_index;
            }
            return table_items->first[bucket_num];
        };

        AsofJoinDispatcher::dispatch_and_process(table_items, keys, is_nulls, direct_mapping_index_strategy);

        table_items->finalize_asof_lookup_vectors();
    } else if (is_nulls == nullptr) {
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
                                               const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                                               const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();
    const size_t probe_row_count = probe_state->probe_row_count;

    if (is_nulls == nullptr) {
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
    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        table_items->resize_asof_lookup_vectors(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                    HashTableProbeState* probe_state, const Buffer<CppType>& build_keys,
                                                    const Buffer<CppType>& probe_keys,
                                                    const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;

    if (is_nulls == nullptr) {
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
                                                             const Buffer<CppType>& keys,
                                                             const Buffer<uint8_t>* is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;
    if (is_nulls == nullptr) {
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
                                                    HashTableProbeState* probe_state, const Buffer<CppType>& build_keys,
                                                    const Buffer<CppType>& probe_keys,
                                                    const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;
    if (is_nulls == nullptr) {
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
    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        table_items->resize_asof_lookup_vectors(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                                  const Buffer<CppType>& build_keys,
                                                                  const Buffer<uint8_t>* build_key_nulls) {
    const uint64_t key_min_value = table_items->min_value;
    const uint32_t num_rows = table_items->row_count + 1;

    const bool is_asof_join = (table_items->join_type == TJoinOp::ASOF_INNER_JOIN ||
                               table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN);

    const uint8_t* build_key_nulls_data = build_key_nulls ? build_key_nulls->data() : nullptr;
    const bool has_key_nulls = (build_key_nulls_data != nullptr);

    const NullColumn* asof_null_column = nullptr;
    if (is_asof_join) {
        const ColumnPtr& asof_temporal_column =
                table_items->build_chunk->get_column_by_slot_id(table_items->asof_join_condition_desc.build_slot_id);
        asof_null_column = ColumnHelper::get_null_column(asof_temporal_column);
    }

    const bool has_asof_nulls = (asof_null_column != nullptr);
    const uint8_t* asof_null_data = has_asof_nulls ? asof_null_column->get_data().data() : nullptr;

    auto compute_dense_slot = [key_min_value](JoinHashTableItems* ti, const Buffer<CppType>& k,
                                              uint32_t row) ALWAYS_INLINE {
        const uint32_t range_offset = static_cast<uint32_t>(k[row] - key_min_value);
        const uint32_t dense_group_index = range_offset / 32;
        const uint32_t bit_index_in_group = range_offset % 32;
        const uint32_t bitmask_before = ti->dense_groups[dense_group_index].bitset & ((1u << bit_index_in_group) - 1u);
        const uint32_t offset_in_group = BitUtil::count_one_bits(bitmask_before);
        return ti->dense_groups[dense_group_index].start_index + offset_in_group;
    };

    auto run_dense_group_bitset = [&]<bool HasNullableKey, bool HasAsofNulls>() {
        for (uint32_t row = 1; row < num_rows; ++row) {
            if constexpr (HasNullableKey) {
                if (build_key_nulls_data[row] != 0) continue;
            }
            if constexpr (HasAsofNulls) {
                if (asof_null_data[row] != 0) continue;
            }
            const uint32_t range_offset = static_cast<uint32_t>(build_keys[row] - key_min_value);
            const uint32_t dense_group_index = range_offset / 32;
            const uint32_t bit_index_in_group = range_offset % 32;
            table_items->dense_groups[dense_group_index].bitset |= (1u << bit_index_in_group);
        }
    };

    auto run_chained_links = [&]<bool HasNullableKey>() {
        for (uint32_t row = 1; row < num_rows; ++row) {
            if constexpr (HasNullableKey) {
                if (build_key_nulls_data[row] != 0) continue;
            }
            const uint32_t dense_slot = compute_dense_slot(table_items, build_keys, row);
            table_items->next[row] = table_items->first[dense_slot];
            table_items->first[dense_slot] = row;
        }
    };

    auto run_bitset_dispatch = [&]<bool HasNullableKey>() {
        if (has_asof_nulls) {
            run_dense_group_bitset.template operator()<HasNullableKey, true>();
        } else {
            run_dense_group_bitset.template operator()<HasNullableKey, false>();
        }
    };

    if (has_key_nulls) {
        run_bitset_dispatch.template operator()<true>();
    } else {
        run_bitset_dispatch.template operator()<false>();
    }

    {
        for (uint32_t running_prefix = 0; auto& group : table_items->dense_groups) {
            group.start_index = running_prefix;
            running_prefix += BitUtil::count_one_bits(group.bitset);
        }
    }

    if (is_asof_join) {
        auto asof_anchor_index_strategy = [compute_dense_slot](JoinHashTableItems* ti, const Buffer<CppType>& k,
                                                               uint32_t row) -> uint32_t {
            const uint32_t dense_slot = compute_dense_slot(ti, k, row);
            if (ti->first[dense_slot] == 0) {
                ti->first[dense_slot] = row;
            }
            return ti->first[dense_slot];
        };
        AsofJoinDispatcher::dispatch_and_process(table_items, build_keys, build_key_nulls, asof_anchor_index_strategy);
        table_items->finalize_asof_lookup_vectors();
        return;
    }

    if (!has_key_nulls) {
        run_chained_links.template operator()<false>();
    } else {
        run_chained_links.template operator()<true>();
    }
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                         HashTableProbeState* probe_state,
                                                         const Buffer<CppType>& build_keys,
                                                         const Buffer<CppType>& probe_keys,
                                                         const Buffer<uint8_t>* is_nulls) {
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
    if (is_nulls == nullptr) {
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
