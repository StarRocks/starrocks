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
                                               const Buffer<CppType>& keys, const Buffer<uint8_t>* is_nulls) {
    const uint32_t row_count = probe_state->probe_row_count;
    const auto* firsts = table_items.first.data();
    const auto* buckets = probe_state->buckets.data();
    auto* nexts = probe_state->next.data();

    if (is_nulls == nullptr) {
        for (uint32_t i = 0; i < row_count; i++) {
            probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items.bucket_size,
                                                                                  table_items.log_bucket_size);
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
                probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<CppType>(keys[i], table_items.bucket_size,
                                                                                      table_items.log_bucket_size);
            }
        }
        SIMDGather::gather(nexts, firsts, buckets, is_nulls_data, row_count);
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
void DirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                                        const Buffer<uint8_t>* is_nulls) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    const auto num_rows = 1 + table_items->row_count;
    if (is_nulls == nullptr) {
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
                                               const Buffer<CppType>& keys, const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();
    const size_t probe_row_count = probe_state->probe_row_count;

    if (is_nulls == nullptr) {
        for (size_t i = 0; i < probe_row_count; i++) {
            probe_state->next[i] = table_items.first[keys[i] - MIN_VALUE];
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < probe_row_count; i++) {
            if (is_nulls_data[i] == 0) {
                probe_state->next[i] = table_items.first[keys[i] - MIN_VALUE];
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

} // namespace starrocks
