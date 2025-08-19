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
        table_items->asof_lookup_vectors.resize(table_items->row_count + 1);
    }
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::construct_hash_table(JoinHashTableItems* table_items,
                                                             const Buffer<CppType>& keys,
                                                             const Buffer<uint8_t>* is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;
    
    LOG(INFO) << "=== RangeDirectMappingJoinHashMap::construct_hash_table ===";
    LOG(INFO) << "min_value: " << min_value;
    LOG(INFO) << "max_value: " << table_items->max_value;
    LOG(INFO) << "num_rows: " << num_rows;
    LOG(INFO) << "bucket_size: " << table_items->bucket_size;
    LOG(INFO) << "join_type: " << table_items->join_type;
    LOG(INFO) << "is_nulls: " << (is_nulls ? "not null" : "null");

    if (table_items->join_type == TJoinOp::ASOF_INNER_JOIN || table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        LogicalType asof_join_build_type = table_items->asof_join_condition_desc.build_logical_type;
        LOG(INFO) << "Processing ASOF_INNER_JOIN with build_type: " << asof_join_build_type;

        auto process_build_rows = [&]<LogicalType ASOF_LT>() {
            using CppType = RunTimeCppType<ASOF_LT>;

            const ColumnPtr& asof_temporal_column = table_items->build_chunk->get_column_by_slot_id(
                    table_items->asof_join_condition_desc.build_slot_id);
            const auto* typed_column = ColumnHelper::get_data_column_by_type<ASOF_LT>(asof_temporal_column.get());
            const NullColumn* nullable_asof_column = ColumnHelper::get_null_column(asof_temporal_column);
            const CppType* probe_temporal_values = typed_column->get_data().data();

            for (uint32_t i = 1; i < num_rows; i++) {
                if (is_nulls != nullptr && is_nulls->data()[i] != 0) {
                    continue;
                }

                if (nullable_asof_column && nullable_asof_column->get_data()[i] != 0) {
                    continue;
                }

                const size_t bucket_num = keys[i] - min_value;
                if (table_items->first[bucket_num] == 0) {
                    table_items->first[bucket_num] = i;
                    LOG(INFO) << "Setting first[" << bucket_num << "] = " << i;
                }

                uint32_t asof_lookup_index = table_items->first[bucket_num];
                LOG(INFO) << "Processing build row " << i << ", bucket_num=" << bucket_num 
                          << ", asof_lookup_index=" << asof_lookup_index;

                if (!table_items->asof_lookup_vectors[asof_lookup_index]) {
                    LOG(INFO) << "Creating new asof_lookup_vector for index " << asof_lookup_index;
                    table_items->asof_lookup_vectors[asof_lookup_index] = create_asof_lookup_vector_base(
                            asof_join_build_type, table_items->asof_join_condition_desc.condition_op);
                } else {
                    LOG(INFO) << "Using existing asof_lookup_vector for index " << asof_lookup_index;
                }

                LOG(INFO) << "Adding row to asof_lookup_vector[" << asof_lookup_index 
                          << "] with asof_value=" << probe_temporal_values[i] << ", row_index=" << i;
                table_items->add_asof_row(asof_lookup_index, probe_temporal_values[i], i);
            }
        };


        switch (asof_join_build_type) {
            case TYPE_BIGINT:
                process_build_rows.template operator()<TYPE_BIGINT>();
                break;
            case TYPE_DATE:
                process_build_rows.template operator()<TYPE_DATE>();
                break;
            case TYPE_DATETIME:
                process_build_rows.template operator()<TYPE_DATETIME>();
                break;
            default:
                CHECK(false) << "ASOF JOIN: Unsupported build_type: " << asof_join_build_type
                             << ". Only TYPE_BIGINT, TYPE_DATE, TYPE_DATETIME are supported.";
                __builtin_unreachable();
        }

        table_items->finalize_asof_lookup_vectors();
        LOG(INFO) << "Completed ASOF_INNER_JOIN processing and finalized lookup vectors";
    } else {
        LOG(INFO) << "Processing non-ASOF join";
        if (is_nulls == nullptr) {
            LOG(INFO) << "Building hash table without null checks";
            for (uint32_t i = 1; i < num_rows; i++) {
                const size_t bucket_num = keys[i] - min_value;
                table_items->next[i] = table_items->first[bucket_num];
                table_items->first[bucket_num] = i;
                if (i % 10000 == 0 || i < 10) {
                    LOG(INFO) << "Processed row " << i << ", key=" << keys[i] 
                              << ", bucket_num=" << bucket_num;
                }
            }
            LOG(INFO) << "Completed building hash table for " << (num_rows - 1) << " rows";
        } else {
            const auto* is_nulls_data = is_nulls->data();
            LOG(INFO) << "Building hash table with null checks";
            uint32_t non_null_count = 0;
            for (uint32_t i = 1; i < num_rows; i++) {
                if (is_nulls_data[i] == 0) {
                    const size_t bucket_num = keys[i] - min_value;
                    table_items->next[i] = table_items->first[bucket_num];
                    table_items->first[bucket_num] = i;
                    non_null_count++;
                    if (non_null_count % 10000 == 0 || non_null_count < 10) {
                        LOG(INFO) << "Processed non-null row " << i << ", key=" << keys[i] 
                                  << ", bucket_num=" << bucket_num;
                    }
                } else {
                    if (i < 10) {
                        LOG(INFO) << "Skipped null row " << i;
                    }
                }
            }
            LOG(INFO) << "Completed building hash table: processed " << non_null_count 
                      << " non-null rows out of " << (num_rows - 1) << " total rows";
        }
    }
    LOG(INFO) << "=== RangeDirectMappingJoinHashMap::construct_hash_table completed ===";
}

template <LogicalType LT>
void RangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                    HashTableProbeState* probe_state, const Buffer<CppType>& keys,
                                                    const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;

    LOG(INFO) << "=== RangeDirectMappingJoinHashMap::lookup_init ===";
    LOG(INFO) << "min_value: " << min_value;
    LOG(INFO) << "max_value: " << max_value;
    LOG(INFO) << "num_rows: " << num_rows;
    LOG(INFO) << "table_items.first.size(): " << table_items.first.size();

    if (is_nulls == nullptr) {
        for (size_t i = 0; i < num_rows; i++) {
            LOG(INFO) << "Processing probe row " << i << ", key=" << keys[i];
            if ((keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t bucket_id = keys[i] - min_value;
                probe_state->next[i] = table_items.first[bucket_id];

                LOG(INFO) << "  In range: bucket_id=" << bucket_id << ", build_index=" << table_items.first[bucket_id];

                // 检查对应的asof_lookup_vector是否存在
                uint32_t build_index = table_items.first[bucket_id];
                if (table_items.asof_lookup_vectors.size() > build_index && table_items.asof_lookup_vectors[build_index]) {
                    LOG(INFO) << "  asof_lookup_vectors[" << build_index << "] exists with size=" << table_items.asof_lookup_vectors[build_index]->size();
                } else {
                    LOG(INFO) << "  asof_lookup_vectors[" << build_index << "] does NOT exist or is null! vectors.size()=" << table_items.asof_lookup_vectors.size();
                }
            } else {
                LOG(INFO) << "  Out of range: setting build_index=0";
            }
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < num_rows; i++) {
            LOG(INFO) << "Processing probe row " << i << ", key=" << keys[i]
                      << ", is_null=" << (is_nulls_data[i] != 0 ? "true" : "false");
            if ((is_nulls_data[i] == 0) & (keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t bucket_id = keys[i] - min_value;
                probe_state->next[i] = table_items.first[bucket_id];
                LOG(INFO) << "  Valid and in range: bucket_id=" << bucket_id
                          << ", build_index=" << table_items.first[bucket_id];
                
                // 检查对应的asof_lookup_vector是否存在
                uint32_t build_index = table_items.first[bucket_id];
                if (table_items.asof_lookup_vectors.size() > build_index && table_items.asof_lookup_vectors[build_index]) {
                    LOG(INFO) << "  asof_lookup_vectors[" << build_index << "] exists with size=" << table_items.asof_lookup_vectors[build_index]->size();
                } else {
                    LOG(INFO) << "  asof_lookup_vectors[" << build_index << "] does NOT exist or is null! vectors.size()=" << table_items.asof_lookup_vectors.size();
                }
            } else {
                probe_state->next[i] = 0;
                LOG(INFO) << "  Null or out of range: setting build_index=0";
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
                                                    HashTableProbeState* probe_state, const Buffer<CppType>& keys,
                                                    const Buffer<uint8_t>* is_nulls) {
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    const int64_t min_value = table_items.min_value;
    const int64_t max_value = table_items.max_value;
    const size_t num_rows = probe_state->probe_row_count;
    if (is_nulls == nullptr) {
        for (size_t i = 0; i < num_rows; i++) {
            if ((keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t index = keys[i] - min_value;
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
            if ((is_nulls_data[i] == 0) & (keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t index = keys[i] - min_value;
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
                                                                  const Buffer<CppType>& keys,
                                                                  const Buffer<uint8_t>* is_nulls) {
    const uint64_t min_value = table_items->min_value;
    const auto num_rows = 1 + table_items->row_count;

    const uint8_t* is_nulls_data = is_nulls == nullptr ? nullptr : is_nulls->data();
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

    if (is_nulls == nullptr) {
        process.template operator()<false>();
    } else {
        process.template operator()<true>();
    }
}

template <LogicalType LT>
void DenseRangeDirectMappingJoinHashMap<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                         HashTableProbeState* probe_state, const Buffer<CppType>& keys,
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
            if ((keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t bucket_num = keys[i] - min_value;
                probe_state->next[i] = get_dense_first(bucket_num);
            } else {
                probe_state->next[i] = 0;
            }
        }
    } else {
        const auto* is_nulls_data = is_nulls->data();
        for (size_t i = 0; i < num_rows; i++) {
            if ((is_nulls_data[i] == 0) & (keys[i] >= min_value) & (keys[i] <= max_value)) {
                const uint64_t bucket_num = keys[i] - min_value;
                probe_state->next[i] = get_dense_first(bucket_num);
            } else {
                probe_state->next[i] = 0;
            }
        }
    }
}

} // namespace starrocks
