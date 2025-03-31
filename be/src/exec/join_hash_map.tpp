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

#include "simd/gather.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_TPP

#ifndef JOIN_HASH_MAP_H
#include "join_hash_map.h"
#endif

namespace starrocks {
template <LogicalType LT>
void JoinBuildFunc<LT>::prepare(RuntimeState* runtime, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
const Buffer<typename JoinBuildFunc<LT>::CppType>& JoinBuildFunc<LT>::get_key_data(
        const JoinHashTableItems& table_items) {
    ColumnPtr data_column;
    if (table_items.key_columns[0]->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        data_column = null_column->data_column();
    } else {
        data_column = table_items.key_columns[0];
    }

    if constexpr (lt_is_string<LT>) {
        if (UNLIKELY(data_column->is_large_binary())) {
            return ColumnHelper::as_raw_column<LargeBinaryColumn>(data_column)->get_data();
        } else {
            return ColumnHelper::as_raw_column<BinaryColumn>(data_column)->get_data();
        }
    } else {
        return ColumnHelper::as_raw_column<ColumnType>(data_column)->get_data();
    }
}

template <LogicalType LT>
void JoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                             HashTableProbeState* probe_state) {
    auto& data = get_key_data(*table_items);
    if (table_items->key_columns[0]->is_nullable() && table_items->key_columns[0]->has_null()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[0]);
        const auto& null_array = nullable_column->null_column()->get_data();
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            if (null_array[i] == 0) {
                const uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<CppType>(
                        data[i], table_items->bucket_size, table_items->log_bucket_size);
                table_items->next[i] = table_items->first[bucket_num];
                table_items->first[bucket_num] = i;
            }
        }
    } else {
        auto* __restrict next = table_items->next.data();
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            const uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<CppType>(data[i], table_items->bucket_size,
                                                                                    table_items->log_bucket_size);
            // Use `next` stores `bucket_num` temporarily.
            next[i] = bucket_num;
        }

        auto* __restrict first = table_items->first.data();
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            const uint32_t bucket_num = next[i];
            next[i] = first[bucket_num];
            first[bucket_num] = i;
        }
    }

    table_items->calculate_ht_info(table_items->key_columns[0]->byte_size());
}

template <LogicalType LT>
void DirectMappingJoinBuildFunc<LT>::prepare(RuntimeState* runtime, JoinHashTableItems* table_items) {
    static constexpr size_t BUCKET_SIZE =
            (int64_t)(RunTimeTypeLimits<LT>::max_value()) - (int64_t)(RunTimeTypeLimits<LT>::min_value()) + 1L;
    table_items->bucket_size = BUCKET_SIZE;
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
}

template <LogicalType LT>
const Buffer<typename DirectMappingJoinBuildFunc<LT>::CppType>& DirectMappingJoinBuildFunc<LT>::get_key_data(
        const JoinHashTableItems& table_items) {
    if (table_items.key_columns[0]->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        return ColumnHelper::as_raw_column<ColumnType>(null_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>(table_items.key_columns[0])->get_data();
}

template <LogicalType LT>
void DirectMappingJoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                          HashTableProbeState* probe_state) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();

    auto& data = get_key_data(*table_items);
    if (table_items->key_columns[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[0]);
        auto& null_array = nullable_column->null_column()->get_data();
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            if (null_array[i] == 0) {
                size_t buckets = data[i] - MIN_VALUE;
                table_items->next[i] = table_items->first[buckets];
                table_items->first[buckets] = i;
            }
        }
    } else {
        for (size_t i = 1; i < table_items->row_count + 1; i++) {
            size_t buckets = data[i] - MIN_VALUE;
            table_items->next[i] = table_items->first[buckets];
            table_items->first[buckets] = i;
        }
    }
    table_items->calculate_ht_info(table_items->key_columns[0]->byte_size());
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->log_bucket_size = __builtin_ctz(table_items->bucket_size);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    table_items->build_key_column = ColumnType::create(table_items->row_count + 1);
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                      HashTableProbeState* probe_state) {
    uint32_t row_count = table_items->row_count;

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;
    for (size_t i = 0; i < table_items->key_columns.size(); i++) {
        if (table_items->join_keys[i].is_null_safe_equal) {
            data_columns.emplace_back(table_items->key_columns[i]);
        } else if (table_items->key_columns[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if (table_items->key_columns[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back(table_items->key_columns[i]);
        }
    }

    // serialize and build hash table
    uint32_t quo = row_count / state->chunk_size();
    uint32_t rem = row_count % state->chunk_size();

    if (!null_columns.empty()) {
        for (size_t i = 0; i < quo; i++) {
            _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * i,
                                    state->chunk_size());
        }
        _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * quo,
                                rem);
    } else {
        for (size_t i = 0; i < quo; i++) {
            _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * i, state->chunk_size());
        }
        _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * quo, rem);
    }
    table_items->calculate_ht_info(table_items->build_key_column->byte_size());
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::_build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                                const Columns& data_columns, uint32_t start, uint32_t count) {
    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, table_items->build_key_column.get(), start,
                                                           count);

    const auto& data = get_key_data(*table_items);
    JoinHashMapHelper::calc_bucket_nums<CppType>(data, table_items->bucket_size, table_items->log_bucket_size,
                                                 &probe_state->buckets, start, count);

    for (uint32_t i = 0; i < count; i++) {
        table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
        table_items->first[probe_state->buckets[i]] = start + i;
    }
}

template <LogicalType LT>
void FixedSizeJoinBuildFunc<LT>::_build_nullable_columns(JoinHashTableItems* table_items,
                                                         HashTableProbeState* probe_state, const Columns& data_columns,
                                                         const NullColumns& null_columns, uint32_t start,
                                                         uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[start + i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[start + j];
        }
    }

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, table_items->build_key_column.get(), start,
                                                           count);
    const auto& data = get_key_data(*table_items);
    JoinHashMapHelper::calc_bucket_nums<CppType>(data, table_items->bucket_size, table_items->log_bucket_size,
                                                 &probe_state->buckets, start, count);

    for (size_t i = 0; i < count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
            table_items->first[probe_state->buckets[i]] = start + i;
        }
    }
}

template <LogicalType LT>
void DirectMappingJoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    static constexpr CppType MIN_VALUE = RunTimeTypeLimits<LT>::min_value();
    size_t probe_row_count = probe_state->probe_row_count;
    auto& data = get_key_data(*probe_state);
    probe_state->active_coroutines = 0; // the ht data is not large, so disable it always.

    if ((*probe_state->key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);

        if (nullable_column->has_null()) {
            auto& null_array = nullable_column->null_column()->get_data();
            for (size_t i = 0; i < probe_row_count; i++) {
                if (null_array[i] == 0) {
                    probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
                } else {
                    probe_state->next[i] = 0;
                }
            }
            probe_state->null_array = &null_array;
        } else {
            for (size_t i = 0; i < probe_row_count; i++) {
                probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
            }
            probe_state->null_array = nullptr;
        }
        return;
    }

    for (size_t i = 0; i < probe_row_count; i++) {
        probe_state->next[i] = table_items.first[data[i] - MIN_VALUE];
    }
    probe_state->null_array = nullptr;
}

template <LogicalType LT>
const Buffer<typename DirectMappingJoinProbeFunc<LT>::CppType>& DirectMappingJoinProbeFunc<LT>::get_key_data(
        const HashTableProbeState& probe_state) {
    if ((*probe_state.key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state.key_columns)[0]);
        return ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>((*probe_state.key_columns)[0])->get_data();
}

template <LogicalType LT>
void JoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state) {
    const size_t probe_row_count = probe_state->probe_row_count;
    auto& data = get_key_data(*probe_state);
    JoinHashMapHelper::calc_bucket_nums<CppType>(data, table_items.bucket_size, table_items.log_bucket_size,
                                                 &probe_state->buckets, 0, data.size());

    const auto* firsts = table_items.first.data();
    const auto* buckets = probe_state->buckets.data();
    auto* nexts = probe_state->next.data();

    if ((*probe_state->key_columns)[0]->is_nullable()) {
        const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);
        if (nullable_column->has_null()) {
            const auto* is_nulls = nullable_column->null_column()->get_data().data();
            SIMDGather::gather(nexts, firsts, buckets, is_nulls, probe_row_count);

            probe_state->null_array = &nullable_column->null_column()->get_data();
            probe_state->consider_probe_time_locality();
            return;
        }
    }

    SIMDGather::gather(nexts, firsts, buckets, probe_row_count);

    probe_state->null_array = nullptr;
    probe_state->consider_probe_time_locality();
}

template <LogicalType LT>
const Buffer<typename JoinProbeFunc<LT>::CppType>& JoinProbeFunc<LT>::get_key_data(
        const HashTableProbeState& probe_state) {
    if ((*probe_state.key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state.key_columns)[0]);
        return ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>((*probe_state.key_columns)[0])->get_data();
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state) {
    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            if ((*probe_state->key_columns)[i]->is_nullable()) {
                data_columns.emplace_back((*probe_state->key_columns)[i]);
            } else {
                auto tmp_column = NullableColumn::create((*probe_state->key_columns)[i],
                                                         NullColumn::create(probe_state->probe_row_count, 0));
                data_columns.emplace_back(tmp_column);
            }
        } else if ((*probe_state->key_columns)[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if ((*probe_state->key_columns)[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back((*probe_state->key_columns)[i]);
        }
    }

    // serialize and init search
    if (!null_columns.empty()) {
        _probe_nullable_column(table_items, probe_state, data_columns, null_columns);
    } else {
        _probe_column(table_items, probe_state, data_columns);
    }
    probe_state->consider_probe_time_locality();
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::_probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                               const Columns& data_columns) {
    uint32_t row_count = probe_state->probe_row_count;

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, probe_state->probe_key_column.get(), 0,
                                                           row_count);
    const auto& data = get_key_data(*probe_state);
    JoinHashMapHelper::calc_bucket_nums<CppType>(data, table_items.bucket_size, table_items.log_bucket_size,
                                                 &probe_state->buckets, 0, row_count);
    probe_state->null_array = nullptr;
    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->next[i] = table_items.first[probe_state->buckets[i]];
    }
}

template <LogicalType LT>
void FixedSizeJoinProbeFunc<LT>::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                        HashTableProbeState* probe_state, const Columns& data_columns,
                                                        const NullColumns& null_columns) {
    uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < row_count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[j];
        }
    }
    probe_state->null_array = &null_columns[0]->get_data();

    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, probe_state->probe_key_column.get(), 0,
                                                           row_count);
    const auto& data = get_key_data(*probe_state);
    JoinHashMapHelper::calc_bucket_nums<CppType>(data, table_items.bucket_size, table_items.log_bucket_size,
                                                 &probe_state->buckets, 0, row_count);

    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->next[i] = table_items.first[probe_state->buckets[i]];
        } else {
            probe_state->next[i] = 0;
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::build_prepare(RuntimeState* state) {
    BuildFunc().prepare(state, _table_items);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe_prepare(RuntimeState* state) {
    size_t chunk_size = state->chunk_size();
    _probe_state->build_index.resize(chunk_size + 8);
    _probe_state->probe_index.resize(chunk_size + 8);
    _probe_state->next.resize(chunk_size);
    _probe_state->probe_match_index.resize(chunk_size);
    _probe_state->probe_match_filter.resize(chunk_size);
    _probe_state->buckets.resize(chunk_size);

    if (_table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN || _table_items->join_type == TJoinOp::FULL_OUTER_JOIN ||
        _table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        _probe_state->build_match_index.resize(_table_items->row_count + 1, 0);
        _probe_state->build_match_index[0] = 1;
    }

    ProbeFunc().prepare(state, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::build(RuntimeState* state) {
    BuildFunc().construct_hash_table(state, _table_items, _probe_state);
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe(RuntimeState* state, const Columns& key_columns,
                                                  ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* has_remain) {
    _probe_state->key_columns = &key_columns;
    {
        SCOPED_TIMER(_probe_state->search_ht_timer);
        _search_ht(state, probe_chunk);
        if (_probe_state->count <= 0) {
            *has_remain = false;
            return;
        }

        *has_remain = _probe_state->has_remain;

        if (UNLIKELY(!_probe_state->has_remain && !_probe_state->handles.empty())) {
            std::string msg =
                    "HashJoin probe haven't remain tuples but have coroutines, likely leaking coroutines, please set "
                    "global interleaving_group_size = 0 to disable coroutines, rerun this query and report to SR";
            LOG(ERROR) << "fragment = " << print_id(state->fragment_instance_id()) << " " << msg;
            throw std::runtime_error(msg);
        }
    }

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        // right semi join without other join conjunct
        // right anti join without other join conjunct
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            if (_table_items->with_other_conjunct) {
                _probe_output<false>(probe_chunk, chunk);
            }
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<false>(chunk);
        }
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // left semi join without other join conjunct
        // left anti join without other join conjunct
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<false>(probe_chunk, chunk);
        }
        {
            // output default values for build-columns as placeholder.
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            if (_table_items->mor_reader_mode) {
                return;
            }

            if (!_table_items->with_other_conjunct) {
                // When the project doesn't require any cols from join, FE will select the first col in the build table
                // of join as the output col for simple, wo we also need output build column here
                _build_default_output(chunk, _probe_state->count);
            } else {
                _build_output<false>(chunk);
            }
        }
    } else {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<false>(probe_chunk, chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<false>(chunk);
        }
    }

    if (_table_items->enable_late_materialization) {
        _probe_index_output(chunk);
        _build_index_output(chunk);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
    _search_ht_remain(state);
    if (_probe_state->count <= 0) {
        *has_remain = false;
        return;
    }
    *has_remain = _probe_state->has_remain;

    if (_table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN || _table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN) {
        // right anti/semi join without other conjunct output default value of probe-columns as placeholder.
        if (_table_items->with_other_conjunct) {
            _probe_null_output<false>(chunk, _probe_state->count);
        }
        _build_output<false>(chunk);
    } else {
        // RIGHT_OUTER_JOIN || FULL_OUTER_JOIN
        _probe_null_output<false>(chunk, _probe_state->count);
        _build_output<false>(chunk);
    }

    if (_table_items->enable_late_materialization) {
        _build_index_output(chunk);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
    bool to_nullable = _table_items->left_to_nullable;

    for (size_t i = 0; i < _table_items->probe_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
            if (!column->is_nullable()) {
                _copy_probe_column(&column, chunk, slot, to_nullable);
            } else {
                _copy_probe_nullable_column(&column, chunk, slot);
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_remain>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk,
                                                        ChunkPtr* result_chunk) {
    if ((*result_chunk)->num_rows() < _probe_state->count) {
        _probe_state->match_flag = JoinMatchFlag::NORMAL;
        _probe_state->count = (*result_chunk)->num_rows();
    }

    (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_BUILD_INDEX_SLOT_ID);
    (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<true>(probe_chunk, result_chunk);
        }
    } else if (_table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN ||
               _table_items->join_type == TJoinOp::FULL_OUTER_JOIN) {
        if constexpr (is_remain) {
            {
                SCOPED_TIMER(_probe_state->output_probe_column_timer);
                _probe_null_output<true>(result_chunk, _probe_state->count);
            }
        } else {
            {
                SCOPED_TIMER(_probe_state->output_probe_column_timer);
                _probe_output<true>(probe_chunk, result_chunk);
            }
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    } else {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output<true>(probe_chunk, result_chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output<true>(result_chunk);
        }
    }

    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_null_output(ChunkPtr* chunk, size_t count) {
    for (size_t i = 0; i < _table_items->probe_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            ColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(count);
            (*chunk)->append_column(std::move(column), slot->id());
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool is_lazy>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_build_output(ChunkPtr* chunk) {
    bool to_nullable = _table_items->right_to_nullable;
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;

        bool need_output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
        if (need_output) {
            ColumnPtr& column = _table_items->build_chunk->columns()[i];
            if (!column->is_nullable()) {
                _copy_build_column(column, chunk, slot, to_nullable);
            } else {
                _copy_build_nullable_column(column, chunk, slot);
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_build_default_output(ChunkPtr* chunk, size_t count) {
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        auto hash_tablet_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_tablet_slot.slot;
        if (hash_tablet_slot.need_output) {
            ColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(count);
            (*chunk)->append_column(std::move(column), slot->id());
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                               const SlotDescriptor* slot, bool to_nullable) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        if (to_nullable) {
            ColumnPtr dest_column =
                    NullableColumn::create((*src_column)->as_mutable_ptr(), NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        if (to_nullable) {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            ColumnPtr dest_column =
                    NullableColumn::create((*src_column)->as_mutable_ptr(), NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                                        const SlotDescriptor* slot) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        (*chunk)->append_column(*src_column, slot->id());
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
        (*chunk)->append_column(*src_column, slot->id());
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk,
                                                               const SlotDescriptor* slot, bool to_nullable) {
    if (to_nullable) {
        auto data_column = src_column->clone_empty();
        data_column->append_selective(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);

        // When left outer join is executed,
        // build_index[i] Equal to 0 means it is not found in the hash table,
        // but append_selective() has set item of NullColumn to not null
        // so NullColumn needs to be set back to null
        auto null_column = NullColumn::create(_probe_state->count, 0);
        size_t end = _probe_state->count;
        for (size_t i = 0; i < end; i++) {
            if (_probe_state->build_index[i] == 0) {
                null_column->get_data()[i] = 1;
            }
        }
        auto dest_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        (*chunk)->append_column(std::move(dest_column), slot->id());
    } else {
        auto dest_column = src_column->clone_empty();
        dest_column->append_selective(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk,
                                                                        const SlotDescriptor* slot) {
    const uint32_t num_rows = _probe_state->count;
    const auto* build_index = _probe_state->build_index.data();

    const auto num_new_nulls = SIMD::count_zero(build_index, num_rows);
    ColumnPtr dest_column = src_column->clone_empty();
    if (num_new_nulls == num_rows) {
        dest_column->append_nulls(num_rows);
    } else {
        dest_column->append_selective(*src_column, build_index, 0, num_rows);
        // When left outer join is executed,
        // build_index[i] Equal to 0 means it is not found in the hash table,
        // but append_selective() has set item of NullColumn to not null
        // so NullColumn needs to be set back to null
        if (num_new_nulls > 0) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dest_column);
            auto* is_nulls = nullable_column->null_column_data().data();
            for (uint32_t i = 0; i < num_rows; i++) {
                is_nulls[i] |= build_index[i] == 0;
            }
            nullable_column->set_has_null(true);
        }
    }

    (*chunk)->append_column(std::move(dest_column), slot->id());
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht(RuntimeState* state, ChunkPtr* probe_chunk) {
    if (_table_items->enable_late_materialization) {
        _probe_state->probe_index.resize(state->chunk_size() + 8);
        _probe_state->build_index.resize(state->chunk_size() + 8);
    }
    if (!_probe_state->has_remain) {
        _probe_state->probe_row_count = (*probe_chunk)->num_rows();
        _probe_state->active_coroutines = state->query_options().interleaving_group_size;
        // disable adaptively interleaving if the ht may encounter seriously cache misses.
        if (state->query_options().interleaving_group_size > 0 && !_table_items->ht_cache_miss_serious()) {
            _probe_state->active_coroutines = 0;
        }
        ProbeFunc().lookup_init(*_table_items, _probe_state);

        auto& build_data = BuildFunc().get_key_data(*_table_items);
        auto& probe_data = ProbeFunc().get_key_data(*_probe_state);
        _search_ht_impl<true>(state, build_data, probe_data);
    } else {
        auto& build_data = BuildFunc().get_key_data(*_table_items);
        auto& probe_data = ProbeFunc().get_key_data(*_probe_state);
        _search_ht_impl<false>(state, build_data, probe_data);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht_remain(RuntimeState* state) {
    if (!_probe_state->has_remain) {
        size_t zero_count = SIMD::count_zero(_probe_state->build_match_index);
        if (zero_count <= 0) {
            _probe_state->count = 0;
            _probe_state->has_remain = false;
            return;
        }
        _probe_state->cur_build_index = 0;
    }

    if (_table_items->enable_late_materialization) {
        _probe_state->build_index.resize(state->chunk_size() + 8);
    }

    size_t match_count = 0;
    size_t i = _probe_state->cur_build_index;
    for (; i < _probe_state->build_match_index.size(); i++) {
        if (_probe_state->build_match_index[i] == 0) {
            _probe_state->build_index[match_count] = i;
            _probe_state->probe_index[match_count] = 0;
            match_count++;

            if (match_count >= state->chunk_size()) {
                i++;
                _probe_state->cur_build_index = i;
                _probe_state->has_remain = i < _probe_state->build_match_index.size();
                _probe_state->count = match_count;
                return;
            }
        }
    }

    _probe_state->cur_build_index = i;
    _probe_state->has_remain = false;
    _probe_state->count = match_count;
}

#define DO_PROBE(X)                                                                                                  \
    if (_probe_state->active_coroutines != 0) {                                                                      \
        if constexpr (first_probe) {                                                                                 \
            auto group_size = std::abs(state->query_options().interleaving_group_size);                              \
            _probe_state->cur_probe_index = 0;                                                                       \
            if (!_probe_state->handles.empty()) {                                                                    \
                for (auto& h : _probe_state->handles) {                                                              \
                    h.destroy();                                                                                     \
                }                                                                                                    \
                _probe_state->handles.clear();                                                                       \
                std::string msg =                                                                                    \
                        "HashJoin probe leaks coroutines, please set global interleaving_group_size = 0 to disable " \
                        "coroutines, rerun this query and report to SR";                                             \
                LOG(ERROR) << "fragment = " + print_id(state->fragment_instance_id()) << " " << msg;                 \
                throw std::runtime_error(msg);                                                                       \
            }                                                                                                        \
            for (int i = 0; i < group_size; ++i) {                                                                   \
                _probe_state->handles.insert(X(state, build_data, data));                                            \
            }                                                                                                        \
            _probe_state->active_coroutines = group_size;                                                            \
        }                                                                                                            \
        _probe_coroutine<first_probe>(state, build_data, data);                                                      \
    } else {                                                                                                         \
        X<first_probe>(state, build_data, data);                                                                     \
    }

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data,
                                                            const Buffer<CppType>& data) {
    if (!_table_items->with_other_conjunct) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_left_outer_join);
            break;
        case TJoinOp::LEFT_SEMI_JOIN:
            DO_PROBE(_probe_from_ht_for_left_semi_join);
            break;
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
            DO_PROBE(_probe_from_ht_for_left_anti_join);
            break;
        case TJoinOp::RIGHT_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_right_outer_join);
            break;
        case TJoinOp::RIGHT_SEMI_JOIN:
            DO_PROBE(_probe_from_ht_for_right_semi_join);
            break;
        case TJoinOp::RIGHT_ANTI_JOIN:
            DO_PROBE(_probe_from_ht_for_right_anti_join);
            break;
        case TJoinOp::FULL_OUTER_JOIN:
            DO_PROBE(_probe_from_ht_for_full_outer_join);
            break;
        default:
            DO_PROBE(_probe_from_ht);
            break;
        }
    } else {
        // as probing results of join keys are not clustered in one chunk, `probe_match_index` and `build_match_index`
        // are not completely right, resulting in wrong results when filtering other conjunct.
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_SEMI_JOIN:
            _probe_from_ht_for_left_semi_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
            _probe_from_ht_for_null_aware_anti_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
        case TJoinOp::RIGHT_OUTER_JOIN:
        case TJoinOp::RIGHT_SEMI_JOIN:
        case TJoinOp::RIGHT_ANTI_JOIN:
            _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct<first_probe>(
                    state, build_data, data);
            break;
        case TJoinOp::LEFT_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::FULL_OUTER_JOIN:
            _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct<first_probe>(state, build_data,
                                                                                                     data);
            break;
        default:
            // can't reach here
            _probe_from_ht<first_probe>(state, build_data, data);
            break;
        }
    }
}

#define CHECK_MATCH()                                                                                          \
    if (match_count > 0 && !one_to_many) {                                                                     \
        size_t zero_count = SIMD::count_zero(_probe_state->probe_match_filter, _probe_state->probe_row_count); \
        if (zero_count == 0) {                                                                                 \
            _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;                                           \
        } else if (zero_count < _probe_state->probe_row_count - zero_count) {                                  \
            _probe_state->match_flag = JoinMatchFlag::MOST_MATCH_ONE;                                          \
        }                                                                                                      \
    }

#define CHECK_ALL_MATCH()                                        \
    if (match_count > 0 && !one_to_many) {                       \
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE; \
    }

#define RETURN_IF_CHUNK_FULL()                                   \
    if (UNLIKELY(match_count > state->chunk_size())) {           \
        _probe_state->next[i] = _table_items->next[build_index]; \
        _probe_state->cur_probe_index = i;                       \
        _probe_state->cur_build_index = build_index;             \
        _probe_state->has_remain = true;                         \
        _probe_state->count = state->chunk_size();               \
        return;                                                  \
    }

#define RETURN_IF_CHUNK_FULL2()                                  \
    if (UNLIKELY(match_count > state->chunk_size())) {           \
        _probe_state->next[i] = _table_items->next[build_index]; \
        _probe_state->cur_probe_index = i;                       \
        _probe_state->cur_build_index = build_index;             \
        _probe_state->has_remain = true;                         \
        _probe_state->count = state->chunk_size();               \
        _probe_state->cur_row_match_count = cur_row_match_count; \
        return;                                                  \
    }

#define COWAIT_IF_CHUNK_FULL()                              \
    if (_probe_state->match_count == state->chunk_size()) { \
        _probe_state->has_remain = true;                    \
        _probe_state->count = state->chunk_size();          \
        co_await std::suspend_always{};                     \
    }

#define REORDER_PROBE_INDEX()                                                                                         \
    if (_probe_state->match_flag != JoinMatchFlag::NORMAL) {                                                          \
        Buffer<uint32_t> permutation(_probe_state->probe_index.size(), -1);                                           \
        for (auto i = 0; i < _probe_state->match_count; ++i) {                                                        \
            permutation[_probe_state->probe_index[i]] = i;                                                            \
        }                                                                                                             \
        Buffer<uint32_t> new_order(_probe_state->build_index.size(), 0);                                              \
        uint32_t count = 0;                                                                                           \
        for (auto i = 0; i < _probe_state->probe_row_count; ++i) {                                                    \
            if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE ||                                           \
                (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE && _probe_state->probe_match_filter[i])) { \
                DCHECK(permutation[i] != -1);                                                                         \
                new_order[count++] = _probe_state->build_index[permutation[i]];                                       \
            }                                                                                                         \
        }                                                                                                             \
        if (UNLIKELY(count != _probe_state->match_count)) {                                                           \
            auto msg = fmt::format("Coroutine join match count {} != expected {}", count, _probe_state->match_count); \
            LOG(WARNING) << msg;                                                                                      \
            throw std::runtime_error(msg);                                                                            \
        }                                                                                                             \
        _probe_state->build_index.swap(new_order);                                                                    \
    }

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86)) /* _mm_prefetch() not defined outside of x86/x64 */
#include <mmintrin.h> /* https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx */
#define XXH_PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#elif defined(__GNUC__) && ((__GNUC__ >= 4) || ((__GNUC__ == 3) && (__GNUC_MINOR__ >= 1)))
#define XXH_PREFETCH(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#endif

#define PREFETCH_AND_COWAIT(x, y) \
    XXH_PREFETCH(x);              \
    XXH_PREFETCH(y);              \
    co_await std::suspend_always{};

// When a probe row corresponds to multiple Build rows,
// a Probe Chunk may generate multiple ResultChunks,
// so each probe will have search one more row to determine whether it has reached the boundary,
// so the next probe will start from the last recorded position
#define PROCESS_PROBE_STAGE_FOR_RIGHT_JOIN_WITH_OTHER_CONJUNCT()      \
    if constexpr (!first_probe) {                                     \
        _probe_state->probe_index[0] = _probe_state->cur_probe_index; \
        _probe_state->build_index[0] = _probe_state->cur_build_index; \
        match_count = 1;                                              \
        if (_probe_state->next[i] == 0) {                             \
            i++;                                                      \
        }                                                             \
    }

#define PROBE_OVER()                   \
    _probe_state->has_remain = false;  \
    _probe_state->cur_probe_index = 0; \
    _probe_state->cur_build_index = 0; \
    _probe_state->count = match_count; \
    _probe_state->cur_row_match_count = 0;

#define MATCH_RIGHT_TABLE_ROWS()                \
    _probe_state->probe_index[match_count] = i; \
    _probe_state->build_index[match_count] = j; \
    _probe_state->probe_match_index[i]++;       \
    match_count++;                              \
    _probe_state->cur_row_match_count++;

/// TODO (fzh): calculate hash distribution, skew or not.
// NOTE: coroutine only SIMD code of SSE but not AVX
template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_coroutine(RuntimeState* state, const Buffer<CppType>& build_data,
                                                             const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    _probe_state->match_count = 0;
    _probe_state->cur_row_match_count = 0;
    _probe_state->count = 0;
    // disorder probe id as matching steps are different for each probe
    while (!_probe_state->handles.empty()) {
        for (auto it = _probe_state->handles.begin(); it != _probe_state->handles.end();) {
            if (it->promise().exception != nullptr) {
                LOG(WARNING) << print_id(state->fragment_instance_id()) << " coroutine rethrow exceptions";
                std::rethrow_exception(it->promise().exception);
            }
            if (it->done()) {
                it->destroy();
                it = _probe_state->handles.erase(it);
            } else {
                it->resume();
                it++;
            }
            if (_probe_state->count == state->chunk_size() && _probe_state->has_remain) {
                return;
            }
        }
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                           const Buffer<CppType>& probe_data) {
    if (_table_items->is_collision_free_and_unique) {
        _do_probe_from_ht<first_probe, true>(state, build_data, probe_data);
    } else {
        _do_probe_from_ht<first_probe, false>(state, build_data, probe_data);
    }
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe, bool is_collision_free_and_unique>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_do_probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                              const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    size_t match_count = 0;
    bool one_to_many = false;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) { // chunk_size + 1 probe
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    }

    [[maybe_unused]] size_t probe_cont = 0;

    if constexpr (first_probe) {
        memset(_probe_state->probe_match_filter.data(), 0, _probe_state->probe_row_count * sizeof(uint8_t));
    }

    const size_t probe_row_count = _probe_state->probe_row_count;
    const auto* probe_buckets = _probe_state->next.data();
    // Only `!is_collision_free_and_unique` needs to record and check `cur_row_match_count`.
    uint32_t cur_row_match_count = _probe_state->cur_row_match_count;

    for (; i < probe_row_count; i++) {
        uint32_t build_index = probe_buckets[i];

        if (build_index == 0) {
            continue;
        }

        do {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                if constexpr (first_probe) {
                    if constexpr (!is_collision_free_and_unique) {
                        cur_row_match_count++;
                    }
                    _probe_state->probe_match_filter[i] = 1;
                }

                if constexpr (!is_collision_free_and_unique) {
                    RETURN_IF_CHUNK_FULL2()
                }
            }

            if constexpr (is_collision_free_and_unique) {
                break;
            }

            probe_cont++;
            build_index = _table_items->next[build_index];
        } while (build_index != 0);

        if constexpr (first_probe && !is_collision_free_and_unique) {
            if (cur_row_match_count > 1) {
                one_to_many = true;
            }
            cur_row_match_count = 0;
        }
    }

    // COUNTER_UPDATE(_probe_state->probe_counter, probe_cont);

    _probe_state->cur_row_match_count = cur_row_match_count;

    if constexpr (first_probe) {
        CHECK_MATCH()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        _probe_state->probe_match_filter[i] = 0;
        uint32_t cur_row_match_count = 0;
        size_t build_index = _probe_state->next[i];
        if (build_index != 0) {
            do {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    COWAIT_IF_CHUNK_FULL()
                    _probe_state->probe_index[_probe_state->match_count] = i;
                    _probe_state->build_index[_probe_state->match_count] = build_index;
                    _probe_state->match_count++;
                    cur_row_match_count++;
                    _probe_state->probe_match_filter[i] = 1;
                }
                build_index = _table_items->next[build_index];
            } while (build_index != 0);

            if (cur_row_match_count > 1) {
                _probe_state->cur_row_match_count = cur_row_match_count; // means one_to_many match
            }
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    bool one_to_many = _probe_state->cur_row_match_count > 1;
    if (!_probe_state->has_remain) {
        CHECK_MATCH()
        REORDER_PROBE_INDEX()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        int cur_row_match_count = 0;
        size_t build_index = _probe_state->next[i];
        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->match_count++;
                cur_row_match_count++;
            }
            build_index = _table_items->next[build_index];
        }
        if (cur_row_match_count <= 0) {
            COWAIT_IF_CHUNK_FULL()
            // one key of left table match none key of right table
            _probe_state->probe_index[_probe_state->match_count] = i;
            _probe_state->build_index[_probe_state->match_count] = 0;
            _probe_state->match_count++;
        } else if (cur_row_match_count > 1) {
            // one key of left table match multi key of right table
            _probe_state->cur_row_match_count = cur_row_match_count;
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    bool one_to_many = _probe_state->cur_row_match_count > 1;
    if (!_probe_state->has_remain) {
        CHECK_ALL_MATCH()
        REORDER_PROBE_INDEX()
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    size_t match_count = 0;
    bool one_to_many = false;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    match_count++;
                    _probe_state->cur_row_match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
                build_index = _table_items->next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                // one key of left table match none key of right table
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            } else if (_probe_state->cur_row_match_count > 1) {
                // one key of left table match multi key of right table
                if constexpr (first_probe) {
                    one_to_many = true;
                }
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    if constexpr (first_probe) {
        CHECK_ALL_MATCH()
    }
    PROBE_OVER()
}
template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                break;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                              const Buffer<CppType>& build_data,
                                                                              const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t probe_row_count = _probe_state->probe_row_count;
    for (size_t i = 0; i < probe_row_count; i++) {
        size_t index = _probe_state->next[i];
        if (index == 0) {
            continue;
        }

        while (index != 0) {
            if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
                break;
            }
            index = _table_items->next[index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                              const Buffer<CppType>& build_data,
                                                                              const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t probe_row_count = _probe_state->probe_row_count;
    DCHECK_LT(0, _table_items->row_count);
    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        // process left anti join from not in
        for (size_t i = 0; i < probe_row_count; i++) {
            size_t index = _probe_state->next[i];
            if ((*_probe_state->null_array).at(i) == 1) {
                continue;
            }

            if (index == 0) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
                continue;
            }

            bool found = false;
            while (index != 0) {
                if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                    found = true;
                    break;
                }
                index = _table_items->next[index];
            }
            if (!found) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
            }
        }
    } else {
        for (size_t i = 0; i < probe_row_count; i++) {
            size_t index = _probe_state->next[i];
            if (index == 0) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
                continue;
            }
            bool found = false;
            while (index != 0) {
                if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                    found = true;
                    break;
                }
                index = _table_items->next[index];
            }
            if (!found) {
                _probe_state->probe_index[match_count] = i;
                match_count++;
            }
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    DCHECK_LT(0, _table_items->row_count);
    if (_table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _probe_state->null_array != nullptr) {
        // process left anti join from not in
        for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
             i = _probe_state->cur_probe_index++) {
            size_t build_index = _probe_state->next[i];
            if ((*_probe_state->null_array).at(i) == 1) {
                continue;
            }

            if (build_index == 0) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                continue;
            }

            bool found = false;
            while (build_index != 0) {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    found = true;
                    break;
                }
                build_index = _table_items->next[build_index];
            }
            if (!found) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
            }
        }
    } else {
        for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
             i = _probe_state->cur_probe_index++) {
            size_t build_index = _probe_state->next[i];
            if (build_index == 0) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
                continue;
            }
            bool found = false;
            while (build_index != 0) {
                PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    found = true;
                    break;
                }
                build_index = _table_items->next[build_index];
            }
            if (!found) {
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->match_count++;
            }
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_outer_join(RuntimeState* state,
                                                                                const Buffer<CppType>& build_data,
                                                                                const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    // TODO: all match optimized
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                _probe_state->match_count++;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    // TODO: all match optimized
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_semi_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                if (_probe_state->build_match_index[build_index] == 0) {
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_semi_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                if (_probe_state->build_match_index[build_index] == 0) {
                    COWAIT_IF_CHUNK_FULL()
                    _probe_state->build_index[_probe_state->match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    _probe_state->match_count++;
                }
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_anti_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t probe_row_count = _probe_state->probe_row_count;
    for (size_t i = 0; i < probe_row_count; i++) {
        size_t index = _probe_state->next[i];
        if (index == 0) {
            continue;
        }

        while (index != 0) {
            if (ProbeFunc().equal(build_data[index], probe_data[i])) {
                _probe_state->build_match_index[index] = 1;
            }
            index = _table_items->next[index];
        }
    }
    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_right_anti_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->build_match_index[build_index] = 1;
            }
            build_index = _table_items->next[build_index];
        }
    }
    _probe_state->count = 0;
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_full_outer_join(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->build_match_index[build_index] = 1;
                    _probe_state->cur_row_match_count++;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
                build_index = _table_items->next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_full_outer_join(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        int cur_row_match_count = 0;
        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->build_match_index[build_index] = 1;
                _probe_state->match_count++;
                cur_row_match_count++;
            }
            build_index = _table_items->next[build_index];
        }
        if (cur_row_match_count <= 0) {
            COWAIT_IF_CHUNK_FULL()
            _probe_state->probe_index[_probe_state->match_count] = i;
            _probe_state->build_index[_probe_state->match_count] = 0;
            _probe_state->match_count++;
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
HashTableProbeState::ProbeCoroutine
JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_semi_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    for (size_t i = _probe_state->cur_probe_index++; i < _probe_state->probe_row_count;
         i = _probe_state->cur_probe_index++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            PREFETCH_AND_COWAIT((build_data.data() + build_index), (_table_items->next.data() + build_index))
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                COWAIT_IF_CHUNK_FULL()
                _probe_state->probe_index[_probe_state->match_count] = i;
                _probe_state->build_index[_probe_state->match_count] = build_index;
                _probe_state->match_count++;
            }
            build_index = _table_items->next[build_index];
        }
    }

    if (--_probe_state->active_coroutines > 0) {
        co_return;
    }
    // only the last coroutine does
    auto match_count = _probe_state->match_count;
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_null_aware_anti_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (_probe_state->null_array != nullptr && (*_probe_state->null_array).at(i) == 1) {
            // when left table col value is null needs match all rows in right table
            for (size_t j = 1; j < _table_items->row_count + 1; j++) {
                MATCH_RIGHT_TABLE_ROWS()
                RETURN_IF_CHUNK_FULL()
            }
        } else if (_table_items->key_columns[0]->is_nullable()) {
            // when left table col value not hits in hash table needs match all null value rows in right table
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(_table_items->key_columns[0]);
            auto& null_array = nullable_column->null_column()->get_data();
            // TODO: optimize me
            for (size_t j = 1; j < _table_items->row_count + 1; j++) {
                if (null_array[j] == 1) {
                    MATCH_RIGHT_TABLE_ROWS()
                    RETURN_IF_CHUNK_FULL()
                }
            }
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                _probe_state->probe_match_index[i]++;
                match_count++;
                _probe_state->cur_row_match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }

        if (_probe_state->cur_row_match_count <= 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        }
        _probe_state->cur_row_match_count = 0;
    }
    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::
        _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
                RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;
    size_t i = _probe_state->cur_probe_index;

    PROCESS_PROBE_STAGE_FOR_RIGHT_JOIN_WITH_OTHER_CONJUNCT()

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            continue;
        }

        while (build_index != 0) {
            if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = build_index;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
            build_index = _table_items->next[build_index];
        }
    }

    PROBE_OVER()
}

template <LogicalType LT, class BuildFunc, class ProbeFunc>
template <bool first_probe>
void JoinHashMap<LT, BuildFunc, ProbeFunc>::_probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(
        RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data) {
    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->cur_probe_index;
        _probe_state->build_index[0] = _probe_state->cur_build_index;
        match_count = 1;
        if (_probe_state->next[i] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[i];
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (ProbeFunc().equal(build_data[build_index], probe_data[i])) {
                    _probe_state->probe_index[match_count] = i;
                    _probe_state->build_index[match_count] = build_index;
                    _probe_state->probe_match_index[i]++;
                    _probe_state->cur_row_match_count++;
                    match_count++;

                    RETURN_IF_CHUNK_FULL()
                }
                build_index = _table_items->next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                _probe_state->probe_index[match_count] = i;
                _probe_state->build_index[match_count] = 0;
                match_count++;

                RETURN_IF_CHUNK_FULL()
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    PROBE_OVER()
}

template <bool is_remain>
Status JoinHashTable::lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
    switch (_hash_map_type) {
#define M(NAME)                                                            \
    case JoinHashMapType::NAME:                                            \
        _##NAME->lazy_output<is_remain>(state, probe_chunk, result_chunk); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*result_chunk)->downgrade());
    }
    return Status::OK();
}

#undef JOIN_HASH_MAP_TPP
} // namespace starrocks
