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

#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_H

#include "join_hash_table_descriptor.h"
#include "join_type_traits.h"

#if defined(__aarch64__)
#include "arm_acle.h"
#endif

namespace starrocks {

class ColumnRef;

// ------------------------------------------------------------------------------------
// JoinHashMapForEmpty
// ------------------------------------------------------------------------------------

// When hash table is empty, specific its implemention.
// TODO: Merge with JoinHashMap?
class JoinHashMapForEmpty {
public:
    explicit JoinHashMapForEmpty(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state) {}
    void probe_prepare(RuntimeState* state) {}
    void build(RuntimeState* state) {}
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain) {
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
        switch (_table_items->join_type) {
        case TJoinOp::FULL_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        case TJoinOp::LEFT_OUTER_JOIN:
        case TJoinOp::ASOF_LEFT_OUTER_JOIN: {
            _probe_state->count = (*probe_chunk)->num_rows();
            _probe_output<false>(probe_chunk, chunk);
            _build_output<false>(chunk);

            if (_table_items->enable_late_materialization) {
                _probe_index_output(chunk);
            }
            break;
        }
        default: {
            break;
        }
        }
        return;
    }
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
        // For RIGHT ANTI-JOIN, RIGHT SEMI-JOIN, FULL OUTER-JOIN, right table is empty,
        // do nothing for probe_remain.
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
        return;
    }

    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
        if ((*result_chunk)->num_rows() < _probe_state->count) {
            _probe_state->match_flag = JoinMatchFlag::NORMAL;
            _probe_state->count = (*result_chunk)->num_rows();
        }

        (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);

        _probe_output<true>(probe_chunk, result_chunk);
        _build_output<true>(result_chunk);
        _probe_state->count = 0;
    }

private:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_probe_column_timer);
        bool to_nullable = _table_items->left_to_nullable;
        for (size_t i = 0; i < _table_items->probe_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
                if (!column->is_nullable()) {
                    _copy_probe_column(&column, chunk, slot, to_nullable);
                } else {
                    _copy_probe_nullable_column(&column, chunk, slot);
                }
            }
        }
    }

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            if (to_nullable) {
                MutableColumnPtr dest_column = NullableColumn::create((*src_column)->as_mutable_ptr(),
                                                                      NullColumn::create(_probe_state->count));
                (*chunk)->append_column(std::move(dest_column), slot->id());
            } else {
                (*chunk)->append_column(*src_column, slot->id());
            }
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            (*chunk)->append_column(*src_column, slot->id());
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_build_column_timer);

        for (size_t i = 0; i < _table_items->build_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
                dest_column->append_nulls(_probe_state->count);
                (*chunk)->append_column(std::move(dest_column), slot->id());
            }
        }
    }

    void _probe_index_output(ChunkPtr* chunk) {
        _probe_state->probe_index_column->resize(_probe_state->count);
        auto* col = down_cast<UInt32Column*>(_probe_state->probe_index_column.get());
        std::iota(col->get_data().begin(), col->get_data().end(), 0);
        (*chunk)->append_column(_probe_state->probe_index_column, Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);
    }

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

// ------------------------------------------------------------------------------------
// JoinHashMap
// ------------------------------------------------------------------------------------

// The JoinHashMap uses the following three template classes:
// - BuildKeyConstructor and ProbeKeyConstructor are employed to construct join keys from the input builder and prober data segments.
// - JoinHashMapMethod facilitates the creation and probing of the hash map structure.
//
// The BuildKeyConstructor, ProbeKeyConstructor, and JoinHashMapMethod use `prepare` or `prepare_build` methods to allocate required memory structures.
//
// There are two phases:
// - Building Phase:
//   - Invoke `BuildKeyConstructor::build_key` to generate the builder join key,
//     then `JoinHashMapMethod::construct_hash_table` to construct the hash map.
// - Probing Phase:
//   - Invoke `ProbeKeyConstructor::build_key` to assemble the prober join key,
//     then `JoinHashMapMethod::lookup_init` to initialize the chained buckets for each row.

template <LogicalType LT, JoinKeyConstructorType CT, JoinHashMapMethodType MT>
class JoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using BuildKeyConstructor = typename JoinKeyConstructorTypeTraits<CT, LT>::BuildType;
    using ProbeKeyConstructor = typename JoinKeyConstructorTypeTraits<CT, LT>::ProbeType;
    using HashMapMethod = typename JoinHashMapMethodTypeTraits<MT, LT>::MapType;

    explicit JoinHashMap(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state);
    void probe_prepare(RuntimeState* state);

    void build(RuntimeState* state);
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain);
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain);
    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

private:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk);
    template <bool is_lazy>
    void _probe_null_output(ChunkPtr* chunk, size_t count);

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk);
    void _build_default_output(ChunkPtr* chunk, size_t count);

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _probe_index_output(ChunkPtr* chunk);
    void _build_index_output(ChunkPtr* chunk);

    void _search_ht(RuntimeState* state, ChunkPtr* probe_chunk);
    void _search_ht_remain(RuntimeState* state);

    template <bool first_probe, bool is_collision_free_and_unique>
    void _search_ht_impl(RuntimeState* state, const ImmBuffer<CppType> build_data, const ImmBuffer<CppType> data);

    // for one key inner join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht(RuntimeState* state, const ImmBuffer<CppType> build_data, const ImmBuffer<CppType> probe_data);

    HashTableProbeState::ProbeCoroutine _probe_from_ht(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                                       const ImmBuffer<CppType> probe_data);

    template <bool first_probe>
    void _probe_coroutine(RuntimeState* state, const ImmBuffer<CppType> build_data,
                          const ImmBuffer<CppType> probe_data);

    // for one key left outer join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_left_outer_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                            const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                           const ImmBuffer<CppType> build_data,
                                                                           const ImmBuffer<CppType> probe_data);
    bool _contains_probe_row(RuntimeState* state, const ImmBuffer<CppType> build_data,
                             const ImmBuffer<CppType> probe_data, uint32_t probe_index);
    // for one key left semi join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_left_semi_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                           const ImmBuffer<CppType> probe_data);

    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                          const ImmBuffer<CppType> build_data,
                                                                          const ImmBuffer<CppType> probe_data);
    // for one key left anti join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_left_anti_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                           const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                          const ImmBuffer<CppType> build_data,
                                                                          const ImmBuffer<CppType> probe_data);

    // for one key right outer join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_right_outer_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                             const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_outer_join(RuntimeState* state,
                                                                            const ImmBuffer<CppType> build_data,
                                                                            const ImmBuffer<CppType> probe_data);

    // for one key right semi join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_right_semi_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                            const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_semi_join(RuntimeState* state,
                                                                           const ImmBuffer<CppType> build_data,
                                                                           const ImmBuffer<CppType> probe_data);

    // for one key right anti join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_right_anti_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                            const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_anti_join(RuntimeState* state,
                                                                           const ImmBuffer<CppType> build_data,
                                                                           const ImmBuffer<CppType> probe_data);

    // for one key full outer join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_full_outer_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                            const ImmBuffer<CppType> probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_full_outer_join(RuntimeState* state,
                                                                           const ImmBuffer<CppType> build_data,
                                                                           const ImmBuffer<CppType> probe_data);

    // for left semi join with other join conjunct
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_left_semi_join_with_other_conjunct(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                                               const ImmBuffer<CppType> probe_data);

    // for null aware anti join with other join conjunct
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_null_aware_anti_join_with_other_conjunct(RuntimeState* state,
                                                                     const ImmBuffer<CppType> build_data,
                                                                     const ImmBuffer<CppType> probe_data);

    // for one key right outer join with other conjunct
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
            RuntimeState* state, const ImmBuffer<CppType> build_data, const ImmBuffer<CppType> probe_data);

    // for one key full outer join with other join conjunct
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(
            RuntimeState* state, const ImmBuffer<CppType> build_data, const ImmBuffer<CppType> probe_data);

    // for AsOf inner join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_asof_inner_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                            const ImmBuffer<CppType> probe_data);
    // Note: This coroutine version is never called for ASOF JOIN as coroutines are disabled
    // It exists only to satisfy template instantiation requirements
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_asof_inner_join(RuntimeState* state,
                                                                           const ImmBuffer<CppType> build_data,
                                                                           const ImmBuffer<CppType> probe_data) {
        co_return;
    };

    // for asof left outer join
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_asof_left_outer_join(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                                 const ImmBuffer<CppType> probe_data);
    // Note: This coroutine version is never called for ASOF JOIN as coroutines are disabled
    // It exists only to satisfy template instantiation requirements
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_asof_left_outer_join(RuntimeState* state,
                                                                                const ImmBuffer<CppType> build_data,
                                                                                const ImmBuffer<CppType> probe_data) {
        co_return;
    };

    // for asof left outer join with other conjunct
    template <bool first_probe, bool is_collision_free_and_unique>
    void _probe_from_ht_for_asof_left_outer_join_with_other_conjunct(RuntimeState* state, const ImmBuffer<CppType> build_data,
                                                                     const ImmBuffer<CppType> probe_data);

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

// ------------------------------------------------------------------------------------
// JoinHashTable
// ------------------------------------------------------------------------------------

class JoinHashTable {
public:
    JoinHashTable() = default;
    ~JoinHashTable() = default;

    // Disable copy ctor and assignment.
    JoinHashTable(const JoinHashTable&) = delete;
    JoinHashTable& operator=(const JoinHashTable&) = delete;
    // Enable move ctor and move assignment.
    JoinHashTable(JoinHashTable&&) = default;
    JoinHashTable& operator=(JoinHashTable&&) = default;

    // Clone a new hash table with the same hash table as this,
    // and the different probe state from this.
    JoinHashTable clone_readable_table();
    void set_probe_profile(RuntimeProfile::Counter* search_ht_timer, RuntimeProfile::Counter* output_probe_column_timer,
                           RuntimeProfile::Counter* output_build_column_timer, RuntimeProfile::Counter* probe_counter);

    void create(const HashTableParam& param);
    void close();

    Status build(RuntimeState* state);
    void reset_probe_state(RuntimeState* state);
    Status probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* eos);
    Status probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    template <bool is_remain>
    Status lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

    void append_chunk(const ChunkPtr& chunk, const Columns& key_columns);
    void merge_ht(const JoinHashTable& ht);
    // convert input column to spill schema order
    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const;

    const ChunkPtr& get_build_chunk() const { return _table_items->build_chunk; }
    Columns& get_key_columns() { return _table_items->key_columns; }
    const Columns& get_key_columns() const { return _table_items->key_columns; }
    uint32_t get_row_count() const { return _table_items->row_count; }
    size_t get_probe_column_count() const { return _table_items->probe_column_count; }
    size_t get_output_probe_column_count() const { return _table_items->output_probe_column_count; }
    size_t get_build_column_count() const { return _table_items->build_column_count; }
    size_t get_output_build_column_count() const { return _table_items->output_build_column_count; }
    size_t get_bucket_size() const { return _table_items->bucket_size; }
    float get_keys_per_bucket() const;
    std::string get_hash_map_type() const;
    void remove_duplicate_index(Filter* filter);
    JoinHashTableItems* table_items() const { return _table_items.get(); }

    int64_t mem_usage() const;

    template <class Visitor>
    auto visit(Visitor&& visitor) const {
        return std::visit(std::forward<Visitor>(visitor), _hash_map);
    }

private:
    void _init_probe_column(const HashTableParam& param);
    void _init_build_column(const HashTableParam& param);
    void _init_join_keys();

    Status _upgrade_key_columns_if_overflow();

    void _remove_duplicate_index_for_left_outer_join(Filter* filter);
    void _remove_duplicate_index_for_left_semi_join(Filter* filter);
    void _remove_duplicate_index_for_left_anti_join(Filter* filter);
    void _remove_duplicate_index_for_right_outer_join(Filter* filter);
    void _remove_duplicate_index_for_right_semi_join(Filter* filter);
    void _remove_duplicate_index_for_right_anti_join(Filter* filter);
    void _remove_duplicate_index_for_full_outer_join(Filter* filter);

#define JoinHashMapForIntBigintKey(MT)                                                                                \
    std::unique_ptr<JoinHashMap<TYPE_INT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,               \
            std::unique_ptr<JoinHashMap<TYPE_BIGINT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,    \
            std::unique_ptr<                                                                                          \
                    JoinHashMap<TYPE_INT, JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, JoinHashMapMethodType::MT>>, \
            std::unique_ptr<JoinHashMap<TYPE_BIGINT, JoinKeyConstructorType::SERIALIZED_FIXED_SIZE,                   \
                                        JoinHashMapMethodType::MT>>

#define JoinHashMapForSmallKey(MT)                                                                                  \
    std::unique_ptr<JoinHashMap<TYPE_BOOLEAN, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,         \
            std::unique_ptr<JoinHashMap<TYPE_TINYINT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>, \
            std::unique_ptr<JoinHashMap<TYPE_SMALLINT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>

#define JoinHashMapForNonSmallKey(MT)                                                                                  \
    std::unique_ptr<JoinHashMap<TYPE_INT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,                \
            std::unique_ptr<JoinHashMap<TYPE_BIGINT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,     \
            std::unique_ptr<JoinHashMap<TYPE_LARGEINT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,   \
            std::unique_ptr<JoinHashMap<TYPE_FLOAT, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,      \
            std::unique_ptr<JoinHashMap<TYPE_DOUBLE, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,     \
            std::unique_ptr<JoinHashMap<TYPE_DATE, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,       \
            std::unique_ptr<JoinHashMap<TYPE_DATETIME, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,   \
            std::unique_ptr<JoinHashMap<TYPE_DECIMALV2, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,  \
            std::unique_ptr<JoinHashMap<TYPE_DECIMAL32, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,  \
            std::unique_ptr<JoinHashMap<TYPE_DECIMAL64, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,  \
            std::unique_ptr<JoinHashMap<TYPE_DECIMAL128, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>, \
            std::unique_ptr<JoinHashMap<TYPE_VARCHAR, JoinKeyConstructorType::ONE_KEY, JoinHashMapMethodType::MT>>,    \
                                                                                                                       \
            std::unique_ptr<                                                                                           \
                    JoinHashMap<TYPE_INT, JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, JoinHashMapMethodType::MT>>,  \
            std::unique_ptr<JoinHashMap<TYPE_BIGINT, JoinKeyConstructorType::SERIALIZED_FIXED_SIZE,                    \
                                        JoinHashMapMethodType::MT>>,                                                   \
            std::unique_ptr<JoinHashMap<TYPE_LARGEINT, JoinKeyConstructorType::SERIALIZED_FIXED_SIZE,                  \
                                        JoinHashMapMethodType::MT>>,                                                   \
                                                                                                                       \
            std::unique_ptr<JoinHashMap<TYPE_VARCHAR, JoinKeyConstructorType::SERIALIZED, JoinHashMapMethodType::MT>>

    using JoinHashMapVariant = std::variant<std::unique_ptr<JoinHashMapForEmpty>,
                                            JoinHashMapForSmallKey(DIRECT_MAPPING),                //
                                            JoinHashMapForNonSmallKey(BUCKET_CHAINED),             //
                                            JoinHashMapForNonSmallKey(LINEAR_CHAINED),             //
                                            JoinHashMapForNonSmallKey(LINEAR_CHAINED_SET),         //
                                            JoinHashMapForNonSmallKey(LINEAR_CHAINED_ASOF),        //
                                            JoinHashMapForIntBigintKey(RANGE_DIRECT_MAPPING),      //
                                            JoinHashMapForIntBigintKey(RANGE_DIRECT_MAPPING_SET),  //
                                            JoinHashMapForIntBigintKey(DENSE_RANGE_DIRECT_MAPPING) //
                                            >;

#undef JoinHashMapForNonSmallKey
#undef JoinHashMapForSmallKey
#undef JoinHashMapForIntBigintKey

    bool _is_empty_map = true;
    JoinKeyConstructorUnaryType _key_constructor_type;
    JoinHashMapMethodUnaryType _hash_map_method_type;
    JoinHashMapVariant _hash_map;

    std::shared_ptr<JoinHashTableItems> _table_items;
    std::unique_ptr<HashTableProbeState> _probe_state = std::make_unique<HashTableProbeState>();
};

} // namespace starrocks

#ifndef JOIN_HASH_MAP_TPP
#include "join_hash_map.tpp"

#endif

#undef JOIN_HASH_MAP_H
