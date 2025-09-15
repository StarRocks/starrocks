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

#include "join_hash_map.h"

#include <column/chunk.h>
#include <runtime/descriptors.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/hash_join_node.h"
#include "serde/column_array_serde.h"
#include "simd/simd.h"
#include "types/logical_type_infra.h"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// JoinHashMapSelector
// ------------------------------------------------------------------------------------

class JoinHashMapSelector {
public:
    static std::tuple<JoinKeyConstructorUnaryType, JoinHashMapMethodUnaryType> construct_key_and_determine_hash_map(
            RuntimeState* state, JoinHashTableItems* table_items);

private:
    static size_t _get_size_of_fixed_and_contiguous_type(LogicalType data_type);
    static JoinKeyConstructorUnaryType _determine_key_constructor(JoinHashTableItems* table_items);
    static JoinHashMapMethodUnaryType _determine_hash_map_method(RuntimeState* state, JoinHashTableItems* table_items,
                                                                 JoinKeyConstructorUnaryType key_constructor_type);
    // @return: <can_use, JoinHashMapMethodUnaryType>, where `JoinHashMapMethodUnaryType` is effective only when `can_use` is true.
    template <LogicalType LT>
    static std::pair<bool, JoinHashMapMethodUnaryType> _try_use_range_direct_mapping(RuntimeState* state,
                                                                                     JoinHashTableItems* table_items);
    // @return: <can_use, JoinHashMapMethodUnaryType>, where `JoinHashMapMethodUnaryType` is effective only when `can_use` is true.
    template <LogicalType LT>
    static std::pair<bool, JoinHashMapMethodUnaryType> _try_use_linear_chained(RuntimeState* state,
                                                                               JoinHashTableItems* table_items);
    
    // Helper method to get fallback hash map method type based on join type
    template <LogicalType LT>
    static std::pair<bool, JoinHashMapMethodUnaryType> _get_fallback_method(bool is_asof_join_type);
};

std::tuple<JoinKeyConstructorUnaryType, JoinHashMapMethodUnaryType>
JoinHashMapSelector::construct_key_and_determine_hash_map(RuntimeState* state, JoinHashTableItems* table_items) {
    DCHECK_GT(table_items->row_count, 0);

    const auto key_constructor_type = _determine_key_constructor(table_items);
    dispatch_join_key_constructor_unary(key_constructor_type, [&]<JoinKeyConstructorUnaryType CT> {
        using KeyConstructor = typename JoinKeyConstructorUnaryTypeTraits<CT>::BuildType;
        KeyConstructor().prepare(state, table_items);
        KeyConstructor().build_key(state, table_items);
    });

    const auto method_type = _determine_hash_map_method(state, table_items, key_constructor_type);
    return {key_constructor_type, method_type};
}

size_t JoinHashMapSelector::_get_size_of_fixed_and_contiguous_type(const LogicalType data_type) {
    switch (data_type) {
    case LogicalType::TYPE_BOOLEAN:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_BOOLEAN>::CppType);
    case LogicalType::TYPE_TINYINT:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_TINYINT>::CppType);
    case LogicalType::TYPE_SMALLINT:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_SMALLINT>::CppType);
    case LogicalType::TYPE_INT:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_INT>::CppType);
    case LogicalType::TYPE_BIGINT:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_BIGINT>::CppType);
    case LogicalType::TYPE_FLOAT:
        // float will be convert to double, so current can't reach here
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_FLOAT>::CppType);
    case LogicalType::TYPE_DOUBLE:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_DOUBLE>::CppType);
    case LogicalType::TYPE_DATE:
        // date will be convert to datetime, so current can't reach here
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_DATE>::CppType);
    case LogicalType::TYPE_DATETIME:
        return sizeof(RunTimeTypeTraits<LogicalType::TYPE_DATETIME>::CppType);
    default:
        return 0;
    }
}

JoinKeyConstructorUnaryType JoinHashMapSelector::_determine_key_constructor(JoinHashTableItems* table_items) {
    const size_t num_keys = table_items->join_keys.size();
    DCHECK_GT(num_keys, 0);

    for (size_t i = 0; i < num_keys; i++) {
        if (!table_items->key_columns[i]->has_null()) {
            table_items->join_keys[i].is_null_safe_equal = false;
        }
    }

    if (num_keys == 1 && !table_items->join_keys[0].is_null_safe_equal) {
        return dispatch_join_logical_type(
                table_items->join_keys[0].type->type, JoinKeyConstructorUnaryType::SERIALIZED_VARCHAR,
                []<LogicalType LT>() {
                    static constexpr auto MAPPING_LT = LT == TYPE_CHAR ? TYPE_VARCHAR : LT;
                    return JoinKeyConstructorTypeTraits<JoinKeyConstructorType::ONE_KEY, MAPPING_LT>::unary_type;
                });
    }

    size_t total_size_in_byte = 0;
    for (const auto& join_key : table_items->join_keys) {
        if (join_key.is_null_safe_equal) {
            total_size_in_byte += 1;
        }
        size_t s = _get_size_of_fixed_and_contiguous_type(join_key.type->type);
        if (s > 0) {
            total_size_in_byte += s;
        } else {
            return JoinKeyConstructorUnaryType::SERIALIZED_VARCHAR;
        }
    }

    if (total_size_in_byte <= 4) {
        return JoinKeyConstructorUnaryType::SERIALIZED_FIXED_SIZE_INT;
    }
    if (total_size_in_byte <= 8) {
        return JoinKeyConstructorUnaryType::SERIALIZED_FIXED_SIZE_BIGINT;
    }
    if (total_size_in_byte <= 16) {
        return JoinKeyConstructorUnaryType::SERIALIZED_FIXED_SIZE_LARGEINT;
    }

    return JoinKeyConstructorUnaryType::SERIALIZED_VARCHAR;
}

JoinHashMapMethodUnaryType JoinHashMapSelector::_determine_hash_map_method(
        RuntimeState* state, JoinHashTableItems* table_items, JoinKeyConstructorUnaryType key_constructor_type) {
    return dispatch_join_key_constructor_unary(key_constructor_type, [&]<JoinKeyConstructorUnaryType CUT>() {
        static constexpr auto LT = JoinKeyConstructorUnaryTypeTraits<CUT>::logical_type;
        static constexpr auto CT = JoinKeyConstructorUnaryTypeTraits<CUT>::key_constructor_type;

        if constexpr (LT == TYPE_BOOLEAN || LT == TYPE_TINYINT || LT == TYPE_SMALLINT) {
            return JoinHashMapMethodTypeTraits<JoinHashMapMethodType::DIRECT_MAPPING, LT>::unary_type;
        } else {
            if constexpr (CT == JoinKeyConstructorType::ONE_KEY && (LT == TYPE_INT || LT == TYPE_BIGINT)) {
                const auto [can_use, hash_map_type] = _try_use_range_direct_mapping<LT>(state, table_items);
                if (can_use) {
                    return hash_map_type;
                }
            }

            if (const auto [can_use, hash_map_type] = _try_use_linear_chained<LT>(state, table_items); can_use) {
                return hash_map_type;
            }

            return _get_fallback_method<LT>(is_asof_join(table_items->join_type)).second;
        }
    });
}

template <LogicalType LT>
std::pair<bool, JoinHashMapMethodUnaryType> JoinHashMapSelector::_get_fallback_method(bool is_asof_join_type) {
    return {false, is_asof_join_type ?
            JoinHashMapMethodTypeTraits<JoinHashMapMethodType::LINEAR_CHAINED_ASOF, LT>::unary_type :
            JoinHashMapMethodTypeTraits<JoinHashMapMethodType::BUCKET_CHAINED, LT>::unary_type};
}

template <LogicalType LT>
std::pair<bool, JoinHashMapMethodUnaryType> JoinHashMapSelector::_try_use_range_direct_mapping(
        RuntimeState* state, JoinHashTableItems* table_items) {
    bool is_asof_join_type = is_asof_join(table_items->join_type);

    if (!state->enable_hash_join_range_direct_mapping_opt()) {
        return _get_fallback_method<LT>(is_asof_join_type);
    }

    using KeyConstructor = typename JoinKeyConstructorTypeTraits<JoinKeyConstructorType::ONE_KEY, LT>::BuildType;
    const auto* keys = KeyConstructor().get_key_data(*table_items).data();
    const size_t num_rows = table_items->row_count + 1;
    const int64_t min_value = *std::min_element(keys + 1, keys + num_rows);
    const int64_t max_value = *std::max_element(keys + 1, keys + num_rows);

    // `max_value - min_value + 1` will be overflow.
    if (min_value == std::numeric_limits<int64_t>::min() && max_value == std::numeric_limits<int64_t>::max()) {
        return _get_fallback_method<LT>(is_asof_join_type);
    }

    const uint64_t value_interval = static_cast<uint64_t>(max_value) - min_value + 1;
    if (value_interval >= std::numeric_limits<uint32_t>::max()) {
        return _get_fallback_method<LT>(is_asof_join_type);
    }

    table_items->min_value = min_value;
    table_items->max_value = max_value;

    const uint64_t row_count = table_items->row_count;
    const uint64_t bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    static const size_t HALF_L3_CACHE_SIZE = [] {
        static constexpr size_t DEFAULT_L3_CACHE_SIZE = 32 * 1024 * 1024;
        const auto& cache_sizes = CpuInfo::get_cache_sizes();
        const auto l3_cache = cache_sizes[CpuInfo::L3_CACHE] ? cache_sizes[CpuInfo::L3_CACHE] : DEFAULT_L3_CACHE_SIZE;
        return l3_cache / 2;
    }();
    static const size_t L2_CACHE_SIZE = CpuInfo::get_l2_cache_size();

    if ((table_items->join_type == TJoinOp::LEFT_ANTI_JOIN || table_items->join_type == TJoinOp::LEFT_SEMI_JOIN) &&
        !table_items->with_other_conjunct) {
        const uint64_t memory_usage = (value_interval + 7) / 8;
        // one bit vs. 8 bytes(`first` and `next`)
        if (memory_usage <= bucket_size * 64 || memory_usage <= HALF_L3_CACHE_SIZE) {
            return {true, JoinHashMapMethodTypeTraits<JoinHashMapMethodType::RANGE_DIRECT_MAPPING_SET, LT>::unary_type};
        }
    } else {
        if (value_interval <= bucket_size || value_interval <= L2_CACHE_SIZE) {
            return {true, JoinHashMapMethodTypeTraits<JoinHashMapMethodType::RANGE_DIRECT_MAPPING, LT>::unary_type};
        }

        // BUCKET_CHAINE:
        // - The size of `table_items.first` is `bucket_size`.
        // - Assuming we can use 10% more memory than `BUCKET_CHAINED`, so the total is `(bucket_size + bucket_size / 10) * 4`.
        // DENSE_RANGE_DIRECT_MAPPING`:
        // - Each value index uses 2 bits, so totally uses `value_interval / 4` bytes.
        // - The size of `table_items.first` only needs `row_count`.
        if (value_interval / 4 + row_count * 4 <= (bucket_size + bucket_size / 10) * 4) {
            return {true,
                    JoinHashMapMethodTypeTraits<JoinHashMapMethodType::DENSE_RANGE_DIRECT_MAPPING, LT>::unary_type};
        }
    }

    return _get_fallback_method<LT>(is_asof_join_type);
}

template <LogicalType LT>
std::pair<bool, JoinHashMapMethodUnaryType> JoinHashMapSelector::_try_use_linear_chained(
        RuntimeState* state, JoinHashTableItems* table_items) {
    bool is_asof_join_type = is_asof_join(table_items->join_type);
    if (!state->enable_hash_join_linear_chained_opt()) {
        return _get_fallback_method<LT>(is_asof_join_type);
    }

    const uint64_t bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    if (bucket_size > LinearChainedJoinHashMap<LT>::max_supported_bucket_size()) {
        return _get_fallback_method<LT>(is_asof_join_type);
    }

    const bool is_left_anti_join_without_other_conjunct =
            (table_items->join_type == TJoinOp::LEFT_ANTI_JOIN || table_items->join_type == TJoinOp::LEFT_SEMI_JOIN) &&
            !table_items->with_other_conjunct;
    if (is_left_anti_join_without_other_conjunct) {
        return {true, JoinHashMapMethodTypeTraits<JoinHashMapMethodType::LINEAR_CHAINED_SET, LT>::unary_type};
    } else {
        return {true, JoinHashMapMethodTypeTraits<JoinHashMapMethodType::LINEAR_CHAINED, LT>::unary_type};
    }
}

// ------------------------------------------------------------------------------------
// JoinHashMap
// ------------------------------------------------------------------------------------

template <LogicalType LT, JoinKeyConstructorType CT, JoinHashMapMethodType MT>
void JoinHashMap<LT, CT, MT>::_probe_index_output(ChunkPtr* chunk) {
    _probe_state->probe_index.resize((*chunk)->num_rows());
    (*chunk)->append_column(_probe_state->probe_index_column, Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);
}

template <LogicalType LT, JoinKeyConstructorType CT, JoinHashMapMethodType MT>
void JoinHashMap<LT, CT, MT>::_build_index_output(ChunkPtr* chunk) {
    _probe_state->build_index.resize(_probe_state->count);
    (*chunk)->append_column(_probe_state->build_index_column, Chunk::HASH_JOIN_BUILD_INDEX_SLOT_ID);
}

// ------------------------------------------------------------------------------------
// JoinHashTable
// ------------------------------------------------------------------------------------

JoinHashTable JoinHashTable::clone_readable_table() {
    JoinHashTable ht;

    ht._is_empty_map = this->_is_empty_map;
    ht._key_constructor_type = this->_key_constructor_type;
    ht._hash_map_method_type = this->_hash_map_method_type;

    ht._table_items = this->_table_items;
    // Clone a new probe state.
    ht._probe_state = std::make_unique<HashTableProbeState>(*this->_probe_state);

    visit([&](const auto& hash_map) {
        using HashMapType = std::decay_t<decltype(*hash_map)>;
        ht._hash_map = std::make_unique<HashMapType>(ht._table_items.get(), ht._probe_state.get());
    });

    return ht;
}

void JoinHashTable::set_probe_profile(RuntimeProfile::Counter* search_ht_timer,
                                      RuntimeProfile::Counter* output_probe_column_timer,
                                      RuntimeProfile::Counter* output_build_column_timer,
                                      RuntimeProfile::Counter* probe_count) {
    if (_probe_state == nullptr) return;
    _probe_state->search_ht_timer = search_ht_timer;
    _probe_state->output_probe_column_timer = output_probe_column_timer;
    _probe_state->output_build_column_timer = output_build_column_timer;
    _probe_state->probe_counter = probe_count;
}

float JoinHashTable::get_keys_per_bucket() const {
    return _table_items->get_keys_per_bucket();
}

std::string JoinHashTable::get_hash_map_type() const {
    if (const bool has_not_built = visit([&](const auto& map) { return map == nullptr; }); has_not_built) {
        return "NONE";
    }

    if (_is_empty_map) {
        return "EMPTY";
    }

    return dispatch_join_hash_map(
            _key_constructor_type, _hash_map_method_type,
            [&]<JoinKeyConstructorUnaryType CUT, JoinHashMapMethodUnaryType MUT>() {
                static constexpr auto LT = JoinKeyConstructorUnaryTypeTraits<CUT>::logical_type;
                static constexpr auto CT = JoinKeyConstructorUnaryTypeTraits<CUT>::key_constructor_type;
                static constexpr auto MT = JoinHashMapMethodUnaryTypeTraits<MUT>::map_method_type;
                return fmt::format("{}-{}-{}", join_key_constructor_type_to_string(CT),
                                   join_hash_map_method_type_to_string(MT), logical_type_to_string(LT));
            });
}

void JoinHashTable::close() {
    _table_items.reset();
    _probe_state.reset();
    _probe_state = nullptr;
    _table_items = nullptr;
}

// may be called more than once if spill
void JoinHashTable::create(const HashTableParam& param) {
    _table_items = std::make_shared<JoinHashTableItems>();
    if (_probe_state == nullptr) {
        _probe_state = std::make_unique<HashTableProbeState>();
        _probe_state->search_ht_timer = param.search_ht_timer;
        _probe_state->output_probe_column_timer = param.output_probe_column_timer;
        _probe_state->output_build_column_timer = param.output_build_column_timer;
        _probe_state->probe_counter = param.probe_counter;
    }

    _table_items->build_chunk = std::make_shared<Chunk>();
    _table_items->with_other_conjunct = param.with_other_conjunct;
    _table_items->join_type = param.join_type;
    _table_items->enable_late_materialization = param.enable_late_materialization;
    _table_items->asof_join_condition_desc = param.asof_join_condition_desc;

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN ||
        _table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN) {
        _table_items->left_to_nullable = true;
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_OUTER_JOIN ||
               _table_items->join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN) {
        _table_items->right_to_nullable = true;
    } else if (_table_items->join_type == TJoinOp::FULL_OUTER_JOIN) {
        _table_items->left_to_nullable = true;
        _table_items->right_to_nullable = true;
    }

    if (is_asof_join(_table_items->join_type)) {
        auto variant_index = get_asof_variant_index(
            _table_items->asof_join_condition_desc.build_logical_type,
            _table_items->asof_join_condition_desc.condition_op);
        _table_items->asof_index_vector = create_asof_index_vector(variant_index);
    }

    _table_items->join_keys = param.join_keys;

    _init_probe_column(param);
    _init_build_column(param);

    _init_join_keys();
}

void JoinHashTable::_init_probe_column(const HashTableParam& param) {
    const auto& probe_desc = *param.probe_row_desc;
    for (const auto& tuple_desc : probe_desc.tuple_descriptors()) {
        for (const auto& slot : tuple_desc->slots()) {
            HashTableSlotDescriptor hash_table_slot;
            hash_table_slot.slot = slot;

            if (param.enable_late_materialization) {
                if (param.probe_output_slots.empty()) {
                    // Empty means need output all
                    hash_table_slot.need_output = true;
                    hash_table_slot.need_lazy_materialize = false;
                    _table_items->output_probe_column_count++;
                } else if (std::find(param.probe_output_slots.begin(), param.probe_output_slots.end(), slot->id()) !=
                           param.probe_output_slots.end()) {
                    if (param.predicate_slots.empty() ||
                        // Empty means no other predicate, so need output all
                        std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                                param.predicate_slots.end()) {
                        hash_table_slot.need_output = true;
                        hash_table_slot.need_lazy_materialize = false;
                        _table_items->output_probe_column_count++;
                    } else {
                        hash_table_slot.need_output = false;
                        hash_table_slot.need_lazy_materialize = true;
                        _table_items->lazy_output_probe_column_count++;
                    }
                } else {
                    if (param.predicate_slots.empty() ||
                        std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                                param.predicate_slots.end()) {
                        hash_table_slot.need_output = true;
                        hash_table_slot.need_lazy_materialize = false;
                        _table_items->output_probe_column_count++;
                    } else {
                        hash_table_slot.need_output = false;
                        hash_table_slot.need_lazy_materialize = false;
                    }
                }
            } else {
                if (param.probe_output_slots.empty() ||
                    std::find(param.probe_output_slots.begin(), param.probe_output_slots.end(), slot->id()) !=
                            param.probe_output_slots.end() ||
                    std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                            param.predicate_slots.end()) {
                    hash_table_slot.need_output = true;
                    _table_items->output_probe_column_count++;
                } else {
                    hash_table_slot.need_output = false;
                }
            }

            _table_items->probe_slots.emplace_back(hash_table_slot);
            _table_items->probe_column_count++;
        }
    }
}

void JoinHashTable::_init_build_column(const HashTableParam& param) {
    const auto& build_desc = *param.build_row_desc;
    std::unordered_set<SlotId> join_key_col_refs;
    for (const auto& join_key : param.join_keys) {
        if (join_key.col_ref != nullptr) {
            join_key_col_refs.insert(join_key.col_ref->slot_id());
        }
    }
    for (const auto& tuple_desc : build_desc.tuple_descriptors()) {
        for (const auto& slot : tuple_desc->slots()) {
            HashTableSlotDescriptor hash_table_slot;
            hash_table_slot.slot = slot;

            if (param.enable_late_materialization) {
                if (param.build_output_slots.empty()) {
                    hash_table_slot.need_output = true;
                    hash_table_slot.need_lazy_materialize = false;
                    _table_items->output_build_column_count++;
                } else if (std::find(param.build_output_slots.begin(), param.build_output_slots.end(), slot->id()) !=
                           param.build_output_slots.end()) {
                    if (param.predicate_slots.empty() ||
                        std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                                param.predicate_slots.end()) {
                        hash_table_slot.need_output = true;
                        hash_table_slot.need_lazy_materialize = false;
                        _table_items->output_build_column_count++;
                    } else {
                        hash_table_slot.need_output = false;
                        hash_table_slot.need_lazy_materialize = true;
                        _table_items->lazy_output_build_column_count++;
                    }
                } else {
                    if (param.predicate_slots.empty() ||
                        std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                                param.predicate_slots.end()) {
                        hash_table_slot.need_output = true;
                        hash_table_slot.need_lazy_materialize = false;
                        _table_items->output_build_column_count++;
                    } else {
                        hash_table_slot.need_output = false;
                        hash_table_slot.need_lazy_materialize = false;
                    }
                }
            } else {
                if (param.build_output_slots.empty() ||
                    std::find(param.build_output_slots.begin(), param.build_output_slots.end(), slot->id()) !=
                            param.build_output_slots.end() ||
                    std::find(param.predicate_slots.begin(), param.predicate_slots.end(), slot->id()) !=
                            param.predicate_slots.end()) {
                    hash_table_slot.need_output = true;
                    _table_items->output_build_column_count++;
                } else {
                    hash_table_slot.need_output = false;
                }
            }
            _table_items->build_slots.emplace_back(hash_table_slot);
            const auto use_view =
                    (join_key_col_refs.find(slot->id()) == join_key_col_refs.end()) &&
                    (param.column_view_concat_rows_limit >= 0 || param.column_view_concat_bytes_limit >= 0);

            MutableColumnPtr column = ColumnHelper::create_column(slot->type(), slot->is_nullable(), use_view,
                                                                  param.column_view_concat_rows_limit,
                                                                  param.column_view_concat_bytes_limit);
            if (column->is_nullable()) {
                auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);
                nullable_column->append_default_not_null_value();
            } else {
                column->append_default();
            }
            _table_items->build_chunk->append_column(std::move(column), slot->id());
            _table_items->build_column_count++;
        }
    }
}

void JoinHashTable::_init_join_keys() {
    for (const auto& key_desc : _table_items->join_keys) {
        if (key_desc.col_ref) {
            _table_items->key_columns.emplace_back(nullptr);
        } else {
            auto key_column = ColumnHelper::create_column(*key_desc.type, false);
            key_column->append_default();
            _table_items->key_columns.emplace_back(std::move(key_column));
        }
    }
}

int64_t JoinHashTable::mem_usage() const {
    // Theoretically, `_table_items` may be a nullptr after a cancel, even though in practice we haven’t observed any
    // cases where `_table_items` was unexpectedly cleared or left uninitialized.
    // To prevent potential null pointer exceptions, we add a defensive check here.
    if (_table_items == nullptr) {
        LOG(WARNING) << "table_items is nullptr in mem_usage, stack:" << get_stack_trace();
        DCHECK(false);
        return 0;
    }

    int64_t usage = 0;
    if (_table_items->build_chunk != nullptr) {
        usage += _table_items->build_chunk->memory_usage();
    }
    usage += _table_items->first.capacity() * sizeof(uint32_t);
    usage += _table_items->next.capacity() * sizeof(uint32_t);
    if (_table_items->build_pool != nullptr) {
        usage += _table_items->build_pool->total_reserved_bytes();
    }
    if (_probe_state->probe_pool != nullptr) {
        usage += _probe_state->probe_pool->total_reserved_bytes();
    }
    if (_table_items->build_key_column != nullptr) {
        usage += _table_items->build_key_column->memory_usage();
    }
    usage += _table_items->build_slice.size() * sizeof(Slice);
    return usage;
}

Status JoinHashTable::build(RuntimeState* state) {
    RETURN_IF_ERROR(_table_items->build_chunk->upgrade_if_overflow());
    _table_items->has_large_column = _table_items->build_chunk->has_large_column();

    // If the join key is column ref of build chunk, fetch from build chunk directly
    size_t join_key_count = _table_items->join_keys.size();
    for (size_t i = 0; i < join_key_count; i++) {
        if (_table_items->join_keys[i].col_ref != nullptr) {
            SlotId slot_id = _table_items->join_keys[i].col_ref->slot_id();
            _table_items->key_columns[i] = _table_items->build_chunk->get_column_by_slot_id(slot_id);
        }
    }

    RETURN_IF_ERROR(_upgrade_key_columns_if_overflow());

    if (_table_items->row_count == 0) {
        _is_empty_map = true;
        _hash_map = std::make_unique<JoinHashMapForEmpty>(_table_items.get(), _probe_state.get());
    } else {
        _is_empty_map = false;
        std::tie(_key_constructor_type, _hash_map_method_type) =
                JoinHashMapSelector::construct_key_and_determine_hash_map(state, _table_items.get());
        auto create_hash_map = [&]<JoinKeyConstructorUnaryType CUT, JoinHashMapMethodUnaryType MUT>() {
            static constexpr auto LT = JoinKeyConstructorUnaryTypeTraits<CUT>::logical_type;
            if constexpr (LT != JoinHashMapMethodUnaryTypeTraits<MUT>::logical_type) {
                return Status::InvalidArgument(
                        strings::Substitute("Join key logical type {} does not match hash map logical type {}",
                                            static_cast<int>(CUT), static_cast<int>(MUT)));
            } else {
                static constexpr auto CT = JoinKeyConstructorUnaryTypeTraits<CUT>::key_constructor_type;
                static constexpr auto MT = JoinHashMapMethodUnaryTypeTraits<MUT>::map_method_type;
                _hash_map = std::make_unique<JoinHashMap<LT, CT, MT>>(_table_items.get(), _probe_state.get());
                return Status::OK();
            }
        };
        RETURN_IF_ERROR(dispatch_join_hash_map(_key_constructor_type, _hash_map_method_type, create_hash_map));
    }
    visit([&](auto& hash_map) {
        hash_map->build_prepare(state);
        hash_map->probe_prepare(state);
        hash_map->build(state);
    });

    return Status::OK();
}

void JoinHashTable::reset_probe_state(RuntimeState* state) {
    visit([&](const auto& hash_map) {
        auto new_hash_map = std::make_unique<std::decay_t<decltype(*hash_map)>>(_table_items.get(), _probe_state.get());
        new_hash_map->probe_prepare(state);
        _hash_map = std::move(new_hash_map);
    });
}

Status JoinHashTable::probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
                            bool* eos) {
    visit([&](const auto& hash_map) { hash_map->probe(state, key_columns, probe_chunk, chunk, eos); });
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*chunk)->downgrade());
    }
    return Status::OK();
}

Status JoinHashTable::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    visit([&](const auto& hash_map) { hash_map->probe_remain(state, chunk, eos); });
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*chunk)->downgrade());
    }
    return Status::OK();
}

void JoinHashTable::append_chunk(const ChunkPtr& chunk, const Columns& key_columns) {
    auto& columns = _table_items->build_chunk->columns();

    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        SlotDescriptor* slot = _table_items->build_slots[i].slot;
        ColumnPtr& column = chunk->get_column_by_slot_id(slot->id());

        if (!columns[i]->is_nullable() && !columns[i]->is_view() && column->is_nullable()) {
            // upgrade to nullable column
            columns[i] = NullableColumn::create(columns[i], NullColumn::create(columns[i]->size(), 0));
        }
        columns[i]->append(*column);
    }

    for (size_t i = 0; i < _table_items->key_columns.size(); i++) {
        // If the join key is slot ref, will get from build chunk directly,
        // otherwise will append from key_column of input
        if (_table_items->join_keys[i].col_ref == nullptr) {
            // upgrade to nullable column
            if (!_table_items->key_columns[i]->is_nullable() && key_columns[i]->is_nullable()) {
                size_t row_count = _table_items->key_columns[i]->size();
                _table_items->key_columns[i] =
                        NullableColumn::create(_table_items->key_columns[i], NullColumn::create(row_count, 0));
            }
            _table_items->key_columns[i]->append(*key_columns[i]);
        }
    }

    _table_items->row_count += chunk->num_rows();
}

void JoinHashTable::merge_ht(const JoinHashTable& ht) {
    _table_items->row_count += ht._table_items->row_count;

    auto& columns = _table_items->build_chunk->columns();
    auto& other_columns = ht._table_items->build_chunk->columns();

    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        if (!columns[i]->is_nullable() && !columns[i]->is_view() && other_columns[i]->is_nullable()) {
            // upgrade to nullable column
            columns[i] = NullableColumn::create(columns[i], NullColumn::create(columns[i]->size(), 0));
        }
        columns[i]->append(*other_columns[i], 1, other_columns[i]->size() - 1);
    }

    auto& key_columns = _table_items->key_columns;
    auto& other_key_columns = ht._table_items->key_columns;
    for (size_t i = 0; i < key_columns.size(); i++) {
        // If the join key is slot ref, will get from build chunk directly,
        // otherwise will append from key_column of input
        if (_table_items->join_keys[i].col_ref == nullptr) {
            // upgrade to nullable column
            if (!key_columns[i]->is_nullable() && other_key_columns[i]->is_nullable()) {
                const size_t row_count = key_columns[i]->size();
                key_columns[i] = NullableColumn::create(key_columns[i], NullColumn::create(row_count, 0));
            }
            key_columns[i]->append(*other_key_columns[i]);
        }
    }
}

ChunkPtr JoinHashTable::convert_to_spill_schema(const ChunkPtr& chunk) const {
    DCHECK(chunk != nullptr && chunk->num_rows() > 0);
    ChunkPtr output = std::make_shared<Chunk>();
    //
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        SlotDescriptor* slot = _table_items->build_slots[i].slot;
        ColumnPtr& column = chunk->get_column_by_slot_id(slot->id());
        if (slot->is_nullable()) {
            column = ColumnHelper::cast_to_nullable_column(column);
        }
        output->append_column(column, slot->id());
    }

    return output;
}

void JoinHashTable::remove_duplicate_index(Filter* filter) {
    if (_is_empty_map) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
        case TJoinOp::ASOF_LEFT_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::FULL_OUTER_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN: {
            size_t row_count = filter->size();
            for (size_t i = 0; i < row_count; i++) {
                (*filter)[i] = 1;
            }
            break;
        }
        default:
            break;
        }
        return;
    }

    DCHECK_LT(0, _table_items->row_count);
    switch (_table_items->join_type) {
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::ASOF_LEFT_OUTER_JOIN:
        _remove_duplicate_index_for_left_outer_join(filter);
        break;
    case TJoinOp::LEFT_SEMI_JOIN:
        _remove_duplicate_index_for_left_semi_join(filter);
        break;
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        _remove_duplicate_index_for_left_anti_join(filter);
        break;
    case TJoinOp::RIGHT_OUTER_JOIN:
        _remove_duplicate_index_for_right_outer_join(filter);
        break;
    case TJoinOp::RIGHT_SEMI_JOIN:
        _remove_duplicate_index_for_right_semi_join(filter);
        break;
    case TJoinOp::RIGHT_ANTI_JOIN:
        _remove_duplicate_index_for_right_anti_join(filter);
        break;
    case TJoinOp::FULL_OUTER_JOIN:
        _remove_duplicate_index_for_full_outer_join(filter);
        break;
    default:
        break;
    }
}

Status JoinHashTable::_upgrade_key_columns_if_overflow() {
    for (auto& column : _table_items->key_columns) {
        auto ret = column->upgrade_if_overflow();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = ret.value();
        } else {
            continue;
        }
    }
    return Status::OK();
}

void JoinHashTable::_remove_duplicate_index_for_left_outer_join(Filter* filter) {
    size_t row_count = filter->size();

    for (size_t i = 0; i < row_count; i++) {
        if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 0) {
            (*filter)[i] = 1;
            continue;
        }

        if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 1) {
            if ((*filter)[i] == 0) {
                (*filter)[i] = 1;
            }
            continue;
        }

        if ((*filter)[i] == 0) {
            _probe_state->probe_match_index[_probe_state->probe_index[i]]--;
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_left_semi_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if ((*filter)[i] == 1) {
            if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 0) {
                _probe_state->probe_match_index[_probe_state->probe_index[i]] = 1;
            } else {
                (*filter)[i] = 0;
            }
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_left_anti_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 0) {
            (*filter)[i] = 1;
        } else if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 1) {
            _probe_state->probe_match_index[_probe_state->probe_index[i]]--;
            (*filter)[i] = !(*filter)[i];
        } else if ((*filter)[i] == 0) {
            _probe_state->probe_match_index[_probe_state->probe_index[i]]--;
        } else {
            (*filter)[i] = 0;
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_right_outer_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if ((*filter)[i] == 1) {
            _probe_state->build_match_index[_probe_state->build_index[i]] = 1;
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_right_semi_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if ((*filter)[i] == 1) {
            if (_probe_state->build_match_index[_probe_state->build_index[i]] == 0) {
                _probe_state->build_match_index[_probe_state->build_index[i]] = 1;
            } else {
                (*filter)[i] = 0;
            }
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_right_anti_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if ((*filter)[i] == 1) {
            _probe_state->build_match_index[_probe_state->build_index[i]] = 1;
        }
    }
}

void JoinHashTable::_remove_duplicate_index_for_full_outer_join(Filter* filter) {
    size_t row_count = filter->size();
    for (size_t i = 0; i < row_count; i++) {
        if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 0) {
            (*filter)[i] = 1;
            continue;
        }

        if (_probe_state->probe_match_index[_probe_state->probe_index[i]] == 1) {
            if ((*filter)[i] == 0) {
                (*filter)[i] = 1;
            } else {
                _probe_state->build_match_index[_probe_state->build_index[i]] = 1;
            }
            continue;
        }

        if ((*filter)[i] == 0) {
            _probe_state->probe_match_index[_probe_state->probe_index[i]]--;
        } else {
            _probe_state->build_match_index[_probe_state->build_index[i]] = 1;
        }
    }
}

} // namespace starrocks
