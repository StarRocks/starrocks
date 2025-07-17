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

#include "exec/join_hash_map.h"

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

namespace starrocks {
// if the same hash values are clustered, after the first probe, all related hash buckets are cached, without too many
// misses. So check time locality of probe keys here.
void HashTableProbeState::consider_probe_time_locality() {
    if (active_coroutines > 0) {
        // redo decision
        if ((probe_chunks & (detect_step - 1)) == 0) {
            int window_size = std::min(active_coroutines * 4, 50);
            if (probe_row_count > window_size) {
                phmap::flat_hash_map<uint32_t, uint32_t, StdHash<uint32_t>> occurrence;
                occurrence.reserve(probe_row_count);
                uint32_t unique_size = 0;
                bool enable_interleaving = true;
                uint32_t target = probe_row_count >> 3;
                for (auto i = 0; i < probe_row_count; i++) {
                    if (occurrence[next[i]] == 0) {
                        ++unique_size;
                        if (unique_size >= target) {
                            break;
                        }
                    }
                    occurrence[next[i]]++;
                    if (i >= window_size) {
                        occurrence[next[i - window_size]]--;
                    }
                }
                if (unique_size < target) {
                    active_coroutines = 0;
                    enable_interleaving = false;
                }
                // enlarge step if the decision is the same, otherwise reduce it
                if (enable_interleaving == last_enable_interleaving) {
                    detect_step = detect_step >= 1024 ? detect_step : (detect_step << 1);
                } else {
                    last_enable_interleaving = enable_interleaving;
                    detect_step = 1;
                }
            } else {
                active_coroutines = 0;
            }
        } else if (!last_enable_interleaving) {
            active_coroutines = 0;
        }
    }
    ++probe_chunks;
}

// ------------------------------------------------------------------------------------
// KeyConstructor
// ------------------------------------------------------------------------------------

void BuildKeyConstructorForSerialized::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->build_slice.resize(table_items->row_count + 1);
    table_items->build_pool = std::make_unique<MemPool>();
}

void BuildKeyConstructorForSerialized::build_key(RuntimeState* state, JoinHashTableItems* table_items) {
    const uint32_t row_count = table_items->row_count;

    // Prepare data and null columns.
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

    // Calc serialize size.
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = table_items->build_pool->allocate(serialize_size);

    // Serialize to key columns and build key is_nulls.
    if (null_columns.empty()) {
        for (uint32_t i = 1; i < 1 + row_count; i++) {
            table_items->build_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
            ptr += table_items->build_slice[i].size;
        }
    } else {
        table_items->build_key_nulls.resize(row_count + 1);
        auto* dest_is_nulls = table_items->build_key_nulls.data();
        std::memcpy(dest_is_nulls, null_columns[0]->get_data().data(), (row_count + 1) * sizeof(NullColumn::ValueType));
        for (uint32_t i = 1; i < null_columns.size(); i++) {
            for (uint32_t j = 1; j < 1 + row_count; j++) {
                dest_is_nulls[j] |= null_columns[i]->get_data()[j];
            }
        }

        for (uint32_t i = 1; i < 1 + row_count; i++) {
            if (dest_is_nulls[i] == 0) {
                table_items->build_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
                ptr += table_items->build_slice[i].size;
            }
        }
    }
}

void ProbeKeyConstructorForSerialized::build_key(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    probe_state->probe_pool->clear();

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            // this means build column is a nullable column and join condition is null safe equal
            // we need convert the probe column to a nullable column when it's a non-nullable column
            // to align the type between build and probe columns.
            data_columns.emplace_back(NullableColumn::wrap_if_necessary((*probe_state->key_columns)[i]));
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

    // allocate memory for serialize key columns
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = probe_state->probe_pool->allocate(serialize_size);

    // serialize and init search
    if (!null_columns.empty()) {
        _probe_nullable_column(table_items, probe_state, data_columns, null_columns, ptr);
    } else {
        _probe_column(table_items, probe_state, data_columns, ptr);
    }
}

void ProbeKeyConstructorForSerialized::_probe_column(const JoinHashTableItems& table_items,
                                                     HashTableProbeState* probe_state, const Columns& data_columns,
                                                     uint8_t* ptr) {
    const uint32_t row_count = probe_state->probe_row_count;
    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
        ptr += probe_state->probe_slice[i].size;
    }

    probe_state->null_array = nullptr;
}

void ProbeKeyConstructorForSerialized::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                              HashTableProbeState* probe_state,
                                                              const Columns& data_columns,
                                                              const NullColumns& null_columns, uint8_t* ptr) {
    const uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < row_count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[j];
        }
    }

    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
            ptr += probe_state->probe_slice[i].size;
        }
    }

    probe_state->null_array = &probe_state->is_nulls;
}

template <LogicalType LT, class BuildKeyConstructor, class ProbeKeyConstructor, typename HashMapMethod>
void JoinHashMap<LT, BuildKeyConstructor, ProbeKeyConstructor, HashMapMethod>::_probe_index_output(ChunkPtr* chunk) {
    _probe_state->probe_index.resize((*chunk)->num_rows());
    (*chunk)->append_column(_probe_state->probe_index_column, Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);
}

template <LogicalType LT, class BuildKeyConstructor, class ProbeKeyConstructor, typename HashMapMethod>
void JoinHashMap<LT, BuildKeyConstructor, ProbeKeyConstructor, HashMapMethod>::_build_index_output(ChunkPtr* chunk) {
    _probe_state->build_index.resize(_probe_state->count);
    (*chunk)->append_column(_probe_state->build_index_column, Chunk::HASH_JOIN_BUILD_INDEX_SLOT_ID);
}

// ------------------------------------------------------------------------------------
// JoinHashMapSelector
// ------------------------------------------------------------------------------------

class JoinHashMapResolver {
public:
    JoinHashMapResolver();
    static JoinHashMapResolver& instance();

    StatusOr<JoinHashMapType> get_unary_type(JoinKeyConstructorType key_builder_type, LogicalType key_type,
                                             JoinHashMapMethodType hash_map_method_type);

private:
    phmap::flat_hash_map<std::tuple<JoinKeyConstructorType, LogicalType, JoinHashMapMethodType>, JoinHashMapType>
            _types;
};

JoinHashMapResolver::JoinHashMapResolver() {
#define ADD_TYPE(KEY_BUILDER_TYPE, LOGICAL_TYPE, METHOD_TYPE, MAP_TYPE)                    \
    _types.emplace(std::make_tuple(JoinKeyConstructorType::KEY_BUILDER_TYPE, LOGICAL_TYPE, \
                                   JoinHashMapMethodType::METHOD_TYPE),                    \
                   JoinHashMapType::MAP_TYPE);

    ADD_TYPE(ONE_KEY, TYPE_BOOLEAN, DIRECT_MAPPING, keyboolean);
    ADD_TYPE(ONE_KEY, TYPE_TINYINT, DIRECT_MAPPING, key8);
    ADD_TYPE(ONE_KEY, TYPE_SMALLINT, DIRECT_MAPPING, key16);

    ADD_TYPE(ONE_KEY, TYPE_INT, BUCKET_CHAINED, key32);
    ADD_TYPE(ONE_KEY, TYPE_BIGINT, BUCKET_CHAINED, key64);
    ADD_TYPE(ONE_KEY, TYPE_LARGEINT, BUCKET_CHAINED, key128);
    ADD_TYPE(ONE_KEY, TYPE_FLOAT, BUCKET_CHAINED, keyfloat);
    ADD_TYPE(ONE_KEY, TYPE_DOUBLE, BUCKET_CHAINED, keydouble);
    ADD_TYPE(ONE_KEY, TYPE_DATE, BUCKET_CHAINED, keydate);
    ADD_TYPE(ONE_KEY, TYPE_DATETIME, BUCKET_CHAINED, keydatetime);
    ADD_TYPE(ONE_KEY, TYPE_DECIMALV2, BUCKET_CHAINED, keydecimal);
    ADD_TYPE(ONE_KEY, TYPE_DECIMAL32, BUCKET_CHAINED, keydecimal32);
    ADD_TYPE(ONE_KEY, TYPE_DECIMAL64, BUCKET_CHAINED, keydecimal64);
    ADD_TYPE(ONE_KEY, TYPE_DECIMAL128, BUCKET_CHAINED, keydecimal128);
    ADD_TYPE(ONE_KEY, TYPE_VARCHAR, BUCKET_CHAINED, keystring);

    ADD_TYPE(SERIALIZED_FIXED_SIZE, TYPE_INT, BUCKET_CHAINED, fixed32);
    ADD_TYPE(SERIALIZED_FIXED_SIZE, TYPE_BIGINT, BUCKET_CHAINED, fixed64);
    ADD_TYPE(SERIALIZED_FIXED_SIZE, TYPE_LARGEINT, BUCKET_CHAINED, fixed128);

    ADD_TYPE(SERIALIZED, TYPE_VARCHAR, BUCKET_CHAINED, slice);
#undef ADD_TYPE
}

JoinHashMapResolver& JoinHashMapResolver::instance() {
    static JoinHashMapResolver resolver;
    return resolver;
}

StatusOr<JoinHashMapType> JoinHashMapResolver::get_unary_type(JoinKeyConstructorType key_builder_type,
                                                              LogicalType key_type,
                                                              JoinHashMapMethodType hash_map_method_type) {
    if (const auto it = _types.find(std::make_tuple(key_builder_type, key_type, hash_map_method_type));
        it != _types.end()) {
        return it->second;
    }
    return Status::InternalError(strings::Substitute(
            "Unsupported join hash map type: key_builder_type={}, key_type={}, hash_map_method_type={}",
            static_cast<int>(key_builder_type), key_type, static_cast<int>(hash_map_method_type)));
}

class JoinHashMapSelector {
public:
    static StatusOr<JoinHashMapType> construct_key_and_determine_hash_map(RuntimeState* state,
                                                                          JoinHashTableItems* table_items);

private:
    static size_t _get_size_of_fixed_and_contiguous_type(LogicalType data_type);
    static std::pair<JoinKeyConstructorType, LogicalType> _determine_key_constructor(JoinHashTableItems* table_items);
    static void _construct_key(RuntimeState* state, JoinHashTableItems* table_items,
                               JoinKeyConstructorType key_builder_type, LogicalType key_type);
    static JoinHashMapMethodType _determine_hash_map_method(JoinKeyConstructorType key_builder_type,
                                                            LogicalType key_type);
};

StatusOr<JoinHashMapType> JoinHashMapSelector::construct_key_and_determine_hash_map(RuntimeState* state,
                                                                                    JoinHashTableItems* table_items) {
    if (table_items->row_count == 0) {
        return JoinHashMapType::empty;
    }

    const auto [key_builder_type, key_type] = _determine_key_constructor(table_items);
    _construct_key(state, table_items, key_builder_type, key_type);

    const auto method_type = _determine_hash_map_method(key_builder_type, key_type);
    return JoinHashMapResolver::instance().get_unary_type(key_builder_type, key_type, method_type);
}

template <class Functor, class Ret, class... Args>
static auto logical_type_dispatch_join(LogicalType ltype, Ret default_value, Functor fun, Args... args) {
#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

    switch (ltype) {
        _TYPE_DISPATCH_CASE(TYPE_TINYINT)
        _TYPE_DISPATCH_CASE(TYPE_SMALLINT)
        _TYPE_DISPATCH_CASE(TYPE_INT)
        _TYPE_DISPATCH_CASE(TYPE_BIGINT)
        _TYPE_DISPATCH_CASE(TYPE_LARGEINT)
        // float will be convert to double, so current can't reach here
        _TYPE_DISPATCH_CASE(TYPE_FLOAT)
        _TYPE_DISPATCH_CASE(TYPE_DOUBLE)
        _TYPE_DISPATCH_CASE(TYPE_DECIMALV2)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL32)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL64)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL128)
        _TYPE_DISPATCH_CASE(TYPE_BOOLEAN)
        // date will be convert to datetime, so current can't reach here
        _TYPE_DISPATCH_CASE(TYPE_DATE)
        _TYPE_DISPATCH_CASE(TYPE_DATETIME)
        _TYPE_DISPATCH_CASE(TYPE_CHAR)
        _TYPE_DISPATCH_CASE(TYPE_VARCHAR)
    default:
        return default_value;
    }

#undef _TYPE_DISPATCH_CASE
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

std::pair<JoinKeyConstructorType, LogicalType> JoinHashMapSelector::_determine_key_constructor(
        JoinHashTableItems* table_items) {
    const size_t num_keys = table_items->join_keys.size();
    DCHECK_GT(num_keys, 0);

    for (size_t i = 0; i < num_keys; i++) {
        if (!table_items->key_columns[i]->has_null()) {
            table_items->join_keys[i].is_null_safe_equal = false;
        }
    }

    if (num_keys == 1 && !table_items->join_keys[0].is_null_safe_equal) {
        return logical_type_dispatch_join(
                table_items->join_keys[0].type->type,
                std::make_pair(JoinKeyConstructorType::SERIALIZED, LogicalType::TYPE_VARCHAR),
                [&]<LogicalType LT>() { return std::make_pair(JoinKeyConstructorType::ONE_KEY, LT); });
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
            return {JoinKeyConstructorType::SERIALIZED, LogicalType::TYPE_VARCHAR};
        }
    }

    if (total_size_in_byte <= 4) {
        return {JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, LogicalType::TYPE_INT};
    }
    if (total_size_in_byte <= 8) {
        return {JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, LogicalType::TYPE_BIGINT};
    }
    if (total_size_in_byte <= 16) {
        return {JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, LogicalType::TYPE_LARGEINT};
    }

    return {JoinKeyConstructorType::SERIALIZED, LogicalType::TYPE_VARCHAR};
}

void JoinHashMapSelector::_construct_key(RuntimeState* state, JoinHashTableItems* table_items,
                                         JoinKeyConstructorType key_builder_type, LogicalType key_type) {
    auto process = [&]<typename JoinKeyConstructorType>() {
        JoinKeyConstructorType().prepare(state, table_items);
        JoinKeyConstructorType().build_key(state, table_items);
    };

    switch (key_builder_type) {
    case JoinKeyConstructorType::ONE_KEY:
        logical_type_dispatch_join(key_type, std::nullopt, [&]<LogicalType LT>() {
            DCHECK_NE(LT, LogicalType::TYPE_CHAR);
            process.operator()<BuildKeyConstructorForOneKey<LT>>();
            return std::nullopt;
        });
        break;
    case JoinKeyConstructorType::SERIALIZED_FIXED_SIZE:
        if (key_type == LogicalType::TYPE_INT) {
            process.operator()<BuildKeyConstructorForSerializedFixedSize<LogicalType::TYPE_INT>>();
        } else if (key_type == LogicalType::TYPE_BIGINT) {
            process.operator()<BuildKeyConstructorForSerializedFixedSize<LogicalType::TYPE_BIGINT>>();
        } else if (key_type == LogicalType::TYPE_LARGEINT) {
            process.operator()<BuildKeyConstructorForSerializedFixedSize<LogicalType::TYPE_LARGEINT>>();
        } else {
            DCHECK(false) << "Unsupported key type for fixed size serialized join build func: " << key_type;
        }
        break;
    case JoinKeyConstructorType::SERIALIZED:
    default:
        process.operator()<BuildKeyConstructorForSerialized>();
    }
}

JoinHashMapMethodType JoinHashMapSelector::_determine_hash_map_method(JoinKeyConstructorType key_builder_type,
                                                                      LogicalType key_type) {
    if (key_builder_type == JoinKeyConstructorType::ONE_KEY &&
        (key_type == LogicalType::TYPE_BOOLEAN || key_type == LogicalType::TYPE_TINYINT ||
         key_type == LogicalType::TYPE_SMALLINT)) {
        return JoinHashMapMethodType::DIRECT_MAPPING;
    }
    return JoinHashMapMethodType::BUCKET_CHAINED;
}

// ------------------------------------------------------------------------------------
// JoinHashTable
// ------------------------------------------------------------------------------------

JoinHashTable JoinHashTable::clone_readable_table() {
    JoinHashTable ht;

    ht._hash_map_type = this->_hash_map_type;

    ht._table_items = this->_table_items;
    // Clone a new probe state.
    ht._probe_state = std::make_unique<HashTableProbeState>(*this->_probe_state);

    switch (ht._hash_map_type) {
#define M(NAME)                                                                                         \
    case JoinHashMapType::NAME:                                                                         \
        ht._##NAME = std::make_unique<typename decltype(_##NAME)::element_type>(ht._table_items.get(),  \
                                                                                ht._probe_state.get()); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        DCHECK(false) << "Unsupported hash_map_type";
    }

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

    if (_table_items->join_type == TJoinOp::RIGHT_SEMI_JOIN || _table_items->join_type == TJoinOp::RIGHT_ANTI_JOIN ||
        _table_items->join_type == TJoinOp::RIGHT_OUTER_JOIN) {
        _table_items->left_to_nullable = true;
    } else if (_table_items->join_type == TJoinOp::LEFT_SEMI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
               _table_items->join_type == TJoinOp::LEFT_OUTER_JOIN) {
        _table_items->right_to_nullable = true;
    } else if (_table_items->join_type == TJoinOp::FULL_OUTER_JOIN) {
        _table_items->left_to_nullable = true;
        _table_items->right_to_nullable = true;
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

    ASSIGN_OR_RETURN(_hash_map_type,
                     JoinHashMapSelector::construct_key_and_determine_hash_map(state, _table_items.get()));

    switch (_hash_map_type) {
#define M(NAME)                                                                                                       \
    case JoinHashMapType::NAME:                                                                                       \
        _##NAME = std::make_unique<typename decltype(_##NAME)::element_type>(_table_items.get(), _probe_state.get()); \
        _##NAME->build_prepare(state);                                                                                \
        _##NAME->probe_prepare(state);                                                                                \
        _##NAME->build(state);                                                                                        \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }

    return Status::OK();
}

void JoinHashTable::reset_probe_state(starrocks::RuntimeState* state) {
    switch (_hash_map_type) {
#define M(NAME)                                                                                                       \
    case JoinHashMapType::NAME:                                                                                       \
        _##NAME = std::make_unique<typename decltype(_##NAME)::element_type>(_table_items.get(), _probe_state.get()); \
        _##NAME->probe_prepare(state);                                                                                \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }
}

Status JoinHashTable::probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
                            bool* eos) {
    switch (_hash_map_type) {
#define M(NAME)                                                      \
    case JoinHashMapType::NAME:                                      \
        _##NAME->probe(state, key_columns, probe_chunk, chunk, eos); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }
    if (_table_items->has_large_column) {
        RETURN_IF_ERROR((*chunk)->downgrade());
    }
    return Status::OK();
}

Status JoinHashTable::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    switch (_hash_map_type) {
#define M(NAME)                                   \
    case JoinHashMapType::NAME:                   \
        _##NAME->probe_remain(state, chunk, eos); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }
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
    if (_hash_map_type == JoinHashMapType::empty) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
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

template class JoinHashMapForDirectMapping(TYPE_BOOLEAN);
template class JoinHashMapForDirectMapping(TYPE_TINYINT);
template class JoinHashMapForDirectMapping(TYPE_SMALLINT);
template class JoinHashMapForOneKey(TYPE_INT);
template class JoinHashMapForOneKey(TYPE_BIGINT);
template class JoinHashMapForOneKey(TYPE_LARGEINT);
template class JoinHashMapForOneKey(TYPE_FLOAT);
template class JoinHashMapForOneKey(TYPE_DOUBLE);
template class JoinHashMapForOneKey(TYPE_VARCHAR);
template class JoinHashMapForOneKey(TYPE_DATE);
template class JoinHashMapForOneKey(TYPE_DATETIME);
template class JoinHashMapForOneKey(TYPE_DECIMALV2);
template class JoinHashMapForOneKey(TYPE_DECIMAL32);
template class JoinHashMapForOneKey(TYPE_DECIMAL64);
template class JoinHashMapForOneKey(TYPE_DECIMAL128);
template class JoinHashMapForSerializedKey(TYPE_VARCHAR);
template class JoinHashMapForFixedSizeKey(TYPE_INT);
template class JoinHashMapForFixedSizeKey(TYPE_BIGINT);
template class JoinHashMapForFixedSizeKey(TYPE_LARGEINT);

} // namespace starrocks
