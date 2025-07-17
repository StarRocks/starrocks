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

namespace starrocks {

// ------------------------------------------------------------------------------------
// Type Traits
// ------------------------------------------------------------------------------------

// <JoinKeyConstructorType, LogicalType> -> KeyConstructorImpl
template <JoinKeyConstructorType, LogicalType>
struct JoinKeyConstructorTypeTraits;

// JoinHashMapType -> JoinHashMapImpl
template <JoinHashMapType>
struct JoinHashMapTypeTraits;

// <JoinKeyConstructorType, LogicalType, JoinHashMapMethodType> -> JoinHashMapType
class JoinHashMapResolver {
public:
    static JoinHashMapResolver& instance() {
        static JoinHashMapResolver resolver;
        return resolver;
    }

    void register_type(JoinKeyConstructorType key_builder_type, LogicalType key_type,
                       JoinHashMapMethodType hash_map_method_type, JoinHashMapType hash_map_type) {
        CHECK(_types.emplace(std::make_tuple(key_builder_type, key_type, hash_map_method_type), hash_map_type).second);
    }

    StatusOr<JoinHashMapType> get_unary_type(JoinKeyConstructorType key_builder_type, LogicalType key_type,
                                             JoinHashMapMethodType hash_map_method_type) {
        if (const auto it = _types.find(std::make_tuple(key_builder_type, key_type, hash_map_method_type));
            it != _types.end()) {
            return it->second;
        }
        return Status::InternalError(strings::Substitute(
                "Unsupported join hash map type: key_builder_type={}, key_type={}, hash_map_method_type={}",
                static_cast<int>(key_builder_type), key_type, static_cast<int>(hash_map_method_type)));
    }

private:
    phmap::flat_hash_map<std::tuple<JoinKeyConstructorType, LogicalType, JoinHashMapMethodType>, JoinHashMapType>
            _types;
};

#define REGISTER_KEY_BUILDER(KEY_BUILDER_TYPE, LOGICAL_TYPE, KEY_BUILDER_IMPL)                    \
    template <>                                                                                   \
    struct JoinKeyConstructorTypeTraits<JoinKeyConstructorType::KEY_BUILDER_TYPE, LOGICAL_TYPE> { \
        using BuildType = KEY_BUILDER_IMPL;                                                       \
    };

#define REGISTER_JOIN_MAP_TYPE(MAP_TYPE, MAP_IMPL)            \
    template <>                                               \
    struct JoinHashMapTypeTraits<JoinHashMapType::MAP_TYPE> { \
        using HashMapType = MAP_IMPL;                         \
    };

#define REGISTER_JOIN_MAP(KEY_BUILDER_TYPE, LOGICAL_TYPE, METHOD_TYPE, MAP_TYPE, MAP_IMPL)                        \
    namespace {                                                                                                   \
    struct JoinHashMapRegisterer_##MAP_TYPE {                                                                     \
        JoinHashMapRegisterer_##MAP_TYPE() {                                                                      \
            JoinHashMapResolver::instance().register_type(JoinKeyConstructorType::KEY_BUILDER_TYPE, LOGICAL_TYPE, \
                                                          JoinHashMapMethodType::METHOD_TYPE,                     \
                                                          JoinHashMapType::MAP_TYPE);                             \
        };                                                                                                        \
    } registerer_##MAP_TYPE;                                                                                      \
    }                                                                                                             \
    REGISTER_JOIN_MAP_TYPE(MAP_TYPE, MAP_IMPL(##LOGICAL_TYPE))

// REGISTER_KEY_BUILDER
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_BOOLEAN, BuildKeyConstructorForOneKey<TYPE_BOOLEAN>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_SMALLINT, BuildKeyConstructorForOneKey<TYPE_SMALLINT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_TINYINT, BuildKeyConstructorForOneKey<TYPE_TINYINT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_INT, BuildKeyConstructorForOneKey<TYPE_INT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_BIGINT, BuildKeyConstructorForOneKey<TYPE_BIGINT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_LARGEINT, BuildKeyConstructorForOneKey<TYPE_LARGEINT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_FLOAT, BuildKeyConstructorForOneKey<TYPE_FLOAT>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DOUBLE, BuildKeyConstructorForOneKey<TYPE_DOUBLE>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DECIMALV2, BuildKeyConstructorForOneKey<TYPE_DECIMALV2>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DECIMAL32, BuildKeyConstructorForOneKey<TYPE_DECIMAL32>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DECIMAL64, BuildKeyConstructorForOneKey<TYPE_DECIMAL64>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DECIMAL128, BuildKeyConstructorForOneKey<TYPE_DECIMAL128>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DATE, BuildKeyConstructorForOneKey<TYPE_DATE>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_DATETIME, BuildKeyConstructorForOneKey<TYPE_DATETIME>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_CHAR, BuildKeyConstructorForOneKey<TYPE_VARCHAR>);
REGISTER_KEY_BUILDER(ONE_KEY, TYPE_VARCHAR, BuildKeyConstructorForOneKey<TYPE_VARCHAR>);
REGISTER_KEY_BUILDER(SERIALIZED_FIXED_SIZE, TYPE_INT, BuildKeyConstructorForSerializedFixedSize<TYPE_INT>);
REGISTER_KEY_BUILDER(SERIALIZED_FIXED_SIZE, TYPE_BIGINT, BuildKeyConstructorForSerializedFixedSize<TYPE_BIGINT>);
REGISTER_KEY_BUILDER(SERIALIZED_FIXED_SIZE, TYPE_LARGEINT, BuildKeyConstructorForSerializedFixedSize<TYPE_LARGEINT>);
REGISTER_KEY_BUILDER(SERIALIZED, TYPE_VARCHAR, BuildKeyConstructorForSerialized);

// REGISTER_JOIN_MAP
REGISTER_JOIN_MAP_TYPE(empty, JoinHashMapForEmpty);

REGISTER_JOIN_MAP(ONE_KEY, TYPE_BOOLEAN, DIRECT_MAPPING, keyboolean, JoinHashMapForDirectMapping);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_TINYINT, DIRECT_MAPPING, key8, JoinHashMapForDirectMapping);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_SMALLINT, DIRECT_MAPPING, key16, JoinHashMapForDirectMapping);

REGISTER_JOIN_MAP(ONE_KEY, TYPE_INT, BUCKET_CHAINED, key32, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_BIGINT, BUCKET_CHAINED, key64, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_LARGEINT, BUCKET_CHAINED, key128, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_FLOAT, BUCKET_CHAINED, keyfloat, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DOUBLE, BUCKET_CHAINED, keydouble, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DATE, BUCKET_CHAINED, keydate, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DATETIME, BUCKET_CHAINED, keydatetime, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DECIMALV2, BUCKET_CHAINED, keydecimal, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DECIMAL32, BUCKET_CHAINED, keydecimal32, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DECIMAL64, BUCKET_CHAINED, keydecimal64, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_DECIMAL128, BUCKET_CHAINED, keydecimal128, JoinHashMapForOneKey);
REGISTER_JOIN_MAP(ONE_KEY, TYPE_VARCHAR, BUCKET_CHAINED, keystring, JoinHashMapForOneKey);

REGISTER_JOIN_MAP(SERIALIZED_FIXED_SIZE, TYPE_INT, BUCKET_CHAINED, fixed32, JoinHashMapForFixedSizeKey);
REGISTER_JOIN_MAP(SERIALIZED_FIXED_SIZE, TYPE_BIGINT, BUCKET_CHAINED, fixed64, JoinHashMapForFixedSizeKey);
REGISTER_JOIN_MAP(SERIALIZED_FIXED_SIZE, TYPE_LARGEINT, BUCKET_CHAINED, fixed128, JoinHashMapForFixedSizeKey);

REGISTER_JOIN_MAP(SERIALIZED, TYPE_VARCHAR, BUCKET_CHAINED, slice, JoinHashMapForSerializedKey);

#undef REGISTER_JOIN_MAP
#undef REGISTER_JOIN_MAP_TYPE
#undef REGISTER_KEY_BUILDER

// ------------------------------------------------------------------------------------
// JoinHashMapSelector
// ------------------------------------------------------------------------------------

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

template <typename Visitor>
static void _dispatch_join_key_constructor(JoinKeyConstructorType key_builder_type, LogicalType key_type,
                                           Visitor&& visitor) {
    switch (key_builder_type) {
    case JoinKeyConstructorType::ONE_KEY:
        logical_type_dispatch_join(key_type, std::nullopt, [&]<LogicalType LT>() {
            visitor.template operator()<JoinKeyConstructorType::ONE_KEY, LT>();
            return std::nullopt;
        });
        break;
    case JoinKeyConstructorType::SERIALIZED_FIXED_SIZE:
        if (key_type == LogicalType::TYPE_INT) {
            visitor.template operator()<JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, TYPE_INT>();
        } else if (key_type == LogicalType::TYPE_BIGINT) {
            visitor.template operator()<JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, TYPE_BIGINT>();
        } else if (key_type == LogicalType::TYPE_LARGEINT) {
            visitor.template operator()<JoinKeyConstructorType::SERIALIZED_FIXED_SIZE, TYPE_LARGEINT>();
        } else {
            DCHECK(false) << "Unsupported key type for fixed size serialized join build func: " << key_type;
            __builtin_unreachable();
        }
        break;
    case JoinKeyConstructorType::SERIALIZED:
    default:
        DCHECK_EQ(key_type, LogicalType::TYPE_VARCHAR);
        visitor.template operator()<JoinKeyConstructorType::SERIALIZED, TYPE_VARCHAR>();
    }
}

class JoinHashMapSelector {
public:
    static StatusOr<JoinHashMapType> construct_key_and_determine_hash_map(RuntimeState* state,
                                                                          JoinHashTableItems* table_items);

private:
    static size_t _get_size_of_fixed_and_contiguous_type(LogicalType data_type);
    static std::pair<JoinKeyConstructorType, LogicalType> _determine_key_constructor(JoinHashTableItems* table_items);
    static JoinHashMapMethodType _determine_hash_map_method(JoinKeyConstructorType key_builder_type,
                                                            LogicalType key_type);
};

StatusOr<JoinHashMapType> JoinHashMapSelector::construct_key_and_determine_hash_map(RuntimeState* state,
                                                                                    JoinHashTableItems* table_items) {
    if (table_items->row_count == 0) {
        return JoinHashMapType::empty;
    }

    const auto [key_builder_type, key_type] = _determine_key_constructor(table_items);
    _dispatch_join_key_constructor(
            key_builder_type, key_type, [&]<JoinKeyConstructorType ConstructorType, LogicalType LT>() {
                using JoinKeyConstructor = typename JoinKeyConstructorTypeTraits<ConstructorType, LT>::BuildType;
                JoinKeyConstructor().prepare(state, table_items);
                JoinKeyConstructor().build_key(state, table_items);
            });

    const auto method_type = _determine_hash_map_method(key_builder_type, key_type);
    return JoinHashMapResolver::instance().get_unary_type(key_builder_type, key_type, method_type);
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
// JoinHashMap
// ------------------------------------------------------------------------------------

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
// JoinHashTable
// ------------------------------------------------------------------------------------

JoinHashTable JoinHashTable::clone_readable_table() {
    JoinHashTable ht;

    ht._hash_map_type = this->_hash_map_type;

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
#define M(NAME)                                                                                 \
    case JoinHashMapType::NAME: {                                                               \
        using HashMapType = typename JoinHashMapTypeTraits<JoinHashMapType::NAME>::HashMapType; \
        auto hash_map = std::make_unique<HashMapType>(_table_items.get(), _probe_state.get());  \
        hash_map->build_prepare(state);                                                         \
        hash_map->probe_prepare(state);                                                         \
        hash_map->build(state);                                                                 \
        _hash_map = std::move(hash_map);                                                        \
        break;                                                                                  \
    }

        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    default:
        assert(false);
    }

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
