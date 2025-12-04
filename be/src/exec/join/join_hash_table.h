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

#include "exec/join/join_hash_map.h"
#include "util/runtime_profile.h"

namespace starrocks {

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

private:
    template <class Visitor>
    auto visit(Visitor&& visitor) const {
        return std::visit(std::forward<Visitor>(visitor), _hash_map);
    }

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