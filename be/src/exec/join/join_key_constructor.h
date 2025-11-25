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

#include <gen_cpp/PlanNodes_types.h>
#include <runtime/descriptors.h>
#include <runtime/runtime_state.h>

#include <coroutine>
#include <cstdint>
#include <optional>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "join_hash_map_helper.h"
#include "join_hash_table_descriptor.h"
#include "simd/simd.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// KeyConstructorForOneKey
// ------------------------------------------------------------------------------------

template <LogicalType LT>
class BuildKeyConstructorForOneKey {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, JoinHashTableItems* table_items) {}
    static void build_key(RuntimeState* state, JoinHashTableItems* table_items) {}
    static size_t get_key_column_bytes(const JoinHashTableItems& table_items) {
        return table_items.key_columns[0]->byte_size();
    }
    static const ImmBuffer<CppType> get_key_data(const JoinHashTableItems& table_items);
    static const std::optional<ImmBuffer<uint8_t>> get_is_nulls(const JoinHashTableItems& table_items);
};

template <LogicalType LT>
class ProbeKeyConstructorForOneKey {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {}
    static void build_key(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);
    static const ImmBuffer<CppType> get_key_data(const HashTableProbeState& probe_state);
};

// ------------------------------------------------------------------------------------
// KeyConstructorForSerializedFixedSize
// ------------------------------------------------------------------------------------

template <LogicalType LT>
class BuildKeyConstructorForSerializedFixedSize {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void build_key(RuntimeState* state, JoinHashTableItems* table_items);

    static size_t get_key_column_bytes(const JoinHashTableItems& table_items) {
        return table_items.build_key_column->byte_size();
    }
    static const ImmBuffer<CppType> get_key_data(const JoinHashTableItems& table_items) {
        return ColumnHelper::as_raw_column<const ColumnType>(table_items.build_key_column)->immutable_data();
    }
    static const std::optional<ImmBuffer<uint8_t>> get_is_nulls(const JoinHashTableItems& table_items) {
        if (table_items.build_key_nulls.empty()) {
            return std::nullopt;
        }
        return table_items.build_key_nulls;
    }
};

template <LogicalType LT>
class ProbeKeyConstructorForSerializedFixedSize {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->is_nulls.resize(state->chunk_size());
        probe_state->probe_key_column = ColumnType::create(state->chunk_size());
    }

    static void build_key(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const ImmBuffer<CppType> get_key_data(const HashTableProbeState& probe_state) {
        return ColumnHelper::as_raw_column<ColumnType>(probe_state.probe_key_column)->get_data();
    }
};

// ------------------------------------------------------------------------------------
// KeyConstructorForSerialized
// ------------------------------------------------------------------------------------

class BuildKeyConstructorForSerialized {
public:
    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void build_key(RuntimeState* state, JoinHashTableItems* table_items);

    static size_t get_key_column_bytes(const JoinHashTableItems& table_items) {
        return table_items.build_pool->total_allocated_bytes();
    }
    static const ImmBuffer<Slice> get_key_data(const JoinHashTableItems& table_items) {
        return table_items.build_slice;
    }
    static const std::optional<ImmBuffer<uint8_t>> get_is_nulls(const JoinHashTableItems& table_items) {
        if (table_items.build_key_nulls.empty()) {
            return std::nullopt;
        }
        return table_items.build_key_nulls;
    }
};

class ProbeKeyConstructorForSerialized {
public:
    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->is_nulls.resize(state->chunk_size());
        probe_state->probe_pool = std::make_unique<MemPool>();
        probe_state->probe_slice.resize(state->chunk_size());
    }

    static void build_key(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const ImmBuffer<Slice> get_key_data(const HashTableProbeState& probe_state) {
        return probe_state.probe_slice;
    }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns, uint8_t* ptr);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns, uint8_t* ptr);
};
} // namespace starrocks