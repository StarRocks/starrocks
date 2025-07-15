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

#include "join_hash_map_helper.h"
#include "join_hash_table_descriptor.h"

namespace starrocks {

template <LogicalType LT>
class BucketChainedJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& keys, const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return x == y; }
};

template <LogicalType LT>
class DirectMappingJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& keys, const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }
};

} // namespace starrocks
