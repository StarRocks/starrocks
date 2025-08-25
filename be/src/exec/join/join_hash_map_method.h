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

// The `first` and `next` together form a bucket-chained linked list.
//   - `first` stores the first element of the list,
//   - and `next` stores the next element in the list.
//
// `BucketChainedJoinHashMap` maps to a position in `first` using a hash function.
//
// The following diagram illustrates the structure of `BucketChainedJoinHashMap`:
//
// build keys                       first       next
//                                  ┌───┐       ┌───┐
//                                  │   │       │   │◄───┐
//                                  │   │       │   │◄┐  │
//                                  ├───┤       ├───┤ │  │
//                                  │   ├─┐     │   │ │  │
//                         ┌───────►│   │ │     │   │ │  │
//                         │        ├───┤ │     ├───┤ │  │
//              ┌────────┐ │        │   │ │     │   ├─┘  │
//  ┌──────┐    │        │ │        │   │ │     │   │◄─┐ │
//  │ key  ├───►│  Hash  ├─┘        ├───┤ │     ├───┤  │ │
//  └──────┘    │        │          │   │ │     │   │  │ │
//              └────────┘          │   │ │     │   │  │ │
//                                  ├───┤ │     ├───┤  │ │
//                                  │   │ └────►│   │  │ │
//                                  │   │       │   ├──┘ │
//                                  ├───┤       ├───┤    │
//                                  │   │       │   │    │
//                                  │   │       │   │    │
//                                  ├───┤       ├───┤    │
//                                  │   │       │   │    │
//                                  │   ├──────►│   ├────┘
//                                  └───┘       └───┘
template <LogicalType LT>
class BucketChainedJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = false;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return x == y; }
};

// The `LinearChainedJoinHashMap` uses linear probing to store distinct keys and chained to storage for linked lists of
// identical keys.
// - `first` stores the build index of the header for the linked list for each distinct key.
// - `next` maintains the linked list structure for each distinct key.
//
// Fingerprint
// - Each `first` entry uses the highest 1 byte to store the fingerprint and the lower 3 bytes for the build index,
//   thus supporting up to 0xFFFFFF buckets.
// - The fingerprint is generated via hashing.
//   During hashing, `bucket_num_with_fp = hash % (bucket_size * 8)` is computed instead of `hash % bucket_size`.
//   - The lower 8 bits of `bucket_num_with_fp` represent the fingerprint (`fp`),
//   - while `bucket_num_with_fp >> 8` yields the bucket number.
//
// Insert and probe
// - During insertion, linear probing is used in `first` to locate either the first empty bucket or an existing matching key.
//   The new build index is then inserted into the corresponding linked list in `next`.
// - During probing, linear probing is used in `first` to locate either an empty bucket or the bucket_num for a matching key.
//   - If an empty bucket is found, it indicates no matching key exists.
//   - If a matching key exists, the entire linked list (with `first[bucket_num]` as its header) in `next` stores build
//     indexes for all the same keys.
//
// The following diagram illustrates the structure of `LinearChainedJoinHashMap`:
//
// build keys                  first              next
//                             ┌──────────────┐   ┌───┐
//                             │FP|build_index│   │   │◄───┐
//                             │1B     3B     │   │   │◄┐  │
//                             ├──────────────┤   ├───┤ │  │
//                    ┌───────►│              │   │   │ │  │
//           ┌────┐   │     ┌──┤              │   │   │ │  │
// ┌──────┐  │    │   │     │  ├──────────────┤   ├───┤ │  │
// │ key  ├─►│hash├───┘     └─►│              │   │   ├─┘  │
// └──────┘  │    │         ┌──┤              │   │   │◄─┐ │
//           └────┘         │  ├──────────────┤   ├───┤  │ │
//                          │  │              │   │   │  │ │
//                          │  │              │   │   │  │ │
//                          │  ├──────────────┤   ├───┤  │ │
//                          └─►│              ├──►│   │  │ │
//                             │              │   │   ├──┘ │
//                             ├──────────────┤   ├───┤    │
//                             │              │   │   │    │
//                             │              │   │   │    │
//                             ├──────────────┤   ├───┤    │
//                             │              │   │   │    │
//                             │              ├──►│   ├────┘
//                             └──────────────┘   └───┘
template <LogicalType LT, bool NeedBuildChained = true>
class LinearChainedJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = true;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }

    static uint32_t max_supported_bucket_size() { return DATA_MASK; }

private:
    static constexpr uint32_t FP_BITS = 8;
    static constexpr uint32_t FP_MASK = 0xFF00'0000ul;
    static constexpr uint32_t DATA_MASK = 0x00FF'FFFFul;

    static uint32_t _combine_data_fp(const uint32_t data, const uint32_t fp) { return fp | data; }
    static uint32_t _extract_data(const uint32_t v) { return v & DATA_MASK; }
    static uint32_t _extract_fp(const uint32_t v) { return v & FP_MASK; }

    static uint32_t _get_bucket_num_from_hash(const uint32_t hash) { return hash >> FP_BITS; }
    static uint32_t _get_fp_from_hash(const uint32_t hash) { return hash << (32 - FP_BITS); }
};

template <LogicalType LT>
using LinearChainedJoinHashSet = LinearChainedJoinHashMap<LT, false>;

// The bucket-chained linked list formed by first` and `next` is the same as that of `BucketChainedJoinHashMap`.
//
// `DirectMappingJoinHashMap` maps to a position in `first` using `key-MIN_VALUE`.
//
// The following diagram illustrates the structure of `DirectMappingJoinHashMap`:
//
// build keys               first       next
//                          ┌───┐       ┌───┐
//                          │   │       │   │◄───┐
//                          │   │       │   │◄┐  │
//                          ├───┤       ├───┤ │  │
//          key-MIN_VALUE   │   ├─┐     │   │ │  │
//                 ┌───────►│   │ │     │   │ │  │
// ┌──────┐        │        ├───┤ │     ├───┤ │  │
// │ key  │────────┘        │   │ │     │   ├─┘  │
// └──────┘                 │   │ │     │   │◄─┐ │
//                          ├───┤ │     ├───┤  │ │
//                          │   │ │     │   │  │ │
//                          │   │ │     │   │  │ │
//                          ├───┤ │     ├───┤  │ │
//                          │   │ └────►│   │  │ │
//                          │   │       │   ├──┘ │
//                          ├───┤       ├───┤    │
//                          │   │       │   │    │
//                          │   │       │   │    │
//                          ├───┤       ├───┤    │
//                          │   │       │   │    │
//                          │   ├──────►│   ├────┘
//                          └───┘       └───┘
template <LogicalType LT>
class DirectMappingJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = true;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }
};

// The bucket-chained linked list formed by first` and `next` is the same as that of `BucketChainedJoinHashMap`.
//
// `RangeDirectMappingJoinHashMap` maps to a position in `first` using `key-min_value`, where `min_value` is the
// minimum value of all the builder's keys.
// Therefore, the probing key needs to be checked whether it is in the range of [min_value, max_value] during probing.
//
// The following diagram illustrates the structure of `DirectMappingJoinHashMap`:
//
// build keys               first       next
//                          ┌───┐       ┌───┐
//                          │   │       │   │◄───┐
//                          │   │       │   │◄┐  │
//                          ├───┤       ├───┤ │  │
//          key-min_value   │   ├─┐     │   │ │  │
//                 ┌───────►│   │ │     │   │ │  │
// ┌──────┐        │        ├───┤ │     ├───┤ │  │
// │ key  ├────────┘        │   │ │     │   ├─┘  │
// └──────┘                 │   │ │     │   │◄─┐ │
//                          ├───┤ │     ├───┤  │ │
//                          │   │ │     │   │  │ │
//                          │   │ │     │   │  │ │
//                          ├───┤ │     ├───┤  │ │
//                          │   │ └────►│   │  │ │
//                          │   │       │   ├──┘ │
//                          ├───┤       ├───┤    │
//                          │   │       │   │    │
//                          │   │       │   │    │
//                          ├───┤       ├───┤    │
//                          │   │       │   │    │
//                          │   ├──────►│   ├────┘
//                          └───┘       └───┘

template <LogicalType LT>
class RangeDirectMappingJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = true;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }
};

// `RangeDirectMappingJoinHashSet` is used for LEFT_SEMI/LEFT_ANTI JOIN scenarios where no additional JOIN ON conditions
// exist, while `RangeDirectMappingJoinHashMap` is employed for all other cases.
// `RangeDirectMappingJoinHashSet` uses only one bit to store each `value - min_value`.
template <LogicalType LT>
class RangeDirectMappingJoinHashSet {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = true;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }
};

// The bucket-chained linked list formed by first` and `next` is the same as that of `BucketChainedJoinHashMap`.
//
// As for a key, the position in `first` is obtained through the following steps:
// 1. Calculate `bucket = key - min_value`
// 2. Compress the `bucket` using dense groups to obtain the `dense bucket`.
//
// Using dense groups, the size of `first` is squeezed down to `row_count`, meaning empty first positions are discarded.
// Each dense group stores information for 32 first positions, represented using 64 bits (start_index: 32 bits, bitset: 32 bits).
// Thus, a single value_interval position is represented with 2 bits (64 bits / 32 = 2 bits).
// - start_index: Indicates the starting position of this dense group within the dense first array.
// - bitset: Uses 32 bits to represent which of the 32 positions in this dense group are non-empty.
//
// The following diagram illustrates the structure of `DenseRangeDirectMappingJoinHashMap`:
//
// build keys              dense groups       first       next
//                         ┌───────────┐      ┌───┐       ┌───┐
//                         │start_index┼─┐    │   │       │ 0 │◄───┐
//                         │bitset     │ │    │   │       │   │◄┐  │
//                         ├───────────┤ │    ├───┤       ├───┤ │  │
//                         │           │ │    │   │─┐     │   │ │  │
//                         │           │ └───►│   │ │     │   │ │  │
//                         ├───────────┤      ├───┤ │     ├───┤ │  │
//                         │           │      │   │ │     │   │─┘  │
//  ┌──────┐key-min_value  │           │      │   │ │     │   │◄─┐ │
//  │ key  ├─────────────► ├───────────┤      ├───┤ │     ├───┤  │ │
//  └──────┘               │           │      │   │ │     │   │  │ │
//                         │           │      │   │ │     │   │  │ │
//                         ├───────────┤      ├───┤ │     ├───┤  │ │
//                         │           │      │   │ └────►│   │  │ │
//                         │           │      │   │       │   │──┘ │
//                         ├───────────┤      ├───┤       ├───┤    │
//                         │           │      │   │       │   │    │
//                         │           │      │   │       │   │    │
//                         ├───────────┤      ├───┤       ├───┤    │
//                         │           │      │   │       │   │    │
//                         │           │      │   │──────►│   │────┘
//                         └───────────┘      └───┘       └───┘
template <LogicalType LT>
class DenseRangeDirectMappingJoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static constexpr bool AreKeysInChainIdentical = true;

    static void build_prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static void construct_hash_table(JoinHashTableItems* table_items, const Buffer<CppType>& keys,
                                     const Buffer<uint8_t>* is_nulls);

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                            const Buffer<CppType>& build_keys, const Buffer<CppType>& probe_keys,
                            const Buffer<uint8_t>* is_nulls);

    static bool equal(const CppType& x, const CppType& y) { return true; }
};

} // namespace starrocks
