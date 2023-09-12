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

// This file is based on code available under the Apache license here:
//   https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Aggregator.h

#pragma once

#include <type_traits>
#include <utility>
#include <variant>

#include "column/hash_set.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_profile.h"
#include "types/logical_type.h"

namespace starrocks {

#define APPLY_FOR_AGG_VARIANT_ALL(M) \
    M(phase1_uint8)                  \
    M(phase1_int8)                   \
    M(phase1_int16)                  \
    M(phase1_int32)                  \
    M(phase1_int64)                  \
    M(phase1_int128)                 \
    M(phase1_decimal32)              \
    M(phase1_decimal64)              \
    M(phase1_decimal128)             \
    M(phase1_date)                   \
    M(phase1_timestamp)              \
    M(phase1_string)                 \
    M(phase1_slice)                  \
    M(phase1_null_uint8)             \
    M(phase1_null_int8)              \
    M(phase1_null_int16)             \
    M(phase1_null_int32)             \
    M(phase1_null_int64)             \
    M(phase1_null_int128)            \
    M(phase1_null_decimal32)         \
    M(phase1_null_decimal64)         \
    M(phase1_null_decimal128)        \
    M(phase1_null_date)              \
    M(phase1_null_timestamp)         \
    M(phase1_null_string)            \
    M(phase1_slice_two_level)        \
    M(phase1_int32_two_level)        \
    M(phase2_uint8)                  \
    M(phase2_int8)                   \
    M(phase2_int16)                  \
    M(phase2_int32)                  \
    M(phase2_int64)                  \
    M(phase2_int128)                 \
    M(phase2_decimal32)              \
    M(phase2_decimal64)              \
    M(phase2_decimal128)             \
    M(phase2_date)                   \
    M(phase2_timestamp)              \
    M(phase2_string)                 \
    M(phase2_slice)                  \
    M(phase2_null_uint8)             \
    M(phase2_null_int8)              \
    M(phase2_null_int16)             \
    M(phase2_null_int32)             \
    M(phase2_null_int64)             \
    M(phase2_null_int128)            \
    M(phase2_null_decimal32)         \
    M(phase2_null_decimal64)         \
    M(phase2_null_decimal128)        \
    M(phase2_null_date)              \
    M(phase2_null_timestamp)         \
    M(phase2_null_string)            \
    M(phase2_slice_two_level)        \
    M(phase2_int32_two_level)        \
    M(phase1_slice_fx4)              \
    M(phase1_slice_fx8)              \
    M(phase1_slice_fx16)             \
    M(phase2_slice_fx4)              \
    M(phase2_slice_fx8)              \
    M(phase2_slice_fx16)

// Aggregate Hash maps

// no-nullable single key maps:
template <PhmapSeed seed>
using UInt8AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_BOOLEAN, Int8AggHashMap<seed>>;
template <PhmapSeed seed>
using Int8AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_TINYINT, Int8AggHashMap<seed>>;
template <PhmapSeed seed>
using Int16AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_SMALLINT, Int16AggHashMap<seed>>;
template <PhmapSeed seed>
using Int32AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_INT, Int32AggHashMap<seed>>;
template <PhmapSeed seed>
using Int64AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_BIGINT, Int64AggHashMap<seed>>;
template <PhmapSeed seed>
using Int128AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_LARGEINT, Int128AggHashMap<seed>>;
template <PhmapSeed seed>
using Decimal32AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_DECIMAL32, Int32AggHashMap<seed>>;
template <PhmapSeed seed>
using Decimal64AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_DECIMAL64, Int64AggHashMap<seed>>;
template <PhmapSeed seed>
using Decimal128AggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_DECIMAL128, Int128AggHashMap<seed>>;
template <PhmapSeed seed>
using DateAggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_DATE, DateAggHashMap<seed>>;
template <PhmapSeed seed>
using TimeStampAggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_DATETIME, TimeStampAggHashMap<seed>>;
// nullable single key maps:
template <PhmapSeed seed>
using NullUInt8AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_BOOLEAN, Int8AggHashMap<seed>>;
template <PhmapSeed seed>
using NullInt8AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_TINYINT, Int8AggHashMap<seed>>;
template <PhmapSeed seed>
using NullInt16AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_SMALLINT, Int16AggHashMap<seed>>;
template <PhmapSeed seed>
using NullInt32AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_INT, Int32AggHashMap<seed>>;
template <PhmapSeed seed>
using NullInt64AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_BIGINT, Int64AggHashMap<seed>>;
template <PhmapSeed seed>
using NullInt128AggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_LARGEINT, Int128AggHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal32AggHashMapWithOneNumberKey =
        AggHashMapWithOneNullableNumberKey<TYPE_DECIMAL32, Int32AggHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal64AggHashMapWithOneNumberKey =
        AggHashMapWithOneNullableNumberKey<TYPE_DECIMAL64, Int64AggHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal128AggHashMapWithOneNumberKey =
        AggHashMapWithOneNullableNumberKey<TYPE_DECIMAL128, Int128AggHashMap<seed>>;

template <PhmapSeed seed>
using NullDateAggHashMapWithOneNumberKey = AggHashMapWithOneNullableNumberKey<TYPE_DATE, DateAggHashMap<seed>>;
template <PhmapSeed seed>
using NullTimeStampAggHashMapWithOneNumberKey =
        AggHashMapWithOneNullableNumberKey<TYPE_DATETIME, TimeStampAggHashMap<seed>>;

// For string type, we use slice type as hashmap key
template <PhmapSeed seed>
using OneStringAggHashMap = AggHashMapWithOneStringKey<SliceAggHashMap<seed>>;
template <PhmapSeed seed>
using NullOneStringAggHashMap = AggHashMapWithOneNullableStringKey<SliceAggHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyAggHashMap = AggHashMapWithSerializedKey<SliceAggHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyTwoLevelAggHashMap = AggHashMapWithSerializedKey<SliceAggTwoLevelHashMap<seed>>;
template <PhmapSeed seed>
using Int32TwoLevelAggHashMapWithOneNumberKey = AggHashMapWithOneNumberKey<TYPE_INT, Int32AggTwoLevelHashMap<seed>>;

// fixed slice key type.
template <PhmapSeed seed>
using SerializedKeyFixedSize4AggHashMap = AggHashMapWithSerializedKeyFixedSize<FixedSize4SliceAggHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyFixedSize8AggHashMap = AggHashMapWithSerializedKeyFixedSize<FixedSize8SliceAggHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyFixedSize16AggHashMap = AggHashMapWithSerializedKeyFixedSize<FixedSize16SliceAggHashMap<seed>>;

// Hash sets
//
template <PhmapSeed seed>
using UInt8AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_BOOLEAN, Int8AggHashSet<seed>>;
template <PhmapSeed seed>
using Int8AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_TINYINT, Int8AggHashSet<seed>>;
template <PhmapSeed seed>
using Int16AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_SMALLINT, Int16AggHashSet<seed>>;
template <PhmapSeed seed>
using Int32AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_INT, Int32AggHashSet<seed>>;
template <PhmapSeed seed>
using Int64AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_BIGINT, Int64AggHashSet<seed>>;
template <PhmapSeed seed>
using Int128AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_LARGEINT, Int128AggHashSet<seed>>;
template <PhmapSeed seed>
using Decimal32AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_DECIMAL32, Int32AggHashSet<seed>>;
template <PhmapSeed seed>
using Decimal64AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_DECIMAL64, Int64AggHashSet<seed>>;
template <PhmapSeed seed>
using Decimal128AggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_DECIMAL128, Int128AggHashSet<seed>>;

template <PhmapSeed seed>
using DateAggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_DATE, DateAggHashSet<seed>>;
template <PhmapSeed seed>
using TimeStampAggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_DATETIME, TimeStampAggHashSet<seed>>;

template <PhmapSeed seed>
using NullUInt8AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_BOOLEAN, Int8AggHashSet<seed>>;
template <PhmapSeed seed>
using NullInt8AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_TINYINT, Int8AggHashSet<seed>>;
template <PhmapSeed seed>
using NullInt16AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_SMALLINT, Int16AggHashSet<seed>>;
template <PhmapSeed seed>
using NullInt32AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_INT, Int32AggHashSet<seed>>;
template <PhmapSeed seed>
using NullInt64AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_BIGINT, Int64AggHashSet<seed>>;
template <PhmapSeed seed>
using NullInt128AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_LARGEINT, Int128AggHashSet<seed>>;

template <PhmapSeed seed>
using NullDecimal32AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_DECIMAL32, Int32AggHashSet<seed>>;
template <PhmapSeed seed>
using NullDecimal64AggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_DECIMAL64, Int64AggHashSet<seed>>;
template <PhmapSeed seed>
using NullDecimal128AggHashSetOfOneNumberKey =
        AggHashSetOfOneNullableNumberKey<TYPE_DECIMAL128, Int128AggHashSet<seed>>;
template <PhmapSeed seed>
using NullDateAggHashSetOfOneNumberKey = AggHashSetOfOneNullableNumberKey<TYPE_DATE, DateAggHashSet<seed>>;
template <PhmapSeed seed>
using NullTimeStampAggHashSetOfOneNumberKey =
        AggHashSetOfOneNullableNumberKey<TYPE_DATETIME, TimeStampAggHashSet<seed>>;

// For string type, we use slice type as hash key
template <PhmapSeed seed>
using OneStringAggHashSet = AggHashSetOfOneStringKey<SliceAggHashSet<seed>>;
template <PhmapSeed seed>
using NullOneStringAggHashSet = AggHashSetOfOneNullableStringKey<SliceAggHashSet<seed>>;
template <PhmapSeed seed>
using SerializedKeyAggHashSet = AggHashSetOfSerializedKey<SliceAggHashSet<seed>>;
template <PhmapSeed seed>
using SerializedTwoLevelKeyAggHashSet = AggHashSetOfSerializedKey<SliceAggTwoLevelHashSet<seed>>;
template <PhmapSeed seed>
using Int32TwoLevelAggHashSetOfOneNumberKey = AggHashSetOfOneNumberKey<TYPE_INT, Int32AggTwoLevelHashSet<seed>>;

// For fixed slice type.
template <PhmapSeed seed>
using SerializedKeyAggHashSetFixedSize4 = AggHashSetOfSerializedKeyFixedSize<FixedSize4SliceAggHashSet<seed>>;

template <PhmapSeed seed>
using SerializedKeyAggHashSetFixedSize8 = AggHashSetOfSerializedKeyFixedSize<FixedSize8SliceAggHashSet<seed>>;

template <PhmapSeed seed>
using SerializedKeyAggHashSetFixedSize16 = AggHashSetOfSerializedKeyFixedSize<FixedSize16SliceAggHashSet<seed>>;

// aggregate key
template <class HashMapWithKey>
struct CombinedFixedSizeKey {
    static auto constexpr value = false;
};

template <typename HashMap>
struct CombinedFixedSizeKey<AggHashMapWithSerializedKeyFixedSize<HashMap>> {
    static auto constexpr value = true;
};

template <typename HashSet>
struct CombinedFixedSizeKey<AggHashSetOfSerializedKeyFixedSize<HashSet>> {
    static auto constexpr value = true;
};

template <typename HashMapOrSetWithKey>
inline constexpr bool is_combined_fixed_size_key = CombinedFixedSizeKey<HashMapOrSetWithKey>::value;

static_assert(is_combined_fixed_size_key<SerializedKeyFixedSize4AggHashMap<PhmapSeed1>>);
static_assert(!is_combined_fixed_size_key<Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed1>>);
static_assert(is_combined_fixed_size_key<SerializedKeyAggHashSetFixedSize4<PhmapSeed1>>);
static_assert(!is_combined_fixed_size_key<Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>>);

// 1) For different group by columns type, size, cardinality, volume, we should choose different
// hash functions and different hashmaps.
// When runtime, we will only have one hashmap.
// The type name and hashmap name is the same, so we could easily call hashmap by type.
// Before you use the underlying hashmap, you should call init method firstly.
//
// 2) Distributed aggregation is divided into two stages.
// The first stage is local aggregation.
// After the data is shuffled, the second stage is final aggregation.
// If the same Hash function and Seed are used,
// if the open address HashTable is used in the second stage,
// it will lead to a high conflict rate, resulting in performance degradation in the second stage.
namespace detail {
using AggHashMapWithKeyPtr = std::variant<
        std::unique_ptr<UInt8AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int8AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int16AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int32AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int64AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int128AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal32AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal64AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal128AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<DateAggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<TimeStampAggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<OneStringAggHashMap<PhmapSeed1>>,
        std::unique_ptr<NullUInt8AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt8AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt16AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt64AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt128AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDateAggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullOneStringAggHashMap<PhmapSeed1>>, std::unique_ptr<SerializedKeyAggHashMap<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyTwoLevelAggHashMap<PhmapSeed1>>,
        std::unique_ptr<Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyFixedSize4AggHashMap<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyFixedSize8AggHashMap<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyFixedSize16AggHashMap<PhmapSeed1>>,
        std::unique_ptr<UInt8AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int8AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int16AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int32AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int64AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int128AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal32AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal64AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal128AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<DateAggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<TimeStampAggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<OneStringAggHashMap<PhmapSeed2>>,
        std::unique_ptr<NullUInt8AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt8AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt16AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt32AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt64AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt128AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDateAggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullOneStringAggHashMap<PhmapSeed2>>, std::unique_ptr<SerializedKeyAggHashMap<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyTwoLevelAggHashMap<PhmapSeed2>>,
        std::unique_ptr<Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyFixedSize4AggHashMap<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyFixedSize8AggHashMap<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyFixedSize16AggHashMap<PhmapSeed2>>>;

using AggHashSetWithKeyPtr = std::variant<
        std::unique_ptr<UInt8AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int8AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int16AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int32AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int64AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Int128AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal32AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal64AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<Decimal128AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<DateAggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<TimeStampAggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<OneStringAggHashSet<PhmapSeed1>>,
        std::unique_ptr<NullUInt8AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt8AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt16AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt32AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt64AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullInt128AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullDateAggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<NullOneStringAggHashSet<PhmapSeed1>>, std::unique_ptr<SerializedKeyAggHashSet<PhmapSeed1>>,
        std::unique_ptr<SerializedTwoLevelKeyAggHashSet<PhmapSeed1>>,
        std::unique_ptr<Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed1>>,
        std::unique_ptr<UInt8AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int8AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int16AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int32AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int64AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Int128AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal32AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal64AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<Decimal128AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<DateAggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<TimeStampAggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<OneStringAggHashSet<PhmapSeed2>>,
        std::unique_ptr<NullUInt8AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt8AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt16AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt32AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt64AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullInt128AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullDateAggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<NullOneStringAggHashSet<PhmapSeed2>>, std::unique_ptr<SerializedKeyAggHashSet<PhmapSeed2>>,
        std::unique_ptr<SerializedTwoLevelKeyAggHashSet<PhmapSeed2>>,
        std::unique_ptr<Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize4<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize8<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize16<PhmapSeed1>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize4<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize8<PhmapSeed2>>,
        std::unique_ptr<SerializedKeyAggHashSetFixedSize16<PhmapSeed2>>>;
} // namespace detail
struct AggHashMapVariant {
    enum class Type {
        phase1_uint8 = 0,
        phase1_int8,
        phase1_int16,
        phase1_int32,
        phase1_int64,
        phase1_int128,
        phase1_decimal32,
        phase1_decimal64,
        phase1_decimal128,
        phase1_date,
        phase1_timestamp,
        phase1_string,
        phase1_null_uint8,
        phase1_null_int8,
        phase1_null_int16,
        phase1_null_int32,
        phase1_null_int64,
        phase1_null_int128,
        phase1_null_decimal32,
        phase1_null_decimal64,
        phase1_null_decimal128,
        phase1_null_date,
        phase1_null_timestamp,
        phase1_null_string,
        phase1_slice,
        phase1_slice_two_level,
        phase1_int32_two_level,

        phase1_slice_fx4,
        phase1_slice_fx8,
        phase1_slice_fx16,

        phase2_uint8,
        phase2_int8,
        phase2_int16,
        phase2_int32,
        phase2_int64,
        phase2_int128,
        phase2_decimal32,
        phase2_decimal64,
        phase2_decimal128,
        phase2_date,
        phase2_timestamp,
        phase2_string,
        phase2_null_uint8,
        phase2_null_int8,
        phase2_null_int16,
        phase2_null_int32,
        phase2_null_int64,
        phase2_null_int128,
        phase2_null_decimal32,
        phase2_null_decimal64,
        phase2_null_decimal128,
        phase2_null_date,
        phase2_null_timestamp,
        phase2_null_string,
        phase2_slice,
        phase2_slice_two_level,
        phase2_int32_two_level,

        phase2_slice_fx4,
        phase2_slice_fx8,
        phase2_slice_fx16,
    };

    detail::AggHashMapWithKeyPtr hash_map_with_key;
    auto& get_variant() { return hash_map_with_key; }

    template <class Vistor>
    auto visit(Vistor&& vistor) const {
        return std::visit(std::forward<Vistor>(vistor), hash_map_with_key);
    }

    void init(RuntimeState* state, Type type, AggStatistics* agg_statis);

    void convert_to_two_level(RuntimeState* state);

    // release the hash table
    void reset();

    size_t capacity() const;

    size_t size() const;

    bool need_expand(size_t increasement) const;

    size_t reserved_memory_usage(const MemPool* pool) const;

    size_t allocated_memory_usage(const MemPool* pool) const;

private:
    Type _type = Type::phase1_slice;
    AggStatistics* _agg_stat = nullptr;
};

// 1) AggHashSetVariant is alike HashMapVariant, while a set only holds keys, no associated value.
//
// 2) Distributed aggregation is divided into two stages.
// The first stage is local aggregation.
// After the data is shuffled, the second stage is final aggregation.
// If the same Hash function and Seed are used,
// if the open address HashTable is used in the second stage,
// it will lead to a high conflict rate, resulting in performance degradation in the second stage.
struct AggHashSetVariant {
    enum class Type {
        phase1_uint8 = 0,
        phase1_int8,
        phase1_int16,
        phase1_int32,
        phase1_int64,
        phase1_int128,
        phase1_decimal32,
        phase1_decimal64,
        phase1_decimal128,
        phase1_date,
        phase1_timestamp,
        phase1_string,
        phase1_null_uint8,
        phase1_null_int8,
        phase1_null_int16,
        phase1_null_int32,
        phase1_null_int64,
        phase1_null_int128,
        phase1_null_decimal32,
        phase1_null_decimal64,
        phase1_null_decimal128,
        phase1_null_date,
        phase1_null_timestamp,
        phase1_null_string,
        phase1_slice,
        phase1_slice_two_level,
        phase1_int32_two_level,
        phase2_uint8,
        phase2_int8,
        phase2_int16,
        phase2_int32,
        phase2_int64,
        phase2_int128,
        phase2_decimal32,
        phase2_decimal64,
        phase2_decimal128,
        phase2_date,
        phase2_timestamp,
        phase2_string,
        phase2_null_uint8,
        phase2_null_int8,
        phase2_null_int16,
        phase2_null_int32,
        phase2_null_int64,
        phase2_null_int128,
        phase2_null_decimal32,
        phase2_null_decimal64,
        phase2_null_decimal128,
        phase2_null_date,
        phase2_null_timestamp,
        phase2_null_string,
        phase2_slice,
        phase2_slice_two_level,
        phase2_int32_two_level,

        phase1_slice_fx4,
        phase1_slice_fx8,
        phase1_slice_fx16,
        phase2_slice_fx4,
        phase2_slice_fx8,
        phase2_slice_fx16,
    };

    detail::AggHashSetWithKeyPtr hash_set_with_key;
    auto& get_variant() { return hash_set_with_key; }

    template <class Vistor>
    auto visit(Vistor&& vistor) const {
        return std::visit(std::forward<Vistor>(vistor), hash_set_with_key);
    }

    void init(RuntimeState* state, Type type_, AggStatistics* agg_stat);

    void convert_to_two_level(RuntimeState* state);

    void reset();

    size_t capacity() const;

    size_t size() const;

    bool need_expand(size_t increasement) const;

    size_t reserved_memory_usage(const MemPool* pool) const;

    size_t allocated_memory_usage(const MemPool* pool) const;

private:
    Type _type = Type::phase1_slice;
};

} // namespace starrocks
