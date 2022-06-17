// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Aggregator.h

#pragma once

#include "column/hash_set.h"
#include "exec/vectorized/aggregate/agg_hash_map.h"
#include "exec/vectorized/aggregate/agg_hash_set.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

#define APPLY_FOR_AGG_VARIANT_NOT_NULL(M) \
    M(phase1_uint8)                       \
    M(phase1_int8)                        \
    M(phase1_int16)                       \
    M(phase1_int32)                       \
    M(phase1_int64)                       \
    M(phase1_int128)                      \
    M(phase1_decimal32)                   \
    M(phase1_decimal64)                   \
    M(phase1_decimal128)                  \
    M(phase1_date)                        \
    M(phase1_timestamp)                   \
    M(phase1_string)                      \
    M(phase1_slice)                       \
    M(phase1_slice_two_level)             \
    M(phase1_int32_two_level)             \
    M(phase2_uint8)                       \
    M(phase2_int8)                        \
    M(phase2_int16)                       \
    M(phase2_int32)                       \
    M(phase2_int64)                       \
    M(phase2_decimal32)                   \
    M(phase2_decimal64)                   \
    M(phase2_decimal128)                  \
    M(phase2_int128)                      \
    M(phase2_date)                        \
    M(phase2_timestamp)                   \
    M(phase2_string)                      \
    M(phase2_slice)                       \
    M(phase2_slice_two_level)             \
    M(phase2_int32_two_level)             \
    M(phase1_slice_fx4)                   \
    M(phase1_slice_fx8)                   \
    M(phase1_slice_fx16)                  \
    M(phase2_slice_fx4)                   \
    M(phase2_slice_fx8)                   \
    M(phase2_slice_fx16)

#define APPLY_FOR_AGG_VARIANT_NULL(M) \
    M(phase1_null_uint8)              \
    M(phase1_null_int8)               \
    M(phase1_null_int16)              \
    M(phase1_null_int32)              \
    M(phase1_null_int64)              \
    M(phase1_null_int128)             \
    M(phase1_null_decimal32)          \
    M(phase1_null_decimal64)          \
    M(phase1_null_decimal128)         \
    M(phase1_null_date)               \
    M(phase1_null_timestamp)          \
    M(phase1_null_string)             \
    M(phase2_null_uint8)              \
    M(phase2_null_int8)               \
    M(phase2_null_int16)              \
    M(phase2_null_int32)              \
    M(phase2_null_int64)              \
    M(phase2_null_int128)             \
    M(phase2_null_decimal32)          \
    M(phase2_null_decimal64)          \
    M(phase2_null_decimal128)         \
    M(phase2_null_date)               \
    M(phase2_null_timestamp)          \
    M(phase2_null_string)

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

// Hash maps for phase1
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
    Type type = Type::phase1_slice;

    std::unique_ptr<UInt8AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_uint8;
    std::unique_ptr<Int8AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int8;
    std::unique_ptr<Int16AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int16;
    std::unique_ptr<Int32AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int32;
    std::unique_ptr<Int64AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int64;
    std::unique_ptr<Int128AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int128;
    std::unique_ptr<Decimal32AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal32;
    std::unique_ptr<Decimal64AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal64;
    std::unique_ptr<Decimal128AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal128;

    std::unique_ptr<DateAggHashMapWithOneNumberKey<PhmapSeed1>> phase1_date;
    std::unique_ptr<TimeStampAggHashMapWithOneNumberKey<PhmapSeed1>> phase1_timestamp;
    std::unique_ptr<OneStringAggHashMap<PhmapSeed1>> phase1_string;

    std::unique_ptr<NullUInt8AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_uint8;
    std::unique_ptr<NullInt8AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int8;
    std::unique_ptr<NullInt16AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int16;
    std::unique_ptr<NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int32;
    std::unique_ptr<NullInt64AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int64;
    std::unique_ptr<NullInt128AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int128;

    std::unique_ptr<NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal32;
    std::unique_ptr<NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal64;
    std::unique_ptr<NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal128;

    std::unique_ptr<NullDateAggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_date;
    std::unique_ptr<NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_timestamp;
    std::unique_ptr<NullOneStringAggHashMap<PhmapSeed1>> phase1_null_string;
    std::unique_ptr<SerializedKeyAggHashMap<PhmapSeed1>> phase1_slice;
    std::unique_ptr<SerializedKeyTwoLevelAggHashMap<PhmapSeed1>> phase1_slice_two_level;
    std::unique_ptr<Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>> phase1_int32_two_level;

    std::unique_ptr<SerializedKeyFixedSize4AggHashMap<PhmapSeed1>> phase1_slice_fx4;
    std::unique_ptr<SerializedKeyFixedSize8AggHashMap<PhmapSeed1>> phase1_slice_fx8;
    std::unique_ptr<SerializedKeyFixedSize16AggHashMap<PhmapSeed1>> phase1_slice_fx16;

    std::unique_ptr<UInt8AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_uint8;
    std::unique_ptr<Int8AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int8;
    std::unique_ptr<Int16AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int16;
    std::unique_ptr<Int32AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int32;
    std::unique_ptr<Int64AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int64;
    std::unique_ptr<Int128AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int128;

    std::unique_ptr<Decimal32AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_decimal32;
    std::unique_ptr<Decimal64AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_decimal64;
    std::unique_ptr<Decimal128AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_decimal128;

    std::unique_ptr<DateAggHashMapWithOneNumberKey<PhmapSeed2>> phase2_date;
    std::unique_ptr<TimeStampAggHashMapWithOneNumberKey<PhmapSeed2>> phase2_timestamp;
    std::unique_ptr<OneStringAggHashMap<PhmapSeed2>> phase2_string;
    std::unique_ptr<NullUInt8AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_uint8;
    std::unique_ptr<NullInt8AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_int8;
    std::unique_ptr<NullInt16AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_int16;
    std::unique_ptr<NullInt32AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_int32;
    std::unique_ptr<NullInt64AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_int64;
    std::unique_ptr<NullInt128AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_int128;

    std::unique_ptr<NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_decimal32;
    std::unique_ptr<NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_decimal64;
    std::unique_ptr<NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_decimal128;

    std::unique_ptr<NullDateAggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_date;
    std::unique_ptr<NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed2>> phase2_null_timestamp;
    std::unique_ptr<NullOneStringAggHashMap<PhmapSeed2>> phase2_null_string;
    std::unique_ptr<SerializedKeyAggHashMap<PhmapSeed2>> phase2_slice;
    std::unique_ptr<SerializedKeyTwoLevelAggHashMap<PhmapSeed2>> phase2_slice_two_level;
    std::unique_ptr<Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed2>> phase2_int32_two_level;

    std::unique_ptr<SerializedKeyFixedSize4AggHashMap<PhmapSeed2>> phase2_slice_fx4;
    std::unique_ptr<SerializedKeyFixedSize8AggHashMap<PhmapSeed2>> phase2_slice_fx8;
    std::unique_ptr<SerializedKeyFixedSize16AggHashMap<PhmapSeed2>> phase2_slice_fx16;

    void init(RuntimeState* state, Type type_) {
        type = type_;
        switch (type_) {
#define M(NAME)                                                                     \
    case Type::NAME:                                                                \
        NAME = std::make_unique<decltype(NAME)::element_type>(state->chunk_size()); \
        break;
            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }
    }

    size_t capacity() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.capacity();
            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }
        return 0;
    }

    size_t size() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.size() + (NAME->null_key_data != nullptr);
            APPLY_FOR_AGG_VARIANT_NULL(M)
#undef M

#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.size();
            APPLY_FOR_AGG_VARIANT_NOT_NULL(M)
#undef M
        }
        return 0;
    }

    size_t reserved_memory_usage(const MemPool* pool) const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.dump_bound() + pool->total_reserved_bytes();

            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }

        return 0;
    }

    size_t allocated_memory_usage(const MemPool* pool) const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return sizeof(decltype(NAME)::element_type::KeyType) * NAME->hash_map.size() + pool->total_allocated_bytes();

            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }

        return 0;
    }
};

// Hash sets
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
    Type type = Type::phase1_slice;

    std::unique_ptr<UInt8AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_uint8;
    std::unique_ptr<Int8AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int8;
    std::unique_ptr<Int16AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int16;
    std::unique_ptr<Int32AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int32;
    std::unique_ptr<Int64AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int64;
    std::unique_ptr<Int128AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int128;

    std::unique_ptr<Decimal32AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_decimal32;
    std::unique_ptr<Decimal64AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_decimal64;
    std::unique_ptr<Decimal128AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_decimal128;

    std::unique_ptr<DateAggHashSetOfOneNumberKey<PhmapSeed1>> phase1_date;
    std::unique_ptr<TimeStampAggHashSetOfOneNumberKey<PhmapSeed1>> phase1_timestamp;
    std::unique_ptr<OneStringAggHashSet<PhmapSeed1>> phase1_string;

    std::unique_ptr<NullUInt8AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_uint8;
    std::unique_ptr<NullInt8AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_int8;
    std::unique_ptr<NullInt16AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_int16;
    std::unique_ptr<NullInt32AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_int32;
    std::unique_ptr<NullInt64AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_int64;
    std::unique_ptr<NullInt128AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_int128;

    std::unique_ptr<NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_decimal32;
    std::unique_ptr<NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_decimal64;
    std::unique_ptr<NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_decimal128;

    std::unique_ptr<NullDateAggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_date;
    std::unique_ptr<NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed1>> phase1_null_timestamp;
    std::unique_ptr<NullOneStringAggHashSet<PhmapSeed1>> phase1_null_string;

    std::unique_ptr<SerializedKeyAggHashSet<PhmapSeed1>> phase1_slice;
    std::unique_ptr<SerializedTwoLevelKeyAggHashSet<PhmapSeed1>> phase1_slice_two_level;
    std::unique_ptr<Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed1>> phase1_int32_two_level;

    std::unique_ptr<UInt8AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_uint8;
    std::unique_ptr<Int8AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int8;
    std::unique_ptr<Int16AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int16;
    std::unique_ptr<Int32AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int32;
    std::unique_ptr<Int64AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int64;
    std::unique_ptr<Int128AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int128;

    std::unique_ptr<Decimal32AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_decimal32;
    std::unique_ptr<Decimal64AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_decimal64;
    std::unique_ptr<Decimal128AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_decimal128;

    std::unique_ptr<DateAggHashSetOfOneNumberKey<PhmapSeed2>> phase2_date;
    std::unique_ptr<TimeStampAggHashSetOfOneNumberKey<PhmapSeed2>> phase2_timestamp;
    std::unique_ptr<OneStringAggHashSet<PhmapSeed2>> phase2_string;

    std::unique_ptr<NullUInt8AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_uint8;
    std::unique_ptr<NullInt8AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_int8;
    std::unique_ptr<NullInt16AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_int16;
    std::unique_ptr<NullInt32AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_int32;
    std::unique_ptr<NullInt64AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_int64;
    std::unique_ptr<NullInt128AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_int128;

    std::unique_ptr<NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_decimal32;
    std::unique_ptr<NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_decimal64;
    std::unique_ptr<NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_decimal128;

    std::unique_ptr<NullDateAggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_date;
    std::unique_ptr<NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed2>> phase2_null_timestamp;
    std::unique_ptr<NullOneStringAggHashSet<PhmapSeed2>> phase2_null_string;

    std::unique_ptr<SerializedKeyAggHashSet<PhmapSeed2>> phase2_slice;
    std::unique_ptr<SerializedTwoLevelKeyAggHashSet<PhmapSeed2>> phase2_slice_two_level;
    std::unique_ptr<Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed2>> phase2_int32_two_level;

    std::unique_ptr<SerializedKeyAggHashSetFixedSize4<PhmapSeed1>> phase1_slice_fx4;
    std::unique_ptr<SerializedKeyAggHashSetFixedSize8<PhmapSeed1>> phase1_slice_fx8;
    std::unique_ptr<SerializedKeyAggHashSetFixedSize16<PhmapSeed1>> phase1_slice_fx16;
    std::unique_ptr<SerializedKeyAggHashSetFixedSize4<PhmapSeed2>> phase2_slice_fx4;
    std::unique_ptr<SerializedKeyAggHashSetFixedSize8<PhmapSeed2>> phase2_slice_fx8;
    std::unique_ptr<SerializedKeyAggHashSetFixedSize16<PhmapSeed2>> phase2_slice_fx16;

    void init(RuntimeState* state, Type type_) {
        type = type_;
        switch (type_) {
#define M(NAME)                                                                     \
    case Type::NAME:                                                                \
        NAME = std::make_unique<decltype(NAME)::element_type>(state->chunk_size()); \
        break;
            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }
    }

    size_t capacity() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_set.capacity();
            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }
        return 0;
    }

    size_t size() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_set.size() + (NAME->has_null_key ? 1 : 0);
            APPLY_FOR_AGG_VARIANT_NULL(M)
#undef M

#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_set.size();
            APPLY_FOR_AGG_VARIANT_NOT_NULL(M)
#undef M
        }
        return 0;
    }

    size_t reserved_memory_usage(const MemPool* pool) const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_set.dump_bound() + pool->total_reserved_bytes();
            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }
        return 0;
    }

    size_t allocated_memory_usage(const MemPool* pool) const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return sizeof(decltype(NAME)::element_type::KeyType) * NAME->hash_set.size() + pool->total_allocated_bytes();

            APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
        }

        return 0;
    }
};

} // namespace starrocks::vectorized
