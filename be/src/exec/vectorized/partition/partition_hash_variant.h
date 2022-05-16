// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/vectorized/partition/partition_hash_map.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

#define APPLY_FOR_PARTITION_VARIANT_NOT_NULL(M) \
    M(phase1_uint8)                             \
    M(phase1_int8)                              \
    M(phase1_int16)                             \
    M(phase1_int32)                             \
    M(phase1_int64)                             \
    M(phase1_int128)                            \
    M(phase1_decimal32)                         \
    M(phase1_decimal64)                         \
    M(phase1_decimal128)                        \
    M(phase1_date)                              \
    M(phase1_timestamp)                         \
    M(phase1_string)                            \
    M(phase1_slice)                             \
    M(phase1_slice_fx4)                         \
    M(phase1_slice_fx8)                         \
    M(phase1_slice_fx16)

#define APPLY_FOR_PARTITION_VARIANT_NULL(M) \
    M(phase1_null_uint8)                    \
    M(phase1_null_int8)                     \
    M(phase1_null_int16)                    \
    M(phase1_null_int32)                    \
    M(phase1_null_int64)                    \
    M(phase1_null_int128)                   \
    M(phase1_null_decimal32)                \
    M(phase1_null_decimal64)                \
    M(phase1_null_decimal128)               \
    M(phase1_null_date)                     \
    M(phase1_null_timestamp)                \
    M(phase1_null_string)

#define APPLY_FOR_PARTITION_VARIANT_ALL(M) \
    M(phase1_uint8)                        \
    M(phase1_int8)                         \
    M(phase1_int16)                        \
    M(phase1_int32)                        \
    M(phase1_int64)                        \
    M(phase1_int128)                       \
    M(phase1_decimal32)                    \
    M(phase1_decimal64)                    \
    M(phase1_decimal128)                   \
    M(phase1_date)                         \
    M(phase1_timestamp)                    \
    M(phase1_string)                       \
    M(phase1_slice)                        \
    M(phase1_null_uint8)                   \
    M(phase1_null_int8)                    \
    M(phase1_null_int16)                   \
    M(phase1_null_int32)                   \
    M(phase1_null_int64)                   \
    M(phase1_null_int128)                  \
    M(phase1_null_decimal32)               \
    M(phase1_null_decimal64)               \
    M(phase1_null_decimal128)              \
    M(phase1_null_date)                    \
    M(phase1_null_timestamp)               \
    M(phase1_null_string)                  \
    M(phase1_slice_fx4)                    \
    M(phase1_slice_fx8)                    \
    M(phase1_slice_fx16)

// Hash maps for phase1
template <PhmapSeed seed>
using UInt8PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_BOOLEAN, Int8PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Int8PartitionHashMapWithOneNumberKey = PartitionHashMapWithOneNumberKey<TYPE_TINYINT, Int8PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Int16PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_SMALLINT, Int16PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Int32PartitionHashMapWithOneNumberKey = PartitionHashMapWithOneNumberKey<TYPE_INT, Int32PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Int64PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_BIGINT, Int64PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Int128PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_LARGEINT, Int128PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Decimal32PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_DECIMAL32, Int32PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Decimal64PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_DECIMAL64, Int64PartitionHashMap<seed>>;
template <PhmapSeed seed>
using Decimal128PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_DECIMAL128, Int128PartitionHashMap<seed>>;

template <PhmapSeed seed>
using DatePartitionHashMapWithOneNumberKey = PartitionHashMapWithOneNumberKey<TYPE_DATE, DatePartitionHashMap<seed>>;
template <PhmapSeed seed>
using TimeStampPartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNumberKey<TYPE_DATETIME, TimeStampPartitionHashMap<seed>>;

template <PhmapSeed seed>
using NullUInt8PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_BOOLEAN, Int8PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullInt8PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_TINYINT, Int8PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullInt16PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_SMALLINT, Int16PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullInt32PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_INT, Int32PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullInt64PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_BIGINT, Int64PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullInt128PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_LARGEINT, Int128PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal32PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_DECIMAL32, Int32PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal64PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_DECIMAL64, Int64PartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullDecimal128PartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_DECIMAL128, Int128PartitionHashMap<seed>>;

template <PhmapSeed seed>
using NullDatePartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_DATE, DatePartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullTimeStampPartitionHashMapWithOneNumberKey =
        PartitionHashMapWithOneNullableNumberKey<TYPE_DATETIME, TimeStampPartitionHashMap<seed>>;

// For string type, we use slice type as hashmap key
template <PhmapSeed seed>
using OneStringPartitionHashMap = PartitionHashMapWithOneStringKey<SlicePartitionHashMap<seed>>;
template <PhmapSeed seed>
using NullOneStringPartitionHashMap = PartitionHashMapWithOneNullableStringKey<SlicePartitionHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyPartitionHashMap = PartitionHashMapWithSerializedKey<SlicePartitionHashMap<seed>>;

// fixed slice key type.
template <PhmapSeed seed>
using SerializedKeyFixedSize4PartitionHashMap =
        PartitionHashMapWithSerializedKeyFixedSize<FixedSize4SlicePartitionHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyFixedSize8PartitionHashMap =
        PartitionHashMapWithSerializedKeyFixedSize<FixedSize8SlicePartitionHashMap<seed>>;
template <PhmapSeed seed>
using SerializedKeyFixedSize16PartitionHashMap =
        PartitionHashMapWithSerializedKeyFixedSize<FixedSize16SlicePartitionHashMap<seed>>;

// For different partition columns type, size, cardinality, volume, we should choose different
// hash functions and different hashmaps.
// When runtime, we will only have one hashmap.
// The type name and hashmap name is the same, so we could easily call hashmap by type.
// Before you use the underlying hashmap, you should call init method firstly.
struct PartitionHashMapVariant {
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

        phase1_slice_fx4,
        phase1_slice_fx8,
        phase1_slice_fx16,

    };
    Type type = Type::phase1_slice;

    std::unique_ptr<UInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_uint8;
    std::unique_ptr<Int8PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_int8;
    std::unique_ptr<Int16PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_int16;
    std::unique_ptr<Int32PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_int32;
    std::unique_ptr<Int64PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_int64;
    std::unique_ptr<Int128PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_int128;
    std::unique_ptr<Decimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal32;
    std::unique_ptr<Decimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal64;
    std::unique_ptr<Decimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_decimal128;

    std::unique_ptr<DatePartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_date;
    std::unique_ptr<TimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_timestamp;
    std::unique_ptr<OneStringPartitionHashMap<PhmapSeed1>> phase1_string;

    std::unique_ptr<NullUInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_uint8;
    std::unique_ptr<NullInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int8;
    std::unique_ptr<NullInt16PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int16;
    std::unique_ptr<NullInt32PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int32;
    std::unique_ptr<NullInt64PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int64;
    std::unique_ptr<NullInt128PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_int128;

    std::unique_ptr<NullDecimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal32;
    std::unique_ptr<NullDecimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal64;
    std::unique_ptr<NullDecimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_decimal128;

    std::unique_ptr<NullDatePartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_date;
    std::unique_ptr<NullTimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>> phase1_null_timestamp;
    std::unique_ptr<NullOneStringPartitionHashMap<PhmapSeed1>> phase1_null_string;
    std::unique_ptr<SerializedKeyPartitionHashMap<PhmapSeed1>> phase1_slice;

    std::unique_ptr<SerializedKeyFixedSize4PartitionHashMap<PhmapSeed1>> phase1_slice_fx4;
    std::unique_ptr<SerializedKeyFixedSize8PartitionHashMap<PhmapSeed1>> phase1_slice_fx8;
    std::unique_ptr<SerializedKeyFixedSize16PartitionHashMap<PhmapSeed1>> phase1_slice_fx16;

    void init(RuntimeState* state, Type type_) {
        type = type_;
        switch (type_) {
#define M(NAME)                                                                     \
    case Type::NAME:                                                                \
        NAME = std::make_unique<decltype(NAME)::element_type>(state->chunk_size()); \
        break;
            APPLY_FOR_PARTITION_VARIANT_ALL(M)
#undef M
        }
    }

    size_t capacity() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.capacity();
            APPLY_FOR_PARTITION_VARIANT_ALL(M)
#undef M
        }
        return 0;
    }

    size_t size() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.size() + (NAME->null_key_value.chunks.empty() ? 0 : 1);
            APPLY_FOR_PARTITION_VARIANT_NULL(M)
#undef M

#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.size();
            APPLY_FOR_PARTITION_VARIANT_NOT_NULL(M)
#undef M
        }
        return 0;
    }

    size_t memory_usage() const {
        switch (type) {
#define M(NAME)      \
    case Type::NAME: \
        return NAME->hash_map.dump_bound();
            APPLY_FOR_PARTITION_VARIANT_ALL(M)
#undef M
        }
        return 0;
    }
};
} // namespace starrocks::vectorized
