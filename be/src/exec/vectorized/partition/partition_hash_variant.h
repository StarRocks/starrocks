// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/partition/partition_hash_map.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

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

namespace detail {
using PartitionHashMapWithKeyPtr =
        std::variant<std::unique_ptr<UInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Int8PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Int16PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Int32PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Int64PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Int128PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Decimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Decimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<Decimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>>,

                     std::unique_ptr<DatePartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<TimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<OneStringPartitionHashMap<PhmapSeed1>>,
                     std::unique_ptr<SerializedKeyPartitionHashMap<PhmapSeed1>>,

                     std::unique_ptr<NullUInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullInt16PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullInt32PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullInt64PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullInt128PartitionHashMapWithOneNumberKey<PhmapSeed1>>,

                     std::unique_ptr<NullDecimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullDecimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullDecimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>>,

                     std::unique_ptr<NullDatePartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullTimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>>,
                     std::unique_ptr<NullOneStringPartitionHashMap<PhmapSeed1>>,

                     std::unique_ptr<SerializedKeyFixedSize4PartitionHashMap<PhmapSeed1>>,
                     std::unique_ptr<SerializedKeyFixedSize8PartitionHashMap<PhmapSeed1>>,
                     std::unique_ptr<SerializedKeyFixedSize16PartitionHashMap<PhmapSeed1>>>;
}

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
    detail::PartitionHashMapWithKeyPtr hash_map_with_key;
    Type type = Type::phase1_slice;

    template <class Vistor>
    auto visit(Vistor&& vistor) const {
        return std::visit(std::forward<Vistor>(vistor), hash_map_with_key);
    }

    void init(RuntimeState* state, Type type_);

    void reset();

    size_t capacity() const;

    size_t size() const;

    size_t memory_usage() const;

    bool is_nullable() const;
};
} // namespace starrocks::vectorized
