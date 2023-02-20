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

#include "exec/partition/partition_hash_variant.h"

#include "exec/partition/partition_hash_map.h"

namespace starrocks {

namespace detail {
template <PartitionHashMapVariant::Type>
struct PartitionHashMapVariantTypeTraits;

#define DEFINE_PARTITION_MAP_TRAITS(enum_value, type)      \
    template <>                                            \
    struct PartitionHashMapVariantTypeTraits<enum_value> { \
        using HashMapWithKeyType = type;                   \
    }

DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_uint8,
                            UInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_int8,
                            Int8PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_int16,
                            Int16PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_int32,
                            Int32PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_int64,
                            Int64PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_int128,
                            Int128PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_decimal32,
                            Decimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_decimal64,
                            Decimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_decimal128,
                            Decimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_date,
                            DatePartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_timestamp,
                            TimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_string, OneStringPartitionHashMap<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_slice, SerializedKeyPartitionHashMap<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_uint8,
                            NullUInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_int8,
                            NullInt8PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_int16,
                            NullInt16PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_int32,
                            NullInt32PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_int64,
                            NullInt64PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_int128,
                            NullInt128PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_decimal32,
                            NullDecimal32PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_decimal64,
                            NullDecimal64PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_decimal128,
                            NullDecimal128PartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_date,
                            NullDatePartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_timestamp,
                            NullTimeStampPartitionHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_null_string,
                            NullOneStringPartitionHashMap<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_slice_fx4,
                            SerializedKeyFixedSize4PartitionHashMap<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_slice_fx8,
                            SerializedKeyFixedSize8PartitionHashMap<PhmapSeed1>);
DEFINE_PARTITION_MAP_TRAITS(PartitionHashMapVariant::Type::phase1_slice_fx16,
                            SerializedKeyFixedSize16PartitionHashMap<PhmapSeed1>);

} // namespace detail

void PartitionHashMapVariant::init(RuntimeState* state, Type type_) {
    type = type_;
    switch (type_) {
#define M(NAME)                                                                                              \
    case Type::NAME:                                                                                         \
        hash_map_with_key =                                                                                  \
                std::make_unique<detail::PartitionHashMapVariantTypeTraits<Type::NAME>::HashMapWithKeyType>( \
                        state->chunk_size());                                                                \
        break;
        M(phase1_uint8);
        M(phase1_int8);
        M(phase1_int16);
        M(phase1_int32);
        M(phase1_int64);
        M(phase1_int128);
        M(phase1_decimal32);
        M(phase1_decimal64);
        M(phase1_decimal128);
        M(phase1_date);
        M(phase1_timestamp);
        M(phase1_string);
        M(phase1_slice);
        M(phase1_null_uint8);
        M(phase1_null_int8);
        M(phase1_null_int16);
        M(phase1_null_int32);
        M(phase1_null_int64);
        M(phase1_null_int128);
        M(phase1_null_decimal32);
        M(phase1_null_decimal64);
        M(phase1_null_decimal128);
        M(phase1_null_date);
        M(phase1_null_timestamp);
        M(phase1_null_string);
        M(phase1_slice_fx4);
        M(phase1_slice_fx8);
        M(phase1_slice_fx16);
#undef M
    }
}

size_t PartitionHashMapVariant::capacity() const {
    return visit([](const auto& hash_map_with_key) { return hash_map_with_key->hash_map.capacity(); });
}

size_t PartitionHashMapVariant::size() const {
    return visit([](const auto& hash_map_with_key) {
        size_t size = hash_map_with_key->hash_map.size();
        if constexpr (std::decay_t<decltype(*hash_map_with_key)>::is_nullable) {
            size += (hash_map_with_key->null_key_value.chunks.empty() ? 0 : 1);
        }
        return size;
    });
}

size_t PartitionHashMapVariant::memory_usage() const {
    return visit([](const auto& hash_map_with_key) { return hash_map_with_key->hash_map.dump_bound(); });
}
} // namespace starrocks
