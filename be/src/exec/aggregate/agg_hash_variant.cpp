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
//
#include "exec/aggregate/agg_hash_variant.h"

#include <type_traits>
#include <variant>

#include "util/phmap/phmap.h"

namespace starrocks {

namespace detail {
template <AggHashMapVariant::Type>
struct AggHashMapVariantTypeTraits;

#define DEFINE_MAP_TYPE(enum_value, type)            \
    template <>                                      \
    struct AggHashMapVariantTypeTraits<enum_value> { \
        using HashMapWithKeyType = type;             \
    }

DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_date, DateAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_string, OneStringAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_null_string, NullOneStringAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice, SerializedKeyAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase1_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed1>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_uint8, UInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int8, Int8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int16, Int16AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int32, Int32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int64, Int64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int128, Int128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal32, Decimal32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal64, Decimal64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_decimal128, Decimal128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_date, DateAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_timestamp, TimeStampAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_string, OneStringAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_uint8, NullUInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int8, NullInt8AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int16, NullInt16AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int32, NullInt32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int64, NullInt64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_int128, NullInt128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal32, NullDecimal32AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal64, NullDecimal64AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_decimal128, NullDecimal128AggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_date, NullDateAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_timestamp, NullTimeStampAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_null_string, NullOneStringAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice, SerializedKeyAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_two_level, SerializedKeyTwoLevelAggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_int32_two_level, Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx4, SerializedKeyFixedSize4AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx8, SerializedKeyFixedSize8AggHashMap<PhmapSeed2>);
DEFINE_MAP_TYPE(AggHashMapVariant::Type::phase2_slice_fx16, SerializedKeyFixedSize16AggHashMap<PhmapSeed2>);

template <AggHashSetVariant::Type>
struct AggHashSetVariantTypeTraits;

#define DEFINE_SET_TYPE(enum_value, type)            \
    template <>                                      \
    struct AggHashSetVariantTypeTraits<enum_value> { \
        using HashSetWithKeyType = type;             \
    }

DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_uint8, UInt8AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int8, Int8AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int16, Int16AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int32, Int32AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int64, Int64AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int128, Int128AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_decimal32, Decimal32AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_decimal64, Decimal64AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_decimal128, Decimal128AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_date, DateAggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_timestamp, TimeStampAggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_string, OneStringAggHashSet<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_uint8, NullUInt8AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_int8, NullInt8AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_int16, NullInt16AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_int32, NullInt32AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_int64, NullInt64AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_int128, NullInt128AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_decimal32, NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_decimal64, NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_decimal128, NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_date, NullDateAggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_timestamp, NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_null_string, NullOneStringAggHashSet<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_slice, SerializedKeyAggHashSet<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_slice_two_level, SerializedTwoLevelKeyAggHashSet<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_int32_two_level, Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_uint8, UInt8AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int8, Int8AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int16, Int16AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int32, Int32AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int64, Int64AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int128, Int128AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_decimal32, Decimal32AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_decimal64, Decimal64AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_decimal128, Decimal128AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_date, DateAggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_timestamp, TimeStampAggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_string, OneStringAggHashSet<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_uint8, NullUInt8AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_int8, NullInt8AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_int16, NullInt16AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_int32, NullInt32AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_int64, NullInt64AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_int128, NullInt128AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_decimal32, NullDecimal32AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_decimal64, NullDecimal64AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_decimal128, NullDecimal128AggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_date, NullDateAggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_timestamp, NullTimeStampAggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_null_string, NullOneStringAggHashSet<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_slice, SerializedKeyAggHashSet<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_slice_two_level, SerializedTwoLevelKeyAggHashSet<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_int32_two_level, Int32TwoLevelAggHashSetOfOneNumberKey<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_slice_fx4, SerializedKeyAggHashSetFixedSize4<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_slice_fx8, SerializedKeyAggHashSetFixedSize8<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase1_slice_fx16, SerializedKeyAggHashSetFixedSize16<PhmapSeed1>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_slice_fx4, SerializedKeyAggHashSetFixedSize4<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_slice_fx8, SerializedKeyAggHashSetFixedSize8<PhmapSeed2>);
DEFINE_SET_TYPE(AggHashSetVariant::Type::phase2_slice_fx16, SerializedKeyAggHashSetFixedSize16<PhmapSeed2>);

} // namespace detail
void AggHashMapVariant::init(RuntimeState* state, Type type, AggStatistics* agg_stat) {
    _type = type;
    _agg_stat = agg_stat;
    switch (_type) {
#define M(NAME)                                                                                                    \
    case Type::NAME:                                                                                               \
        hash_map_with_key = std::make_unique<detail::AggHashMapVariantTypeTraits<Type::NAME>::HashMapWithKeyType>( \
                state->chunk_size(), _agg_stat);                                                                   \
        break;
        APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
    }
}

#define CONVERT_TO_TWO_LEVEL_MAP(DST, SRC)                                                                            \
    if (_type == AggHashMapVariant::Type::SRC) {                                                                      \
        auto dst = std::make_unique<detail::AggHashMapVariantTypeTraits<Type::DST>::HashMapWithKeyType>(              \
                state->chunk_size(), _agg_stat);                                                                      \
        std::visit(                                                                                                   \
                [&](auto& hash_map_with_key) {                                                                        \
                    if constexpr (std::is_same_v<typename decltype(hash_map_with_key->hash_map)::key_type,            \
                                                 typename decltype(dst->hash_map)::key_type>) {                       \
                        dst->hash_map.reserve(hash_map_with_key->hash_map.capacity());                                \
                        dst->hash_map.insert(hash_map_with_key->hash_map.begin(), hash_map_with_key->hash_map.end()); \
                    }                                                                                                 \
                },                                                                                                    \
                hash_map_with_key);                                                                                   \
                                                                                                                      \
        _type = AggHashMapVariant::Type::DST;                                                                         \
        hash_map_with_key = std::move(dst);                                                                           \
        return;                                                                                                       \
    }

void AggHashMapVariant::convert_to_two_level(RuntimeState* state) {
    CONVERT_TO_TWO_LEVEL_MAP(phase1_slice_two_level, phase1_slice);
    CONVERT_TO_TWO_LEVEL_MAP(phase2_slice_two_level, phase2_slice);
}

void AggHashMapVariant::reset() {
    detail::AggHashMapWithKeyPtr ptr;
    hash_map_with_key = std::move(ptr);
}

size_t AggHashMapVariant::capacity() const {
    return visit([](const auto& hash_map_with_key) { return hash_map_with_key->hash_map.capacity(); });
}

size_t AggHashMapVariant::size() const {
    return visit([](const auto& hash_map_with_key) {
        return hash_map_with_key->hash_map.size() + (hash_map_with_key->get_null_key_data() != nullptr);
    });
}

bool AggHashMapVariant::need_expand(size_t increasement) const {
    size_t capacity = this->capacity();
    // TODO: think about two-level hashmap
    size_t size = this->size() + increasement;
    // see detail implement in reset_growth_left
    return size >= capacity - capacity / 8;
}

size_t AggHashMapVariant::reserved_memory_usage(const MemPool* pool) const {
    return visit([pool](const auto& hash_map_with_key) {
        size_t pool_bytes = (pool != nullptr) ? pool->total_reserved_bytes() : 0;
        return hash_map_with_key->hash_map.dump_bound() + pool_bytes;
    });
}

size_t AggHashMapVariant::allocated_memory_usage(const MemPool* pool) const {
    return visit([pool](const auto& hash_map_with_key) {
        return sizeof(typename decltype(hash_map_with_key->hash_map)::key_type) *
                       hash_map_with_key->hash_map.capacity() +
               pool->total_allocated_bytes();
    });
}

void AggHashSetVariant::init(RuntimeState* state, Type type, AggStatistics* agg_stat) {
    _type = type;
    _agg_stat = agg_stat;
    switch (_type) {
#define M(NAME)                                                                                                    \
    case Type::NAME:                                                                                               \
        hash_set_with_key = std::make_unique<detail::AggHashSetVariantTypeTraits<Type::NAME>::HashSetWithKeyType>( \
                state->chunk_size(), _agg_stat);                                                                   \
        break;
        APPLY_FOR_AGG_VARIANT_ALL(M)
#undef M
    }
}

#define CONVERT_TO_TWO_LEVEL_SET(DST, SRC)                                                                            \
    if (_type == AggHashSetVariant::Type::SRC) {                                                                      \
        auto dst = std::make_unique<detail::AggHashSetVariantTypeTraits<Type::DST>::HashSetWithKeyType>(              \
                state->chunk_size(), _agg_stat);                                                                      \
        std::visit(                                                                                                   \
                [&](auto& hash_set_with_key) {                                                                        \
                    if constexpr (std::is_same_v<typename decltype(hash_set_with_key->hash_set)::key_type,            \
                                                 typename decltype(dst->hash_set)::key_type>) {                       \
                        dst->hash_set.reserve(hash_set_with_key->hash_set.capacity());                                \
                        dst->hash_set.insert(hash_set_with_key->hash_set.begin(), hash_set_with_key->hash_set.end()); \
                    }                                                                                                 \
                },                                                                                                    \
                hash_set_with_key);                                                                                   \
        _type = AggHashSetVariant::Type::DST;                                                                         \
        hash_set_with_key = std::move(dst);                                                                           \
        return;                                                                                                       \
    }

void AggHashSetVariant::convert_to_two_level(RuntimeState* state) {
    CONVERT_TO_TWO_LEVEL_SET(phase1_slice_two_level, phase1_slice);
    CONVERT_TO_TWO_LEVEL_SET(phase2_slice_two_level, phase2_slice);
}

void AggHashSetVariant::reset() {
    detail::AggHashSetWithKeyPtr ptr;
    hash_set_with_key = std::move(ptr);
}

size_t AggHashSetVariant::capacity() const {
    return visit([](auto& hash_set_with_key) { return hash_set_with_key->hash_set.capacity(); });
}

size_t AggHashSetVariant::size() const {
    return visit([](auto& hash_set_with_key) {
        size_t sz = hash_set_with_key->hash_set.size();
        if constexpr (std::decay_t<decltype(*hash_set_with_key)>::has_single_null_key) {
            sz += hash_set_with_key->has_null_key ? 1 : 0;
        }
        return sz;
    });
}

bool AggHashSetVariant::need_expand(size_t increasement) const {
    size_t capacity = this->capacity();
    size_t size = this->size() + increasement;
    // see detail implement in reset_growth_left
    return size >= capacity - capacity / 8;
}

size_t AggHashSetVariant::reserved_memory_usage(const MemPool* pool) const {
    return visit([&](auto& hash_set_with_key) {
        size_t pool_bytes = pool != nullptr ? pool->total_reserved_bytes() : 0;
        return hash_set_with_key->hash_set.dump_bound() + pool_bytes;
    });
}

size_t AggHashSetVariant::allocated_memory_usage(const MemPool* pool) const {
    return visit([&](auto& hash_set_with_key) {
        return sizeof(typename decltype(hash_set_with_key->hash_set)::key_type) *
                       hash_set_with_key->hash_set.capacity() +
               pool->total_allocated_bytes();
    });
}

} // namespace starrocks
