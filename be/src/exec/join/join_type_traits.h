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

#include "util/runtime_profile.h"

#define JOIN_HASH_MAP_H

#include "join_hash_map_helper.h"
#include "join_hash_map_method.hpp"
#include "join_hash_table_descriptor.h"
#include "join_key_constructor.hpp"

#if defined(__aarch64__)
#include "arm_acle.h"
#endif

namespace starrocks {

// ------------------------------------------------------------------------------------
// Types
// ------------------------------------------------------------------------------------

#define APPLY_JOIN_KEY_CONSTRUCTOR_TYPE(M) \
    M(ONE_KEY)                             \
    M(SERIALIZED_FIXED_SIZE)               \
    M(SERIALIZED)

#define APPLY_JOIN_HASH_MAP_METHOD_TYPE(M) \
    M(BUCKET_CHAINED)                      \
    M(DIRECT_MAPPING)                      \
    M(RANGE_DIRECT_MAPPING)                \
    M(RANGE_DIRECT_MAPPING_SET)            \
    M(DENSE_RANGE_DIRECT_MAPPING)          \
    M(LINEAR_CHAINED)                      \
    M(LINEAR_CHAINED_SET)                  \
    M(LINEAR_CHAINED_ASOF)

#define APPLY_JOIN_KEY_CONSTRUCTOR_UNARY_TYPE(M) \
    M(ONE_KEY_BOOLEAN)                           \
    M(ONE_KEY_TINYINT)                           \
    M(ONE_KEY_SMALLINT)                          \
    M(ONE_KEY_INT)                               \
    M(ONE_KEY_BIGINT)                            \
    M(ONE_KEY_LARGEINT)                          \
    M(ONE_KEY_FLOAT)                             \
    M(ONE_KEY_DOUBLE)                            \
    M(ONE_KEY_DECIMALV2)                         \
    M(ONE_KEY_DECIMAL32)                         \
    M(ONE_KEY_DECIMAL64)                         \
    M(ONE_KEY_DECIMAL128)                        \
    M(ONE_KEY_DATE)                              \
    M(ONE_KEY_DATETIME)                          \
    M(ONE_KEY_VARCHAR)                           \
    M(SERIALIZED_FIXED_SIZE_INT)                 \
    M(SERIALIZED_FIXED_SIZE_BIGINT)              \
    M(SERIALIZED_FIXED_SIZE_LARGEINT)            \
    M(SERIALIZED_VARCHAR)

#define APPLY_JOIN_HASH_MAP_METHOD_UNARY_TYPE(M) \
    M(DIRECT_MAPPING_BOOLEAN)                    \
    M(DIRECT_MAPPING_TINYINT)                    \
    M(DIRECT_MAPPING_SMALLINT)                   \
                                                 \
    M(BUCKET_CHAINED_INT)                        \
    M(BUCKET_CHAINED_BIGINT)                     \
    M(BUCKET_CHAINED_LARGEINT)                   \
    M(BUCKET_CHAINED_FLOAT)                      \
    M(BUCKET_CHAINED_DOUBLE)                     \
    M(BUCKET_CHAINED_DATE)                       \
    M(BUCKET_CHAINED_DATETIME)                   \
    M(BUCKET_CHAINED_DECIMALV2)                  \
    M(BUCKET_CHAINED_DECIMAL32)                  \
    M(BUCKET_CHAINED_DECIMAL64)                  \
    M(BUCKET_CHAINED_DECIMAL128)                 \
    M(BUCKET_CHAINED_VARCHAR)                    \
                                                 \
    M(RANGE_DIRECT_MAPPING_INT)                  \
    M(RANGE_DIRECT_MAPPING_BIGINT)               \
    M(RANGE_DIRECT_MAPPING_SET_INT)              \
    M(RANGE_DIRECT_MAPPING_SET_BIGINT)           \
    M(DENSE_RANGE_DIRECT_MAPPING_INT)            \
    M(DENSE_RANGE_DIRECT_MAPPING_BIGINT)         \
                                                 \
    M(LINEAR_CHAINED_INT)                        \
    M(LINEAR_CHAINED_BIGINT)                     \
    M(LINEAR_CHAINED_LARGEINT)                   \
    M(LINEAR_CHAINED_FLOAT)                      \
    M(LINEAR_CHAINED_DOUBLE)                     \
    M(LINEAR_CHAINED_DATE)                       \
    M(LINEAR_CHAINED_DATETIME)                   \
    M(LINEAR_CHAINED_DECIMALV2)                  \
    M(LINEAR_CHAINED_DECIMAL32)                  \
    M(LINEAR_CHAINED_DECIMAL64)                  \
    M(LINEAR_CHAINED_DECIMAL128)                 \
    M(LINEAR_CHAINED_VARCHAR)                    \
                                                 \
    M(LINEAR_CHAINED_SET_INT)                    \
    M(LINEAR_CHAINED_SET_BIGINT)                 \
    M(LINEAR_CHAINED_SET_LARGEINT)               \
    M(LINEAR_CHAINED_SET_FLOAT)                  \
    M(LINEAR_CHAINED_SET_DOUBLE)                 \
    M(LINEAR_CHAINED_SET_DATE)                   \
    M(LINEAR_CHAINED_SET_DATETIME)               \
    M(LINEAR_CHAINED_SET_DECIMALV2)              \
    M(LINEAR_CHAINED_SET_DECIMAL32)              \
    M(LINEAR_CHAINED_SET_DECIMAL64)              \
    M(LINEAR_CHAINED_SET_DECIMAL128)             \
    M(LINEAR_CHAINED_SET_VARCHAR)                \
                                                 \
    M(LINEAR_CHAINED_ASOF_INT)                   \
    M(LINEAR_CHAINED_ASOF_BIGINT)                \
    M(LINEAR_CHAINED_ASOF_LARGEINT)              \
    M(LINEAR_CHAINED_ASOF_FLOAT)                 \
    M(LINEAR_CHAINED_ASOF_DOUBLE)                \
    M(LINEAR_CHAINED_ASOF_DATE)                  \
    M(LINEAR_CHAINED_ASOF_DATETIME)              \
    M(LINEAR_CHAINED_ASOF_DECIMALV2)             \
    M(LINEAR_CHAINED_ASOF_DECIMAL32)             \
    M(LINEAR_CHAINED_ASOF_DECIMAL64)             \
    M(LINEAR_CHAINED_ASOF_DECIMAL128)            \
    M(LINEAR_CHAINED_ASOF_VARCHAR)

enum class JoinKeyConstructorType {
#define NAME_TO_ENUM(NAME) NAME,
    APPLY_JOIN_KEY_CONSTRUCTOR_TYPE(NAME_TO_ENUM)
#undef NAME_TO_ENUM
};

enum class JoinHashMapMethodType {
#define NAME_TO_ENUM(NAME) NAME,
    APPLY_JOIN_HASH_MAP_METHOD_TYPE(NAME_TO_ENUM)
#undef NAME_TO_ENUM
};

enum class JoinKeyConstructorUnaryType {
#define NAME_TO_ENUM(NAME) NAME,
    APPLY_JOIN_KEY_CONSTRUCTOR_UNARY_TYPE(NAME_TO_ENUM)
#undef NAME_TO_ENUM
};

enum class JoinHashMapMethodUnaryType {
#define NAME_TO_ENUM(NAME) NAME,
    APPLY_JOIN_HASH_MAP_METHOD_UNARY_TYPE(NAME_TO_ENUM)
#undef NAME_TO_ENUM
};

inline std::string join_key_constructor_type_to_string(JoinKeyConstructorType type) {
    switch (type) {
#define M(NAME)                        \
    case JoinKeyConstructorType::NAME: \
        return #NAME;
        APPLY_JOIN_KEY_CONSTRUCTOR_TYPE(M)
#undef M
    default:
        DCHECK(false) << "Unknown JoinKeyConstructorType: " << static_cast<int>(type);
        __builtin_unreachable();
    }
}

inline std::string join_hash_map_method_type_to_string(JoinHashMapMethodType type) {
    switch (type) {
#define M(NAME)                       \
    case JoinHashMapMethodType::NAME: \
        return #NAME;
        APPLY_JOIN_HASH_MAP_METHOD_TYPE(M)
#undef M
    default:
        DCHECK(false) << "Unknown JoinHashMapMethodType: " << static_cast<int>(type);
        __builtin_unreachable();
    }
}

// ------------------------------------------------------------------------------------
// Type Traits
// ------------------------------------------------------------------------------------

template <JoinKeyConstructorType, LogicalType>
struct JoinKeyConstructorTypeTraits {};
template <JoinKeyConstructorUnaryType>
struct JoinKeyConstructorUnaryTypeTraits {};

#define REGISTER_KEY_CONSTRUCTOR(KEY_CONSTRUCTOR_TYPE, LOGICAL_TYPE, KEY_CONSTRUCTOR_IMPL, UNARY_TYPE)               \
    template <>                                                                                                      \
    struct JoinKeyConstructorTypeTraits<JoinKeyConstructorType::KEY_CONSTRUCTOR_TYPE, LOGICAL_TYPE> {                \
        using BuildType = Build##KEY_CONSTRUCTOR_IMPL;                                                               \
        using ProbeType = Probe##KEY_CONSTRUCTOR_IMPL;                                                               \
        static constexpr JoinKeyConstructorUnaryType unary_type = JoinKeyConstructorUnaryType::UNARY_TYPE;           \
    };                                                                                                               \
    template <>                                                                                                      \
    struct JoinKeyConstructorUnaryTypeTraits<JoinKeyConstructorUnaryType::UNARY_TYPE> {                              \
        using BuildType = Build##KEY_CONSTRUCTOR_IMPL;                                                               \
        using ProbeType = Probe##KEY_CONSTRUCTOR_IMPL;                                                               \
        static constexpr JoinKeyConstructorType key_constructor_type = JoinKeyConstructorType::KEY_CONSTRUCTOR_TYPE; \
        static constexpr LogicalType logical_type = LOGICAL_TYPE;                                                    \
    };

REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_BOOLEAN, KeyConstructorForOneKey<TYPE_BOOLEAN>, ONE_KEY_BOOLEAN);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_SMALLINT, KeyConstructorForOneKey<TYPE_SMALLINT>, ONE_KEY_SMALLINT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_TINYINT, KeyConstructorForOneKey<TYPE_TINYINT>, ONE_KEY_TINYINT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_INT, KeyConstructorForOneKey<TYPE_INT>, ONE_KEY_INT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_BIGINT, KeyConstructorForOneKey<TYPE_BIGINT>, ONE_KEY_BIGINT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_LARGEINT, KeyConstructorForOneKey<TYPE_LARGEINT>, ONE_KEY_LARGEINT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_FLOAT, KeyConstructorForOneKey<TYPE_FLOAT>, ONE_KEY_FLOAT);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DOUBLE, KeyConstructorForOneKey<TYPE_DOUBLE>, ONE_KEY_DOUBLE);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DECIMALV2, KeyConstructorForOneKey<TYPE_DECIMALV2>, ONE_KEY_DECIMALV2);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DECIMAL32, KeyConstructorForOneKey<TYPE_DECIMAL32>, ONE_KEY_DECIMAL32);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DECIMAL64, KeyConstructorForOneKey<TYPE_DECIMAL64>, ONE_KEY_DECIMAL64);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DECIMAL128, KeyConstructorForOneKey<TYPE_DECIMAL128>, ONE_KEY_DECIMAL128);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DATE, KeyConstructorForOneKey<TYPE_DATE>, ONE_KEY_DATE);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_DATETIME, KeyConstructorForOneKey<TYPE_DATETIME>, ONE_KEY_DATETIME);
REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_VARCHAR, KeyConstructorForOneKey<TYPE_VARCHAR>, ONE_KEY_VARCHAR);
REGISTER_KEY_CONSTRUCTOR(SERIALIZED_FIXED_SIZE, TYPE_INT, KeyConstructorForSerializedFixedSize<TYPE_INT>,
                         SERIALIZED_FIXED_SIZE_INT);
REGISTER_KEY_CONSTRUCTOR(SERIALIZED_FIXED_SIZE, TYPE_BIGINT, KeyConstructorForSerializedFixedSize<TYPE_BIGINT>,
                         SERIALIZED_FIXED_SIZE_BIGINT);
REGISTER_KEY_CONSTRUCTOR(SERIALIZED_FIXED_SIZE, TYPE_LARGEINT, KeyConstructorForSerializedFixedSize<TYPE_LARGEINT>,
                         SERIALIZED_FIXED_SIZE_LARGEINT);
REGISTER_KEY_CONSTRUCTOR(SERIALIZED, TYPE_VARCHAR, KeyConstructorForSerialized, SERIALIZED_VARCHAR);

#undef REGISTER_KEY_CONSTRUCTOR

template <JoinHashMapMethodType, LogicalType>
struct JoinHashMapMethodTypeTraits {};
template <JoinHashMapMethodUnaryType>
struct JoinHashMapMethodUnaryTypeTraits {};

#define REGISTER_JOIN_MAP_METHOD_TYPE(MAP_TYPE, LOGICAL_TYPE, MAP_IMPL, UNARY_TYPE)                      \
    template <>                                                                                          \
    struct JoinHashMapMethodTypeTraits<JoinHashMapMethodType::MAP_TYPE, LOGICAL_TYPE> {                  \
        using MapType = MAP_IMPL<LOGICAL_TYPE>;                                                          \
        static constexpr JoinHashMapMethodUnaryType unary_type = JoinHashMapMethodUnaryType::UNARY_TYPE; \
    };                                                                                                   \
    template <>                                                                                          \
    struct JoinHashMapMethodUnaryTypeTraits<JoinHashMapMethodUnaryType::UNARY_TYPE> {                    \
        using MapType = MAP_IMPL<LOGICAL_TYPE>;                                                          \
        static constexpr JoinHashMapMethodType map_method_type = JoinHashMapMethodType::MAP_TYPE;        \
        static constexpr LogicalType logical_type = LOGICAL_TYPE;                                        \
    };

REGISTER_JOIN_MAP_METHOD_TYPE(DIRECT_MAPPING, TYPE_BOOLEAN, DirectMappingJoinHashMap, DIRECT_MAPPING_BOOLEAN);
REGISTER_JOIN_MAP_METHOD_TYPE(DIRECT_MAPPING, TYPE_TINYINT, DirectMappingJoinHashMap, DIRECT_MAPPING_TINYINT);
REGISTER_JOIN_MAP_METHOD_TYPE(DIRECT_MAPPING, TYPE_SMALLINT, DirectMappingJoinHashMap, DIRECT_MAPPING_SMALLINT);

REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_INT, BucketChainedJoinHashMap, BUCKET_CHAINED_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_BIGINT, BucketChainedJoinHashMap, BUCKET_CHAINED_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_LARGEINT, BucketChainedJoinHashMap, BUCKET_CHAINED_LARGEINT);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_FLOAT, BucketChainedJoinHashMap, BUCKET_CHAINED_FLOAT);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DOUBLE, BucketChainedJoinHashMap, BUCKET_CHAINED_DOUBLE);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DATE, BucketChainedJoinHashMap, BUCKET_CHAINED_DATE);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DATETIME, BucketChainedJoinHashMap, BUCKET_CHAINED_DATETIME);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DECIMALV2, BucketChainedJoinHashMap, BUCKET_CHAINED_DECIMALV2);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DECIMAL32, BucketChainedJoinHashMap, BUCKET_CHAINED_DECIMAL32);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DECIMAL64, BucketChainedJoinHashMap, BUCKET_CHAINED_DECIMAL64);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_DECIMAL128, BucketChainedJoinHashMap, BUCKET_CHAINED_DECIMAL128);
REGISTER_JOIN_MAP_METHOD_TYPE(BUCKET_CHAINED, TYPE_VARCHAR, BucketChainedJoinHashMap, BUCKET_CHAINED_VARCHAR);

REGISTER_JOIN_MAP_METHOD_TYPE(RANGE_DIRECT_MAPPING, TYPE_INT, RangeDirectMappingJoinHashMap, RANGE_DIRECT_MAPPING_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(RANGE_DIRECT_MAPPING, TYPE_BIGINT, RangeDirectMappingJoinHashMap,
                              RANGE_DIRECT_MAPPING_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(RANGE_DIRECT_MAPPING_SET, TYPE_INT, RangeDirectMappingJoinHashSet,
                              RANGE_DIRECT_MAPPING_SET_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(RANGE_DIRECT_MAPPING_SET, TYPE_BIGINT, RangeDirectMappingJoinHashSet,
                              RANGE_DIRECT_MAPPING_SET_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(DENSE_RANGE_DIRECT_MAPPING, TYPE_INT, DenseRangeDirectMappingJoinHashMap,
                              DENSE_RANGE_DIRECT_MAPPING_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(DENSE_RANGE_DIRECT_MAPPING, TYPE_BIGINT, DenseRangeDirectMappingJoinHashMap,
                              DENSE_RANGE_DIRECT_MAPPING_BIGINT);

REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_INT, LinearChainedJoinHashMap, LINEAR_CHAINED_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_BIGINT, LinearChainedJoinHashMap, LINEAR_CHAINED_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_LARGEINT, LinearChainedJoinHashMap, LINEAR_CHAINED_LARGEINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_FLOAT, LinearChainedJoinHashMap, LINEAR_CHAINED_FLOAT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DOUBLE, LinearChainedJoinHashMap, LINEAR_CHAINED_DOUBLE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DATE, LinearChainedJoinHashMap, LINEAR_CHAINED_DATE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DATETIME, LinearChainedJoinHashMap, LINEAR_CHAINED_DATETIME);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DECIMALV2, LinearChainedJoinHashMap, LINEAR_CHAINED_DECIMALV2);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DECIMAL32, LinearChainedJoinHashMap, LINEAR_CHAINED_DECIMAL32);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DECIMAL64, LinearChainedJoinHashMap, LINEAR_CHAINED_DECIMAL64);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_DECIMAL128, LinearChainedJoinHashMap, LINEAR_CHAINED_DECIMAL128);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED, TYPE_VARCHAR, LinearChainedJoinHashMap, LINEAR_CHAINED_VARCHAR);

REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_INT, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_BIGINT, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_LARGEINT, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_LARGEINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_FLOAT, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_FLOAT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DOUBLE, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_DOUBLE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DATE, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_DATE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DATETIME, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_DATETIME);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DECIMALV2, LinearChainedJoinHashSet,
                              LINEAR_CHAINED_SET_DECIMALV2);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DECIMAL32, LinearChainedJoinHashSet,
                              LINEAR_CHAINED_SET_DECIMAL32);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DECIMAL64, LinearChainedJoinHashSet,
                              LINEAR_CHAINED_SET_DECIMAL64);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_DECIMAL128, LinearChainedJoinHashSet,
                              LINEAR_CHAINED_SET_DECIMAL128);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_SET, TYPE_VARCHAR, LinearChainedJoinHashSet, LINEAR_CHAINED_SET_VARCHAR);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_INT, LinearChainedAsofJoinHashMap, LINEAR_CHAINED_ASOF_INT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_BIGINT, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_BIGINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_LARGEINT, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_LARGEINT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_FLOAT, LinearChainedAsofJoinHashMap, LINEAR_CHAINED_ASOF_FLOAT);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DOUBLE, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DOUBLE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DATE, LinearChainedAsofJoinHashMap, LINEAR_CHAINED_ASOF_DATE);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DATETIME, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DATETIME);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DECIMALV2, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DECIMALV2);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DECIMAL32, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DECIMAL32);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DECIMAL64, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DECIMAL64);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_DECIMAL128, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_DECIMAL128);
REGISTER_JOIN_MAP_METHOD_TYPE(LINEAR_CHAINED_ASOF, TYPE_VARCHAR, LinearChainedAsofJoinHashMap,
                              LINEAR_CHAINED_ASOF_VARCHAR);

#undef REGISTER_JOIN_MAP_TYPE

// ------------------------------------------------------------------------------------
// Type Dispatch
// ------------------------------------------------------------------------------------

template <typename Functor, typename Ret, typename... Args>
auto dispatch_join_logical_type(LogicalType ltype, Ret default_value, Functor fun, Args... args) {
#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

    switch (ltype) {
        _TYPE_DISPATCH_CASE(TYPE_BOOLEAN)
        _TYPE_DISPATCH_CASE(TYPE_TINYINT)
        _TYPE_DISPATCH_CASE(TYPE_SMALLINT)
        _TYPE_DISPATCH_CASE(TYPE_INT)
        _TYPE_DISPATCH_CASE(TYPE_BIGINT)
        _TYPE_DISPATCH_CASE(TYPE_LARGEINT)
        // float will be convert to double, so current can't reach here
        _TYPE_DISPATCH_CASE(TYPE_FLOAT)
        _TYPE_DISPATCH_CASE(TYPE_DOUBLE)
        _TYPE_DISPATCH_CASE(TYPE_DECIMALV2)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL32)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL64)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL128)
        // date will be convert to datetime, so current can't reach here
        _TYPE_DISPATCH_CASE(TYPE_DATE)
        _TYPE_DISPATCH_CASE(TYPE_DATETIME)
        _TYPE_DISPATCH_CASE(TYPE_CHAR)
        _TYPE_DISPATCH_CASE(TYPE_VARCHAR)
    default:
        return default_value;
    }
#undef _TYPE_DISPATCH_CASE
}

template <typename Visitor>
auto dispatch_join_key_constructor_unary(JoinKeyConstructorUnaryType ctype, Visitor&& visitor) {
    switch (ctype) {
#define M(NAME)                                                                  \
    case JoinKeyConstructorUnaryType::NAME:                                      \
        return visitor.template operator()<JoinKeyConstructorUnaryType::NAME>(); \
        break;
        APPLY_JOIN_KEY_CONSTRUCTOR_UNARY_TYPE(M)
#undef M
    default:
        DCHECK(false) << "Unknown JoinKeyConstructorUnaryType: " << static_cast<int>(ctype);
        __builtin_unreachable();
    }
}

template <typename Visitor>
auto dispatch_join_hash_map_method_unary(JoinHashMapMethodUnaryType mtype, Visitor&& visitor) {
    switch (mtype) {
#define M(NAME)                                                                 \
    case JoinHashMapMethodUnaryType::NAME:                                      \
        return visitor.template operator()<JoinHashMapMethodUnaryType::NAME>(); \
        break;
        APPLY_JOIN_HASH_MAP_METHOD_UNARY_TYPE(M)
#undef M
    default:
        DCHECK(false) << "Unknown JoinHashMapMethodUnaryType: " << static_cast<int>(mtype);
        __builtin_unreachable();
    }
}

template <typename Visitor>
auto dispatch_join_hash_map(JoinKeyConstructorUnaryType ctype, JoinHashMapMethodUnaryType mtype, Visitor&& visitor) {
    return dispatch_join_key_constructor_unary(ctype, [&]<JoinKeyConstructorUnaryType CUT>() {
        return dispatch_join_hash_map_method_unary(
                mtype, [&]<JoinHashMapMethodUnaryType MUT>() { return visitor.template operator()<CUT, MUT>(); });
    });
}

} // namespace starrocks
