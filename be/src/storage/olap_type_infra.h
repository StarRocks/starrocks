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

#include "common/logging.h"
#include "common/status.h"
#include "types/logical_type.h"

// Infra to build the type system:
// 1. Macro `APPLY_FOR*` to build generic codes
// 2. field_type_dispatch* function dynamic dispatch types

namespace starrocks {

#define APPLY_FOR_TYPE_INTEGER(M) \
    M(TYPE_TINYINT)               \
    M(TYPE_SMALLINT)              \
    M(TYPE_BIGINT)                \
    M(TYPE_LARGEINT)              \
    M(TYPE_INT)

#define APPLY_FOR_TYPE_DECIMAL(M) \
    M(TYPE_DECIMAL)               \
    M(TYPE_DECIMALV2)             \
    M(TYPE_DECIMAL32)             \
    M(TYPE_DECIMAL64)             \
    M(TYPE_DECIMAL128)

#define APPLY_FOR_TYPE_TIME(M) \
    M(TYPE_DATE_V1)            \
    M(TYPE_DATE)               \
    M(TYPE_DATETIME_V1)        \
    M(TYPE_DATETIME)

// Types that support bitmap index
#define APPLY_FOR_BITMAP_INDEX_TYPE(M) \
    APPLY_FOR_TYPE_INTEGER(M)          \
    APPLY_FOR_TYPE_TIME(M)             \
    M(TYPE_UNSIGNED_INT)               \
    M(TYPE_FLOAT)                      \
    M(TYPE_DOUBLE)                     \
    M(TYPE_CHAR)                       \
    M(TYPE_VARCHAR)                    \
    M(TYPE_BOOLEAN)                    \
    M(TYPE_DECIMAL)                    \
    M(TYPE_DECIMALV2)

// Types that support bloomfilter(exclude tinyint/float/double)
#define APPLY_FOR_BLOOMFILTER_TYPE(M) \
    APPLY_FOR_TYPE_INTEGER(M)         \
    APPLY_FOR_TYPE_DECIMAL(M)         \
    APPLY_FOR_TYPE_TIME(M)            \
    M(TYPE_UNSIGNED_INT)              \
    M(TYPE_CHAR)                      \
    M(TYPE_VARCHAR)

// These types should be synced with LogicalType in olap_common.h
#define APPLY_FOR_BASIC_LOGICAL_TYPE(M) \
    APPLY_FOR_BITMAP_INDEX_TYPE(M)      \
    M(TYPE_JSON)                        \
    M(TYPE_DECIMAL32)                   \
    M(TYPE_DECIMAL64)                   \
    M(TYPE_DECIMAL128)                  \
    M(TYPE_VARBINARY)

#define APPLY_FOR_UNSIGNED_LOGICAL_TYPE(M) \
    M(TYPE_UNSIGNED_INT)                   \
    M(TYPE_UNSIGNED_TINYINT)               \
    M(TYPE_UNSIGNED_SMALLINT)              \
    M(TYPE_UNSIGNED_BIGINT)

#define APPLY_FOR_COMPLEX_LOGICAL_TYPE(M) \
    M(TYPE_HLL)                           \
    M(TYPE_STRUCT)                        \
    M(TYPE_MAP)                           \
    M(TYPE_OBJECT)                        \
    M(TYPE_PERCENTILE)

#define APPLY_FOR_METRIC_FIELD_TYPE(M) \
    M(TYPE_HLL)                        \
    M(TYPE_OBJECT)                     \
    M(TYPE_PERCENTILE)

#define APPLY_FOR_SUPPORTED_FIELD_TYPE(M) \
    APPLY_FOR_BASIC_LOGICAL_TYPE(M)       \
    M(TYPE_UNSIGNED_TINYINT)              \
    M(TYPE_UNSIGNED_SMALLINT)             \
    M(TYPE_UNSIGNED_BIGINT)               \
    M(TYPE_HLL)                           \
    M(TYPE_OBJECT)                        \
    M(TYPE_PERCENTILE)

#define APPLY_FOR_EXTRA_LOGICAL_TYPE(M) \
    APPLY_FOR_UNSIGNED_LOGICAL_TYPE(M)  \
    APPLY_FOR_COMPLEX_LOGICAL_TYPE(M)   \
    M(TYPE_DISCRETE_DOUBLE)             \
    M(TYPE_ARRAY)

#define APPLY_FOR_TYPE_CONVERT_FROM_VARCHAR(M) \
    APPLY_FOR_TYPE_INTEGER(M)                  \
    APPLY_FOR_TYPE_TIME(M)                     \
    APPLY_FOR_TYPE_DECIMAL(M)                  \
    M(TYPE_FLOAT)                              \
    M(TYPE_DOUBLE)                             \
    M(TYPE_JSON)                               \
    M(TYPE_BOOLEAN)

#define APPLY_FOR_TYPE_CONVERT_TO_VARCHAR(M) \
    APPLY_FOR_TYPE_INTEGER(M)                \
    APPLY_FOR_TYPE_TIME(M)                   \
    APPLY_FOR_TYPE_DECIMAL(M)                \
    M(TYPE_FLOAT)                            \
    M(TYPE_JSON)                             \
    M(TYPE_DOUBLE)

#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

template <class Functor, class... Args>
auto field_type_dispatch_basic(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_LOGICAL_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "unknown type " << ftype;
        __builtin_unreachable();
    }
}

// Types could built into columns
template <class Functor, class... Args>
auto field_type_dispatch_column(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_LOGICAL_TYPE(_TYPE_DISPATCH_CASE)
        APPLY_FOR_METRIC_FIELD_TYPE(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(TYPE_ARRAY)
        _TYPE_DISPATCH_CASE(TYPE_MAP)
        _TYPE_DISPATCH_CASE(TYPE_STRUCT)
    default:
        CHECK(false) << "unknown type " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_all_extra(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_LOGICAL_TYPE(_TYPE_DISPATCH_CASE)
        APPLY_FOR_COMPLEX_LOGICAL_TYPE(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(TYPE_DISCRETE_DOUBLE)
        _TYPE_DISPATCH_CASE(TYPE_ARRAY)
        _TYPE_DISPATCH_CASE(TYPE_UNSIGNED_TINYINT)
        _TYPE_DISPATCH_CASE(TYPE_UNSIGNED_SMALLINT)
        _TYPE_DISPATCH_CASE(TYPE_UNSIGNED_BIGINT)
    default:
        CHECK(false) << "Unknown type: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_bitmap_index(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BITMAP_INDEX_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type for bitmap: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_bloomfilter(LogicalType ftype, Functor fun, Args... args) {
    // tinyint is not supported specially
    if (ftype == TYPE_TINYINT) {
        return Status::NotSupported("unsupported type for bloom filter: " + std::to_string(ftype));
    }
    switch (ftype) {
        APPLY_FOR_BLOOMFILTER_TYPE(_TYPE_DISPATCH_CASE)
    default:
        return Status::NotSupported("unsupported type for bloom filter: " + std::to_string(ftype));
    }
    return Status::OK();
}

template <class Functor, class... Args>
auto field_type_dispatch_zonemap_index(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_LOGICAL_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type for bitmap: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_supported(LogicalType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_SUPPORTED_FIELD_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type: " << ftype;
        __builtin_unreachable();
    }
}

#undef _TYPE_DISPATCH_CASE

} // namespace starrocks
