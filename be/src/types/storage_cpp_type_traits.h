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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/types.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>

#include "base/string/slice.h"
#include "base/types/decimal12.h"
#include "base/types/int256.h"
#include "base/types/uint24.h"
#include "types/collection.h"
#include "types/decimalv2_value.h"
#include "types/logical_type.h"

namespace starrocks {

// StorageCppTypeTraits: maps LogicalType to the on-disk C++ primitive type.
// This header has no dependency on column/ so it can be used by TypesCore.
template <LogicalType field_type>
struct StorageCppTypeTraits {};

template <>
struct StorageCppTypeTraits<TYPE_BOOLEAN> {
    using CppType = bool;
    using UnsignedCppType = bool;
};
template <>
struct StorageCppTypeTraits<TYPE_NONE> {
    using CppType = bool;
    using UnsignedCppType = bool;
};
template <>
struct StorageCppTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct StorageCppTypeTraits<TYPE_UNSIGNED_TINYINT> {
    using CppType = uint8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct StorageCppTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct StorageCppTypeTraits<TYPE_UNSIGNED_SMALLINT> {
    using CppType = uint16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct StorageCppTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct StorageCppTypeTraits<TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct StorageCppTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct StorageCppTypeTraits<TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct StorageCppTypeTraits<TYPE_LARGEINT> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct StorageCppTypeTraits<TYPE_INT256> {
    using CppType = int256_t;
    using UnsignedCppType = int256_t;
};
template <>
struct StorageCppTypeTraits<TYPE_FLOAT> {
    using CppType = float;
};
template <>
struct StorageCppTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMAL> {
    using CppType = decimal12_t;
    using UnsignedCppType = decimal12_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    using UnsignedCppType = DecimalV2Value;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMAL32> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMAL64> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMAL128> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DECIMAL256> {
    using CppType = int256_t;
    using UnsignedCppType = int256_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DATE_V1> {
    using CppType = uint24_t;
    using UnsignedCppType = uint24_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DATE> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DATETIME_V1> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct StorageCppTypeTraits<TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct StorageCppTypeTraits<TYPE_CHAR> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_VARCHAR> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_HLL> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_OBJECT> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_PERCENTILE> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_JSON> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_VARIANT> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_VARBINARY> {
    using CppType = Slice;
};
template <>
struct StorageCppTypeTraits<TYPE_ARRAY> {
    using CppType = Collection;
};

template <LogicalType Type>
using StorageCppType = typename StorageCppTypeTraits<Type>::CppType;

template <LogicalType Type>
using StorageUnsignedCppType = typename StorageCppTypeTraits<Type>::UnsignedCppType;

template <LogicalType Type>
constexpr int32_t StorageCppTypeSize = sizeof(StorageCppType<Type>);

} // namespace starrocks