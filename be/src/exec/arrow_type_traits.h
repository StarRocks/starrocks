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

#include <arrow/array.h>
#include <arrow/status.h>

#include "arrow/type.h"
#include "util/pred_guard.h"

namespace starrocks {
using ArrowTypeId = ::arrow::Type::type;

template <ArrowTypeId AT>
struct ArrowTypeStructTraits {};
#define M_ArrowTypeIdToTypeStruct(v, s)                          \
    template <>                                                  \
    struct ArrowTypeStructTraits<v> {                            \
        using TypeStruct = std::enable_if_t<s::type_id == v, s>; \
    }

M_ArrowTypeIdToTypeStruct(ArrowTypeId::BOOL, arrow::BooleanType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::INT8, arrow::Int8Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::UINT8, arrow::UInt8Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::INT16, arrow::Int16Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::UINT16, arrow::UInt16Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::INT32, arrow::Int32Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::UINT32, arrow::UInt32Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::INT64, arrow::Int64Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::UINT64, arrow::UInt64Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::HALF_FLOAT, arrow::HalfFloatType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::FLOAT, arrow::FloatType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::DOUBLE, arrow::DoubleType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::BINARY, arrow::BinaryType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::STRING, arrow::StringType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::FIXED_SIZE_BINARY, arrow::FixedSizeBinaryType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::LARGE_BINARY, arrow::LargeBinaryType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::LARGE_STRING, arrow::LargeStringType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::DECIMAL, arrow::Decimal128Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::DATE32, arrow::Date32Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::DATE64, arrow::Date64Type);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::TIMESTAMP, arrow::TimestampType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::LIST, arrow::ListType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::LARGE_LIST, arrow::LargeListType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::FIXED_SIZE_LIST, arrow::FixedSizeListType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::MAP, arrow::MapType);
M_ArrowTypeIdToTypeStruct(ArrowTypeId::STRUCT, arrow::StructType);

template <ArrowTypeId AT>
using ArrowTypeIdToType = typename ArrowTypeStructTraits<AT>::TypeStruct;

template <ArrowTypeId AT>
using ArrowTypeIdToArrayType = typename arrow::TypeTraits<ArrowTypeIdToType<AT>>::ArrayType;
template <ArrowTypeId AT, typename = guard::Guard>
struct ArrowTypeIdToCppTypeStruct {
    using type = typename ArrowTypeIdToArrayType<AT>::value_type;
};
template <>
struct ArrowTypeIdToCppTypeStruct<ArrowTypeId::BOOL, guard::Guard> {
    using type = bool;
};

VALUE_GUARD(ArrowTypeId, BinaryATGuard, at_is_binary, ArrowTypeId::BINARY, ArrowTypeId::STRING,
            ArrowTypeId::FIXED_SIZE_BINARY, ArrowTypeId::LARGE_BINARY, ArrowTypeId::LARGE_STRING)

template <ArrowTypeId AT>
struct ArrowTypeIdToCppTypeStruct<AT, BinaryATGuard<AT>> {
    using type = const uint8_t*;
};
template <>
struct ArrowTypeIdToCppTypeStruct<ArrowTypeId::DECIMAL, guard::Guard> {
    using type = const uint8_t*;
};
template <>
struct ArrowTypeIdToCppTypeStruct<ArrowTypeId::MAP, guard::Guard> {
    using type = const uint8_t*;
};
template <>
struct ArrowTypeIdToCppTypeStruct<ArrowTypeId::LIST, guard::Guard> {
    using type = const uint8_t*;
};

template <ArrowTypeId AT>
using ArrowTypeIdToCppType = typename ArrowTypeIdToCppTypeStruct<AT>::type;
} // namespace starrocks
