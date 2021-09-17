// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#pragma once

#include <arrow/array.h>
#include <arrow/status.h>

#include "util/pred_guard.h"

namespace starrocks::vectorized {
using ArrowTypeId = ::arrow::Type::type;

template <ArrowTypeId AT>
struct ArrowTypeStructTraits {};
#define M_ArrowTypeValueToTypeStruct(v, s)                       \
    template <>                                                  \
    struct ArrowTypeStructTraits<v> {                            \
        using TypeStruct = std::enable_if_t<s::type_id == v, s>; \
    }

M_ArrowTypeValueToTypeStruct(ArrowTypeId::BOOL, arrow::BooleanType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::INT8, arrow::Int8Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::UINT8, arrow::UInt8Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::INT16, arrow::Int16Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::UINT16, arrow::UInt16Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::INT32, arrow::Int32Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::UINT32, arrow::UInt32Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::INT64, arrow::Int64Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::UINT64, arrow::UInt64Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::HALF_FLOAT, arrow::HalfFloatType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::FLOAT, arrow::FloatType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::DOUBLE, arrow::DoubleType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::BINARY, arrow::BinaryType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::STRING, arrow::StringType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::FIXED_SIZE_BINARY, arrow::FixedSizeBinaryType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::LARGE_BINARY, arrow::LargeBinaryType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::LARGE_STRING, arrow::LargeStringType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::DECIMAL, arrow::Decimal128Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::DATE32, arrow::Date32Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::DATE64, arrow::Date64Type);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::TIMESTAMP, arrow::TimestampType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::LIST, arrow::ListType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::LARGE_LIST, arrow::LargeListType);
M_ArrowTypeValueToTypeStruct(ArrowTypeId::FIXED_SIZE_LIST, arrow::FixedSizeListType);

template <ArrowTypeId AT>
using ArrowTypeValueToStruct = typename ArrowTypeStructTraits<AT>::TypeStruct;

template <ArrowTypeId AT>
using ArrowTypeValueToArrayType = typename arrow::TypeTraits<ArrowTypeValueToStruct<AT>>::ArrayType;
template <ArrowTypeId AT, typename = guard::Guard>
struct ArrowTypeValueToCppTypeStruct {
    using type = typename ArrowTypeValueToArrayType<AT>::value_type;
};
template <>
struct ArrowTypeValueToCppTypeStruct<ArrowTypeId::BOOL, guard::Guard> {
    using type = bool;
};

VALUE_GUARD(ArrowTypeId, BinaryATGuard, at_is_binary, ArrowTypeId::BINARY, ArrowTypeId::STRING,
            ArrowTypeId::FIXED_SIZE_BINARY, ArrowTypeId::LARGE_BINARY, ArrowTypeId::LARGE_STRING)

template <ArrowTypeId AT>
struct ArrowTypeValueToCppTypeStruct<AT, BinaryATGuard<AT>> {
    using type = const uint8_t*;
};
template <>
struct ArrowTypeValueToCppTypeStruct<ArrowTypeId::DECIMAL, guard::Guard> {
    using type = const uint8_t*;
};

template <ArrowTypeId AT>
using ArrowTypeValueToCppType = typename ArrowTypeValueToCppTypeStruct<AT>::type;
} // namespace starrocks::vectorized
