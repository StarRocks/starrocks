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

#include <limits>

#include "column/binary_column.h"
#include "column/decimalv3_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "types/constexpr.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

template <bool B, typename T>
struct cond {
    static constexpr bool value = B;
    using type = T;
};

template <typename T>
constexpr bool IsInt128 = false;
template <>
inline constexpr bool IsInt128<int128_t> = true;

template <typename T>
constexpr bool IsSlice = false;
template <>
inline constexpr bool IsSlice<Slice> = true;

template <typename T>
constexpr bool IsDateTime = false;
template <>
inline constexpr bool IsDateTime<TimestampValue> = true;
template <>
inline constexpr bool IsDateTime<DateValue> = true;

template <typename T>
using is_starrocks_arithmetic = std::integral_constant<bool, std::is_arithmetic_v<T> || IsDecimal<T>>;

// If isArithmeticLT is true, means this type support +,-,*,/
template <LogicalType logical_type>
constexpr bool isArithmeticLT = true;

template <>
inline constexpr bool isArithmeticLT<TYPE_CHAR> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_VARCHAR> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_DATE> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_DATETIME> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_HLL> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_OBJECT> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_PERCENTILE> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_JSON> = false;
template <>
inline constexpr bool isArithmeticLT<TYPE_VARBINARY> = false;

template <LogicalType logical_type>
constexpr bool isSliceLT = false;

template <>
inline constexpr bool isSliceLT<TYPE_CHAR> = true;

template <>
inline constexpr bool isSliceLT<TYPE_VARCHAR> = true;

template <>
inline constexpr bool isSliceLT<TYPE_VARBINARY> = true;

template <LogicalType logical_type>
struct RunTimeTypeTraits {};

template <>
struct RunTimeTypeTraits<TYPE_BOOLEAN> {
    using CppType = uint8_t;
    using ColumnType = BooleanColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using ColumnType = Int8Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_UNSIGNED_TINYINT> {
    using CppType = uint8_t;
    using ColumnType = UInt8Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using ColumnType = Int16Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_UNSIGNED_SMALLINT> {
    using CppType = uint16_t;
    using ColumnType = UInt16Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using ColumnType = Int32Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using ColumnType = UInt32Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using ColumnType = Int64Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using ColumnType = UInt64Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_LARGEINT> {
    using CppType = int128_t;
    using ColumnType = Int128Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using ColumnType = FloatColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using ColumnType = DoubleColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    using ColumnType = DecimalColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL32> {
    using CppType = int32_t;
    using ColumnType = Decimal32Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL64> {
    using CppType = int64_t;
    using ColumnType = Decimal64Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL128> {
    using CppType = int128_t;
    using ColumnType = Decimal128Column;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_NULL> {
    using CppType = uint8_t;
    using ColumnType = NullColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_CHAR> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
    using ProxyContainerType = ColumnType::BinaryDataProxyContainer;
};

template <>
struct RunTimeTypeTraits<TYPE_VARCHAR> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
    using ProxyContainerType = ColumnType::BinaryDataProxyContainer;
};

template <>
struct RunTimeTypeTraits<TYPE_DATE> {
    using CppType = DateValue;
    using ColumnType = DateColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_DATETIME> {
    using CppType = TimestampValue;
    using ColumnType = TimestampColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_TIME> {
    using CppType = double;
    using ColumnType = DoubleColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_HLL> {
    using CppType = HyperLogLog*;
    using ColumnType = HyperLogLogColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_OBJECT> {
    using CppType = BitmapValue*;
    using ColumnType = BitmapColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_PERCENTILE> {
    using CppType = PercentileValue*;
    using ColumnType = PercentileColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_JSON> {
    using CppType = JsonValue*;
    using ColumnType = JsonColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_BINARY> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
    using ProxyContainerType = ColumnType::BinaryDataProxyContainer;
};

template <>
struct RunTimeTypeTraits<TYPE_VARBINARY> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
    using ProxyContainerType = ColumnType::BinaryDataProxyContainer;
};

template <>
struct RunTimeTypeTraits<TYPE_STRUCT> {
    using CppType = DatumStruct;
    using ColumnType = StructColumn;
    using ProxyContainerType = ColumnType::Container;
};

template <>
struct RunTimeTypeTraits<TYPE_MAP> {
    using CppType = DatumMap;
    using ColumnType = MapColumn;
    using ProxyContainerType = void;
};

template <>
struct RunTimeTypeTraits<TYPE_ARRAY> {
    using CppType = DatumArray;
    using ColumnType = ArrayColumn;
    using ProxyContainerType = void;
};

template <LogicalType Type>
using RunTimeCppType = typename RunTimeTypeTraits<Type>::CppType;

template <LogicalType Type>
using RunTimeColumnType = typename RunTimeTypeTraits<Type>::ColumnType;

template <LogicalType Type>
using RunTimeProxyContainerType = typename RunTimeTypeTraits<Type>::ProxyContainerType;

// Movable: rvalue reference type
template <LogicalType Type>
using RunTimeCppMovableType = std::add_rvalue_reference_t<std::remove_pointer_t<RunTimeCppType<Type>>>;

template <LogicalType Type>
using RunTimeCppValueType = std::remove_pointer_t<RunTimeCppType<Type>>;

// Value type instead of pointer type

template <typename T>
struct ColumnTraits {};

template <>
struct ColumnTraits<bool> {
    using ColumnType = BooleanColumn;
};

template <>
struct ColumnTraits<int8_t> {
    using ColumnType = Int8Column;
};

template <>
struct ColumnTraits<uint8_t> {
    using ColumnType = UInt8Column;
};

template <>
struct ColumnTraits<int16_t> {
    using ColumnType = Int16Column;
};

template <>
struct ColumnTraits<uint16_t> {
    using ColumnType = UInt16Column;
};

template <>
struct ColumnTraits<int32_t> {
    using ColumnType = Int32Column;
};

template <>
struct ColumnTraits<int64_t> {
    using ColumnType = Int64Column;
};

template <>
struct ColumnTraits<int128_t> {
    using ColumnType = Int128Column;
};

template <>
struct ColumnTraits<float> {
    using ColumnType = FloatColumn;
};

template <>
struct ColumnTraits<double> {
    using ColumnType = DoubleColumn;
};

template <>
struct ColumnTraits<DecimalV2Value> {
    using ColumnType = DecimalColumn;
};

template <>
struct ColumnTraits<Slice> {
    using ColumnType = BinaryColumn;
};

template <>
struct ColumnTraits<DateValue> {
    using ColumnType = DateColumn;
};

template <>
struct ColumnTraits<TimestampValue> {
    using ColumnType = TimestampColumn;
};

// Length of fixed-length type, 0 for dynamic-length type
template <LogicalType ltype, typename = guard::Guard>
struct RunTimeFixedTypeLength {
    static constexpr size_t value = 0;
};

template <LogicalType ltype>
struct RunTimeFixedTypeLength<ltype, FixedLengthLTGuard<ltype>> {
    static constexpr size_t value = sizeof(RunTimeCppType<ltype>);
};

template <LogicalType ltype, typename = guard::Guard>
struct RunTimeTypeLimits {};

template <LogicalType ltype>
struct RunTimeTypeLimits<ltype, ArithmeticLTGuard<ltype>> {
    // Cpp type of this logical type
    using value_type = RunTimeCppType<ltype>;

    static constexpr value_type min_value() { return std::numeric_limits<value_type>::lowest(); }
    static constexpr value_type max_value() { return std::numeric_limits<value_type>::max(); }
};

template <>
struct RunTimeTypeLimits<TYPE_LARGEINT> {
    using value_type = RunTimeCppType<TYPE_LARGEINT>;

    static constexpr value_type min_value() { return MIN_INT128; }
    static constexpr value_type max_value() { return MAX_INT128; }
};

template <LogicalType ltype>
struct RunTimeTypeLimits<ltype, StringLTGuard<ltype>> {
    using value_type = RunTimeCppType<ltype>;

    static constexpr value_type min_value() { return Slice(&_min, 0); }
    static constexpr value_type max_value() { return Slice(&_max, 1); }

private:
    static inline char _min = 0x00;
    static inline char _max = 0xff;
};

template <>
struct RunTimeTypeLimits<TYPE_DATE> {
    using value_type = RunTimeCppType<TYPE_DATE>;

    static value_type min_value() { return DateValue::MIN_DATE_VALUE; }
    static value_type max_value() { return DateValue::MAX_DATE_VALUE; }
};

template <>
struct RunTimeTypeLimits<TYPE_DATETIME> {
    using value_type = RunTimeCppType<TYPE_DATETIME>;

    static value_type min_value() { return TimestampValue::MIN_TIMESTAMP_VALUE; }
    static value_type max_value() { return TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct RunTimeTypeLimits<TYPE_DECIMALV2> {
    using value_type = RunTimeCppType<TYPE_DECIMALV2>;

    static value_type min_value() { return DecimalV2Value::get_min_decimal(); }
    static value_type max_value() { return DecimalV2Value::get_max_decimal(); }
};

template <LogicalType ltype>
struct RunTimeTypeLimits<ltype, DecimalLTGuard<ltype>> {
    using value_type = RunTimeCppType<ltype>;

    static constexpr value_type min_value() { return get_min_decimal<value_type>(); }
    static constexpr value_type max_value() { return get_max_decimal<value_type>(); }
};

template <>
struct RunTimeTypeLimits<TYPE_JSON> {
    using value_type = JsonValue;

    static value_type min_value() { return JsonValue{vpack::Slice::minKeySlice()}; }
    static value_type max_value() { return JsonValue{vpack::Slice::maxKeySlice()}; }
};

} // namespace starrocks
