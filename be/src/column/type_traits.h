// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>

#include "column/binary_column.h"
#include "column/decimalv3_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/primitive_type.h"
#include "types/constexpr.h"
#include "util/json.h"

namespace starrocks {

namespace vectorized {

template <bool B, typename T>
struct cond {
    static constexpr bool value = B;
    using type = T;
};

template <typename Condition, typename... OtherConditions>
struct type_select {
    using type = std::conditional_t<Condition::value, typename Condition::type,
                                    typename type_select<OtherConditions...>::type>;
};

template <typename Condition>
struct type_select<Condition> {
    using type = std::conditional_t<Condition::value, typename Condition::type, void>;
};

template <typename Condition, typename... OtherConditions>
using type_select_t = typename type_select<Condition, OtherConditions...>::type;

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
constexpr bool IsObject = false;
template <>
inline constexpr bool IsObject<HyperLogLog> = true;
template <>
inline constexpr bool IsObject<BitmapValue> = true;
template <>
inline constexpr bool IsObject<PercentileValue> = true;
template <>
inline constexpr bool IsObject<JsonValue> = true;

template <typename T>
using is_starrocks_arithmetic = std::integral_constant<bool, std::is_arithmetic_v<T> || IsDecimal<T>>;

template <typename T>
using is_sum_bigint = std::integral_constant<bool, std::is_integral_v<T> && !IsInt128<T>>;

// If isArithmeticPT is true, means this type support +,-,*,/
template <PrimitiveType primitive_type>
constexpr bool isArithmeticPT = true;

template <>
inline constexpr bool isArithmeticPT<TYPE_CHAR> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_VARCHAR> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_DATE> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_DATETIME> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_HLL> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_OBJECT> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_PERCENTILE> = false;
template <>
inline constexpr bool isArithmeticPT<TYPE_JSON> = false;

template <PrimitiveType primitive_type>
constexpr bool isSlicePT = false;

template <>
inline constexpr bool isSlicePT<TYPE_CHAR> = true;

template <>
inline constexpr bool isSlicePT<TYPE_VARCHAR> = true;

template <PrimitiveType primitive_type>
struct RunTimeTypeTraits {};

template <>
struct RunTimeTypeTraits<TYPE_BOOLEAN> {
    using CppType = uint8_t;
    using ColumnType = BooleanColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using ColumnType = Int8Column;
};

template <>
struct RunTimeTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using ColumnType = Int16Column;
};

template <>
struct RunTimeTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using ColumnType = Int32Column;
};

template <>
struct RunTimeTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using ColumnType = Int64Column;
};

template <>
struct RunTimeTypeTraits<TYPE_LARGEINT> {
    using CppType = int128_t;
    using ColumnType = Int128Column;
};

template <>
struct RunTimeTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using ColumnType = FloatColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using ColumnType = DoubleColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    using ColumnType = DecimalColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL32> {
    using CppType = int32_t;
    using ColumnType = Decimal32Column;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL64> {
    using CppType = int64_t;
    using ColumnType = Decimal64Column;
};

template <>
struct RunTimeTypeTraits<TYPE_DECIMAL128> {
    using CppType = int128_t;
    using ColumnType = Decimal128Column;
};

template <>
struct RunTimeTypeTraits<TYPE_NULL> {
    using CppType = uint8_t;
    using ColumnType = NullColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_CHAR> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_VARCHAR> {
    using CppType = Slice;
    using ColumnType = BinaryColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_DATE> {
    using CppType = DateValue;
    using ColumnType = DateColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_DATETIME> {
    using CppType = TimestampValue;
    using ColumnType = TimestampColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_TIME> {
    using CppType = double;
    using ColumnType = DoubleColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_HLL> {
    using CppType = HyperLogLog*;
    using ColumnType = HyperLogLogColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_OBJECT> {
    using CppType = BitmapValue*;
    using ColumnType = BitmapColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_PERCENTILE> {
    using CppType = PercentileValue*;
    using ColumnType = PercentileColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_JSON> {
    using CppType = JsonValue*;
    using ColumnType = JsonColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_STRUCT> {
    using CppType = DatumStruct;
    using ColumnType = StructColumn;
};

template <>
struct RunTimeTypeTraits<TYPE_MAP> {
    using CppType = DatumMap;
    using ColumnType = MapColumn;
};

template <PrimitiveType Type>
using RunTimeCppType = typename RunTimeTypeTraits<Type>::CppType;

template <PrimitiveType Type>
using RunTimeColumnType = typename RunTimeTypeTraits<Type>::ColumnType;

// Movable: rvalue reference type
template <PrimitiveType Type>
using RunTimeCppMovableType = std::add_rvalue_reference_t<std::remove_pointer_t<RunTimeCppType<Type>>>;

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
struct ColumnTraits<int16_t> {
    using ColumnType = Int16Column;
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
template <PrimitiveType ptype, typename = guard::Guard>
struct RunTimeFixedTypeLength {
    static constexpr size_t value = 0;
};

template <PrimitiveType ptype>
struct RunTimeFixedTypeLength<ptype, FixedLengthPTGuard<ptype>> {
    static constexpr size_t value = sizeof(RunTimeCppType<ptype>);
};

template <PrimitiveType ptype, typename = guard::Guard>
struct RunTimeTypeLimits {};

template <PrimitiveType ptype>
struct RunTimeTypeLimits<ptype, ArithmeticPTGuard<ptype>> {
    // Cpp type of this primitive type
    using value_type = RunTimeCppType<ptype>;

    static constexpr value_type min_value() { return std::numeric_limits<value_type>::lowest(); }
    static constexpr value_type max_value() { return std::numeric_limits<value_type>::max(); }
};

template <>
struct RunTimeTypeLimits<TYPE_LARGEINT> {
    using value_type = RunTimeCppType<TYPE_LARGEINT>;

    static constexpr value_type min_value() { return MIN_INT128; }
    static constexpr value_type max_value() { return MAX_INT128; }
};

template <PrimitiveType ptype>
struct RunTimeTypeLimits<ptype, StringPTGuard<ptype>> {
    using value_type = RunTimeCppType<ptype>;

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

template <PrimitiveType ptype>
struct RunTimeTypeLimits<ptype, DecimalPTGuard<ptype>> {
    using value_type = RunTimeCppType<ptype>;

    static constexpr value_type min_value() { return get_min_decimal<value_type>(); }
    static constexpr value_type max_value() { return get_max_decimal<value_type>(); }
};

template <>
struct RunTimeTypeLimits<TYPE_JSON> {
    using value_type = JsonValue;

    static value_type min_value() { return JsonValue{vpack::Slice::minKeySlice()}; }
    static value_type max_value() { return JsonValue{vpack::Slice::maxKeySlice()}; }
};

} // namespace vectorized
} // namespace starrocks
