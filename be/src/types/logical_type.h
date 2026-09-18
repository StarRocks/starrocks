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

#include <fmt/format.h>

#include <ostream>
#include <utility>

#include "base/utility/guard.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

enum LogicalType {
    TYPE_UNKNOWN = 0, // UNKNOW Type
    TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    TYPE_UNSIGNED_TINYINT = 2,
    TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    TYPE_UNSIGNED_SMALLINT = 4,
    TYPE_INT = 5, // MYSQL_TYPE_LONG
    TYPE_UNSIGNED_INT = 6,
    TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    TYPE_UNSIGNED_BIGINT = 8,
    TYPE_LARGEINT = 9,
    TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    TYPE_DISCRETE_DOUBLE = 12,
    TYPE_CHAR = 13,        // MYSQL_TYPE_STRING
    TYPE_DATE_V1 = 14,     // MySQL_TYPE_NEWDATE
    TYPE_DATETIME_V1 = 15, // MySQL_TYPE_DATETIME
    TYPE_DECIMAL = 16,     // DECIMAL, using different store format against MySQL
    TYPE_VARCHAR = 17,

    TYPE_STRUCT = 18, // Struct
    TYPE_ARRAY = 19,  // ARRAY
    TYPE_MAP = 20,    // Map
    TYPE_NONE = 22,
    TYPE_HLL = 23,
    TYPE_BOOLEAN = 24,
    TYPE_OBJECT = 25,

    // Added by StarRocks
    TYPE_DECIMAL256 = 26,
    TYPE_INT256 = 27,
    TYPE_GEOGRAPHY = 28,
    TYPE_GEOMETRY = 29,
    // Reserved some field for commutiy version

    TYPE_NULL = 42,
    TYPE_FUNCTION = 43,
    TYPE_TIME = 44,
    TYPE_BINARY = 45,
    TYPE_VARBINARY = 46,
    // decimal v3 type
    TYPE_DECIMAL32 = 47,
    TYPE_DECIMAL64 = 48,
    TYPE_DECIMAL128 = 49,
    TYPE_DATE = 50,
    TYPE_DATETIME = 51,
    TYPE_DECIMALV2 = 52,
    TYPE_PERCENTILE = 53,

    TYPE_JSON = 54,
    TYPE_VARIANT = 55,

    // max value of LogicalType, newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    TYPE_MAX_VALUE = 56
};

// TODO(lism): support varbinary for zone map.
// VARIANT is excluded for the same reason as JSON: its on-disk bytes depend on how the value was
// shredded, so a min/max over them orders nothing meaningful and only bloats the segment footer.
inline bool is_zone_map_key_type(LogicalType type) {
    return type != TYPE_CHAR && type != TYPE_VARCHAR && type != TYPE_JSON && type != TYPE_VARIANT &&
           type != TYPE_VARBINARY && type != TYPE_OBJECT && type != TYPE_HLL && type != TYPE_PERCENTILE &&
           type != TYPE_GEOGRAPHY && type != TYPE_GEOMETRY;
}

// The approximation of FLOAT/DOUBLE in a certain precision range, the binary of byte is not
// a fixed value, so these two types are ignored in calculating checksum.
// And also HLL/OBJCET/PERCENTILE is too large to calculate the checksum.
// And VARIANT, like JSON, has a per-replica physical encoding (shredding), so equal values can
// serialize to different bytes and a checksum mismatch would not mean the replicas diverged.
inline bool is_support_checksum_type(LogicalType type) {
    return type != TYPE_FLOAT && type != TYPE_DOUBLE && type != TYPE_HLL && type != TYPE_OBJECT &&
           type != TYPE_PERCENTILE && type != TYPE_JSON && type != TYPE_VARIANT && type != TYPE_GEOGRAPHY &&
           type != TYPE_GEOMETRY;
}

template <LogicalType TYPE>
inline constexpr LogicalType DelegateType = TYPE;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL32> = TYPE_INT;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL64> = TYPE_BIGINT;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL128> = TYPE_LARGEINT;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL256> = TYPE_INT256;

inline LogicalType delegate_type(LogicalType type) {
    switch (type) {
    case TYPE_DECIMAL32:
        return TYPE_INT;
    case TYPE_DECIMAL64:
        return TYPE_BIGINT;
    case TYPE_DECIMAL128:
        return TYPE_LARGEINT;
    case TYPE_DECIMAL256:
        return TYPE_INT256;
    default:
        return type;
    }
}

inline bool is_integer_type(LogicalType type) {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT || type == TYPE_BIGINT ||
           type == TYPE_LARGEINT;
}

inline bool is_float_type(LogicalType type) {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
}

constexpr bool is_string_type(LogicalType type) {
    return type == LogicalType::TYPE_CHAR || type == LogicalType::TYPE_VARCHAR;
}

// Types backed by ObjectColumn<T>. Compile-time counterpart: lt_is_object_family below; a
// static_assert there pins the two definitions to the same set.
constexpr bool is_object_type(LogicalType type) {
    return type == LogicalType::TYPE_HLL || type == LogicalType::TYPE_OBJECT || type == LogicalType::TYPE_JSON ||
           type == LogicalType::TYPE_PERCENTILE || type == TYPE_VARIANT;
}

inline bool is_decimalv3_field_type(LogicalType type) {
    return type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128 || type == TYPE_DECIMAL256;
}

LogicalType string_to_logical_type(const std::string& type_str);
const char* logical_type_to_string(LogicalType type);

constexpr bool is_binary_type(LogicalType type) {
    return type == TYPE_BINARY || type == TYPE_VARBINARY;
}

inline bool is_scalar_field_type(LogicalType type) {
    switch (type) {
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL256:
        return false;
    default:
        return true;
    }
}

inline bool is_semi_type(LogicalType type) {
    switch (type) {
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_JSON:
    case TYPE_VARIANT:
        return true;
    default:
        return false;
    }
}

inline bool is_complex_metric_type(LogicalType type) {
    switch (type) {
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
    case TYPE_HLL:
        return true;
    default:
        return false;
    }
}

constexpr bool is_enumeration_type(LogicalType type) {
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
        return true;
    default:
        return false;
    }
}

inline bool is_type_compatible(LogicalType lhs, LogicalType rhs) {
    if (lhs == TYPE_FUNCTION || rhs == TYPE_FUNCTION) {
        return false;
    }

    if (lhs == TYPE_VARCHAR) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL || rhs == TYPE_OBJECT;
    }

    if (lhs == TYPE_OBJECT) {
        return rhs == TYPE_VARCHAR || rhs == TYPE_OBJECT;
    }

    if (lhs == TYPE_CHAR || lhs == TYPE_HLL) {
        return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_HLL;
    }

    return lhs == rhs;
}

constexpr bool is_scalar_logical_type(LogicalType ltype) {
    switch (ltype) {
    case TYPE_BOOLEAN:  /* 2 */
    case TYPE_TINYINT:  /* 3 */
    case TYPE_SMALLINT: /* 4 */
    case TYPE_INT:      /* 5 */
    case TYPE_BIGINT:   /* 6 */
    case TYPE_LARGEINT: /* 7 */
    case TYPE_FLOAT:    /* 8 */
    case TYPE_DOUBLE:   /* 9 */
    case TYPE_VARCHAR:  /* 10 */
    case TYPE_DATE:     /* 11 */
    case TYPE_DATETIME: /* 12 */
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        /* 13 */          // Not implemented
    case TYPE_DECIMAL:    /* 14 */
    case TYPE_CHAR:       /* 15 */
    case TYPE_DECIMALV2:  /* 20 */
    case TYPE_TIME:       /* 21 */
    case TYPE_DECIMAL32:  /* 24 */
    case TYPE_DECIMAL64:  /* 25 */
    case TYPE_DECIMAL128: /* 26 */
    case TYPE_DECIMAL256: /* 27 */
    case TYPE_JSON:
    case TYPE_VARIANT:
        return true;
    default:
        return false;
    }
}

constexpr bool support_column_expr_predicate(LogicalType ltype) {
    switch (ltype) {
    case TYPE_BOOLEAN:  /* 2 */
    case TYPE_TINYINT:  /* 3 */
    case TYPE_SMALLINT: /* 4 */
    case TYPE_INT:      /* 5 */
    case TYPE_BIGINT:   /* 6 */
    case TYPE_LARGEINT: /* 7 */
    case TYPE_VARCHAR:  /* 10 */
    case TYPE_DATE:     /* 11 */
    case TYPE_DATETIME: /* 12 */
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        /* 13 */          // Not implemented
    case TYPE_DECIMAL:    /* 14 */
    case TYPE_CHAR:       /* 15 */
    case TYPE_DECIMALV2:  /* 20 */
    case TYPE_TIME:       /* 21 */
    case TYPE_DECIMAL32:  /* 24 */
    case TYPE_DECIMAL64:  /* 25 */
    case TYPE_DECIMAL128: /* 26 */
    case TYPE_DECIMAL256: /* 27 */
    case TYPE_STRUCT:
        return true;
    default:
        return false;
    }
}

constexpr size_t type_estimated_overhead_bytes(LogicalType ltype) {
    switch (ltype) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_ARRAY:
        return 128;
    case TYPE_JSON:
        // 1KB.
        return 1024;
    case TYPE_HLL:
        // 16KB.
        return 16 * 1024;
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
        // 1MB.
        return 1024 * 1024;
    default:
        return 0;
    }
}

// ---------------------------------------------------------------------------------------------
// Type predicate taxonomy
//
// The guards below answer two INDEPENDENT questions about a logical type. Most of the confusion in
// this file's history came from answering one with a predicate that belongs to the other -- e.g.
// using lt_is_collection, a physical-shape predicate, to mean "has no usable ColumnBuilder".
// When adding a logical type, place it on BOTH axes.
//
// Axis 1 -- physical shape: how values are laid out in a Column, i.e. what a typed code path
//           (ColumnViewer / ColumnBuilder / RunTimeCppType / memcpy) is allowed to assume.
//
//   (flat scalar)          everything not listed below  value stored inline in the column buffer
//   lt_is_string           CHAR, VARCHAR                Slice into a shared byte buffer
//   lt_is_binary           BINARY, VARBINARY            ditto
//   lt_is_object_family    HLL, OBJECT (= BITMAP),      value lives in ObjectColumn<T>::_pool;
//                          PERCENTILE, JSON, VARIANT    RunTimeCppType is `T*`, never memcpy-able
//   lt_is_collection       ARRAY, MAP, STRUCT           values live in child columns, so building a
//                                                       column requires TypeDescriptor::children
//   lt_is_row_wise_append  ARRAY, MAP, STRUCT, VARIANT  no usable flat value container at all, so a
//                                                       result must be built via Column::append()
//
// Axis 2 -- semantics: what the type means to the user, i.e. which operations are meaningful.
//
//   lt_is_semi_structured  JSON, VARIANT                user-visible documents with no fixed
//                                                       sub-schema and no meaningful total order:
//                                                       min/max, zone maps, range predicates and the
//                                                       typed IN predicate do not apply
//   lt_is_opaque_sketch    HLL, OBJECT (= BITMAP),      aggregate intermediates; the user never sees
//                          PERCENTILE                   the underlying value, only what a dedicated
//                                                       function returns
//
// The axes cross, which is exactly why one predicate cannot serve both:
//   - JSON and VARIANT share Axis 1 with HLL/BITMAP/PERCENTILE (all ObjectColumn) but share nothing
//     with them on Axis 2;
//   - VARIANT shares Axis 2 with JSON, yet sits with the nested types on lt_is_row_wise_append,
//     because a shredded VariantColumn keeps its rows outside ObjectColumn::_pool.
// Pick the predicate that names the property the code actually depends on, and do not widen an
// existing one to make a new type fit.
// ---------------------------------------------------------------------------------------------

VALUE_GUARD(LogicalType, BigIntLTGuard, lt_is_bigint, TYPE_BIGINT)
VALUE_GUARD(LogicalType, BooleanLTGuard, lt_is_boolean, TYPE_BOOLEAN)
VALUE_GUARD(LogicalType, LargeIntLTGuard, lt_is_largeint, TYPE_LARGEINT)
VALUE_GUARD(LogicalType, IntegerLTGuard, lt_is_integer, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
            TYPE_LARGEINT)
VALUE_GUARD(LogicalType, SumBigIntLTGuard, lt_is_sum_bigint, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT,
            TYPE_BIGINT)
VALUE_GUARD(LogicalType, UnsignedLTGuard, lt_is_unsigned, TYPE_UNSIGNED_TINYINT, TYPE_UNSIGNED_SMALLINT,
            TYPE_UNSIGNED_INT, TYPE_UNSIGNED_BIGINT)
VALUE_GUARD(LogicalType, FloatLTGuard, lt_is_float, TYPE_FLOAT, TYPE_DOUBLE)
VALUE_GUARD(LogicalType, Decimal32LTGuard, lt_is_decimal32, TYPE_DECIMAL32)
VALUE_GUARD(LogicalType, Decimal64LTGuard, lt_is_decimal64, TYPE_DECIMAL64)
VALUE_GUARD(LogicalType, Decimal128LTGuard, lt_is_decimal128, TYPE_DECIMAL128)
VALUE_GUARD(LogicalType, Decimal256LTGuard, lt_is_decimal256, TYPE_DECIMAL256)
VALUE_GUARD(LogicalType, DecimalLTGuard, lt_is_decimal, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128,
            TYPE_DECIMAL256)
VALUE_GUARD(LogicalType, SumDecimal64LTGuard, lt_is_sum_decimal64, TYPE_DECIMAL32, TYPE_DECIMAL64)
VALUE_GUARD(LogicalType, HllLTGuard, lt_is_hll, TYPE_HLL)
VALUE_GUARD(LogicalType, ObjectLTGuard, lt_is_object, TYPE_OBJECT)
VALUE_GUARD(LogicalType, StringLTGuard, lt_is_string, TYPE_CHAR, TYPE_VARCHAR)
VALUE_GUARD(LogicalType, BinaryLTGuard, lt_is_binary, TYPE_BINARY, TYPE_VARBINARY)
VALUE_GUARD(LogicalType, JsonGuard, lt_is_json, TYPE_JSON)
VALUE_GUARD(LogicalType, VariantGuard, lt_is_variant, TYPE_VARIANT)
VALUE_GUARD(LogicalType, FunctionGuard, lt_is_function, TYPE_FUNCTION)
// Axis 1. Runtime counterpart: is_object_type().
VALUE_GUARD(LogicalType, ObjectFamilyLTGuard, lt_is_object_family, TYPE_JSON, TYPE_HLL, TYPE_OBJECT, TYPE_PERCENTILE,
            TYPE_VARIANT)
// Axis 2. Together these two partition lt_is_object_family, which the static_assert below enforces:
// every ObjectColumn-backed type is either a user-visible document or an opaque aggregate sketch.
VALUE_GUARD(LogicalType, SemiStructuredLTGuard, lt_is_semi_structured, TYPE_JSON, TYPE_VARIANT)
VALUE_GUARD(LogicalType, OpaqueSketchLTGuard, lt_is_opaque_sketch, TYPE_HLL, TYPE_OBJECT, TYPE_PERCENTILE)

namespace detail {
template <size_t... Is>
constexpr bool object_family_matches_runtime(std::index_sequence<Is...>) {
    return ((lt_is_object_family<static_cast<LogicalType>(Is)> == is_object_type(static_cast<LogicalType>(Is))) && ...);
}
template <size_t... Is>
constexpr bool object_family_is_partitioned(std::index_sequence<Is...>) {
    return ((lt_is_object_family<static_cast<LogicalType>(Is)> ==
             (lt_is_semi_structured<static_cast<LogicalType>(Is)> ||
              lt_is_opaque_sketch<static_cast<LogicalType>(Is)>)) &&
            ...) &&
           !((lt_is_semi_structured<static_cast<LogicalType>(Is)> &&
              lt_is_opaque_sketch<static_cast<LogicalType>(Is)>) ||
             ...);
}
} // namespace detail
// lt_is_object_family and is_object_type() are two hand-written spellings of the same set; adding a
// type to only one of them is the kind of drift this catches.
static_assert(detail::object_family_matches_runtime(std::make_index_sequence<TYPE_MAX_VALUE>{}),
              "is_object_type() and lt_is_object_family must describe the same set of types");
// A new ObjectColumn-backed type must be classified on Axis 2 as well, and cannot be both.
static_assert(detail::object_family_is_partitioned(std::make_index_sequence<TYPE_MAX_VALUE>{}),
              "lt_is_semi_structured and lt_is_opaque_sketch must partition lt_is_object_family");

VALUE_GUARD(LogicalType, ArrayGuard, lt_is_array, TYPE_ARRAY)
VALUE_GUARD(LogicalType, MapGuard, lt_is_map, TYPE_MAP)
VALUE_GUARD(LogicalType, StructGurad, lt_is_struct, TYPE_STRUCT)

VALUE_GUARD(LogicalType, DateLTGuard, lt_is_date, TYPE_DATE)
VALUE_GUARD(LogicalType, DateTimeLTGuard, lt_is_datetime, TYPE_DATETIME)
VALUE_GUARD(LogicalType, TimeLTGuard, lt_is_time, TYPE_TIME)
VALUE_GUARD(LogicalType, DecimalV2LTGuard, lt_is_decimalv2, TYPE_DECIMALV2)
VALUE_GUARD(LogicalType, DecimalOfAnyVersionLTGuard, lt_is_decimal_of_any_version, TYPE_DECIMALV2, TYPE_DECIMAL32,
            TYPE_DECIMAL64, TYPE_DECIMAL128)
VALUE_GUARD(LogicalType, DateOrDateTimeLTGuard, lt_is_date_or_datetime, TYPE_DATE, TYPE_DATETIME)
// Nested types: the values live in child columns, so a column of this type can only be built
// recursively from TypeDescriptor::children.
// Do NOT widen this to JSON/VARIANT: ColumnHelper::create_column() and FunctionHelper::create_column()
// rely on it meaning exactly "has children" and treat a collection reaching their type dispatch as a
// bug (LOG(FATAL) / throw), because the nested types are handled by an explicit branch above it.
VALUE_GUARD(LogicalType, CollectionLTGuard, lt_is_collection, TYPE_ARRAY, TYPE_MAP, TYPE_STRUCT)

// Types whose result column must be produced row by row through the virtual Column::append(),
// because ColumnViewer/ColumnBuilder have no usable flat value container for them:
//  - ARRAY/MAP/STRUCT keep their values in child columns;
//  - VARIANT rows may live in the shredded columns (VariantColumn::_typed_columns / _metadata_column
//    / _remain_value_column) instead of ObjectColumn::_pool, which is what ColumnViewer<TYPE_VARIANT>
//    would read.
// JSON deliberately does not belong here: JsonColumn always keeps its rows in _pool, so the
// viewer/builder path stays valid and faster for it.
UNION_VALUE_GUARD(LogicalType, RowWiseAppendLTGuard, lt_is_row_wise_append, lt_is_collection_struct,
                  lt_is_variant_struct)

UNION_VALUE_GUARD(LogicalType, IntegralLTGuard, lt_is_integral, lt_is_boolean_struct, lt_is_integer_struct)

UNION_VALUE_GUARD(LogicalType, ArithmeticLTGuard, lt_is_arithmetic, lt_is_boolean_struct, lt_is_integer_struct,
                  lt_is_float_struct)

UNION_VALUE_GUARD(LogicalType, AvgDoubleLTGuard, lt_is_avg_double, lt_is_boolean_struct, lt_is_integer_struct,
                  lt_is_float_struct, lt_is_date_or_datetime_struct)

UNION_VALUE_GUARD(LogicalType, AvgDecimal64LTGuard, lt_is_avg_decimal64, lt_is_sum_decimal64_struct)

UNION_VALUE_GUARD(LogicalType, NumberLTGuard, lt_is_number, lt_is_boolean_struct, lt_is_integer_struct,
                  lt_is_float_struct)

UNION_VALUE_GUARD(LogicalType, NumericLTGuard, lt_is_numeric, lt_is_number_struct, lt_is_decimal_struct)

UNION_VALUE_GUARD(LogicalType, FixedLengthLTGuard, lt_is_fixedlength, lt_is_arithmetic_struct, lt_is_decimalv2_struct,
                  lt_is_decimal_struct, lt_is_datetime_struct, lt_is_date_struct, lt_is_time_struct)
UNION_VALUE_GUARD(LogicalType, AggregateLTGuard, lt_is_aggregate, lt_is_arithmetic_struct, lt_is_decimalv2_struct,
                  lt_is_decimal_struct, lt_is_datetime_struct, lt_is_date_struct, lt_is_string_struct)
// TODO support more complex type as aggregate function
UNION_VALUE_GUARD(LogicalType, AggregateComplexLTGuard, lt_is_complex_aggregate, lt_is_arithmetic_struct,
                  lt_is_decimalv2_struct, lt_is_decimal_struct, lt_is_datetime_struct, lt_is_date_struct,
                  lt_is_json_struct)

UNION_VALUE_GUARD(LogicalType, StringOrBinaryGuard, lt_is_string_or_binary, lt_is_string_struct, lt_is_binary_struct)

TExprOpcode::type to_in_opcode(LogicalType t);
LogicalType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(LogicalType ltype);
std::string type_to_string(LogicalType t);
std::string type_to_string_v2(LogicalType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);

LogicalType scalar_field_type_to_logical_type(LogicalType field_type);

// Return length of fixed-length type, return 0 for dynamic length type
size_t get_size_of_fixed_length_type(LogicalType ltype);

// return types that can be sorted
const std::vector<LogicalType>& sortable_types();

} // namespace starrocks

inline std::ostream& operator<<(std::ostream& os, starrocks::LogicalType type) {
    os << starrocks::logical_type_to_string(type);
    return os;
}

template <>
struct fmt::formatter<starrocks::LogicalType> : formatter<std::string_view> {
    auto format(starrocks::LogicalType value, format_context& ctx) const -> format_context::iterator;
};
