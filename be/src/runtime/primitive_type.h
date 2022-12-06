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

#include <string>

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/Types_types.h"
#include "types/logical_type.h"
#include "util/guard.h"

namespace starrocks {

inline bool is_binary_type(LogicalType type) {
    switch (type) {
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return true;
    default:
        return false;
    }
}

inline bool is_enumeration_type(LogicalType type) {
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

inline bool is_scalar_primitive_type(LogicalType ptype) {
    switch (ptype) {
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
    /* 13 */              // Not implemented
    case TYPE_DECIMAL:    /* 14 */
    case TYPE_CHAR:       /* 15 */
    case TYPE_DECIMALV2:  /* 20 */
    case TYPE_TIME:       /* 21 */
    case TYPE_DECIMAL32:  /* 24 */
    case TYPE_DECIMAL64:  /* 25 */
    case TYPE_DECIMAL128: /* 26 */
    case TYPE_JSON:
        return true;
    default:
        return false;
    }
}

VALUE_GUARD(LogicalType, BigIntPTGuard, pt_is_bigint, TYPE_BIGINT)
VALUE_GUARD(LogicalType, BooleanPTGuard, pt_is_boolean, TYPE_BOOLEAN)
VALUE_GUARD(LogicalType, LargeIntPTGuard, pt_is_largeint, TYPE_LARGEINT)
VALUE_GUARD(LogicalType, IntegerPTGuard, pt_is_integer, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
            TYPE_LARGEINT)
VALUE_GUARD(LogicalType, SumBigIntPTGuard, pt_is_sum_bigint, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT,
            TYPE_BIGINT)
VALUE_GUARD(LogicalType, FloatPTGuard, pt_is_float, TYPE_FLOAT, TYPE_DOUBLE)
VALUE_GUARD(LogicalType, Decimal32PTGuard, pt_is_decimal32, TYPE_DECIMAL32)
VALUE_GUARD(LogicalType, Decimal64PTGuard, pt_is_decimal64, TYPE_DECIMAL64)
VALUE_GUARD(LogicalType, Decimal128PTGuard, pt_is_decimal128, TYPE_DECIMAL128)
VALUE_GUARD(LogicalType, DecimalPTGuard, pt_is_decimal, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)
VALUE_GUARD(LogicalType, SumDecimal64PTGuard, pt_is_sum_decimal64, TYPE_DECIMAL32, TYPE_DECIMAL64)
VALUE_GUARD(LogicalType, HllPTGuard, pt_is_hll, TYPE_HLL)
VALUE_GUARD(LogicalType, ObjectPTGuard, pt_is_object, TYPE_OBJECT)
VALUE_GUARD(LogicalType, StringPTGuard, pt_is_string, TYPE_CHAR, TYPE_VARCHAR)
VALUE_GUARD(LogicalType, BinaryPTGuard, pt_is_binary, TYPE_BINARY, TYPE_VARBINARY)
VALUE_GUARD(LogicalType, JsonGuard, pt_is_json, TYPE_JSON)
VALUE_GUARD(LogicalType, FunctionGuard, pt_is_function, TYPE_FUNCTION)

VALUE_GUARD(LogicalType, DatePTGuard, pt_is_date, TYPE_DATE)
VALUE_GUARD(LogicalType, DateTimePTGuard, pt_is_datetime, TYPE_DATETIME)
VALUE_GUARD(LogicalType, TimePTGuard, pt_is_time, TYPE_TIME)
VALUE_GUARD(LogicalType, DecimalV2PTGuard, pt_is_decimalv2, TYPE_DECIMALV2)
VALUE_GUARD(LogicalType, DecimalOfAnyVersionPTGuard, pt_is_decimal_of_any_version, TYPE_DECIMALV2, TYPE_DECIMAL32,
            TYPE_DECIMAL64, TYPE_DECIMAL128)
VALUE_GUARD(LogicalType, DateOrDateTimePTGuard, pt_is_date_or_datetime, TYPE_DATE, TYPE_DATETIME)

UNION_VALUE_GUARD(LogicalType, IntegralPTGuard, pt_is_integral, pt_is_boolean_struct, pt_is_integer_struct)

UNION_VALUE_GUARD(LogicalType, ArithmeticPTGuard, pt_is_arithmetic, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct)

UNION_VALUE_GUARD(LogicalType, AvgDoublePTGuard, pt_is_avg_double, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct, pt_is_date_or_datetime_struct)

UNION_VALUE_GUARD(LogicalType, AvgDecimal64PTGuard, pt_is_avg_decimal64, pt_is_sum_decimal64_struct)

UNION_VALUE_GUARD(LogicalType, NumberPTGuard, pt_is_number, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct)

UNION_VALUE_GUARD(LogicalType, NumericPTGuard, pt_is_numeric, pt_is_number_struct, pt_is_decimal_struct)

UNION_VALUE_GUARD(LogicalType, FixedLengthPTGuard, pt_is_fixedlength, pt_is_arithmetic_struct, pt_is_decimalv2_struct,
                  pt_is_decimal_struct, pt_is_datetime_struct, pt_is_date_struct, pt_is_time_struct)
UNION_VALUE_GUARD(LogicalType, AggregatePTGuard, pt_is_aggregate, pt_is_arithmetic_struct, pt_is_decimalv2_struct,
                  pt_is_decimal_struct, pt_is_datetime_struct, pt_is_date_struct)

TExprOpcode::type to_in_opcode(LogicalType t);
LogicalType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(LogicalType ptype);
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype);
std::string type_to_string(LogicalType t);
std::string type_to_string_v2(LogicalType t);
std::string type_to_odbc_string(LogicalType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);
TTypeDesc gen_array_type_desc(const TPrimitiveType::type field_type);

LogicalType scalar_field_type_to_primitive_type(LogicalType field_type);

// Return length of fixed-length type, return 0 for dynamic length type
size_t get_size_of_fixed_length_type(LogicalType ptype);

} // namespace starrocks
