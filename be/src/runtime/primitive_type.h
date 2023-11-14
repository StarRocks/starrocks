// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/primitive_type.h

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

#include <string>

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/Types_types.h"
#include "storage/olap_common.h"
#include "util/guard.h"

namespace starrocks {

enum PrimitiveType {
    INVALID_TYPE = 0,
    TYPE_NULL,     /* 1 */
    TYPE_BOOLEAN,  /* 2 */
    TYPE_TINYINT,  /* 3 */
    TYPE_SMALLINT, /* 4 */
    TYPE_INT,      /* 5 */
    TYPE_BIGINT,   /* 6 */
    TYPE_LARGEINT, /* 7 */
    TYPE_FLOAT,    /* 8 */
    TYPE_DOUBLE,   /* 9 */
    TYPE_VARCHAR,  /* 10 */
    TYPE_DATE,     /* 11 */
    TYPE_DATETIME, /* 12 */
    TYPE_BINARY,
    /* 13 */      // Not implemented
    TYPE_DECIMAL, /* 14 */
    TYPE_CHAR,    /* 15 */

    TYPE_STRUCT,    /* 16 */
    TYPE_ARRAY,     /* 17 */
    TYPE_MAP,       /* 18 */
    TYPE_HLL,       /* 19 */
    TYPE_DECIMALV2, /* 20 */

    TYPE_TIME,       /* 21 */
    TYPE_OBJECT,     /* 22 */
    TYPE_PERCENTILE, /* 23 */
    TYPE_DECIMAL32,  /* 24 */
    TYPE_DECIMAL64,  /* 25 */
    TYPE_DECIMAL128, /* 26 */

    TYPE_JSON,     /* 27 */
    TYPE_FUNCTION, /* 28 */
};

inline bool is_enumeration_type(PrimitiveType type) {
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

constexpr bool is_object_type(PrimitiveType type) {
    return type == PrimitiveType::TYPE_HLL || type == PrimitiveType::TYPE_OBJECT || type == PrimitiveType::TYPE_JSON ||
           type == PrimitiveType::TYPE_PERCENTILE;
}

inline bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs) {
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

inline bool is_scalar_primitive_type(PrimitiveType ptype) {
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

constexpr size_t type_estimated_overhead_bytes(PrimitiveType ptype) {
    switch (ptype) {
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

VALUE_GUARD(PrimitiveType, BigIntPTGuard, pt_is_bigint, TYPE_BIGINT)
VALUE_GUARD(PrimitiveType, BooleanPTGuard, pt_is_boolean, TYPE_BOOLEAN)
VALUE_GUARD(PrimitiveType, LargeIntPTGuard, pt_is_largeint, TYPE_LARGEINT)
VALUE_GUARD(PrimitiveType, IntegerPTGuard, pt_is_integer, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
            TYPE_LARGEINT)
VALUE_GUARD(PrimitiveType, SumBigIntPTGuard, pt_is_sum_bigint, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT,
            TYPE_BIGINT)
VALUE_GUARD(PrimitiveType, FloatPTGuard, pt_is_float, TYPE_FLOAT, TYPE_DOUBLE)
VALUE_GUARD(PrimitiveType, Decimal32PTGuard, pt_is_decimal32, TYPE_DECIMAL32)
VALUE_GUARD(PrimitiveType, Decimal64PTGuard, pt_is_decimal64, TYPE_DECIMAL64)
VALUE_GUARD(PrimitiveType, Decimal128PTGuard, pt_is_decimal128, TYPE_DECIMAL128)
VALUE_GUARD(PrimitiveType, DecimalPTGuard, pt_is_decimal, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)
VALUE_GUARD(PrimitiveType, SumDecimal64PTGuard, pt_is_sum_decimal64, TYPE_DECIMAL32, TYPE_DECIMAL64)
VALUE_GUARD(PrimitiveType, HllPTGuard, pt_is_hll, TYPE_HLL)
VALUE_GUARD(PrimitiveType, ObjectPTGuard, pt_is_object, TYPE_OBJECT)
VALUE_GUARD(PrimitiveType, StringPTGuard, pt_is_string, TYPE_CHAR, TYPE_VARCHAR)
VALUE_GUARD(PrimitiveType, JsonGuard, pt_is_json, TYPE_JSON)
VALUE_GUARD(PrimitiveType, FunctionGuard, pt_is_function, TYPE_FUNCTION)

VALUE_GUARD(PrimitiveType, DatePTGuard, pt_is_date, TYPE_DATE)
VALUE_GUARD(PrimitiveType, DateTimePTGuard, pt_is_datetime, TYPE_DATETIME)
VALUE_GUARD(PrimitiveType, TimePTGuard, pt_is_time, TYPE_TIME)
VALUE_GUARD(PrimitiveType, DecimalV2PTGuard, pt_is_decimalv2, TYPE_DECIMALV2)
VALUE_GUARD(PrimitiveType, DecimalOfAnyVersionPTGuard, pt_is_decimal_of_any_version, TYPE_DECIMALV2, TYPE_DECIMAL32,
            TYPE_DECIMAL64, TYPE_DECIMAL128)
VALUE_GUARD(PrimitiveType, DateOrDateTimePTGuard, pt_is_date_or_datetime, TYPE_DATE, TYPE_DATETIME)

UNION_VALUE_GUARD(PrimitiveType, IntegralPTGuard, pt_is_integral, pt_is_boolean_struct, pt_is_integer_struct)

UNION_VALUE_GUARD(PrimitiveType, ArithmeticPTGuard, pt_is_arithmetic, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct)

UNION_VALUE_GUARD(PrimitiveType, AvgDoublePTGuard, pt_is_avg_double, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct, pt_is_date_or_datetime_struct)

UNION_VALUE_GUARD(PrimitiveType, AvgDecimal64PTGuard, pt_is_avg_decimal64, pt_is_sum_decimal64_struct)

UNION_VALUE_GUARD(PrimitiveType, NumberPTGuard, pt_is_number, pt_is_boolean_struct, pt_is_integer_struct,
                  pt_is_float_struct)

UNION_VALUE_GUARD(PrimitiveType, NumericPTGuard, pt_is_numeric, pt_is_number_struct, pt_is_decimal_struct)

UNION_VALUE_GUARD(PrimitiveType, FixedLengthPTGuard, pt_is_fixedlength, pt_is_arithmetic_struct, pt_is_decimalv2_struct,
                  pt_is_decimal_struct, pt_is_datetime_struct, pt_is_date_struct, pt_is_time_struct)
UNION_VALUE_GUARD(PrimitiveType, AggregatePTGuard, pt_is_aggregate, pt_is_arithmetic_struct, pt_is_decimalv2_struct,
                  pt_is_decimal_struct, pt_is_datetime_struct, pt_is_date_struct, pt_is_string_struct)

TExprOpcode::type to_in_opcode(PrimitiveType t);
PrimitiveType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(PrimitiveType ptype);
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype);
std::string type_to_string(PrimitiveType t);
std::string type_to_string_v2(PrimitiveType t);
std::string type_to_odbc_string(PrimitiveType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);
TTypeDesc gen_array_type_desc(const TPrimitiveType::type field_type);

PrimitiveType scalar_field_type_to_primitive_type(FieldType field_type);

// Return length of fixed-length type, return 0 for dynamic length type
size_t get_size_of_fixed_length_type(PrimitiveType ptype);

// return types that can be sorted
const std::vector<PrimitiveType>& sortable_types();

} // namespace starrocks
