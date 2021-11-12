// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/primitive_type.cpp

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

#include "runtime/primitive_type.h"

#include <sstream>

#include "gen_cpp/Types_types.h"

namespace starrocks {
//to_tcolumn_type_thrift only test
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype) {
    TColumnType t;
    t.__set_type(ttype);
    return t;
}

TExprOpcode::type to_in_opcode(PrimitiveType t) {
    return TExprOpcode::FILTER_IN;
}

PrimitiveType thrift_to_type(TPrimitiveType::type ttype) {
    switch (ttype) {
    case TPrimitiveType::INVALID_TYPE:
        return INVALID_TYPE;

    case TPrimitiveType::NULL_TYPE:
        return TYPE_NULL;

    case TPrimitiveType::BOOLEAN:
        return TYPE_BOOLEAN;

    case TPrimitiveType::TINYINT:
        return TYPE_TINYINT;

    case TPrimitiveType::SMALLINT:
        return TYPE_SMALLINT;

    case TPrimitiveType::INT:
        return TYPE_INT;

    case TPrimitiveType::BIGINT:
        return TYPE_BIGINT;

    case TPrimitiveType::LARGEINT:
        return TYPE_LARGEINT;

    case TPrimitiveType::FLOAT:
        return TYPE_FLOAT;

    case TPrimitiveType::DOUBLE:
        return TYPE_DOUBLE;

    case TPrimitiveType::DATE:
        return TYPE_DATE;

    case TPrimitiveType::DATETIME:
        return TYPE_DATETIME;

    case TPrimitiveType::TIME:
        return TYPE_TIME;

    case TPrimitiveType::VARCHAR:
        return TYPE_VARCHAR;

    case TPrimitiveType::BINARY:
        return TYPE_BINARY;

    case TPrimitiveType::DECIMAL:
        return TYPE_DECIMAL;

    case TPrimitiveType::DECIMALV2:
        return TYPE_DECIMALV2;

    case TPrimitiveType::DECIMAL32:
        return TYPE_DECIMAL32;

    case TPrimitiveType::DECIMAL64:
        return TYPE_DECIMAL64;

    case TPrimitiveType::DECIMAL128:
        return TYPE_DECIMAL128;

    case TPrimitiveType::CHAR:
        return TYPE_CHAR;

    case TPrimitiveType::HLL:
        return TYPE_HLL;

    case TPrimitiveType::OBJECT:
        return TYPE_OBJECT;

    case TPrimitiveType::PERCENTILE:
        return TYPE_PERCENTILE;
    }
    return INVALID_TYPE;
}

TPrimitiveType::type to_thrift(PrimitiveType ptype) {
    switch (ptype) {
    case INVALID_TYPE:
        return TPrimitiveType::INVALID_TYPE;

    case TYPE_NULL:
        return TPrimitiveType::NULL_TYPE;

    case TYPE_BOOLEAN:
        return TPrimitiveType::BOOLEAN;

    case TYPE_TINYINT:
        return TPrimitiveType::TINYINT;

    case TYPE_SMALLINT:
        return TPrimitiveType::SMALLINT;

    case TYPE_INT:
        return TPrimitiveType::INT;

    case TYPE_BIGINT:
        return TPrimitiveType::BIGINT;

    case TYPE_LARGEINT:
        return TPrimitiveType::LARGEINT;

    case TYPE_FLOAT:
        return TPrimitiveType::FLOAT;

    case TYPE_DOUBLE:
        return TPrimitiveType::DOUBLE;

    case TYPE_DATE:
        return TPrimitiveType::DATE;

    case TYPE_DATETIME:
        return TPrimitiveType::DATETIME;

    case TYPE_TIME:
        return TPrimitiveType::TIME;

    case TYPE_VARCHAR:
        return TPrimitiveType::VARCHAR;

    case TYPE_BINARY:
        return TPrimitiveType::BINARY;

    case TYPE_DECIMAL:
        return TPrimitiveType::DECIMAL;

    case TYPE_DECIMALV2:
        return TPrimitiveType::DECIMALV2;

    case TYPE_DECIMAL32:
        return TPrimitiveType::DECIMAL32;

    case TYPE_DECIMAL64:
        return TPrimitiveType::DECIMAL64;

    case TYPE_DECIMAL128:
        return TPrimitiveType::DECIMAL128;

    case TYPE_CHAR:
        return TPrimitiveType::CHAR;

    case TYPE_HLL:
        return TPrimitiveType::HLL;

    case TYPE_OBJECT:
        return TPrimitiveType::OBJECT;

    case TYPE_PERCENTILE:
        return TPrimitiveType::PERCENTILE;

    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT:
        return TPrimitiveType::INVALID_TYPE;
    }
    return TPrimitiveType::INVALID_TYPE;
}

std::string type_to_string(PrimitiveType t) {
    switch (t) {
    case INVALID_TYPE:
        return "INVALID";

    case TYPE_NULL:
        return "NULL";

    case TYPE_BOOLEAN:
        return "BOOL";

    case TYPE_TINYINT:
        return "TINYINT";

    case TYPE_SMALLINT:
        return "SMALLINT";

    case TYPE_INT:
        return "INT";

    case TYPE_BIGINT:
        return "BIGINT";

    case TYPE_LARGEINT:
        return "LARGEINT";

    case TYPE_FLOAT:
        return "FLOAT";

    case TYPE_DOUBLE:
        return "DOUBLE";

    case TYPE_DATE:
        return "DATE";

    case TYPE_DATETIME:
        return "DATETIME";

    case TYPE_TIME:
        return "TIME";

    case TYPE_VARCHAR:
        return "VARCHAR";

    case TYPE_BINARY:
        return "BINARY";

    case TYPE_DECIMAL:
        return "DECIMAL";

    case TYPE_DECIMALV2:
        return "DECIMALV2";

    case TYPE_DECIMAL32:
        return "DECIMAL32";

    case TYPE_DECIMAL64:
        return "DECIMAL64";

    case TYPE_DECIMAL128:
        return "DECIMAL128";

    case TYPE_CHAR:
        return "CHAR";

    case TYPE_HLL:
        return "HLL";

    case TYPE_OBJECT:
        return "OBJECT";

    case TYPE_PERCENTILE:
        return "PERCENTILE";

    case TYPE_STRUCT:
        return "STRUCT";

    case TYPE_ARRAY:
        return "ARRAY";

    case TYPE_MAP:
        return "MAP";
    }
    return "";
}

std::string type_to_odbc_string(PrimitiveType t) {
    // ODBC driver requires types in lower case
    switch (t) {
    default:
    case INVALID_TYPE:
        return "invalid";

    case TYPE_NULL:
        return "null";

    case TYPE_BOOLEAN:
        return "boolean";

    case TYPE_TINYINT:
        return "tinyint";

    case TYPE_SMALLINT:
        return "smallint";

    case TYPE_INT:
        return "int";

    case TYPE_BIGINT:
        return "bigint";

    case TYPE_LARGEINT:
        return "largeint";

    case TYPE_FLOAT:
        return "float";

    case TYPE_DOUBLE:
        return "double";

    case TYPE_DATE:
        return "date";

    case TYPE_DATETIME:
        return "datetime";

    case TYPE_VARCHAR:
        return "string";

    case TYPE_BINARY:
        return "binary";

    case TYPE_DECIMAL:
        return "decimal";

    case TYPE_DECIMALV2:
        return "decimalv2";

    case TYPE_CHAR:
        return "char";

    case TYPE_HLL:
        return "hll";

    case TYPE_OBJECT:
        return "object";

    case TYPE_PERCENTILE:
        return "percentile";

    case TYPE_DECIMAL32:
        return "decimal32";

    case TYPE_DECIMAL64:
        return "decimal64";

    case TYPE_DECIMAL128:
        return "decimal128";
    }

    return "unknown";
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    scalar_type.__set_precision(2);
    scalar_type.__set_scale(2);
    scalar_type.__set_len(10);

    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    std::vector<TStructField> fields;
    TStructField field;
    field.__set_name(name);
    fields.push_back(field);
    type_node.__set_struct_fields(fields);
    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

class ScalarFieldTypeToPrimitiveTypeMapping {
public:
    ScalarFieldTypeToPrimitiveTypeMapping() {
        for (int i = 0; i < OLAP_FIELD_TYPE_MAX_VALUE; i++) {
            _data[i] = INVALID_TYPE;
        }
        _data[OLAP_FIELD_TYPE_BOOL] = TYPE_BOOLEAN;
        _data[OLAP_FIELD_TYPE_TINYINT] = TYPE_TINYINT;
        _data[OLAP_FIELD_TYPE_SMALLINT] = TYPE_SMALLINT;
        _data[OLAP_FIELD_TYPE_INT] = TYPE_INT;
        _data[OLAP_FIELD_TYPE_BIGINT] = TYPE_BIGINT;
        _data[OLAP_FIELD_TYPE_LARGEINT] = TYPE_LARGEINT;
        _data[OLAP_FIELD_TYPE_FLOAT] = TYPE_FLOAT;
        _data[OLAP_FIELD_TYPE_DOUBLE] = TYPE_DOUBLE;
        _data[OLAP_FIELD_TYPE_CHAR] = TYPE_CHAR;
        _data[OLAP_FIELD_TYPE_VARCHAR] = TYPE_VARCHAR;
        _data[OLAP_FIELD_TYPE_DATE] = TYPE_DATE;
        _data[OLAP_FIELD_TYPE_DATE_V2] = TYPE_DATE;
        _data[OLAP_FIELD_TYPE_TIMESTAMP] = TYPE_DATETIME;
        _data[OLAP_FIELD_TYPE_DATETIME] = TYPE_DATETIME;
        _data[OLAP_FIELD_TYPE_DECIMAL] = TYPE_DECIMAL;
        _data[OLAP_FIELD_TYPE_DECIMAL_V2] = TYPE_DECIMALV2;
        _data[OLAP_FIELD_TYPE_DECIMAL32] = TYPE_DECIMAL32;
        _data[OLAP_FIELD_TYPE_DECIMAL64] = TYPE_DECIMAL64;
        _data[OLAP_FIELD_TYPE_DECIMAL128] = TYPE_DECIMAL128;
    }
    PrimitiveType get_primitive_type(FieldType field_type) { return _data[field_type]; }

private:
    PrimitiveType _data[OLAP_FIELD_TYPE_MAX_VALUE];
};

static ScalarFieldTypeToPrimitiveTypeMapping g_scalar_ftype_to_ptype;

PrimitiveType scalar_field_type_to_primitive_type(FieldType field_type) {
    PrimitiveType ptype = g_scalar_ftype_to_ptype.get_primitive_type(field_type);
    DCHECK(ptype != INVALID_TYPE);
    return ptype;
}

} // namespace starrocks
