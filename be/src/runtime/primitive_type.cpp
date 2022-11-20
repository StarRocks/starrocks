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

#include "column/type_traits.h"
#include "gen_cpp/Types_types.h"
#include "runtime/primitive_type_infra.h"

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
    // TODO(mofei) rename these two type
    case TPrimitiveType::INVALID_TYPE:
        return TYPE_UNKNOWN;
    case TPrimitiveType::NULL_TYPE:
        return TYPE_NULL;
#define M(ttype)                \
    case TPrimitiveType::ttype: \
        return TYPE_##ttype;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
#undef M
    }

    return TYPE_UNKNOWN;
}

TPrimitiveType::type to_thrift(PrimitiveType ptype) {
    switch (ptype) {
    // TODO(mofei) rename these two type
    case TYPE_UNKNOWN:
        return TPrimitiveType::INVALID_TYPE;
    case TYPE_NULL:
        return TPrimitiveType::NULL_TYPE;

#define M(thrift_name)       \
    case TYPE_##thrift_name: \
        return TPrimitiveType::thrift_name;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
#undef M

    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT:
        return TPrimitiveType::INVALID_TYPE;
    }
    return TPrimitiveType::INVALID_TYPE;
}

std::string type_to_string(PrimitiveType t) {
    switch (t) {
    case TYPE_UNKNOWN:
        return "INVALID";
    case TYPE_NULL:
        return "NULL";
#define M(ttype)       \
    case TYPE_##ttype: \
        return #ttype;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
        APPLY_FOR_COMPLEX_THRIFT_TYPE(M)
#undef M
    }
    return "";
}

std::string type_to_string_v2(PrimitiveType t) {
    // change OBJECT to BITMAP for better display
    std::string raw_str = type_to_string(t);
    return raw_str == "OBJECT" ? "BITMAP" : raw_str;
}

std::string type_to_odbc_string(PrimitiveType t) {
    // ODBC driver requires types in lower case
    switch (t) {
    case TYPE_UNKNOWN:
        return "invalid";

#define M(ttype)       \
    case TYPE_##ttype: \
        return #ttype;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
#undef M

    default:
        return "unknown";
    }
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

TTypeDesc gen_array_type_desc(const TPrimitiveType::type field_type) {
    std::vector<TTypeNode> types_list;
    TTypeDesc type_desc;

    TTypeNode type_array;
    type_array.type = TTypeNodeType::ARRAY;
    types_list.push_back(type_array);

    TTypeNode type_scalar;
    TScalarType scalar_type;
    scalar_type.__set_type(field_type);
    scalar_type.__set_precision(0);
    scalar_type.__set_scale(0);
    scalar_type.__set_len(0);
    type_scalar.__set_scalar_type(scalar_type);
    types_list.push_back(type_scalar);

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
        for (auto& i : _data) {
            i = TYPE_UNKNOWN;
        }
        _data[LOGICAL_TYPE_BOOLEAN] = TYPE_BOOLEAN;
        _data[LOGICAL_TYPE_TINYINT] = TYPE_TINYINT;
        _data[LOGICAL_TYPE_SMALLINT] = TYPE_SMALLINT;
        _data[LOGICAL_TYPE_INT] = TYPE_INT;
        _data[LOGICAL_TYPE_BIGINT] = TYPE_BIGINT;
        _data[LOGICAL_TYPE_LARGEINT] = TYPE_LARGEINT;
        _data[LOGICAL_TYPE_FLOAT] = TYPE_FLOAT;
        _data[LOGICAL_TYPE_DOUBLE] = TYPE_DOUBLE;
        _data[LOGICAL_TYPE_CHAR] = TYPE_CHAR;
        _data[LOGICAL_TYPE_VARCHAR] = TYPE_VARCHAR;
        _data[LOGICAL_TYPE_DATE_V1] = TYPE_DATE;
        _data[LOGICAL_TYPE_DATE] = TYPE_DATE;
        _data[LOGICAL_TYPE_DATETIME] = TYPE_DATETIME;
        _data[LOGICAL_TYPE_DATETIME_V1] = TYPE_DATETIME;
        _data[LOGICAL_TYPE_DECIMAL] = TYPE_DECIMAL;
        _data[LOGICAL_TYPE_DECIMALV2] = TYPE_DECIMALV2;
        _data[LOGICAL_TYPE_DECIMAL32] = TYPE_DECIMAL32;
        _data[LOGICAL_TYPE_DECIMAL64] = TYPE_DECIMAL64;
        _data[LOGICAL_TYPE_DECIMAL128] = TYPE_DECIMAL128;
        _data[LOGICAL_TYPE_JSON] = TYPE_JSON;
        _data[LOGICAL_TYPE_VARBINARY] = TYPE_VARBINARY;
    }
    PrimitiveType get_primitive_type(LogicalType field_type) { return _data[field_type]; }

private:
    PrimitiveType _data[LOGICAL_TYPE_MAX_VALUE];
};

static ScalarFieldTypeToPrimitiveTypeMapping g_scalar_ftype_to_ptype;

PrimitiveType scalar_field_type_to_primitive_type(LogicalType field_type) {
    PrimitiveType ptype = g_scalar_ftype_to_ptype.get_primitive_type(field_type);
    DCHECK(ptype != TYPE_UNKNOWN);
    return ptype;
}

struct FixedLengthTypeGetter {
    template <PrimitiveType ptype>
    size_t operator()() {
        return vectorized::RunTimeFixedTypeLength<ptype>::value;
    }
};

size_t get_size_of_fixed_length_type(PrimitiveType ptype) {
    return type_dispatch_all(ptype, FixedLengthTypeGetter());
}

} // namespace starrocks
