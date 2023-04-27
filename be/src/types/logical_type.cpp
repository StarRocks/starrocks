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

#include "types/logical_type.h"

#include <algorithm>

#include "column/type_traits.h"
#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "types/logical_type_infra.h"

namespace starrocks {

LogicalType string_to_logical_type(const std::string& type_str) {
    std::string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(), ::toupper);
    if (upper_type_str == "TINYINT") return TYPE_TINYINT;
    if (upper_type_str == "SMALLINT") return TYPE_SMALLINT;
    if (upper_type_str == "INT") return TYPE_INT;
    if (upper_type_str == "BIGINT") return TYPE_BIGINT;
    if (upper_type_str == "LARGEINT") return TYPE_LARGEINT;
    if (upper_type_str == "UNSIGNED_TINYINT") return TYPE_UNSIGNED_TINYINT;
    if (upper_type_str == "UNSIGNED_SMALLINT") return TYPE_UNSIGNED_SMALLINT;
    if (upper_type_str == "UNSIGNED_INT") return TYPE_UNSIGNED_INT;
    if (upper_type_str == "UNSIGNED_BIGINT") return TYPE_UNSIGNED_BIGINT;
    if (upper_type_str == "FLOAT") return TYPE_FLOAT;
    if (upper_type_str == "DISCRETE_DOUBLE") return TYPE_DISCRETE_DOUBLE;
    if (upper_type_str == "DOUBLE") return TYPE_DOUBLE;
    if (upper_type_str == "CHAR") return TYPE_CHAR;
    if (upper_type_str == "DATE_V2") return TYPE_DATE;
    if (upper_type_str == "DATE") return TYPE_DATE_V1;
    if (upper_type_str == "DATETIME") return TYPE_DATETIME_V1;
    if (upper_type_str == "TIMESTAMP") return TYPE_DATETIME;
    if (upper_type_str == "DECIMAL_V2") return TYPE_DECIMALV2;
    if (upper_type_str == "DECIMAL") return TYPE_DECIMAL;
    if (upper_type_str == "VARCHAR") return TYPE_VARCHAR;
    if (upper_type_str == "BOOLEAN") return TYPE_BOOLEAN;
    if (upper_type_str == "HLL") return TYPE_HLL;
    if (upper_type_str == "STRUCT") return TYPE_STRUCT;
    if (upper_type_str == "ARRAY") return TYPE_ARRAY;
    if (upper_type_str == "MAP") return TYPE_MAP;
    if (upper_type_str == "OBJECT") return TYPE_OBJECT;
    if (upper_type_str == "PERCENTILE") return TYPE_PERCENTILE;
    if (upper_type_str == "DECIMAL32") return TYPE_DECIMAL32;
    if (upper_type_str == "DECIMAL64") return TYPE_DECIMAL64;
    if (upper_type_str == "DECIMAL128") return TYPE_DECIMAL128;
    if (upper_type_str == "JSON") return TYPE_JSON;
    if (upper_type_str == "VARBINARY") return TYPE_VARBINARY;
    LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
    return TYPE_UNKNOWN;
}

const char* logical_type_to_string(LogicalType type) {
    switch (type) {
    case TYPE_TINYINT:
        return "TINYINT";
    case TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED_TINYINT";
    case TYPE_SMALLINT:
        return "SMALLINT";
    case TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED_SMALLINT";
    case TYPE_INT:
        return "INT";
    case TYPE_UNSIGNED_INT:
        return "UNSIGNED_INT";
    case TYPE_BIGINT:
        return "BIGINT";
    case TYPE_LARGEINT:
        return "LARGEINT";
    case TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED_BIGINT";
    case TYPE_FLOAT:
        return "FLOAT";
    case TYPE_DOUBLE:
        return "DOUBLE";
    case TYPE_DISCRETE_DOUBLE:
        return "DISCRETE_DOUBLE";
    case TYPE_CHAR:
        return "CHAR";
    case TYPE_DATE_V1:
        return "DATE";
    case TYPE_DATE:
        return "DATE_V2";
    case TYPE_DATETIME_V1:
        return "DATETIME";
    case TYPE_DATETIME:
        return "TIMESTAMP";
    case TYPE_DECIMAL:
        return "DECIMAL";
    case TYPE_DECIMALV2:
        return "DECIMAL_V2";
    case TYPE_DECIMAL32:
        return "DECIMAL32";
    case TYPE_DECIMAL64:
        return "DECIMAL64";
    case TYPE_DECIMAL128:
        return "DECIMAL128";
    case TYPE_VARCHAR:
        return "VARCHAR";
    case TYPE_BOOLEAN:
        return "BOOLEAN";
    case TYPE_HLL:
        return "HLL";
    case TYPE_STRUCT:
        return "STRUCT";
    case TYPE_ARRAY:
        return "ARRAY";
    case TYPE_MAP:
        return "MAP";
    case TYPE_OBJECT:
        return "OBJECT";
    case TYPE_PERCENTILE:
        return "PERCENTILE";
    case TYPE_JSON:
        return "JSON";
    case TYPE_UNKNOWN:
        return "UNKNOWN";
    case TYPE_NONE:
        return "NONE";
    case TYPE_NULL:
        return "NULL";
    case TYPE_FUNCTION:
        return "FUNCTION";
    case TYPE_TIME:
        return "TIME";
    case TYPE_BINARY:
        return "BINARY";
    case TYPE_MAX_VALUE:
        return "MAX_VALUE";
    case TYPE_VARBINARY:
        return "VARBINARY";
    }
    return "";
}

TExprOpcode::type to_in_opcode(LogicalType t) {
    return TExprOpcode::FILTER_IN;
}

LogicalType thrift_to_type(TPrimitiveType::type ttype) {
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

TPrimitiveType::type to_thrift(LogicalType ltype) {
    switch (ltype) {
        // TODO(mofei) rename these two type
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_DATE_V1:
    case TYPE_DATETIME_V1:
    case TYPE_NONE:
    case TYPE_MAX_VALUE:
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

std::string type_to_string(LogicalType t) {
    switch (t) {
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_DATE_V1:
    case TYPE_DATETIME_V1:
    case TYPE_NONE:
    case TYPE_MAX_VALUE:
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

std::string type_to_string_v2(LogicalType t) {
    // change OBJECT to BITMAP for better display
    std::string raw_str = type_to_string(t);
    return raw_str == "OBJECT" ? "BITMAP" : raw_str;
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

class ScalarFieldTypeToLogicalTypeMapping {
public:
    ScalarFieldTypeToLogicalTypeMapping() {
        for (auto& i : _data) {
            i = TYPE_UNKNOWN;
        }
        _data[TYPE_BOOLEAN] = TYPE_BOOLEAN;
        _data[TYPE_TINYINT] = TYPE_TINYINT;
        _data[TYPE_SMALLINT] = TYPE_SMALLINT;
        _data[TYPE_INT] = TYPE_INT;
        _data[TYPE_BIGINT] = TYPE_BIGINT;
        _data[TYPE_LARGEINT] = TYPE_LARGEINT;
        _data[TYPE_FLOAT] = TYPE_FLOAT;
        _data[TYPE_DOUBLE] = TYPE_DOUBLE;
        _data[TYPE_CHAR] = TYPE_CHAR;
        _data[TYPE_VARCHAR] = TYPE_VARCHAR;
        _data[TYPE_DATE_V1] = TYPE_DATE;
        _data[TYPE_DATE] = TYPE_DATE;
        _data[TYPE_DATETIME] = TYPE_DATETIME;
        _data[TYPE_DATETIME_V1] = TYPE_DATETIME;
        _data[TYPE_DECIMAL] = TYPE_DECIMAL;
        _data[TYPE_DECIMALV2] = TYPE_DECIMALV2;
        _data[TYPE_DECIMAL32] = TYPE_DECIMAL32;
        _data[TYPE_DECIMAL64] = TYPE_DECIMAL64;
        _data[TYPE_DECIMAL128] = TYPE_DECIMAL128;
        _data[TYPE_JSON] = TYPE_JSON;
        _data[TYPE_VARBINARY] = TYPE_VARBINARY;
    }
    LogicalType get_logical_type(LogicalType field_type) { return _data[field_type]; }

private:
    LogicalType _data[TYPE_MAX_VALUE];
};

static ScalarFieldTypeToLogicalTypeMapping g_scalar_ftype_to_ltype;

LogicalType scalar_field_type_to_logical_type(LogicalType field_type) {
    LogicalType ltype = g_scalar_ftype_to_ltype.get_logical_type(field_type);
    DCHECK(ltype != TYPE_UNKNOWN);
    return ltype;
}

struct FixedLengthTypeGetter {
    template <LogicalType ltype>
    size_t operator()() {
        return RunTimeFixedTypeLength<ltype>::value;
    }
};

size_t get_size_of_fixed_length_type(LogicalType ltype) {
    return type_dispatch_all(ltype, FixedLengthTypeGetter());
}

const std::vector<LogicalType>& sortable_types() {
    const static std::vector<LogicalType> kTypes{TYPE_BOOLEAN,   TYPE_TINYINT,   TYPE_SMALLINT,  TYPE_INT,
                                                 TYPE_BIGINT,    TYPE_LARGEINT,  TYPE_FLOAT,     TYPE_DOUBLE,
                                                 TYPE_VARCHAR,   TYPE_CHAR,      TYPE_DATE,      TYPE_DATETIME,
                                                 TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128};
    return kTypes;
}

} // namespace starrocks
