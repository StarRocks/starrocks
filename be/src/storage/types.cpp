// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/types.cpp

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

#include "storage/types.h"

#include "storage/decimal_type_info.h"

namespace starrocks {

void (*FieldTypeTraits<OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

template <typename TypeTraitsClass>
ScalarTypeInfo::ScalarTypeInfo([[maybe_unused]] TypeTraitsClass t)
        : _equal(TypeTraitsClass::equal),
          _cmp(TypeTraitsClass::cmp),
          _shallow_copy(TypeTraitsClass::shallow_copy),
          _deep_copy(TypeTraitsClass::deep_copy),
          _copy_object(TypeTraitsClass::copy_object),
          _direct_copy(TypeTraitsClass::direct_copy),
          _convert_from(TypeTraitsClass::convert_from),
          _from_string(TypeTraitsClass::from_string),
          _to_string(TypeTraitsClass::to_string),
          _set_to_max(TypeTraitsClass::set_to_max),
          _set_to_min(TypeTraitsClass::set_to_min),
          _hash_code(TypeTraitsClass::hash_code),
          _datum_cmp(TypeTraitsClass::datum_cmp),
          _size(TypeTraitsClass::size),
          _field_type(TypeTraitsClass::type) {}

const TypeInfoPtr ScalarTypeInfoResolver::get_type_info(FieldType t) {
    if (this->_mapping.find(t) == this->_mapping.end()) {
        return std::make_shared<ScalarTypeInfo>(*this->_mapping[OLAP_FIELD_TYPE_NONE].get());
    }
    return std::make_shared<ScalarTypeInfo>(*this->_mapping[t].get());
}

const ScalarTypeInfo* ScalarTypeInfoResolver::get_scalar_type_info(FieldType t) {
    DCHECK(is_scalar_field_type(t));
    return this->_mapping[t].get();
}

template <FieldType field_type>
void ScalarTypeInfoResolver::add_mapping() {
    TypeTraits<field_type> traits;
    std::unique_ptr<ScalarTypeInfo> scalar_type_info(new ScalarTypeInfo(traits));
    _mapping[field_type] = std::move(scalar_type_info);
}

ScalarTypeInfoResolver::ScalarTypeInfoResolver() {
    add_mapping<OLAP_FIELD_TYPE_TINYINT>();
    add_mapping<OLAP_FIELD_TYPE_SMALLINT>();
    add_mapping<OLAP_FIELD_TYPE_INT>();
    add_mapping<OLAP_FIELD_TYPE_UNSIGNED_INT>();
    add_mapping<OLAP_FIELD_TYPE_BOOL>();
    add_mapping<OLAP_FIELD_TYPE_BIGINT>();
    add_mapping<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
    add_mapping<OLAP_FIELD_TYPE_LARGEINT>();
    add_mapping<OLAP_FIELD_TYPE_FLOAT>();
    add_mapping<OLAP_FIELD_TYPE_DOUBLE>();
    add_mapping<OLAP_FIELD_TYPE_DECIMAL>();
    add_mapping<OLAP_FIELD_TYPE_DECIMAL_V2>();
    add_mapping<OLAP_FIELD_TYPE_DATE>();
    add_mapping<OLAP_FIELD_TYPE_DATE_V2>();
    add_mapping<OLAP_FIELD_TYPE_DATETIME>();
    add_mapping<OLAP_FIELD_TYPE_TIMESTAMP>();
    add_mapping<OLAP_FIELD_TYPE_CHAR>();
    add_mapping<OLAP_FIELD_TYPE_VARCHAR>();
    add_mapping<OLAP_FIELD_TYPE_HLL>();
    add_mapping<OLAP_FIELD_TYPE_OBJECT>();
    add_mapping<OLAP_FIELD_TYPE_PERCENTILE>();
    add_mapping<OLAP_FIELD_TYPE_NONE>();
}

ScalarTypeInfoResolver::~ScalarTypeInfoResolver() = default;

bool is_scalar_field_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_DECIMAL32:
    case OLAP_FIELD_TYPE_DECIMAL64:
    case OLAP_FIELD_TYPE_DECIMAL128:
        return false;
    default:
        return true;
    }
}

bool is_complex_metric_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_HLL:
        return true;
    default:
        return false;
    }
}

TypeInfoPtr get_type_info(FieldType field_type) {
    return ScalarTypeInfoResolver::instance()->get_type_info(field_type);
}

TypeInfoPtr get_type_info(const segment_v2::ColumnMetaPB& column_meta_pb) {
    FieldType type = static_cast<FieldType>(column_meta_pb.type());
    TypeInfoPtr type_info;
    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const segment_v2::ColumnMetaPB& child = column_meta_pb.children_columns(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        type_info.reset(new ArrayTypeInfo(child_type_info));
        return type_info;
    } else {
        return get_type_info(delegate_type(type));
    }
}

TypeInfoPtr get_type_info(const TabletColumn& col) {
    TypeInfoPtr type_info;
    if (col.type() == OLAP_FIELD_TYPE_ARRAY) {
        const TabletColumn& child = col.subcolumn(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        type_info.reset(new ArrayTypeInfo(child_type_info));
        return type_info;
    } else {
        return get_type_info(col.type(), col.precision(), col.scale());
    }
}

TypeInfoPtr get_type_info(FieldType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale) {
    if (is_scalar_field_type(field_type)) {
        return get_type_info(field_type);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL32) {
        return std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL32>>(precision, scale);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL64) {
        return std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL64>>(precision, scale);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL128) {
        return std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL128>>(precision, scale);
    } else {
        return nullptr;
    }
}

TypeInfoPtr get_type_info(const TypeInfo* type_info) {
    return get_type_info(type_info->type(), type_info->precision(), type_info->scale());
}

const ScalarTypeInfo* get_scalar_type_info(FieldType type) {
    DCHECK(is_scalar_field_type(type));
    return ScalarTypeInfoResolver::instance()->get_scalar_type_info(type);
}

} // namespace starrocks
