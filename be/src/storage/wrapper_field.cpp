// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/wrapper_field.cpp

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

#include "storage/wrapper_field.h"

namespace starrocks {

const size_t DEFAULT_STRING_LENGTH = 50;

WrapperField* WrapperField::create(const TabletColumn& column, uint32_t len) {
    bool is_string_type = (column.type() == OLAP_FIELD_TYPE_CHAR || column.type() == OLAP_FIELD_TYPE_VARCHAR ||
                           column.type() == OLAP_FIELD_TYPE_HLL || column.type() == OLAP_FIELD_TYPE_OBJECT ||
                           column.type() == OLAP_FIELD_TYPE_PERCENTILE || column.type() == OLAP_FIELD_TYPE_JSON);
    if (is_string_type && len > OLAP_STRING_MAX_LENGTH) {
        LOG(WARNING) << "length of string parameter is too long. len=" << len << " max_len=" << OLAP_STRING_MAX_LENGTH;
        return nullptr;
    }

    Field* rep = FieldFactory::create(column);
    if (rep == nullptr) {
        return nullptr;
    }

    size_t variable_len = 0;
    if (column.type() == OLAP_FIELD_TYPE_CHAR) {
        variable_len = std::max(len, (uint32_t)(column.length()));
    } else if (column.type() == OLAP_FIELD_TYPE_VARCHAR || column.type() == OLAP_FIELD_TYPE_HLL) {
        // column.length is the serialized varchar length
        // the first sizeof(StringLengthType) bytes is the length of varchar
        // variable_len is the real length of varchar
        variable_len = std::max(len, static_cast<uint32_t>(column.length() - sizeof(StringLengthType)));
    } else {
        variable_len = column.length();
    }

    return new WrapperField(rep, variable_len, is_string_type);
}

WrapperField* WrapperField::create_by_type(const FieldType& type, int32_t var_length) {
    Field* rep = FieldFactory::create_by_type(type);
    if (rep == nullptr) {
        return nullptr;
    }
    bool is_string_type =
            (type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_HLL ||
             type == OLAP_FIELD_TYPE_OBJECT || type == OLAP_FIELD_TYPE_PERCENTILE || type == OLAP_FIELD_TYPE_JSON);
    auto wrapper = new WrapperField(rep, var_length, is_string_type);
    return wrapper;
}

WrapperField::WrapperField(Field* rep, size_t variable_len, bool is_string_type)
        : _rep(rep), _is_string_type(is_string_type), _var_length(0) {
    size_t fixed_len = _rep->size();
    _length = fixed_len + 1;
    _field_buf = new char[_length];
    memset(_field_buf, 0, _length);
    _owned_buf = _field_buf;
    char* buf = _field_buf + 1;

    if (_is_string_type) {
        _var_length = variable_len > 0 ? variable_len : DEFAULT_STRING_LENGTH;
        auto* slice = reinterpret_cast<Slice*>(buf);
        slice->size = _var_length;
        _string_content.reset(new char[slice->size]);
        slice->data = _string_content.get();
    }
}

} // namespace starrocks
