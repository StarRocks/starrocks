// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/wrapper_field.h

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

#include "storage/field.h"
#include "storage/olap_define.h"
#include "storage/tablet_schema.h"
#include "util/hash_util.hpp"

namespace starrocks {

class WrapperField {
public:
    static WrapperField* create(const TabletColumn& column, uint32_t len = 0);

    WrapperField(Field* rep, size_t variable_len, bool is_string_type);

    virtual ~WrapperField() {
        delete _rep;
        delete[] _owned_buf;
    }

    // Convert internal value to string
    // Only used for debug, do not include the null flag
    std::string to_string() const { return _rep->to_string(_field_buf + 1); }

    // Construct from a serialized string which is terminated by '\0'
    // do not include the null flag
    Status from_string(const std::string& value_string) {
        if (_is_string_type) {
            if (value_string.size() > _var_length) {
                Slice* slice = reinterpret_cast<Slice*>(cell_ptr());
                slice->size = value_string.size();
                _var_length = slice->size;
                _string_content.reset(new char[slice->size]);
                slice->data = _string_content.get();
            }
        }
        return _rep->from_string(_field_buf + 1, value_string);
    }

    bool is_string_type() const { return _is_string_type; }
    char* ptr() const { return _field_buf + 1; }
    size_t size() const { return _rep->size(); }
    bool is_null() const { return *reinterpret_cast<bool*>(_field_buf); }
    void set_is_null(bool is_null) { *reinterpret_cast<bool*>(_field_buf) = is_null; }
    void set_null() { *reinterpret_cast<bool*>(_field_buf) = true; }
    void* cell_ptr() const { return _field_buf + 1; }
    void* mutable_cell_ptr() const { return _field_buf + 1; }
    const Field* field() const { return _rep; }

private:
    Field* _rep = nullptr;
    bool _is_string_type;
    char* _field_buf = nullptr;
    char* _owned_buf = nullptr;

    //include fixed and variable length and null bytes
    size_t _length;
    size_t _var_length;
    // memory for string type field
    std::unique_ptr<char[]> _string_content;
};

} // namespace starrocks
