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

#include <string_view>
#include <utility>

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "gen_cpp/olap_file.pb.h"

namespace starrocks {

// TODO: move constructor and move assignment
class Schema {
public:
    Schema() = default;
    Schema(Schema&&) = default;
    Schema& operator=(Schema&&) = default;

#ifdef BE_TEST
    explicit Schema(Fields fields);
#endif

    explicit Schema(Fields fields, KeysType keys_type, std::vector<ColumnId> sort_key_idxes);

    // if we use this constructor and share the name_to_index with another schema,
    // we must make sure another shema is read only!!!
    explicit Schema(Schema* schema);

    explicit Schema(Schema* schema, const std::vector<ColumnId>& cids);

    explicit Schema(Schema* schema, const std::vector<ColumnId>& cids, const std::vector<ColumnId>& scids);

    // if we use this constructor and share the name_to_index with another schema,
    // we must make sure another shema is read only!!!
    Schema(const Schema& schema);

    // if we use this constructor and share the name_to_index with another schema,
    // we must make sure another shema is read only!!!
    Schema& operator=(const Schema& other);

    size_t num_fields() const { return _fields.size(); }

    size_t num_key_fields() const { return _num_keys; }

    const std::vector<ColumnId> sort_key_idxes() const { return _sort_key_idxes; }

    void reserve(size_t size) { _fields.reserve(size); }

    void append(const FieldPtr& field);
    void insert(size_t idx, const FieldPtr& field);
    void remove(size_t idx);

    void clear() {
        _fields.clear();
        _num_keys = 0;
        _name_to_index.reset();
        _name_to_index_append_buffer.reset();
        _share_name_to_index = false;
    }

    const FieldPtr& field(size_t idx) const;
    const Fields& fields() const { return _fields; }

    std::vector<std::string> field_names() const;

    // return null if name not found
    FieldPtr get_field_by_name(const std::string& name) const;

    size_t get_field_index_by_name(const std::string& name) const;

    void convert_to(Schema* new_schema, const std::vector<LogicalType>& new_types) const;

    KeysType keys_type() const { return static_cast<KeysType>(_keys_type); }

private:
    void _build_index_map(const Fields& fields);

    Fields _fields;
    size_t _num_keys = 0;
    std::vector<ColumnId> _sort_key_idxes;
    std::shared_ptr<std::unordered_map<std::string_view, size_t>> _name_to_index;

    // If we share the same _name_to_index with another vectorized schema,
    // newly append(only append here) fields will be added directly to the
    // _name_to_index_append_buffer. Reasons why we use this: because we share
    // _name_to_index with another vectorized schema, we cannot directly append
    // the newly fields to the shared _name_to_index. One possible solution is
    // COW(copy on write), allocate a new _name_to_index and append the newly
    // fields to it. However, in our scenario, usually we only need to append
    // few fields(maybe only 1) to the new schema, there is not need to use COW
    // to allocate a new _name_to_index, so here we saves new append fileds in
    // _name_to_index_append_buffer.
    std::shared_ptr<std::unordered_map<std::string_view, size_t>> _name_to_index_append_buffer;
    // If _share_name_to_index is true, it means that we share the _name_to_index
    // with another schema, and we only perform read or append to current schema,
    // the append field's name will be written to _name_to_index_append_buffer.
    mutable bool _share_name_to_index = false;

    uint8_t _keys_type = static_cast<uint8_t>(DUP_KEYS);
};

inline std::ostream& operator<<(std::ostream& os, const Schema& schema) {
    const Fields& fields = schema.fields();
    os << "(";
    if (!fields.empty()) {
        os << *fields[0];
    }
    for (size_t i = 1; i < fields.size(); i++) {
        os << ", " << *fields[i];
    }
    os << ")";
    return os;
}

} // namespace starrocks
