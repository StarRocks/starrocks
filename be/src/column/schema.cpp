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

#include "column/schema.h"

#include <algorithm>
#include <utility>

namespace starrocks {

#ifdef BE_TEST

Schema::Schema(Fields fields) : Schema(fields, KeysType::DUP_KEYS, {}) {}

#endif

Schema::Schema(Fields fields, KeysType keys_type, std::vector<ColumnId> sort_key_idxes)
        : _fields(std::move(fields)),
          _sort_key_idxes(std::move(sort_key_idxes)),
          _name_to_index_append_buffer(nullptr),

          _keys_type(static_cast<uint8_t>(keys_type)) {
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

Schema::Schema(Schema* schema, const std::vector<ColumnId>& cids)
        : _name_to_index_append_buffer(nullptr), _keys_type(schema->_keys_type) {
    _fields.resize(cids.size());
    for (int i = 0; i < cids.size(); i++) {
        DCHECK_LT(cids[i], schema->_fields.size());
        _fields[i] = schema->_fields[cids[i]];
    }
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

Schema::Schema(Schema* schema, const std::vector<ColumnId>& cids, const std::vector<ColumnId>& scids)
        : _name_to_index_append_buffer(nullptr), _keys_type(schema->_keys_type) {
    DCHECK(!scids.empty());
    _fields.resize(cids.size());
    for (int i = 0; i < cids.size(); i++) {
        DCHECK_LT(cids[i], schema->_fields.size());
        _fields[i] = schema->_fields[cids[i]];
    }
    _sort_key_idxes = scids;
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

// if we use this constructor and share the name_to_index with another schema,
// we must make sure another shema is read only!!!
Schema::Schema(Schema* schema)
        : _num_keys(schema->_num_keys), _name_to_index_append_buffer(nullptr), _keys_type(schema->_keys_type) {
    DCHECK(schema->_name_to_index_append_buffer == nullptr);
    _fields.resize(schema->num_fields());
    for (int i = 0; i < schema->_fields.size(); i++) {
        _fields[i] = schema->_fields[i];
    }
    _sort_key_idxes = schema->sort_key_idxes();
    if (schema->_name_to_index_append_buffer == nullptr) {
        // share the name_to_index with schema, later append fields will be added to _name_to_index_append_buffer
        schema->_share_name_to_index = true;
        _share_name_to_index = true;
        _name_to_index = schema->_name_to_index;
    } else {
        _share_name_to_index = false;
        _build_index_map(_fields);
    }
}

// if we use this constructor and share the name_to_index with another schema,
// we must make sure another shema is read only!!!
Schema::Schema(const Schema& schema)
        : _num_keys(schema._num_keys), _name_to_index_append_buffer(nullptr), _keys_type(schema._keys_type) {
    _fields.resize(schema.num_fields());
    for (int i = 0; i < schema._fields.size(); i++) {
        _fields[i] = schema._fields[i];
    }
    _sort_key_idxes = schema.sort_key_idxes();
    if (schema._name_to_index_append_buffer == nullptr) {
        // share the name_to_index with schema&, later append fields will be added to _name_to_index_append_buffer
        schema._share_name_to_index = true;
        _share_name_to_index = true;
        _name_to_index = schema._name_to_index;
    } else {
        _share_name_to_index = false;
        _build_index_map(_fields);
    }
}

// if we use this constructor and share the name_to_index with another schema,
// we must make sure another shema is read only!!!
Schema& Schema::operator=(const Schema& other) {
    this->_num_keys = other._num_keys;
    this->_name_to_index_append_buffer = nullptr;
    this->_keys_type = other._keys_type;
    this->_fields.resize(other.num_fields());
    for (int i = 0; i < this->_fields.size(); i++) {
        this->_fields[i] = other._fields[i];
    }
    this->_sort_key_idxes = other.sort_key_idxes();
    if (other._name_to_index_append_buffer == nullptr) {
        // share the name_to_index with schema&, later append fields will be added to _name_to_index_append_buffer
        other._share_name_to_index = true;
        this->_share_name_to_index = true;
        _name_to_index = other._name_to_index;
    } else {
        this->_share_name_to_index = false;
        _build_index_map(this->_fields);
    }
    return *this;
}

void Schema::append(const FieldPtr& field) {
    _fields.emplace_back(field);
    _num_keys += field->is_key();
    if (!_share_name_to_index) {
        if (_name_to_index == nullptr) {
            _name_to_index.reset(new std::unordered_map<std::string_view, size_t>());
        }
        _name_to_index->emplace(field->name(), _fields.size() - 1);
    } else {
        if (_name_to_index_append_buffer == nullptr) {
            _name_to_index_append_buffer.reset(new std::unordered_map<std::string_view, size_t>());
        }
        _name_to_index_append_buffer->emplace(field->name(), _fields.size() - 1);
    }
}

// it's not being used, especially in sort key scenario, so do not handle this case
void Schema::insert(size_t idx, const FieldPtr& field) {
    DCHECK_LT(idx, _fields.size());

    _fields.emplace(_fields.begin() + idx, field);
    _num_keys += field->is_key();

    if (_share_name_to_index) {
        // release the _name_to_index_append_buffer and rebuild the _name_to_index
        _name_to_index_append_buffer.reset();
        _share_name_to_index = false;
    }

    _name_to_index.reset();
    _build_index_map(_fields);
}

// it's not being used, especially in sort key scenario, so do not handle this case
void Schema::remove(size_t idx) {
    DCHECK_LT(idx, _fields.size());
    _num_keys -= _fields[idx]->is_key();
    if (_share_name_to_index && idx == _fields.size() - 1 && _name_to_index_append_buffer != nullptr &&
        _name_to_index_append_buffer->size() > 0) {
        // fastpath for the filed we want to remove is at the end of the _name_to_index_append_buffer
        _name_to_index_append_buffer->erase(_fields[idx]->name());
        _fields.erase(_fields.begin() + idx);
    } else {
        if (idx == _fields.size() - 1) {
            DCHECK(_name_to_index != nullptr);
            _name_to_index->erase(_fields[idx]->name());
            _fields.erase(_fields.begin() + idx);
        } else {
            _fields.erase(_fields.begin() + idx);
            _name_to_index.reset();
            _build_index_map(_fields);
        }
        _name_to_index_append_buffer.reset();
        _share_name_to_index = false;
    }
}

const FieldPtr& Schema::field(size_t idx) const {
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, _fields.size());
    return _fields[idx];
}

std::vector<std::string> Schema::field_names() const {
    std::vector<std::string> names;
    names.reserve(_fields.size());
    for (const auto& field : _fields) {
        names.emplace_back(field->name());
    }
    return names;
}

FieldPtr Schema::get_field_by_name(const std::string& name) const {
    size_t idx = get_field_index_by_name(name);
    return idx == -1 ? nullptr : _fields[idx];
}

void Schema::_build_index_map(const Fields& fields) {
    _name_to_index.reset(new std::unordered_map<std::string_view, size_t>());
    for (size_t i = 0; i < fields.size(); i++) {
        _name_to_index->emplace(fields[i]->name(), i);
    }
}

size_t Schema::get_field_index_by_name(const std::string& name) const {
    DCHECK(_name_to_index != nullptr);
    auto p = _name_to_index->find(name);
    if (p == _name_to_index->end()) {
        if (_name_to_index_append_buffer != nullptr) {
            DCHECK(_share_name_to_index);
            auto p2 = _name_to_index_append_buffer->find(name);
            if (p2 == _name_to_index_append_buffer->end()) {
                return -1;
            } else {
                return p2->second;
            }
        } else {
            return -1;
        }
    }
    return p->second;
}

void Schema::convert_to(Schema* new_schema, const std::vector<LogicalType>& new_types) const {
    size_t num_fields = _fields.size();
    new_schema->_fields.resize(num_fields);
    for (size_t i = 0; i < num_fields; ++i) {
        auto cid = _fields[i]->id();
        auto new_type = new_types[cid];
        if (_fields[i]->type()->type() == new_type) {
            new_schema->_fields[i] = _fields[i]->copy();
        } else {
            new_schema->_fields[i] = _fields[i]->convert_to(new_type);
        }
    }
    new_schema->_num_keys = _num_keys;
    if (!_share_name_to_index) {
        new_schema->_name_to_index = _name_to_index;
    } else {
        new_schema->_build_index_map(new_schema->_fields);
    }
}

} // namespace starrocks
