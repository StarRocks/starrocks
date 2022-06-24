// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/schema.h"

#include <algorithm>

namespace starrocks::vectorized {

#ifdef BE_TEST

Schema::Schema(Fields fields) : Schema(fields, KeysType::DUP_KEYS) {}

#endif

Schema::Schema(Fields fields, KeysType keys_type)
        : _fields(std::move(fields)),
          _name_to_index_append_buffer(nullptr),
          _read_append_only(false),
          _keys_type(static_cast<uint8_t>(keys_type)) {
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

Schema::Schema(Schema* schema, const std::vector<ColumnId>& cids)
        : _name_to_index_append_buffer(nullptr), _read_append_only(false), _keys_type(schema->_keys_type) {
    _fields.resize(cids.size());
    for (int i = 0; i < cids.size(); i++) {
        DCHECK_LT(cids[i], schema->_fields.size());
        _fields[i] = schema->_fields[cids[i]];
    }
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

// if we use the constructor, we must make sure that input (Schema* schema) is read_only
Schema::Schema(Schema* schema)
        : _num_keys(schema->_num_keys),
          _name_to_index_append_buffer(nullptr),
          _read_append_only(true),
          _keys_type(schema->_keys_type) {
    DCHECK(schema->_name_to_index_append_buffer == nullptr);
    _fields.resize(schema->num_fields());
    for (int i = 0; i < schema->_fields.size(); i++) {
        _fields[i] = schema->_fields[i];
    }
    // share the name_to_index with schema*, lator append fields will be added to _name_to_index_append_buffer
    _name_to_index = schema->_name_to_index;
}

void Schema::append(const FieldPtr& field) {
    _fields.emplace_back(field);
    _num_keys += field->is_key();
    if (!_read_append_only) {
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

void Schema::insert(size_t idx, const FieldPtr& field) {
    DCHECK_LT(idx, _fields.size());

    _fields.emplace(_fields.begin() + idx, field);
    _num_keys += field->is_key();

    if (_read_append_only) {
        // release the _name_to_index_append_buffer and rebuild the _name_to_index
        _name_to_index_append_buffer.reset();
        _read_append_only = false;
    }

    _name_to_index.reset();
    _build_index_map(_fields);
}

void Schema::remove(size_t idx) {
    DCHECK_LT(idx, _fields.size());
    _num_keys -= _fields[idx]->is_key();
    if (_read_append_only && idx == _fields.size() - 1 && _name_to_index_append_buffer != nullptr &&
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
        _read_append_only = false;
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
            DCHECK(_read_append_only);
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

void Schema::convert_to(Schema* new_schema, const std::vector<FieldType>& new_types) const {
    int num_fields = _fields.size();
    new_schema->_fields.resize(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        auto cid = _fields[i]->id();
        auto new_type = new_types[cid];
        if (_fields[i]->type()->type() == new_type) {
            new_schema->_fields[i] = _fields[i]->copy();
        } else {
            new_schema->_fields[i] = _fields[i]->convert_to(new_type);
        }
    }
    new_schema->_num_keys = _num_keys;
    if (!_read_append_only) {
        new_schema->_name_to_index = _name_to_index;
    } else {
        new_schema->_build_index_map(new_schema->_fields);
    }
}

} // namespace starrocks::vectorized
