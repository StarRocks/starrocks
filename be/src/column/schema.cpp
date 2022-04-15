// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/schema.h"

#include <algorithm>

namespace starrocks::vectorized {

#ifdef BE_TEST

Schema::Schema(Fields fields) : Schema(fields, KeysType::DUP_KEYS) {}

#endif

Schema::Schema(Fields fields, KeysType keys_type)
        : _fields(std::move(fields)), _keys_type(static_cast<uint8_t>(keys_type)) {
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

void Schema::append(const FieldPtr& field) {
    _fields.emplace_back(field);
    _name_to_index.emplace(field->name(), _fields.size() - 1);
    _num_keys += field->is_key();
}

void Schema::insert(size_t idx, const FieldPtr& field) {
    DCHECK_LT(idx, _fields.size());

    _fields.emplace(_fields.begin() + idx, field);
    _name_to_index.clear();
    _num_keys += field->is_key();
    _build_index_map(_fields);
}

void Schema::remove(size_t idx) {
    DCHECK_LT(idx, _fields.size());
    _num_keys -= _fields[idx]->is_key();
    if (idx == _fields.size() - 1) {
        _name_to_index.erase(_fields[idx]->name());
        _fields.erase(_fields.begin() + idx);
    } else {
        _fields.erase(_fields.begin() + idx);
        _name_to_index.clear();
        _build_index_map(_fields);
    }
}

void Schema::set_fields(Fields fields) {
    _fields = std::move(fields);
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
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
    for (size_t i = 0; i < fields.size(); i++) {
        _name_to_index.emplace(fields[i]->name(), i);
    }
}

size_t Schema::get_field_index_by_name(const std::string& name) const {
    auto p = _name_to_index.find(name);
    if (p == _name_to_index.end()) {
        return -1;
    }
    return p->second;
}

} // namespace starrocks::vectorized
