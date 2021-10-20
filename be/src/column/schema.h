// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/field.h"

namespace starrocks::vectorized {

class Schema {
public:
    Schema() = default;

    explicit Schema(Fields fields);

    size_t num_fields() const { return _fields.size(); }

    size_t num_key_fields() const { return _num_keys; }

    void reserve(size_t size) {
        _fields.reserve(size);
        _name_to_index.reserve(size);
    }

    void append(const FieldPtr& field);
    void insert(size_t idx, const FieldPtr& field);
    void remove(size_t idx);
    void set_fields(Fields fields);
    void clear() {
        _fields.clear();
        _num_keys = 0;
        _name_to_index.clear();
    }

    const FieldPtr& field(size_t idx) const;
    const Fields& fields() const { return _fields; }

    std::vector<std::string> field_names() const;

    // return null if name not found
    FieldPtr get_field_by_name(const std::string& name) const;

    size_t get_field_index_by_name(const std::string& name) const;

    void convert_to(Schema* new_schema, const std::vector<FieldType>& new_types) const {
        // fields
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
        new_schema->_name_to_index = _name_to_index;
    }

private:
    void _build_index_map(const Fields& fields);

    Fields _fields;
    size_t _num_keys = 0;
    std::unordered_map<std::string, size_t> _name_to_index;
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

} // namespace starrocks::vectorized
