// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>
#include <utility>

#include "column/vectorized_fwd.h"
#include "storage/types.h"

namespace starrocks::vectorized {

class AggregateInfo;
class Datum;

class Field {
public:
    Field(ColumnId id, std::string name, FieldType type, int precision, int scale, bool nullable);
    Field(ColumnId id, const std::string& name, FieldType type, bool nullable)
            : Field(id, name, type, -1, -1, nullable) {
        DCHECK(type != OLAP_FIELD_TYPE_DECIMAL32);
        DCHECK(type != OLAP_FIELD_TYPE_DECIMAL64);
        DCHECK(type != OLAP_FIELD_TYPE_DECIMAL128);
        DCHECK(type != OLAP_FIELD_TYPE_ARRAY);
    }

    Field(ColumnId id, std::string name, TypeInfoPtr type, bool is_nullable = true)
            : _id(id), _name(std::move(name)), _type(std::move(type)), _is_nullable(is_nullable) {}

    Field(const Field&) = default;
    Field(Field&&) = default;
    Field& operator=(const Field&) = default;
    Field& operator=(Field&&) = default;

    virtual ~Field() = default;

    // return a copy of this field with the replaced type
    FieldPtr with_type(const TypeInfoPtr& type);

    // return a copy of this field with the replaced name
    FieldPtr with_name(const std::string& name);

    // return a copy of this field with the replaced nullability
    FieldPtr with_nullable(bool is_nullable);

    std::string to_string() const;

    ColumnId id() const { return _id; }
    const std::string& name() const { return _name; }
    const TypeInfoPtr& type() const { return _type; }
    bool is_nullable() const { return _is_nullable; }

    FieldPtr copy() const;

    bool is_key() const { return _is_key; }
    void set_is_key(bool is_key) { _is_key = is_key; }

    size_t short_key_length() const { return _short_key_length; }
    void set_short_key_length(size_t n) { _short_key_length = n; }

    // Encode the first |short_key_length| bytes.
    void encode_ascending(const Datum& value, std::string* buf) const;

    // Encode the full field.
    void full_encode_ascending(const Datum& value, std::string* buf) const;

    // Status decode_ascending(Slice* encoded_key, uint8_t* cell_ptr, MemPool* pool) const;

    void set_aggregate_method(FieldAggregationMethod agg_method) { _agg_method = agg_method; }

    starrocks::FieldAggregationMethod aggregate_method() const { return _agg_method; }

    FieldPtr convert_to(FieldType to_type) const;

    void add_sub_field(const Field& sub_field) { _sub_fields.emplace_back(sub_field); }

    const Field& get_sub_field(int i) const { return _sub_fields[i]; }

    ColumnPtr create_column() const;

private:
    Field() = default;

    ColumnId _id = 0;
    std::string _name;
    TypeInfoPtr _type = nullptr;
    starrocks::FieldAggregationMethod _agg_method = OLAP_FIELD_AGGREGATION_NONE;
    size_t _short_key_length = 0;
    bool _is_nullable = true;
    bool _is_key = false;
    std::vector<Field> _sub_fields;
};

inline std::ostream& operator<<(std::ostream& os, const Field& field) {
    os << field.to_string();
    return os;
}

} // namespace starrocks::vectorized
