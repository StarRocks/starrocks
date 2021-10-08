// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/field.h"

#include <utility>

#include "column/datum.h"
#include "storage/key_coder.h"
#include "storage/types.h"
#include "storage/vectorized/chunk_helper.h"

namespace starrocks::vectorized {

Field::Field(ColumnId id, std::string name, FieldType type, int precision, int scale, bool nullable)
        : _id(id), _name(std::move(name)), _type(get_type_info(type, precision, scale)), _is_nullable(nullable) {}

FieldPtr Field::with_type(const TypeInfoPtr& type) {
    return std::make_shared<Field>(_id, _name, type, _is_nullable);
}

FieldPtr Field::with_name(const std::string& name) {
    return std::make_shared<Field>(_id, name, _type, _is_nullable);
}

FieldPtr Field::with_nullable(bool is_nullable) {
    return std::make_shared<Field>(_id, _name, _type, is_nullable);
}

FieldPtr Field::copy() const {
    return std::make_shared<Field>(*this);
}

void Field::encode_ascending(const Datum& value, std::string* buf) const {
    if (_short_key_length > 0) {
        const KeyCoder* coder = get_key_coder(_type->type());
        coder->encode_ascending(value, _short_key_length, buf);
    }
}

void Field::full_encode_ascending(const Datum& value, std::string* buf) const {
    const KeyCoder* coder = get_key_coder(_type->type());
    coder->full_encode_ascending(value, buf);
}

FieldPtr Field::convert_to(FieldType to_type) const {
    FieldPtr new_field = std::make_shared<Field>(*this);
    new_field->_type = get_type_info(to_type);
    new_field->_short_key_length = new_field->_type->size();
    return new_field;
}

ColumnPtr Field::create_column() const {
    return ChunkHelper::column_from_field(*this);
}

std::string Field::to_string() const {
    std::stringstream os;
    os << id() << ":";
    os << name() << " ";
    os << type()->type() << " ";
    os << (is_nullable() ? "NULL" : "NOT NULL");
    os << (is_key() ? " KEY" : "");
    os << " " << aggregate_method();
    return os.str();
}

} // namespace starrocks::vectorized
