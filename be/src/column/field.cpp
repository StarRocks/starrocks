// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/field.h"

#include "column/datum.h"
#include "storage/chunk_helper.h"
#include "storage/key_coder.h"
#include "storage/types.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
