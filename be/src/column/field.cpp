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

#include "column/field.h"

#include "column/datum.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "storage/key_coder.h"
#include "storage/types.h"

namespace starrocks {

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

FieldPtr Field::convert_to(LogicalType to_type) const {
    FieldPtr new_field = std::make_shared<Field>(*this);
    new_field->_type = get_type_info(to_type);
    new_field->_short_key_length = static_cast<uint8_t>(new_field->_type->size());
    return new_field;
}

MutableColumnPtr Field::create_column() const {
    return ChunkHelper::column_from_field(*this);
}

FieldPtr Field::convert_to_dict_field(const Field& field) {
    if (field.type()->type() == TYPE_VARCHAR) {
        FieldPtr res = std::make_shared<Field>(field);
        res->_type = get_type_info(TYPE_INT);
        return res;
    } else if (field.type()->type() == TYPE_ARRAY && field.sub_field(0).type()->type() == TYPE_VARCHAR) {
        auto child = Field(field.sub_field(0));
        child._type = get_type_info(TYPE_INT);

        FieldPtr res = std::make_shared<Field>(field);
        res->_sub_fields->clear();
        res->_sub_fields->emplace_back(child);
        return res;
    } else {
        DCHECK(false);
    }
    return nullptr;
}

FieldPtr Field::convert_from_slot_desc(const SlotDescriptor& slot) {
    return _build_field_from_type_desc(slot.type(), slot.col_name(), slot.id(), slot.is_nullable());
}

FieldPtr Field::_build_field_from_type_desc(const TypeDescriptor& type_desc, const std::string& name, int32_t id,
                                            bool nullable) {
    TypeInfoPtr type_info = get_type_info(type_desc);
    DCHECK(type_info != nullptr);

    // Construct field instance directly for basic types
    if (type_desc.children.empty()) {
        return std::make_shared<Field>(id, name, type_info, nullable);
    }

    // Recursively construct subfields of complex types
    Fields sub_fields;

    switch (type_desc.type) {
    case TYPE_ARRAY: {
        // children[0] is element type
        const TypeDescriptor& item_type = type_desc.children[0];
        sub_fields.push_back(_build_field_from_type_desc(item_type, name + ".item", id * 10 + 1, true));
        break;
    }
    case TYPE_MAP: {
        // children[0] key, children[1] value
        const TypeDescriptor& key_type = type_desc.children[0];
        const TypeDescriptor& value_type = type_desc.children[1];
        sub_fields.push_back(_build_field_from_type_desc(key_type, name + ".key", id * 10 + 1, true));
        sub_fields.push_back(_build_field_from_type_desc(value_type, name + ".value", id * 10 + 2, true));
        break;
    }
    case TYPE_STRUCT: {
        // children[i] corresponds to field_names[i]
        for (size_t i = 0; i < type_desc.children.size(); ++i) {
            const TypeDescriptor& child_type = type_desc.children[i];
            std::string child_name;
            if (i < type_desc.field_names.size()) {
                child_name = type_desc.field_names[i];
            } else {
                child_name = name + ".field" + std::to_string(i);
            }
            sub_fields.push_back(_build_field_from_type_desc(child_type, child_name, id * 10 + 1 + i, true));
        }
        break;
    }
    default: {
        break;
    }
    }

    auto field = std::make_shared<Field>(id, name, type_info, nullable);
    for (auto& sub_field : sub_fields) {
        field->add_sub_field(*sub_field);
    }
    return field;
}

} // namespace starrocks
