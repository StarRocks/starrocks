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

namespace starrocks {

FieldPtr Field::convert_to(LogicalType to_type) const {
    FieldPtr new_field = std::make_shared<Field>(*this);
    new_field->_type = get_type_info(to_type);
    new_field->_short_key_length = static_cast<uint8_t>(new_field->_type->size());
    return new_field;
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

} // namespace starrocks
