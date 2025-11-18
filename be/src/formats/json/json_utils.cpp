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

#include "formats/json/json_utils.h"

namespace starrocks {
TypeDescriptor JsonUtils::construct_json_type(const TypeDescriptor& src_type) {
    switch (src_type.type) {
    case TYPE_ARRAY: {
        TypeDescriptor json_type(TYPE_ARRAY);
        const auto& child_type = src_type.children[0];
        json_type.children.emplace_back(construct_json_type(child_type));
        return json_type;
    }
    case TYPE_STRUCT: {
        TypeDescriptor json_type(TYPE_STRUCT);
        json_type.field_names = src_type.field_names;
        for (auto& child_type : src_type.children) {
            json_type.children.emplace_back(construct_json_type(child_type));
        }
        return json_type;
    }
    case TYPE_MAP: {
        TypeDescriptor json_type(TYPE_MAP);
        const auto& key_type = src_type.children[0];
        const auto& value_type = src_type.children[1];
        json_type.children.emplace_back(construct_json_type(key_type));
        json_type.children.emplace_back(construct_json_type(value_type));
        return json_type;
    }
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_BIGINT:
    case TYPE_INT:
    case TYPE_SMALLINT:
    case TYPE_TINYINT:
    case TYPE_BOOLEAN:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_JSON: {
        return src_type;
    }
    default:
        // Treat other types as VARCHAR.
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    }
}
} // namespace starrocks