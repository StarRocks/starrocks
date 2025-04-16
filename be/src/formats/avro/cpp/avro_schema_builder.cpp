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

#include "formats/avro/cpp/avro_schema_builder.h"

#include "fmt/format.h"

#include "runtime/descriptors.h"

namespace starrocks {

static Status get_avro_type_from_scalar_type(const avro::NodePtr& node, TypeDescriptor* desc) {
    switch(node->type()) {
    case avro::AVRO_STRING:
        // string type UUID is also promoted as varchar.
        *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        break;

    case avro::AVRO_BYTES: {
        auto logical_type = node->logicalType();
        if (logical_type.type() == avro::LogicalType::DECIMAL) {
            *desc = TypeDescriptor::promote_decimal_type(logical_type.precision(), logical_type.scale());
        } else {
            *desc = TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        break;
    }

    case avro::AVRO_FIXED: {
        auto logical_type = node->logicalType();
        if (logical_type.type() == avro::LogicalType::DECIMAL) {
            *desc = TypeDescriptor::promote_decimal_type(logical_type.precision(), logical_type.scale());
        } else if (logical_type.type() == avro::LogicalType::UUID) {
            *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        } else {
            *desc = TypeDescriptor::create_varbinary_type(node->fixedSize());
        }
        break;
    }

    case avro::AVRO_INT: {
        auto logical_type = node->logicalType();
        if (logical_type.type() == avro::LogicalType::DATE) {
            *desc = TypeDescriptor::from_logical_type(TYPE_DATE);
        } else {
            *desc = TypeDescriptor::from_logical_type(TYPE_INT);
        }
        break;
    }

    case avro::AVRO_LONG: {
        auto logical_type = node->logicalType();
        if (logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS ||
            logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS) {
            *desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
        } else {
            *desc = TypeDescriptor::from_logical_type(TYPE_BIGINT);
        }
        break;
    }

    case avro::AVRO_FLOAT:
        *desc = TypeDescriptor::from_logical_type(TYPE_FLOAT);
        break;

    case avro::AVRO_DOUBLE:
        *desc = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
        break;

    case avro::AVRO_BOOL:
        *desc = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
        break;

    default:
        *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        break;
    }
    return Status::OK();
}

static Status get_avro_type_from_record(const avro::NodePtr& node, TypeDescriptor* desc) {
    auto field_count = node->leaves();
    std::vector<std::string> field_names;
    std::vector<TypeDescriptor> field_types;
    field_names.reserve(field_count);
    field_types.reserve(field_count);

    for (size_t i = 0; i < field_count; ++i) {
        field_names.emplace_back(node->nameAt(i));
        const auto& field = node->leafAt(i);
        auto& field_type_desc = field_types.emplace_back();
        RETURN_IF_ERROR(get_avro_type(field, &field_type_desc));
    }

    *desc = TypeDescriptor::create_struct_type(field_names, field_types);
    return Status::OK();
}

// [int]               -> int
// [null, int]         -> int
// [null, int, string] -> string
static Status get_avro_type_from_union(const avro::NodePtr& node, TypeDescriptor* desc) {
    TypeDescriptor tmp_desc;
    bool not_null = false;
    for (size_t i = 0; i < node->leaves(); ++i) {
        const auto& branch = node->leafAt(i);
        if (branch->type() != avro::AVRO_NULL) {
            if (not_null) {
                tmp_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
                break;
            }

            RETURN_IF_ERROR(get_avro_type(branch, &tmp_desc));
            not_null = true;
        }
    }

    *desc = tmp_desc;
    return Status::OK();
}

static Status get_avro_type_from_array(const avro::NodePtr& node, TypeDescriptor* desc) {
    DCHECK_EQ(node->leaves(), 1);
    TypeDescriptor item_desc;
    RETURN_IF_ERROR(get_avro_type(node->leafAt(0), &item_desc));
    *desc = TypeDescriptor::create_array_type(item_desc);
    return Status::OK();
}

static Status get_avro_type_from_map(const avro::NodePtr& node, TypeDescriptor* desc) {
    DCHECK_EQ(node->leaves(), 2);
    DCHECK_EQ(node->leafAt(0)->type(), avro::AVRO_STRING);
    TypeDescriptor value_desc;
    RETURN_IF_ERROR(get_avro_type(node->leafAt(1), &value_desc));
    *desc = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
                                            value_desc);
    return Status::OK();
}

Status get_avro_type(const avro::NodePtr& node, TypeDescriptor* desc) {
    switch(node->type()) {
    case avro::AVRO_RECORD:
        return get_avro_type_from_record(node, desc);

    case avro::AVRO_ENUM:
        *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        return Status::OK();

    case avro::AVRO_ARRAY:
        return get_avro_type_from_array(node, desc);

    case avro::AVRO_MAP:
        return get_avro_type_from_map(node, desc);

    case avro::AVRO_UNION:
        return get_avro_type_from_union(node, desc);

    default:
        return get_avro_type_from_scalar_type(node, desc);
    }
}

} // namespace starrocks
