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

#include "arrow_schema_converter.h"

#include "fmt/format.h"
#include "gutil/casts.h"

namespace starrocks {

static Status get_parquet_type_from_group(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_primitive(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_list(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);

Status get_parquet_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    if (node->is_group()) {
        return get_parquet_type_from_group(node, type_desc);
    }
    return get_parquet_type_from_primitive(node, type_desc);
}

static Status get_parquet_type_from_primitive(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    DCHECK(node->is_primitive());
    auto primitive_node = down_cast<const ::parquet::schema::PrimitiveNode*>(node.get());

    auto logical_type = node->logical_type();
    auto physical_type = primitive_node->physical_type();

    switch (physical_type) {
    case parquet::Type::BOOLEAN:
        *type_desc = TypeDescriptor(TYPE_BOOLEAN);
        break;
    case parquet::Type::FLOAT:
        *type_desc = TypeDescriptor(TYPE_FLOAT);
        break;
    case parquet::Type::DOUBLE:
        *type_desc = TypeDescriptor(TYPE_DOUBLE);
        break;
    case parquet::Type::INT32:
        if (logical_type->is_int()) {
            *type_desc = TypeDescriptor(TYPE_INT);
        } else if (logical_type->is_date()) {
            *type_desc = TypeDescriptor(TYPE_DATE);
        } else if (logical_type->is_time()) {
            *type_desc = TypeDescriptor(TYPE_TIME);
        } else if (logical_type->is_decimal()) {
            auto decimal_logical_type = std::dynamic_pointer_cast<const parquet::DecimalLogicalType>(logical_type);
            *type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, decimal_logical_type->precision(),
                                                               decimal_logical_type->scale());
        } else {
            *type_desc = TypeDescriptor(TYPE_INT);
        }
        break;
    case parquet::Type::INT64:
        if (logical_type->is_int()) {
            *type_desc = TypeDescriptor(TYPE_BIGINT);
        } else if (logical_type->is_time()) {
            *type_desc = TypeDescriptor(TYPE_TIME);
        } else if (logical_type->is_timestamp()) {
            *type_desc = TypeDescriptor(TYPE_DATETIME);
        } else if (logical_type->is_decimal()) {
            auto decimal_logical_type = std::dynamic_pointer_cast<const parquet::DecimalLogicalType>(logical_type);
            *type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, decimal_logical_type->precision(),
                                                               decimal_logical_type->scale());
        } else {
            *type_desc = TypeDescriptor(TYPE_BIGINT);
        }
        break;
    case parquet::Type::INT96:
        *type_desc = TypeDescriptor(TYPE_DATETIME);
        break;
    case parquet::Type::BYTE_ARRAY:
        if (logical_type->is_string()) {
            *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        } else if (logical_type->is_decimal()) {
            auto decimal_logical_type = std::dynamic_pointer_cast<const parquet::DecimalLogicalType>(logical_type);
            *type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, decimal_logical_type->precision(),
                                                               decimal_logical_type->scale());
        } else {
            *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        if (logical_type->is_decimal()) {
            auto decimal_logical_type = std::dynamic_pointer_cast<const parquet::DecimalLogicalType>(logical_type);
            *type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, decimal_logical_type->precision(),
                                                               decimal_logical_type->scale());
        } else {
            *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        break;
    }
    default:
        return Status::NotSupported(fmt::format("Unkown supported parquet physical type: {}, column name: {}",
                                                physical_type, node->name()));
    }

    return Status::OK();
}

static Status get_parquet_type_from_group(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    DCHECK(node->is_group());
    auto logical_type = node->logical_type();
    if (logical_type->is_list()) {
        return get_parquet_type_from_list(node, type_desc);
    }

    return Status::NotSupported("Unkown supported parquet physical type");
}

/*
LIST is used to annotate types that should be interpreted as lists.

LIST must always annotate a 3-level structure:

<list-repetition> group <name> (LIST) {
  repeated group list {
    <element-repetition> <element-type> element;
  }
}
The outer-most level must be a group annotated with LIST that contains a single field named list. The repetition of this level must be either optional or required and determines whether the list is nullable.
The middle level, named list, must be a repeated group with a single field named element.
The element field encodes the list's element type and repetition. Element repetition must be required or optional.
*/

static Status get_parquet_type_from_list(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // 1st level.
    // <list-repetition> group <name> (LIST) 
    DCHECK(node->is_group());
    DCHECK(node->logical_type()->is_list());
    *type_desc = TypeDescriptor(TYPE_ARRAY);

    auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    DCHECK(group_node->field_count() == 1);

    // 2nd level.
    // repeated group list {
    auto list_node = group_node->field(0);
    auto list_group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(list_node);
    DCHECK(list_group_node->field_count() == 1);
    DCHECK(list_group_node->is_group());

    // 3rd level.
    // <list-repetition> group <name> (LIST) 
    const auto& child_node = list_group_node->field(0);
    TypeDescriptor child_type_desc;
    RETURN_IF_ERROR(get_parquet_type_from_primitive(child_node, &child_type_desc));
    type_desc->children.emplace_back(std::move(child_type_desc));

    return Status::OK();
}

} //namespace starrocks