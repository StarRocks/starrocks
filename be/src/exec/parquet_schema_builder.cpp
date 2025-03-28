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

#include "parquet_schema_builder.h"

#include "fmt/format.h"
#include "gutil/casts.h"

namespace starrocks {

static Status get_parquet_type_from_group(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_primitive(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_list(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_map(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status try_to_infer_struct_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);

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
            *type_desc = TypeDescriptor::promote_decimal_type(decimal_logical_type->precision(),
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
            *type_desc = TypeDescriptor::promote_decimal_type(decimal_logical_type->precision(),
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
            *type_desc = TypeDescriptor::promote_decimal_type(decimal_logical_type->precision(),
                                                              decimal_logical_type->scale());
        } else if (logical_type->is_JSON()) {
            *type_desc = TypeDescriptor::create_json_type();
        } else {
            *type_desc = TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        if (logical_type->is_decimal()) {
            auto decimal_logical_type = std::dynamic_pointer_cast<const parquet::DecimalLogicalType>(logical_type);
            *type_desc = TypeDescriptor::promote_decimal_type(decimal_logical_type->precision(),
                                                              decimal_logical_type->scale());
        } else {
            *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        break;
    }
    default:
        // Treat unsupported types as varbinary type.
        *type_desc = TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    }

    return Status::OK();
}

static Status get_parquet_type_from_group(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    DCHECK(node->is_group());
    auto logical_type = node->logical_type();
    if (logical_type->is_list()) {
        return get_parquet_type_from_list(node, type_desc);
    } else if (logical_type->is_map()) {
        return get_parquet_type_from_map(node, type_desc);
    }

    auto st = try_to_infer_struct_type(node, type_desc);
    if (st.ok()) {
        return Status::OK();
    }

    // Treat unsupported types as VARCHAR.
    *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    return Status::OK();
}

/*
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

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
    RETURN_IF_ERROR(get_parquet_type(child_node, &child_type_desc));
    *type_desc = TypeDescriptor::create_array_type(child_type_desc);

    return Status::OK();
}

/*
https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

MAP is used to annotate types that should be interpreted as a map from keys to values. MAP must annotate a 3-level structure:

<map-repetition> group <name> (MAP) {
  repeated group key_value {
    required <key-type> key;
    <value-repetition> <value-type> value;
  }
}
The outer-most level must be a group annotated with MAP that contains a single field named key_value. The repetition of this level must be either optional or required and determines whether the list is nullable.
The middle level, named key_value, must be a repeated group with a key field for map keys and, optionally, a value field for map values.
The key field encodes the map's key type. This field must have repetition required and must always be present.
The value field encodes the map's value type and repetition. This field can be required, optional, or omitted.
*/

static Status get_parquet_type_from_map(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // 1st level.
    // <map-repetition> group <name> (MAP) {
    DCHECK(node->is_group());
    DCHECK(node->logical_type()->is_map());

    auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    DCHECK(group_node->field_count() == 1);

    // 2nd level.
    // repeated group key_value {
    auto kv_node = group_node->field(0);
    auto kv_group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(kv_node);
    DCHECK(kv_group_node->field_count() == 2);
    DCHECK(kv_node->is_group());

    // 3rd level.
    // <list-repetition> group <name> (LIST)
    const auto& key_node = kv_group_node->field(0);
    TypeDescriptor key_type_desc;
    RETURN_IF_ERROR(get_parquet_type(key_node, &key_type_desc));

    const auto& value_node = kv_group_node->field(1);
    TypeDescriptor value_type_desc;
    RETURN_IF_ERROR(get_parquet_type(value_node, &value_type_desc));

    *type_desc = TypeDescriptor::create_map_type(key_type_desc, value_type_desc);

    return Status::OK();
}

/*
try to infer struct type from group node.

parquet does not have struct type, there is no struct definition in parquet.
try to infer like this.
group <name> {
    type field0;
    type field1;
    ...
}
*/
static Status try_to_infer_struct_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // 1st level.
    // group name
    DCHECK(node->is_group());

    auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    int field_count = group_node->field_count();
    if (field_count == 0) {
        return Status::Unknown("unknown type");
    }

    // 2nd level.
    // field
    std::vector<std::string> field_names;
    std::vector<TypeDescriptor> field_types;
    field_names.reserve(field_count);
    field_types.reserve(field_count);
    for (auto i = 0; i < group_node->field_count(); ++i) {
        const auto& field = group_node->field(i);
        field_names.emplace_back(field->name());
        auto& field_type_desc = field_types.emplace_back();
        RETURN_IF_ERROR(get_parquet_type(field, &field_type_desc));
    }

    *type_desc = TypeDescriptor::create_struct_type(field_names, field_types);

    return Status::OK();
}

} //namespace starrocks