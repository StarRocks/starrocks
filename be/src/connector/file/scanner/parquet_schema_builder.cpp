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

#include "connector/file/scanner/parquet_schema_builder.h"

#include "fmt/format.h"
#include "gutil/casts.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Legacy parquet variant layout: group(metadata, value).
static constexpr int VARIANT_UNSHREDDING_FIELD_COUNT = 2;
// Shredded parquet variant layout: group(metadata, value, typed_value).
static constexpr int VARIANT_SHREDDING_COUNT = 3;
// UUID is encoded as 16 bytes in parquet but represented as a 36-character string
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) in StarRocks VARCHAR.
static constexpr int UUID_VARCHAR_LENGTH = 36;

static Status get_parquet_node_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_group(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_primitive(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_list(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status get_parquet_type_from_map(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static bool is_variant_type(const ::parquet::schema::NodePtr& node);
static Status get_parquet_variant_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);
static Status try_to_infer_struct_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc);

static bool is_list_or_map(const ::parquet::schema::NodePtr& node) {
    return node->is_group() && (node->logical_type()->is_list() || node->logical_type()->is_map());
}

// The inference rules mirror the native reader's schema resolution (formats/parquet/schema.cpp), so that
// files() infers the types the native reader then resolves for the same file.
Status get_parquet_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // LIST/MAP-annotated groups check their own repetition.
    if (node->is_repeated() && !is_list_or_map(node)) {
        // One-level list encoding: a repeated primitive or a repeated group (a list of struct) outside
        // a LIST/MAP-annotated group is a required list of non-null elements.
        //
        // repeated int32 name;
        // repeated group name { ... }
        TypeDescriptor element_type_desc;
        RETURN_IF_ERROR(get_parquet_node_type(node, &element_type_desc));
        *type_desc = TypeDescriptor::create_array_type(element_type_desc);
        return Status::OK();
    }
    return get_parquet_node_type(node, type_desc);
}

// Types the node itself, ignoring its own repetition.
static Status get_parquet_node_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
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
            auto int_logical_type = std::dynamic_pointer_cast<const parquet::IntLogicalType>(logical_type);
            if (int_logical_type != nullptr && !int_logical_type->is_signed()) {
                // StarRocks has no native unsigned types; widen an unsigned INT32-backed value to a
                // signed type that holds its full range (mirrors variant_integer_desc_from_bitwidth in
                // formats/parquet/utils.cpp). The load path zero-extends the same value, so the wider
                // inferred type matches the loaded value.
                switch (int_logical_type->bit_width()) {
                case 8:
                    *type_desc = TypeDescriptor(TYPE_SMALLINT);
                    break;
                case 16:
                    *type_desc = TypeDescriptor(TYPE_INT);
                    break;
                default:
                    *type_desc = TypeDescriptor(TYPE_BIGINT);
                    break;
                }
            } else {
                *type_desc = TypeDescriptor(TYPE_INT);
            }
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
        } else if (logical_type->is_UUID()) {
            // UUID bytes are converted to canonical string form (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
            // by FixedLenByteArrayToUUIDConverter during scan.  Dict filtering is supported for UUID:
            // ScalarColumnReader::rewrite_conjunct_ctxs_to_predicate() sets dict_value_converter so
            // raw 16-byte dict entries are converted to canonical strings before predicates are applied.
            *type_desc = TypeDescriptor::create_varchar_type(UUID_VARCHAR_LENGTH);
        } else {
            // INTERVAL (12B), BSON, and unannotated FLBA all carry raw binary data.
            // VARBINARY is the correct SR type for all of these.
            *type_desc = TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
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
    } else if (is_variant_type(node)) { // TODO: replace with parquet variant logical type when it is supported
        return get_parquet_variant_type(node, type_desc);
    }

    auto st = try_to_infer_struct_type(node, type_desc);
    if (st.ok()) {
        return Status::OK();
    }

    // Treat unsupported types as VARCHAR.
    *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    return Status::OK();
}

static bool is_variant_type(const parquet::schema::NodePtr& node) {
    DCHECK(node->is_group());

    const auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    int field_count = group_node->field_count();
    if (field_count != VARIANT_UNSHREDDING_FIELD_COUNT && field_count != VARIANT_SHREDDING_COUNT) {
        return false;
    }

    int metadata_field_index = -1;
    int value_field_index = -1;
    TypeDescriptor metadata_type_desc;
    TypeDescriptor value_type_desc;
    for (auto i = 0; i < group_node->field_count(); ++i) {
        const auto& field = group_node->field(i);
        TypeDescriptor child_type_desc;
        auto field_type_status = get_parquet_type(field, &child_type_desc);
        if (!field_type_status.ok()) {
            return false;
        }

        if (field->name() == "metadata") {
            metadata_field_index = i;
            metadata_type_desc = child_type_desc;
        } else if (field->name() == "value") {
            value_field_index = i;
            value_type_desc = child_type_desc;
        } else if (field->name() == "typed_value") {
        } else {
            return false;
        }
    }

    if (metadata_field_index == -1 || value_field_index == -1) {
        return false;
    }

    return metadata_type_desc.type == TYPE_VARBINARY && value_type_desc.type == TYPE_VARBINARY;
}

static Status get_parquet_variant_type(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    DCHECK(node->is_group());

    const auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    const int field_count = group_node->field_count();
    if (field_count != VARIANT_UNSHREDDING_FIELD_COUNT && field_count != VARIANT_SHREDDING_COUNT) {
        return Status::InvalidArgument("Not a variant type");
    }
    *type_desc = TypeDescriptor::create_variant_type();
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

Support legacy encodings:
1. List<Integer> (nullable list, non-null elements)
  optional group my_list (LIST) {
    repeated int32 element;
  }

2. List<Tuple<String, Integer>> (nullable list, non-null elements)
  optional group my_list (LIST) {
    repeated group element {
      required binary str (STRING);
      required int32 num;
    }
  }

3. List<List<Integer>> (nullable outer list, non-null elements)
  optional group my_list (LIST) {
    repeated group array (LIST) {
      repeated int32 array;
    }
  }

4. List<OneTuple<String>> (nullable list, non-null elements)
  optional group my_list (LIST) {
    repeated group array {
      required binary str (UTF8);
    };
  }
  optional group my_list (LIST) {
    repeated group my_list_tuple {
      required binary str (UTF8);
    }
  }

The rules match SchemaDescriptor::list_to_field in formats/parquet/schema.cpp.
*/

// Backward-compatibility rule from the format spec: a single-field repeated group named `array` or
// `<list name>_tuple` is the element type itself, i.e. a list of struct.
static bool has_struct_list_name(const std::string& repeated_name, const std::string& list_name) {
    return repeated_name == "array" || repeated_name == list_name + "_tuple";
}

// Also used for a key-only MAP, which is resolved as a list of keys.
static Status get_parquet_type_from_list(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // 1st level.
    DCHECK(node->is_group());
    if (node->is_repeated()) {
        return Status::NotSupported(fmt::format("list 1st level group {} must not be repeated", node->name()));
    }
    const auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    if (group_node->field_count() != 1) {
        return Status::NotSupported(fmt::format("list 1st level group {} must have exactly one child, but got {}",
                                                group_node->name(), group_node->field_count()));
    }

    // 2nd level.
    const auto& list_node = group_node->field(0);
    if (!list_node->is_repeated()) {
        return Status::NotSupported(fmt::format("list 2nd level node {} is not repeated", list_node->name()));
    }

    TypeDescriptor element_type_desc;
    if (list_node->is_group()) {
        const auto list_group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(list_node);
        const int field_count = list_group_node->field_count();
        if (field_count == 0) {
            return Status::NotSupported(
                    fmt::format("list 2nd level group {} must have at least one child", list_group_node->name()));
        }

        // A LIST-annotated repeated group with a single repeated child takes precedence over the name rule:
        // it is a nested list with two-level encoding, whatever the repeated group is called (legacy
        // encoding 3).
        //
        // Otherwise a single child is the element of a 3-level list, unless the repeated group is named
        // `array` or `<list name>_tuple` (legacy encoding 4), even when that child is repeated.
        const bool is_single_element =
                field_count == 1 &&
                ((list_group_node->field(0)->is_repeated() && list_group_node->logical_type()->is_list()) ||
                 !has_struct_list_name(list_group_node->name(), group_node->name()));
        if (is_single_element) {
            // 3rd level, typed with its own repetition: a repeated child is a one-level list.
            RETURN_IF_ERROR(get_parquet_type(list_group_node->field(0), &element_type_desc));
        } else {
            // The repeated group itself is the element: a struct (legacy encodings 2 and 4).
            RETURN_IF_ERROR(try_to_infer_struct_type(list_node, &element_type_desc));
        }
    } else {
        // 2-level encoding (legacy encoding 1): the repeated primitive is the element.
        RETURN_IF_ERROR(get_parquet_node_type(list_node, &element_type_desc));
    }
    *type_desc = TypeDescriptor::create_array_type(element_type_desc);
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

The rules match SchemaDescriptor::map_to_field in formats/parquet/schema.cpp.
*/
static Status get_parquet_type_from_map(const ::parquet::schema::NodePtr& node, TypeDescriptor* type_desc) {
    // 1st level.
    // <map-repetition> group <name> (MAP) {
    DCHECK(node->is_group());
    if (node->is_repeated()) {
        return Status::NotSupported(fmt::format("map 1st level group {} must not be repeated", node->name()));
    }
    const auto group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    if (group_node->field_count() != 1) {
        return Status::NotSupported(fmt::format("map 1st level group {} must have exactly one child, but got {}",
                                                group_node->name(), group_node->field_count()));
    }

    // 2nd level.
    // repeated group key_value {
    const auto& kv_node = group_node->field(0);
    if (!kv_node->is_repeated() || !kv_node->is_group()) {
        return Status::NotSupported(fmt::format("map 2nd level node {} must be a repeated group", kv_node->name()));
    }
    const auto kv_group_node = std::static_pointer_cast<::parquet::schema::GroupNode>(kv_node);
    const int field_count = kv_group_node->field_count();
    if (field_count != 1 && field_count != 2) {
        return Status::NotSupported(fmt::format("map 2nd level group {} must have 1 or 2 children, but got {}",
                                                kv_group_node->name(), field_count));
    }
    if (field_count == 1) {
        // A map without values (a set) is read as a list of keys.
        return get_parquet_type_from_list(node, type_desc);
    }

    // 3rd level.
    const auto& key_node = kv_group_node->field(0);
    if (key_node->is_group()) {
        return Status::NotSupported(fmt::format("map key {} must be a primitive type", key_node->name()));
    }
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
