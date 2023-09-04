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

#include "formats/parquet/schema.h"

#include <boost/algorithm/string.hpp>

#include "gutil/casts.h"
#include "gutil/strings/substitute.h"

namespace starrocks::parquet {

std::string LevelInfo::debug_string() const {
    std::stringstream ss;
    ss << "LevelInfo(max_def_level=" << max_def_level << ",max_rep_level=" << max_rep_level
       << ",immediate_repeated_ancestor_def_level=" << immediate_repeated_ancestor_def_level << ")";
    return ss.str();
}

std::string ParquetField::debug_string() const {
    std::stringstream ss;
    ss << "ParquetField(name=" << name << ",type=" << type.type << ",physical_type=" << physical_type
       << ",physical_column_index=" << physical_column_index << ",levels_info=" << level_info.debug_string();
    if (children.size() > 0) {
        ss << ",children=[";
        for (int i = 0; i < children.size(); ++i) {
            if (i != 0) {
                ss << ",";
            }
            ss << children[i].debug_string();
        }
        ss << "]";
    }
    ss << ")";
    return ss.str();
}

static bool is_group(const tparquet::SchemaElement* schema) {
    return schema->num_children > 0;
}

static bool is_repeated(const tparquet::SchemaElement* schema) {
    return schema->__isset.repetition_type && schema->repetition_type == tparquet::FieldRepetitionType::REPEATED;
}

//static bool is_required(const tparquet::SchemaElement* schema) {
//    return schema->__isset.repetition_type && schema->repetition_type == tparquet::FieldRepetitionType::REQUIRED;
//}

static bool is_optional(const tparquet::SchemaElement* schema) {
    return schema->__isset.repetition_type && schema->repetition_type == tparquet::FieldRepetitionType::OPTIONAL;
}

static bool is_list(const tparquet::SchemaElement* schema) {
    return schema->__isset.converted_type && schema->converted_type == tparquet::ConvertedType::LIST;
}

static bool is_map(const tparquet::SchemaElement* schema) {
    return schema->__isset.converted_type && (schema->converted_type == tparquet::ConvertedType::MAP ||
                                              schema->converted_type == tparquet::ConvertedType::MAP_KEY_VALUE);
}

Status SchemaDescriptor::leaf_to_field(const tparquet::SchemaElement* t_schema, const LevelInfo& cur_level_info,
                                       bool is_nullable, ParquetField* field) {
    field->name = t_schema->name;
    field->schema_element = *t_schema;
    field->is_nullable = is_nullable;
    field->physical_type = t_schema->type;
    field->type_length = t_schema->type_length;
    field->scale = t_schema->scale;
    field->precision = t_schema->precision;
    field->field_id = t_schema->field_id;
    field->level_info = cur_level_info;

    _physical_fields.push_back(field);
    field->physical_column_index = _physical_fields.size() - 1;
    return Status::OK();
}

// Special case mentioned in the format spec:
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
//   If the name is array or ends in _tuple, this should be a list of struct
//   even for single child elements.
bool has_struct_list_name(const std::string& name) {
    static const Slice array_slice("array", 5);
    static const Slice tuple_slice("_tuple", 6);
    Slice slice(name);
    return slice == array_slice || slice.ends_with(tuple_slice);
}

Status SchemaDescriptor::list_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                       LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    ASSIGN_OR_RETURN(const auto* group_schema, _get_schema_element(t_schemas, pos));
    if (group_schema->num_children != 1) {
        return Status::InvalidArgument("LIST-annotated groups must have a single child.");
    }
    if (is_repeated(group_schema)) {
        return Status::InvalidArgument("LIST-annotated groups must not be repeated.");
    }
    _increment(group_schema, &cur_level_info);

    field->children.resize(1);
    auto child_field = &field->children[0];

    ASSIGN_OR_RETURN(const auto* list_node_schema, _get_schema_element(t_schemas, pos + 1));
    if (!is_repeated(list_node_schema)) {
        return Status::InvalidArgument("Non-repeated nodes in a LIST-annotated group are not supported.");
    }

    int16_t last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();
    if (is_group(list_node_schema)) {
        // Resolve 3-level encoding
        //
        // required/optional group name=whatever {
        //   repeated group name=list {
        //     required/optional TYPE item;
        //   }
        // }
        //
        // yields list<item: TYPE ?nullable> ?nullable
        //
        // We distinguish the special case that we have
        //
        // required/optional group name=whatever {
        //   repeated group name=array or $SOMETHING_tuple {
        //     required/optional TYPE item;
        //   }
        // }
        //
        // In this latter case, the inner type of the list should be a struct
        // rather than a primitive value
        //
        // yields list<item: struct<item: TYPE ?nullable> not null> ?nullable
        if (list_node_schema->num_children == 1 && !has_struct_list_name(list_node_schema->name)) {
            RETURN_IF_ERROR(node_to_field(t_schemas, pos + 2, cur_level_info, child_field, next_pos));
        } else {
            RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos + 1, cur_level_info, child_field, next_pos));
        }
    } else {
        // Two-level list encoding
        //
        // required/optional group LIST {
        //   repeated TYPE;
        // }
        RETURN_IF_ERROR(leaf_to_field(list_node_schema, cur_level_info, false, child_field));
        *next_pos = pos + 2;
    }

    field->name = group_schema->name;
    field->field_id = group_schema->field_id;
    field->type.type = TYPE_ARRAY;
    field->type.children.push_back(field->children[0].type);
    field->is_nullable = is_optional(group_schema);
    field->level_info = cur_level_info;
    field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
    return Status::OK();
}

Status SchemaDescriptor::map_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                      LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    ASSIGN_OR_RETURN(const auto* group_schema, _get_schema_element(t_schemas, pos));
    if (group_schema->num_children != 1) {
        return Status::InvalidArgument("MAP-annotated group must have a single child.");
    }
    if (is_repeated(group_schema)) {
        return Status::InvalidArgument("MAP-annotated group must not be repeated.");
    }

    ASSIGN_OR_RETURN(const auto* key_value_schema, _get_schema_element(t_schemas, pos + 1));

    if (!is_repeated(key_value_schema)) {
        return Status::InvalidArgument("Non-repeated key value in a MAP-annotated group are not supported.");
    }
    if (!is_group(key_value_schema)) {
        return Status::InvalidArgument("Key-value node must be a group.");
    }

    if (key_value_schema->num_children != 1 && key_value_schema->num_children != 2) {
        return Status::InvalidArgument(strings::Substitute(
                "Key-value map node must have 1 or 2 child elements. Found: $0", key_value_schema->num_children));
    }

    // Compatible with trino, comment below checks
    // ASSIGN_OR_RETURN(const auto* key_schema, _get_schema_element(t_schemas, pos + 2));
    // if (!is_required(key_schema)) {
    //   return Status::InvalidArgument("Map keys must be annotated as required.");
    // }

    // SR doesn't support 1 column maps (i.e. Sets).  The options are to either
    // make the values column nullable, or process the map as a list.  We choose the latter
    // as it is simpler.
    if (key_value_schema->num_children == 1) {
        return list_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    }

    _increment(group_schema, &cur_level_info);
    // Increment rep levels for key_value_schema
    int16_t last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();

    field->children.resize(2);
    auto key_field = &field->children[0];
    auto value_field = &field->children[1];

    // required/optional group name=whatever {
    //   repeated group name=key_values{
    //     required TYPE key;
    //     required/optional TYPE value;
    //   }
    // }
    //
    RETURN_IF_ERROR(node_to_field(t_schemas, pos + 2, cur_level_info, key_field, next_pos));
    RETURN_IF_ERROR(node_to_field(t_schemas, pos + 3, cur_level_info, value_field, next_pos));

    field->name = group_schema->name;
    // Actually, we don't need to put field_id here
    field->field_id = group_schema->field_id;
    field->type.type = TYPE_MAP;
    field->type.children.emplace_back(key_field->type);
    field->type.children.emplace_back(value_field->type);
    field->is_nullable = is_optional(group_schema);
    field->level_info = cur_level_info;
    field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
    return Status::OK();
}

Status SchemaDescriptor::group_to_struct_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                               LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    ASSIGN_OR_RETURN(const auto* group_schema, _get_schema_element(t_schemas, pos));
    int32_t num_children = group_schema->num_children;
    field->children.resize(num_children);
    *next_pos = pos + 1;

    // All level increments for the node are expected to happen by callers.
    // This is required because repeated elements need to have their own ParquetField.
    for (size_t i = 0; i < num_children; ++i) {
        RETURN_IF_ERROR(node_to_field(t_schemas, *next_pos, cur_level_info, &field->children[i], next_pos));
    }

    field->name = group_schema->name;
    field->is_nullable = is_optional(group_schema);
    field->level_info = cur_level_info;
    field->type.type = TYPE_STRUCT;
    for (size_t i = 0; i < num_children; i++) {
        field->type.children.emplace_back(field->children[i].type);
    }
    for (size_t i = 0; i < num_children; i++) {
        field->type.field_names.emplace_back(field->children[i].name);
    }
    field->field_id = group_schema->field_id;
    return Status::OK();
}

Status SchemaDescriptor::group_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                        LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    ASSIGN_OR_RETURN(const auto* group_schema, _get_schema_element(t_schemas, pos));
    if (is_list(group_schema)) {
        return list_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    } else if (is_map(group_schema)) {
        return map_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    }

    if (is_repeated(group_schema)) {
        // Simple repeated struct
        //
        // repeated group $NAME {
        //   r/o TYPE[0] f0
        //   r/o TYPE[1] f1
        // }
        field->children.resize(1);

        int16_t last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();
        RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos, cur_level_info, &field->children[0], next_pos));

        field->name = group_schema->name;
        field->type.type = TYPE_ARRAY;
        field->is_nullable = false;
        field->level_info = cur_level_info;
        field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
        return Status::OK();
    } else {
        _increment(group_schema, &cur_level_info);
        return group_to_struct_field(t_schemas, pos, cur_level_info, field, next_pos);
    }
}

Status SchemaDescriptor::node_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                       LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    ASSIGN_OR_RETURN(const auto* node_schema, _get_schema_element(t_schemas, pos));

    if (is_group(node_schema)) {
        // A nested field, but we don't know what kind yet
        return group_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    } else {
        // Either a normal flat primitive type, or a list type encoded with 1-level
        // list encoding. Note that the 3-level encoding is the form recommended by
        // the parquet specification, but technically we can have either
        //
        // required/optional $TYPE $FIELD_NAME
        //
        // or
        //
        // repeated $TYPE $FIELD_NAME
        if (is_repeated(node_schema)) {
            // One-level list encoding, e.g.
            // a: repeated int32;
            // This will generate a required list<element> which element is non-null
            int16_t last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();

            field->children.resize(1);
            auto child = &field->children[0];
            RETURN_IF_ERROR(leaf_to_field(node_schema, cur_level_info, false, child));

            field->name = node_schema->name;
            field->type.type = TYPE_ARRAY;
            field->is_nullable = false;
            field->field_id = node_schema->field_id;
            field->level_info = cur_level_info;
            field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
            *next_pos = pos + 1;
        } else {
            _increment(node_schema, &cur_level_info);
            RETURN_IF_ERROR(leaf_to_field(node_schema, cur_level_info, is_optional(node_schema), field));
            *next_pos = pos + 1;
        }
        return Status::OK();
    }
}

// The schema resolve logic is copied from https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/schema.cc
Status SchemaDescriptor::from_thrift(const std::vector<tparquet::SchemaElement>& t_schemas, bool case_sensitive) {
    if (t_schemas.size() == 0) {
        return Status::InvalidArgument("Empty parquet Schema");
    }
    auto* root_schema = &t_schemas[0];

    // root_schema has no field_id, but it's child will have.
    // Below code used to check this parquet field exist field id.
    if (root_schema->num_children > 0) {
        _exist_field_id = t_schemas[1].__isset.field_id;
    }

    if (!is_group(root_schema)) {
        return Status::InvalidArgument("Root Schema is not group");
    }
    _fields.resize(root_schema->num_children);
    // skip root SchemaElement
    // next_pos is the index in t_schemas, t_schemas is a flatten structure
    size_t next_pos = 1;
    for (size_t i = 0; i < root_schema->num_children; ++i) {
        RETURN_IF_ERROR(node_to_field(t_schemas, next_pos, LevelInfo(), &_fields[i], &next_pos));
        if (!case_sensitive) {
            _fields[i].name = boost::algorithm::to_lower_copy(_fields[i].name);
        }
        if (_formatted_column_name_2_field_idx.find(_fields[i].name) != _formatted_column_name_2_field_idx.end()) {
            return Status::InvalidArgument(strings::Substitute("Duplicate field name: $0", _fields[i].name));
        }

        _formatted_column_name_2_field_idx.emplace(_fields[i].name, i);
        _field_id_2_field_idx.emplace(_fields[i].field_id, i);
    }
    _case_sensitive = case_sensitive;

    if (next_pos != t_schemas.size()) {
        return Status::InvalidArgument(strings::Substitute("Remaining $0 unparsed field", t_schemas.size() - next_pos));
    }

    return Status::OK();
}

std::string SchemaDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "fields=[";
    for (int i = 0; i < _fields.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << _fields[i].debug_string();
    }
    ss << "]";
    return ss.str();
}

const int32_t SchemaDescriptor::get_field_idx_by_column_name(const std::string& column_name) const {
    const auto& format_name = _case_sensitive ? column_name : boost::algorithm::to_lower_copy(column_name);
    auto it = _formatted_column_name_2_field_idx.find(format_name);
    if (it == _formatted_column_name_2_field_idx.end()) return -1;
    return it->second;
}

const int32_t SchemaDescriptor::get_field_idx_by_field_id(int32_t field_id) const {
    auto it = _field_id_2_field_idx.find(field_id);
    if (it == _field_id_2_field_idx.end()) {
        return -1;
    }
    return it->second;
}

const ParquetField* SchemaDescriptor::get_stored_column_by_field_id(int32_t field_id) const {
    int32_t idx = get_field_idx_by_field_id(field_id);
    if (idx == -1) return nullptr;
    return &_fields[idx];
}

const ParquetField* SchemaDescriptor::get_stored_column_by_column_name(const std::string& column_name) const {
    int idx = get_field_idx_by_column_name(column_name);
    if (idx == -1) return nullptr;
    return &(_fields[idx]);
}

// Increments levels according to the cardinality of node.
void SchemaDescriptor::_increment(const tparquet::SchemaElement* t_schema, LevelInfo* level_info) {
    if (is_repeated(t_schema)) {
        level_info->increment_repeated();
        return;
    }
    if (is_optional(t_schema)) {
        level_info->increment_optional();
        return;
    }
}
} // namespace starrocks::parquet
