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

static bool is_group(const tparquet::SchemaElement& schema) {
    return schema.num_children > 0;
}

static bool is_list(const tparquet::SchemaElement& schema) {
    return schema.__isset.converted_type && schema.converted_type == tparquet::ConvertedType::LIST;
}

static bool is_map(const tparquet::SchemaElement& schema) {
    return schema.__isset.converted_type && (schema.converted_type == tparquet::ConvertedType::MAP ||
                                             schema.converted_type == tparquet::ConvertedType::MAP_KEY_VALUE);
}

static bool is_repeated(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type && schema.repetition_type == tparquet::FieldRepetitionType::REPEATED;
}

static bool is_required(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type && schema.repetition_type == tparquet::FieldRepetitionType::REQUIRED;
}

static bool schema_is_optional(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type && schema.repetition_type == tparquet::FieldRepetitionType::OPTIONAL;
}

static int schema_num_children(const tparquet::SchemaElement& schema) {
    return schema.__isset.num_children ? schema.num_children : 0;
}

void SchemaDescriptor::leaf_to_field(const tparquet::SchemaElement& t_schema, const LevelInfo& cur_level_info,
                                     bool is_nullable, ParquetField* field) {
    field->name = t_schema.name;
    field->schema_element = t_schema;
    field->is_nullable = is_nullable;
    field->physical_type = t_schema.type;
    field->type_length = t_schema.type_length;
    field->scale = t_schema.scale;
    field->precision = t_schema.precision;
    field->field_id = t_schema.field_id;
    field->level_info = cur_level_info;

    _physical_fields.push_back(field);
    field->physical_column_index = _physical_fields.size() - 1;
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
    // List in parquet will be represented as a 3-level schema, for example
    //   <list-repetition> group <name> (LIST) {
    //     repeated group list {
    //       <element-repetition> <element-type> element;
    //     }
    //   }
    // Following is two example
    // List<String> (list non-null, elements nullable)
    //   required group my_list (LIST) {
    //     repeated group list {
    //       optional binary element (UTF8);
    //     }
    //   }
    //
    // List<String> (list nullable, elements non-null)
    //   optional group my_list (LIST) {
    //     repeated group list {
    //       required binary element (UTF8);
    //     }
    //   }
    auto& level1_schema = t_schemas[pos];
    if (level1_schema.repetition_type == tparquet::FieldRepetitionType::REPEATED) {
        return Status::InvalidArgument("LIST-annotated group must be not repeated");
    }
    if (level1_schema.num_children != 1) {
        return Status::InvalidArgument("LIST-annotated group must have only one child");
    }

    if (pos + 1 >= t_schemas.size()) {
        return Status::InvalidArgument("SchemaElement is not enough to parse");
    }

    // list schema
    auto& level2_schema = t_schemas[pos + 1];
    if (level2_schema.repetition_type != tparquet::FieldRepetitionType::REPEATED) {
        return Status::InvalidArgument("LIST-annotated list should be repeated");
    }

    // This indicate if this list is nullable.
    bool is_optional = schema_is_optional(level1_schema);
    if (is_optional) {
        cur_level_info.max_def_level++;
    }

    auto last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();
    field->children.resize(1);
    ParquetField* element = &field->children[0];

    size_t num_children = schema_num_children(level2_schema);
    if (num_children > 0) {
        if (num_children == 1 && !has_struct_list_name(level2_schema.name)) {
            // this is 3-level case, it will generate a list<element>
            RETURN_IF_ERROR(node_to_field(t_schemas, pos + 2, cur_level_info, element, next_pos));
        } else {
            // Have a required group field with chidren, this will generate a group element
            // list<group<child>>
            // the level2_schema may have converted_type = "MAP", in this scenario we should parse
            // level2_schema as map, otherwise when we create column reader we will get wrong type.
            // optional group col_array_map (LIST) {
            //     repeated group array (MAP) {
            //         repeated group map (MAP_KEY_VALUE) {
            //             required binary key (UTF8);
            //             optional int32 value;
            //         }
            //     }
            // }
            if (is_map(level2_schema)) {
                RETURN_IF_ERROR(map_to_field(t_schemas, pos + 1, cur_level_info, element, next_pos));
            } else {
                RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos + 1, cur_level_info, element, next_pos));
            }
        }
    } else if (num_children == 0) {
        // This is backward-compatibility two-level
        // List<Integer> (nullable list, non-null elements)
        //   optional group my_list (LIST) {
        //     repeated int32 element;
        //   }
        // It will generate a list<int>
        leaf_to_field(level2_schema, cur_level_info, false, element);
        *next_pos = pos + 2;
    }

    field->name = level1_schema.name;
    field->field_id = level1_schema.field_id;
    field->type.type = TYPE_ARRAY;
    field->type.children.push_back(field->children[0].type);
    field->is_nullable = is_optional;
    field->level_info = cur_level_info;
    field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
    return Status::OK();
}

Status SchemaDescriptor::map_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                      LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    // <map-repetition> group <name> (MAP) {
    //   repeated group key_value {
    //     required <key-type> key;
    //     <value-repetition> <value-type> value;
    //   }
    // }
    if (pos + 2 >= t_schemas.size()) {
        return Status::InvalidArgument("SchemaElement is not enough to parse");
    }
    auto& map_schema = t_schemas[pos];
    if (map_schema.num_children != 1) {
        return Status::InvalidArgument("MAP-annotated group must have a single child.");
    }
    // when map as array's element type, there is a scenario, the array's level_2_schema is converted_type = 'MAP'
    // we should parse this as map, and in this scenario the level_2_schema is repeated.
    // if (is_repeated(map_schema)) {
    //     return Status::InvalidArgument("MAP-annotated group must not be repeated.");
    // }
    auto& kv_schema = t_schemas[pos + 1];
    if (!is_group(kv_schema) || !is_repeated(kv_schema)) {
        return Status::InvalidArgument("key_value in map group must be a repeated group");
    }
    auto& key_schema = t_schemas[pos + 2];
    // when key type is char or varchar in hive, not string
    // the real type is BYTE_ARRAY which is OPTIONAL
    // when key type is decimal, the real type is FIXED_LEN_BYTE_ARRAY which is OPTIONAL
    if ((!is_required(key_schema)) && (key_schema.type != tparquet::Type::type::BYTE_ARRAY) &&
        (key_schema.type != tparquet::Type::type::FIXED_LEN_BYTE_ARRAY)) {
        return Status::InvalidArgument("key in map group must be required");
    }

    if (kv_schema.num_children == 1) {
        // This is a set, we see them as a list
        return list_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    } else if (kv_schema.num_children != 2) {
        return Status::InvalidArgument("number of element in map is not 2");
    }

    bool is_optional = schema_is_optional(map_schema);
    if (is_optional) {
        cur_level_info.max_def_level++;
    }
    auto last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();

    // we will generate a field like map<struct<key, value>>
    field->children.resize(1);
    auto kv_field = &field->children[0];
    RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos + 1, cur_level_info, kv_field, next_pos));

    field->name = map_schema.name;
    // Actually, we don't need put field_id here
    field->field_id = map_schema.field_id;
    field->type.type = TYPE_MAP;
    field->is_nullable = is_optional;
    field->level_info = cur_level_info;
    field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;

    return Status::OK();
}

Status SchemaDescriptor::group_to_struct_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                               LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    auto& group_schema = t_schemas[pos];
    bool is_optional = schema_is_optional(group_schema);
    if (is_optional) {
        cur_level_info.max_def_level++;
    }
    auto num_children = group_schema.num_children;
    field->children.resize(num_children);
    *next_pos = pos + 1;
    for (int i = 0; i < num_children; ++i) {
        RETURN_IF_ERROR(node_to_field(t_schemas, *next_pos, cur_level_info, &field->children[i], next_pos));
    }
    field->name = group_schema.name;
    field->is_nullable = is_optional;
    field->level_info = cur_level_info;
    field->type.type = TYPE_STRUCT;
    field->field_id = group_schema.field_id;
    return Status::OK();
}

Status SchemaDescriptor::group_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                        LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    auto& group_schema = t_schemas[pos];
    if (is_list(group_schema)) {
        return list_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    }
    if (is_map(group_schema)) {
        return map_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    }

    if (is_repeated(group_schema)) {
        // For the following struct definition
        // repeated group [] {
        //   optional/required int a;
        //   optional/required int b;
        // }
        // this will generate a non-null list<struct<int, int>>

        auto last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();
        field->children.resize(1);
        auto struct_field = &field->children[0];
        RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos, cur_level_info, struct_field, next_pos));

        field->name = group_schema.name;
        field->type.type = TYPE_ARRAY;
        field->is_nullable = false;
        field->level_info = cur_level_info;
        field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;
    } else {
        RETURN_IF_ERROR(group_to_struct_field(t_schemas, pos, cur_level_info, field, next_pos));
    }

    return Status::OK();
}

Status SchemaDescriptor::node_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                       LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    if (pos >= t_schemas.size()) {
        return Status::InvalidArgument("Access out-of-bounds SchemaElement");
    }
    auto& t_schema = t_schemas[pos];
    if (is_group(t_schema)) {
        return group_to_field(t_schemas, pos, cur_level_info, field, next_pos);
    }
    if (is_repeated(t_schema)) {
        // repeated int
        // This will generate a required list<element> which element is non-null
        auto last_immediate_repeated_ancestor_def_level = cur_level_info.increment_repeated();

        field->children.resize(1);
        auto child = &field->children[0];
        leaf_to_field(t_schema, cur_level_info, false, child);

        field->name = t_schema.name;
        field->type.type = TYPE_ARRAY;
        field->is_nullable = false;
        field->field_id = t_schema.field_id;
        field->level_info = cur_level_info;
        field->level_info.immediate_repeated_ancestor_def_level = last_immediate_repeated_ancestor_def_level;

        *next_pos = pos + 1;
    } else {
        // required int
        // or
        // optional int
        bool is_optional = schema_is_optional(t_schema);
        if (is_optional) {
            cur_level_info.max_def_level++;
        }
        leaf_to_field(t_schema, cur_level_info, is_optional, field);
        *next_pos = pos + 1;
    }
    return Status::OK();
}

Status SchemaDescriptor::from_thrift(const std::vector<tparquet::SchemaElement>& t_schemas, bool case_sensitive) {
    if (t_schemas.size() == 0) {
        return Status::InvalidArgument("Empty parquet Schema");
    }
    auto& root_schema = t_schemas[0];

    // root_schema has no field_id, but it's child will have.
    // Below code used to check this parquet field exist field id.
    if (root_schema.num_children > 0) {
        _exist_field_id = t_schemas[1].__isset.field_id;
    }

    if (!is_group(root_schema)) {
        return Status::InvalidArgument("Root Schema is not group");
    }
    _fields.resize(root_schema.num_children);
    // skip root SchemaElement
    size_t next_pos = 1;
    for (size_t i = 0; i < root_schema.num_children; ++i) {
        RETURN_IF_ERROR(node_to_field(t_schemas, next_pos, LevelInfo(), &_fields[i], &next_pos));
        if (!case_sensitive) {
            _fields[i].name = boost::algorithm::to_lower_copy(_fields[i].name);
        }
        if (_formatted_column_name_2_field_pos.find(_fields[i].name) != _formatted_column_name_2_field_pos.end()) {
            return Status::InvalidArgument(strings::Substitute("Duplicate field name: $0", _fields[i].name));
        }
        _formatted_column_name_2_field_pos.emplace(_fields[i].name, i);
        _field_id_2_field_pos.emplace(_fields[i].field_id, i);
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

const int32_t SchemaDescriptor::get_field_pos_by_column_name(const std::string& column_name) const {
    const auto& format_name = _case_sensitive ? column_name : boost::algorithm::to_lower_copy(column_name);
    auto it = _formatted_column_name_2_field_pos.find(format_name);
    if (it == _formatted_column_name_2_field_pos.end()) return -1;
    return it->second;
}

const int32_t SchemaDescriptor::get_field_pos_by_field_id(int32_t field_id) const {
    auto it = _field_id_2_field_pos.find(field_id);
    if (it == _field_id_2_field_pos.end()) {
        return -1;
    }
    return it->second;
}

const ParquetField* SchemaDescriptor::get_stored_column_by_field_id(int32_t field_id) const {
    int32_t idx = get_field_pos_by_field_id(field_id);
    if (idx == -1) return nullptr;
    return &_fields[idx];
}

const ParquetField* SchemaDescriptor::get_stored_column_by_column_name(const std::string& column_name) const {
    int idx = get_field_pos_by_column_name(column_name);
    if (idx == -1) return nullptr;
    return &(_fields[idx]);
}
} // namespace starrocks::parquet
