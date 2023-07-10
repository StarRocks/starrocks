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

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/types.h"

namespace starrocks::parquet {

struct LevelInfo {
    // The definition level at which the value for the field
    // is considered not null (definition levels greater than
    // or equal to this value indicate a not-null
    // value for the field). For list fields definition levels
    // greater than or equal to this field indicate a present,
    // possibly null, child value.
    int16_t max_def_level = 0;

    // The repetition level corresponding to this element
    // or the closest repeated ancestor. Any repetition
    // level less than this indicates either a new list OR
    // an empty list (which is determined in conjunction
    // with definition levels).
    int16_t max_rep_level = 0;

    // The definition level indicating the level at which the closest
    // repeated ancestor is not empty. This is used to discriminate
    // between a value less than |def_level| being null or excluded entirely.
    // For instance if we have an arrow schema like:
    // list(struct(f0: int)).  Then then there are the following
    // definition levels:
    //   0 = null list
    //   1 = present but empty list.
    //   2 = a null value in the list
    //   3 = a non null struct but null integer.
    //   4 = a present integer.
    // When reconstructing, the struct and integer arrays'
    // repeated_ancestor_def_level would be 2.  Any
    // def_level < 2 indicates that there isn't a corresponding
    // child value in the list.
    // i.e. [null, [], [null], [{f0: null}], [{f0: 1}]]
    // has the def levels [0, 1, 2, 3, 4].  The actual
    // struct array is only of length 3: [not-set, set, set] and
    // the int array is also of length 3: [N/A, null, 1].
    //
    int16_t immediate_repeated_ancestor_def_level = 0;

    int16_t increment_repeated() {
        auto origin_ancestor_rep_levels = immediate_repeated_ancestor_def_level;

        // Repeated fields add both a repetition and definition level. This is used
        // to distinguish between an empty list and a list with an item in it.
        max_def_level++;
        max_rep_level++;

        // For levels >= immediate_repeated_ancestor_def_level it indicates the list was
        // non-null and had at least one element. This is important
        // for later decoding because we need to add a slot for these
        // values. For levels < current_def_level no slots are added
        // to arrays.
        immediate_repeated_ancestor_def_level = max_def_level;
        return origin_ancestor_rep_levels;
    }

    void increment_optional() { max_def_level++; }

    std::string debug_string() const;
};

struct ParquetField {
    std::string name;
    tparquet::SchemaElement schema_element;

    // Used to identify if this field is a nested field.
    TypeDescriptor type;
    bool is_nullable;

    // Only valid when this field is a leaf node
    tparquet::Type::type physical_type;
    // If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the vales.
    int32_t type_length;

    // Used when this column contains decimal data.
    int32_t scale;
    int32_t precision;

    // used to get ColumnChunk in parquet file's metadata
    int physical_column_index;

    // Unique parquet field id
    int32_t field_id;

    LevelInfo level_info;
    std::vector<ParquetField> children;

    int16_t max_def_level() const { return level_info.max_def_level; }
    int16_t max_rep_level() const { return level_info.max_rep_level; }
    std::string debug_string() const;
};

class SchemaDescriptor {
public:
    SchemaDescriptor() = default;
    ~SchemaDescriptor() = default;

    Status from_thrift(const std::vector<tparquet::SchemaElement>& t_schemas, bool case_sensitive);

    std::string debug_string() const;

    const bool exist_filed_id() const { return _exist_field_id; }
    const int32_t get_field_idx_by_column_name(const std::string& column_name) const;
    const int32_t get_field_idx_by_field_id(int32_t field_id) const;
    const ParquetField* get_stored_column_by_field_idx(size_t field_idx) const { return &_fields[field_idx]; }
    const ParquetField* get_stored_column_by_field_id(int32_t field_id) const;
    const ParquetField* get_stored_column_by_column_name(const std::string& column_name) const;
    const std::vector<ParquetField>& get_parquet_fields() const { return _fields; }
    const size_t get_fields_size() const { return _fields.size(); }
    const bool contain_field_id(int32_t field_id) const {
        return _field_id_2_field_idx.find(field_id) != _field_id_2_field_idx.end();
    }

private:
    Status leaf_to_field(const tparquet::SchemaElement* t_schema, const LevelInfo& cur_level_info, bool is_nullable,
                         ParquetField* field);

    Status list_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos, LevelInfo cur_level_info,
                         ParquetField* field, size_t* next_pos);

    Status map_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos, LevelInfo cur_level_info,
                        ParquetField* field, size_t* next_pos);

    Status group_to_struct_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos,
                                 LevelInfo cur_level_info, ParquetField* field, size_t* next_pos);

    Status group_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos, LevelInfo cur_level_info,
                          ParquetField* field, size_t* next_pos);

    Status node_to_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t pos, LevelInfo cur_level_info,
                         ParquetField* field, size_t* next_pos);

    StatusOr<const tparquet::SchemaElement*> _get_schema_element(const std::vector<tparquet::SchemaElement>& t_schemas,
                                                                 size_t pos) {
        if (pos >= t_schemas.size()) {
            return Status::InvalidArgument("Access out-of-bounds SchemaElement");
        }
        return &t_schemas[pos];
    }

    void _increment(const tparquet::SchemaElement* t_schema, LevelInfo* level_info);

    std::vector<ParquetField> _fields;
    std::vector<ParquetField*> _physical_fields;
    // Parquet filed name(formatted with case-sensitive) mapping to field position in schema.
    std::unordered_map<std::string, size_t> _formatted_column_name_2_field_idx;
    // Parquet unique field id mapping to field position in schema.
    std::unordered_map<int32_t, size_t> _field_id_2_field_idx;

    // Not every parquet file contains field id, if field id not existed, we should not read it with iceberg schema.
    bool _exist_field_id = false;
    bool _case_sensitive = false;
};

} // namespace starrocks::parquet
