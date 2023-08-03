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
#include "gen_cpp/parquet_types.h"
#include "runtime/types.h"

namespace starrocks::parquet {

struct LevelInfo {
    int16_t max_def_level = 0;
    int16_t max_rep_level = 0;

    int16_t immediate_repeated_ancestor_def_level = 0;

    bool is_nullable() const { return max_def_level > immediate_repeated_ancestor_def_level; }

    int16_t increment_repeated() {
        auto origin_ancestor_rep_levels = immediate_repeated_ancestor_def_level;
        max_def_level++;
        max_rep_level++;
        immediate_repeated_ancestor_def_level = max_def_level;
        return origin_ancestor_rep_levels;
    }

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
    const size_t get_fields_size() const { return _fields.size(); }
    const bool contain_field_id(int32_t field_id) const {
        return _field_id_2_field_idx.find(field_id) != _field_id_2_field_idx.end();
    }

    size_t estimate_memory() const {
        // This is only a rough approximate statistics, we try to calculate the maximum possible memory usage.
        size_t static_object_size = sizeof(*this);
        size_t fields_elem_size = _fields.size() * sizeof(ParquetField);
        size_t physical_fields_elem_size = _physical_fields.size() * 8;
        // 15 represents an average column name length.
        size_t name2field_elem_size = _formatted_column_name_2_field_idx.size() * (15 + sizeof(size_t));
        size_t id2field_elem_size = _field_id_2_field_idx.size() * (sizeof(int32_t) + sizeof(size_t));
        // We multiply 2 because some container reserve much element space than the elment size.
        // `_fields` is directly resized to the element size, so we skip considering the redundant space. 
        size_t elem_data_size = fields_elem_size +
                                (physical_fields_elem_size + name2field_elem_size + id2field_elem_size) * 2;
        return static_object_size + elem_data_size;
    }

private:
    void leaf_to_field(const tparquet::SchemaElement& t_schema, const LevelInfo& cur_level_info, bool is_nullable,
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
