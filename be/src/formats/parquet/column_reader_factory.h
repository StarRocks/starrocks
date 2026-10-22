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
#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

class ColumnReaderFactory {
public:
    // create a column reader
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type);

    // Create a column reader with iceberg schema
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type,
                                            const TIcebergSchemaField* lake_schema_field);

    static StatusOr<ColumnReaderPtr> create(ColumnReaderPtr raw_reader, const GlobalDictMap* dict, const SlotId slot_id,
                                            int64_t num_rows);

private:
    // for struct type without schema change
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, std::vector<int32_t>& pos);

<<<<<<< HEAD
    // for schema changed
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, const TIcebergSchemaField* lake_schema_field,
                                                  std::vector<int32_t>& pos,
                                                  std::vector<const TIcebergSchemaField*>& lake_schema_subfield);

    static bool _has_valid_subfield_column_reader(
            const std::map<std::string, std::unique_ptr<ColumnReader>>& children_readers);
=======
    // Resolve which parquet child each child of the struct column type `col_type` reads. The single matching rule
    // used by the reader factory and the meta helpers:
    // - Iceberg path (lake_schema_field != nullptr): the Iceberg child by table field name; then the parquet child
    //   by its Iceberg field id if the file has field ids, else by physical name (if any) or table field name.
    // - Otherwise: by field id if the type carries field ids, else by physical name, else by field name. An entry
    //   without an id (-1) or physical name ("") falls back to the field name.
    // Names are compared after Utils::format_name(case_sensitive).
    static StructSubfieldMapping resolve_struct_subfields(const ParquetField& field, const TypeDescriptor& col_type,
                                                          bool case_sensitive,
                                                          const TIcebergSchemaField* lake_schema_field,
                                                          bool parquet_has_field_id);

    // For an Iceberg struct none of whose used subfields is in the file: a stored scalar column read only for the
    // struct's levels (a direct subfield, else a leaf under a struct subfield), or nullptr if there is none. The
    // reader factory and LakeMetaHelper use the same choice.
    static const ParquetField* find_struct_level_source(const ParquetField& field);
>>>>>>> 209630d ([BugFix] Fix Iceberg read follow-ups: unknown transforms in SHOW CREATE, TIME/TIMESTAMPTZ defaults, missing struct subfields, filters on default columns (#81311))
};

} // namespace starrocks::parquet