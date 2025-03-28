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

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "exec/hdfs_scanner.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/metadata.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/descriptors.h"

namespace starrocks {
class SlotDescriptor;
class TIcebergSchema;
class TIcebergSchemaField;

namespace parquet {
class FileMetaData;
struct ParquetField;
} // namespace parquet
} // namespace starrocks

namespace starrocks::parquet {

class MetaHelper {
public:
    MetaHelper(FileMetaData* file_metadata, bool case_sensitive)
            : _file_metadata(file_metadata), _case_sensitive(case_sensitive) {}
    virtual ~MetaHelper() = default;

    virtual void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                      std::vector<GroupReaderParam::Column>& read_cols,
                                      std::unordered_set<std::string>& existed_column_names) const = 0;

protected:
    GroupReaderParam::Column _build_column(int32_t idx_in_parquet, const tparquet::Type::type& type_in_parquet,
                                           SlotDescriptor* slot_desc, bool decode_needed,
                                           const TIcebergSchemaField* t_lake_schema_field = nullptr) const {
        GroupReaderParam::Column column{};
        column.idx_in_parquet = idx_in_parquet;
        column.type_in_parquet = type_in_parquet;
        column.slot_desc = slot_desc;
        column.t_lake_schema_field = t_lake_schema_field;
        column.decode_needed = decode_needed;
        return column;
    }

    FileMetaData* _file_metadata = nullptr;
    bool _case_sensitive = false;
};

class ParquetMetaHelper : public MetaHelper {
public:
    ParquetMetaHelper(FileMetaData* file_metadata, bool case_sensitive) : MetaHelper(file_metadata, case_sensitive) {}
    ~ParquetMetaHelper() override = default;

    void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                              std::vector<GroupReaderParam::Column>& read_cols,
                              std::unordered_set<std::string>& existed_column_names) const override;

private:
    bool _is_valid_type(const ParquetField* parquet_field, const TypeDescriptor* type_descriptor) const;
};

class LakeMetaHelper : public MetaHelper {
public:
    LakeMetaHelper(FileMetaData* file_metadata, bool case_sensitive, const TIcebergSchema* t_lake_schema)
            : MetaHelper(file_metadata, case_sensitive) {
        _lake_schema = t_lake_schema;
        DCHECK(_lake_schema != nullptr);
        _init_field_mapping();
    }

    ~LakeMetaHelper() override = default;

    void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                              std::vector<GroupReaderParam::Column>& read_cols,
                              std::unordered_set<std::string>& existed_column_names) const override;

private:
    void _init_field_mapping();
    bool _is_valid_type(const ParquetField* parquet_field, const TIcebergSchemaField* field_schema,
                        const TypeDescriptor* type_descriptor) const;
    const TIcebergSchema* _lake_schema = nullptr;
    // field name has already been formatted
    std::unordered_map<std::string, const TIcebergSchemaField*> _field_name_2_lake_field;
};

} // namespace starrocks::parquet
