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
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
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
    virtual ~MetaHelper() = default;
    virtual void set_existed_column_names(std::unordered_set<std::string>* names) const = 0;

    virtual void build_column_name_2_pos_in_meta(std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
                                                 const tparquet::RowGroup& row_group,
                                                 const std::vector<SlotDescriptor*>& slots) const = 0;

    virtual void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                      std::vector<GroupReaderParam::Column>& read_cols) const = 0;

    virtual const ParquetField* get_parquet_field(const std::string& col_name) const = 0;

    const tparquet::ColumnMetaData* get_column_meta(
            const std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
            const tparquet::RowGroup& row_group, const std::string& col_name) const {
        auto it = column_name_2_pos_in_meta.find(col_name);
        if (it == column_name_2_pos_in_meta.end()) {
            return nullptr;
        }

        if (it->second >= row_group.columns.size()) {
            DCHECK(false) << "Impossible here";
            return nullptr;
        }

        /**
        * Metadata for each column chunk in row group must have the same order as the SchemaElement list in FileMetaData.
        **/
        return &row_group.columns[it->second].meta_data;
    }

protected:
    GroupReaderParam::Column _build_column(int32_t idx_in_parquet, const tparquet::Type::type& type_in_parquet,
                                           SlotDescriptor* slot_desc, bool decode_needed,
                                           const TIcebergSchemaField* t_iceberg_schema_field = nullptr) const {
        GroupReaderParam::Column column{};
        column.idx_in_parquet = idx_in_parquet;
        column.type_in_parquet = type_in_parquet;
        column.slot_desc = slot_desc;
        column.t_iceberg_schema_field = t_iceberg_schema_field;
        column.decode_needed = decode_needed;
        return column;
    }

    bool _case_sensitive = false;
    FileMetaData* _file_metadata;
};

class ParquetMetaHelper : public MetaHelper {
public:
    ParquetMetaHelper(FileMetaData* file_metadata, bool case_sensitive) {
        _file_metadata = file_metadata;
        _case_sensitive = case_sensitive;
    }
    ~ParquetMetaHelper() override = default;

    void set_existed_column_names(std::unordered_set<std::string>* names) const override;
    void build_column_name_2_pos_in_meta(std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
                                         const tparquet::RowGroup& row_group,
                                         const std::vector<SlotDescriptor*>& slots) const override;
    void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                              std::vector<GroupReaderParam::Column>& read_cols) const override;

    const ParquetField* get_parquet_field(const std::string& col_name) const override;
};

class IcebergMetaHelper : public MetaHelper {
public:
    IcebergMetaHelper(FileMetaData* file_metadata, bool case_sensitive, const TIcebergSchema* t_iceberg_schema) {
        _file_metadata = file_metadata;
        _case_sensitive = case_sensitive;
        _t_iceberg_schema = t_iceberg_schema;
        DCHECK(_t_iceberg_schema != nullptr);
        _init_field_mapping();
    }

    ~IcebergMetaHelper() override = default;

    void set_existed_column_names(std::unordered_set<std::string>* names) const override;
    void build_column_name_2_pos_in_meta(std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
                                         const tparquet::RowGroup& row_group,
                                         const std::vector<SlotDescriptor*>& slots) const override;
    void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                              std::vector<GroupReaderParam::Column>& read_cols) const override;
    const ParquetField* get_parquet_field(const std::string& col_name) const override;

private:
    void _init_field_mapping();
    const TIcebergSchema* _t_iceberg_schema = nullptr;
    // field name has already been formatted
    std::unordered_map<std::string, const TIcebergSchemaField*> _field_name_2_iceberg_field;
};

} // namespace starrocks::parquet
