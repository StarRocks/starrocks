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

#include <string>
#include <unordered_set>
#include <vector>

#include "exec/hdfs_scanner.h"
#include "formats/parquet/group_reader.h"
#include "gen_cpp/Descriptors_types.h"
#include "metadata.h"
#include "runtime/descriptors.h"

namespace starrocks::parquet {

class MetaHelper {
public:
    virtual ~MetaHelper() = default;
    virtual void set_existed_column_names(std::unordered_set<std::string>* names) const = 0;

    virtual void build_column_name_2_pos_in_meta(std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
                                                 const tparquet::RowGroup& row_group,
                                                 const std::vector<SlotDescriptor*>& slots) const = 0;

    virtual void prepare_read_columns(
            const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
            const std::unordered_map<std::string, ColumnAccessPathPtr>* column_access_path_mapping,
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
    GroupReaderParam::Column _build_column(int32_t field_idx_in_parquet, int32_t col_idx_in_chunk,
                                           const tparquet::Type::type& col_type_in_parquet,
                                           const TypeDescriptor& col_type_in_chunk, const SlotId& slot_id,
                                           bool decode_needed, const ColumnAccessPathPtr* column_access_path,
                                           const TIcebergSchemaField* t_iceberg_schema_field = nullptr) const {
        GroupReaderParam::Column column{};
        column.field_idx_in_parquet = field_idx_in_parquet;
        column.col_idx_in_chunk = col_idx_in_chunk;
        column.col_type_in_parquet = col_type_in_parquet;
        column.col_type_in_chunk = col_type_in_chunk;
        column.slot_id = slot_id;
        column.column_access_path = column_access_path;
        column.t_iceberg_schema_field = t_iceberg_schema_field;
        column.decode_needed = decode_needed;
        return column;
    }

    bool _case_sensitive = false;
    std::shared_ptr<FileMetaData> _file_metadata;
};

class ParquetMetaHelper : public MetaHelper {
public:
    ParquetMetaHelper(std::shared_ptr<FileMetaData> file_metadata, bool case_sensitive) {
        _file_metadata = std::move(file_metadata);
        _case_sensitive = case_sensitive;
    }
    ~ParquetMetaHelper() override = default;

    void set_existed_column_names(std::unordered_set<std::string>* names) const override;
    void build_column_name_2_pos_in_meta(std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta,
                                         const tparquet::RowGroup& row_group,
                                         const std::vector<SlotDescriptor*>& slots) const override;
    void prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                              const std::unordered_map<std::string, ColumnAccessPathPtr>* column_access_path_mapping,
                              std::vector<GroupReaderParam::Column>& read_cols) const override;

    const ParquetField* get_parquet_field(const std::string& col_name) const override;
};

class IcebergMetaHelper : public MetaHelper {
public:
    IcebergMetaHelper(std::shared_ptr<FileMetaData> file_metadata, bool case_sensitive,
                      const TIcebergSchema* t_iceberg_schema) {
        _file_metadata = std::move(file_metadata);
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
                              const std::unordered_map<std::string, ColumnAccessPathPtr>* column_access_path_mapping,
                              std::vector<GroupReaderParam::Column>& read_cols) const override;
    const ParquetField* get_parquet_field(const std::string& col_name) const override;

private:
    void _init_field_mapping();
    const TIcebergSchema* _t_iceberg_schema = nullptr;
    // field name has already been formatted
    std::unordered_map<std::string, const TIcebergSchemaField*> _field_name_2_iceberg_field;
};

} // namespace starrocks::parquet