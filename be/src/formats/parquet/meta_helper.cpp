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

#include "meta_helper.h"

#include <boost/algorithm/string.hpp>

namespace starrocks::parquet {

void ParquetMetaHelper::set_existed_column_names(std::unordered_set<std::string>* names) const {
    names->clear();
    for (size_t i = 0; i < _file_metadata->schema().get_fields_size(); i++) {
        names->emplace(_file_metadata->schema().get_stored_column_by_idx(i)->name);
    }
}

void ParquetMetaHelper::build_column_name_2_pos_in_meta(
        std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta, const tparquet::RowGroup& row_group,
        const std::vector<SlotDescriptor*>& slots) const {
    for (const auto& slot : slots) {
        const std::string format_slot_name =
                _case_sensitive ? slot->col_name() : boost::algorithm::to_lower_copy(slot->col_name());
        for (size_t idx = 0; idx < row_group.columns.size(); idx++) {
            const auto& column = row_group.columns[idx];
            // TODO Not support for non-scalar types now.
            const std::string format_column_name =
                    _case_sensitive ? column.meta_data.path_in_schema[0]
                                    : boost::algorithm::to_lower_copy(column.meta_data.path_in_schema[0]);
            if (format_column_name == format_slot_name) {
                // Put SlotDesc's origin column name here!
                column_name_2_pos_in_meta.emplace(slot->col_name(), idx);
                break;
            }
        }
    }
}

void ParquetMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             bool& _is_only_partition_scan) const {
    for (auto& materialized_column : materialized_columns) {
        int32_t field_index = _file_metadata->schema().get_field_pos_by_column_name(materialized_column.col_name);

        if (field_index < 0) continue;

        auto parquet_type = _file_metadata->schema().get_stored_column_by_idx(field_index)->physical_type;
        GroupReaderParam::Column column = _build_column(field_index, materialized_column.col_idx, parquet_type,
                                                        materialized_column.col_type, materialized_column.slot_id);
        read_cols.emplace_back(column);
    }
    _is_only_partition_scan = read_cols.empty();
}

void IcebergMetaHelper::set_existed_column_names(std::unordered_set<std::string>* names) const {
    names->clear();
    // build column name set from iceberg schema
    const auto& fields = _t_iceberg_schema->fields;
    for (const auto& field : fields) {
        if (_file_metadata->schema().contain_field_id(field.field_id)) {
            names->emplace(field.name);
        }
    }
}

void IcebergMetaHelper::build_column_name_2_pos_in_meta(
        std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta, const tparquet::RowGroup& row_group,
        const std::vector<SlotDescriptor*>& slots) const {
    // Key is field name, value is unique iceberg field id.
    std::unordered_map<std::string, int32_t> column_name_2_field_id{};
    for (auto each : _t_iceberg_schema->fields) {
        const std::string& format_field_name = _case_sensitive ? each.name : boost::algorithm::to_lower_copy(each.name);
        column_name_2_field_id.emplace(format_field_name, each.field_id);
    }

    for (const auto& slot : slots) {
        const std::string& format_column_name =
                _case_sensitive ? slot->col_name() : boost::algorithm::to_lower_copy(slot->col_name());
        auto it = column_name_2_field_id.find(format_column_name);
        if (it == column_name_2_field_id.end()) {
            continue;
        }
        // Put SlotDescriptor's origin column name here!
        column_name_2_pos_in_meta.emplace(slot->col_name(),
                                          _file_metadata->schema().get_field_pos_by_field_id(it->second));
    }
}

void IcebergMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             bool& _is_only_partition_scan) const {
    // Build iceberg schema mapping if iceberg_schema existed
    std::unordered_map<std::string, const TIcebergSchemaField*> iceberg_schema_name_2_field{};
    for (const auto& field : _t_iceberg_schema->fields) {
        const std::string& format_field_name =
                _case_sensitive ? field.name : boost::algorithm::to_lower_copy(field.name);
        iceberg_schema_name_2_field.emplace(format_field_name, &field);
    }

    for (auto& materialized_column : materialized_columns) {
        const std::string& format_col_name = _case_sensitive
                                                     ? materialized_column.col_name
                                                     : boost::algorithm::to_lower_copy(materialized_column.col_name);
        auto iceberg_it = iceberg_schema_name_2_field.find(format_col_name);
        if (iceberg_it == iceberg_schema_name_2_field.end()) {
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        int32_t field_pos = _file_metadata->schema().get_field_pos_by_field_id(field_id);
        if (field_pos < 0) {
            continue;
        }

        auto parquet_type = _file_metadata->schema().get_stored_column_by_field_id(field_id)->physical_type;

        GroupReaderParam::Column column =
                _build_column(field_pos, materialized_column.col_idx, parquet_type, materialized_column.col_type,
                              materialized_column.slot_id, iceberg_it->second);
        read_cols.emplace_back(column);
    }
    _is_only_partition_scan = read_cols.empty();
}

} // namespace starrocks::parquet