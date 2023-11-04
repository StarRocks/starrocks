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
        names->emplace(_file_metadata->schema().get_stored_column_by_field_idx(i)->name);
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

void ParquetMetaHelper::prepare_read_columns(
        const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
        const std::unordered_map<std::string, ColumnAccessPathPtr>* column_access_path_mapping,
        std::vector<GroupReaderParam::Column>& read_cols) const {
    for (auto& materialized_column : materialized_columns) {
        int32_t field_idx = _file_metadata->schema().get_field_idx_by_column_name(materialized_column.col_name);
        if (field_idx < 0) continue;

        const ColumnAccessPathPtr* path = ColumnAccessPathUtil::get_column_access_path_from_mapping(
                column_access_path_mapping, materialized_column.col_name);

        auto parquet_type = _file_metadata->schema().get_stored_column_by_field_idx(field_idx)->physical_type;
        GroupReaderParam::Column column =
                _build_column(field_idx, materialized_column.col_idx, parquet_type, materialized_column.col_type,
                              materialized_column.slot_id, materialized_column.decode_needed, path);
        read_cols.emplace_back(column);
    }
}

const ParquetField* ParquetMetaHelper::get_parquet_field(const std::string& col_name) const {
    return _file_metadata->schema().get_stored_column_by_column_name(col_name);
}

void IcebergMetaHelper::_init_field_mapping() {
    for (const auto& each : _t_iceberg_schema->fields) {
        const std::string& formatted_field_name =
                _case_sensitive ? each.name : boost::algorithm::to_lower_copy(each.name);
        _field_name_2_iceberg_field.emplace(formatted_field_name, &each);
    }
}

void IcebergMetaHelper::set_existed_column_names(std::unordered_set<std::string>* names) const {
    names->clear();
    // build column name set from iceberg schema
    const auto& fields = _t_iceberg_schema->fields;
    for (const auto& field : fields) {
        if (_file_metadata->schema().contain_field_id(field.field_id)) {
            // We can't use _file_metadata->schema()'s column name, because column name from
            // _file_metadata->schema() was set by parquet metadata, it may not the same as column name from FE.
            // names->emplace(_file_metadata->schema().get_stored_column_by_field_id(field.field_id)->name);
            names->emplace(_case_sensitive ? field.name : boost::algorithm::to_lower_copy(field.name));
        }
    }
}

void IcebergMetaHelper::build_column_name_2_pos_in_meta(
        std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta, const tparquet::RowGroup& row_group,
        const std::vector<SlotDescriptor*>& slots) const {
    for (const auto& slot : slots) {
        const std::string& format_column_name =
                _case_sensitive ? slot->col_name() : boost::algorithm::to_lower_copy(slot->col_name());
        auto it = _field_name_2_iceberg_field.find(format_column_name);
        if (it == _field_name_2_iceberg_field.end()) {
            continue;
        }
        // Put SlotDescriptor's origin column name here!
        column_name_2_pos_in_meta.emplace(
                slot->col_name(),
                _file_metadata->schema().get_stored_column_by_field_id(it->second->field_id)->physical_column_index);
    }
}

void IcebergMetaHelper::prepare_read_columns(
        const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
        const std::unordered_map<std::string, ColumnAccessPathPtr>* column_access_path_mapping,
        std::vector<GroupReaderParam::Column>& read_cols) const {
    for (auto& materialized_column : materialized_columns) {
        const std::string& format_col_name = _case_sensitive
                                                     ? materialized_column.col_name
                                                     : boost::algorithm::to_lower_copy(materialized_column.col_name);
        auto iceberg_it = _field_name_2_iceberg_field.find(format_col_name);
        if (iceberg_it == _field_name_2_iceberg_field.end()) {
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        int32_t field_idx = _file_metadata->schema().get_field_idx_by_field_id(field_id);
        if (field_idx < 0) continue;

        const ColumnAccessPathPtr* path = ColumnAccessPathUtil::get_column_access_path_from_mapping(
                column_access_path_mapping, materialized_column.col_name);

        auto parquet_type = _file_metadata->schema().get_stored_column_by_field_id(field_id)->physical_type;

        GroupReaderParam::Column column =
                _build_column(field_idx, materialized_column.col_idx, parquet_type, materialized_column.col_type,
                              materialized_column.slot_id, materialized_column.decode_needed, path, iceberg_it->second);
        read_cols.emplace_back(column);
    }
}

const ParquetField* IcebergMetaHelper::get_parquet_field(const std::string& col_name) const {
    const std::string& formatted_col_name = _case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
    auto it = _field_name_2_iceberg_field.find(formatted_col_name);
    if (it == _field_name_2_iceberg_field.end()) {
        return nullptr;
    }
    int32_t field_id = it->second->field_id;
    return _file_metadata->schema().get_stored_column_by_field_id(field_id);
}

} // namespace starrocks::parquet