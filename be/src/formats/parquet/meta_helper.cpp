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

<<<<<<< HEAD
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

void ParquetMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols) const {
    for (auto& materialized_column : materialized_columns) {
        int32_t field_idx = _file_metadata->schema().get_field_idx_by_column_name(materialized_column.name());
        if (field_idx < 0) continue;

        auto parquet_type = _file_metadata->schema().get_stored_column_by_field_idx(field_idx)->physical_type;
        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed);
        read_cols.emplace_back(column);
    }
}

const ParquetField* ParquetMetaHelper::get_parquet_field(const std::string& col_name) const {
    return _file_metadata->schema().get_stored_column_by_column_name(col_name);
=======
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/utils.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks::parquet {

void ParquetMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             std::unordered_set<std::string>& existed_column_names) const {
    for (auto& materialized_column : materialized_columns) {
        const SlotDescriptor* slotDesc = materialized_column.slot_desc;

        int32_t field_idx = -1;
        if (slotDesc->col_unique_id() != -1) {
            field_idx = _file_metadata->schema().get_field_idx_by_field_id(materialized_column.col_unique_id());
            if (field_idx < 0) continue;
        } else if (!slotDesc->col_physical_name().empty()) {
            field_idx = _file_metadata->schema().get_field_idx_by_column_name(materialized_column.col_physical_name());
            if (field_idx < 0) continue;
        } else {
            field_idx = _file_metadata->schema().get_field_idx_by_column_name(materialized_column.name());
            if (field_idx < 0) continue;
        }

        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_idx(field_idx);
        // check is type is invalid
        if (!_is_valid_type(parquet_field, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;
        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed);
        read_cols.emplace_back(column);
        existed_column_names.emplace(Utils::format_name(materialized_column.name(), _case_sensitive));
    }
}

bool ParquetMetaHelper::_is_valid_type(const ParquetField* parquet_field, const TypeDescriptor* type_descriptor) const {
    if (type_descriptor->is_unknown_type()) {
        return false;
    }
    // only check for complex type now
    // if complex type has none valid subfield, we will treat this struct type as invalid type.
    if (!parquet_field->is_complex_type()) {
        return true;
    }

    // check the complex type is matched
    if (!parquet_field->has_same_complex_type(*type_descriptor)) {
        return false;
    }

    bool has_valid_child = false;

    if (parquet_field->type == ColumnType::ARRAY || parquet_field->type == ColumnType::MAP) {
        for (size_t idx = 0; idx < parquet_field->children.size(); idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (parquet_field->type == ColumnType::STRUCT) {
        if (!type_descriptor->field_ids.empty()) {
            std::unordered_map<int32_t, const TypeDescriptor*> field_id_2_type;
            for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
                field_id_2_type.emplace(type_descriptor->field_ids[idx], &type_descriptor->children[idx]);
            }

            // start to check struct type
            for (const auto& child_parquet_field : parquet_field->children) {
                auto it = field_id_2_type.find(child_parquet_field.field_id);
                if (it == field_id_2_type.end()) {
                    continue;
                }

                if (_is_valid_type(&child_parquet_field, it->second)) {
                    has_valid_child = true;
                    break;
                }
            }
        } else {
            std::unordered_map<std::string, const TypeDescriptor*> field_name_2_type;
            if (!type_descriptor->field_physical_names.empty()) {
                for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
                    field_name_2_type.emplace(
                            Utils::format_name(type_descriptor->field_physical_names[idx], _case_sensitive),
                            &type_descriptor->children[idx]);
                }
            } else {
                for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
                    field_name_2_type.emplace(Utils::format_name(type_descriptor->field_names[idx], _case_sensitive),
                                              &type_descriptor->children[idx]);
                }
            }

            // start to check struct type
            for (const auto& child_parquet_field : parquet_field->children) {
                auto it = field_name_2_type.find(Utils::format_name(child_parquet_field.name, _case_sensitive));
                if (it == field_name_2_type.end()) {
                    continue;
                }

                if (_is_valid_type(&child_parquet_field, it->second)) {
                    has_valid_child = true;
                    break;
                }
            }
        }
    }

    return has_valid_child;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}

void IcebergMetaHelper::_init_field_mapping() {
    for (const auto& each : _t_iceberg_schema->fields) {
<<<<<<< HEAD
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
        auto& schema = _file_metadata->schema();
        const ParquetField* field = schema.get_stored_column_by_field_id(it->second->field_id);
        // After the column is added, there is no new column when querying the previously
        // imported parquet file. It is skipped here, and this column will be set to NULL
        // in the FileReader::_read_min_max_chunk.
        if (field == nullptr) continue;
        // Put SlotDescriptor's origin column name here!
        column_name_2_pos_in_meta.emplace(slot->col_name(), field->physical_column_index);
    }
}

void IcebergMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols) const {
    for (auto& materialized_column : materialized_columns) {
        const std::string& name = materialized_column.formatted_name(_case_sensitive);
        auto iceberg_it = _field_name_2_iceberg_field.find(name);
=======
        _field_name_2_iceberg_field.emplace(Utils::format_name(each.name, _case_sensitive), &each);
    }
}

bool IcebergMetaHelper::_is_valid_type(const ParquetField* parquet_field, const TIcebergSchemaField* field_schema,
                                       const TypeDescriptor* type_descriptor) const {
    // only check for complex type now
    // if complex type has none valid subfield, we will treat this struct type as invalid type.
    if (!parquet_field->is_complex_type()) {
        return true;
    }

    if (!parquet_field->has_same_complex_type(*type_descriptor)) {
        return false;
    }

    bool has_valid_child = false;

    if (parquet_field->type == ColumnType::ARRAY || parquet_field->type == ColumnType::MAP) {
        for (size_t idx = 0; idx < parquet_field->children.size(); idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &field_schema->children[idx],
                               &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (parquet_field->type == ColumnType::STRUCT) {
        std::unordered_map<int32_t, const TIcebergSchemaField*> field_id_2_iceberg_schema{};
        std::unordered_map<int32_t, const TypeDescriptor*> field_id_2_type{};
        for (const auto& field : field_schema->children) {
            field_id_2_iceberg_schema.emplace(field.field_id, &field);
            for (size_t i = 0; i < type_descriptor->field_names.size(); i++) {
                if (type_descriptor->field_names[i] == field.name) {
                    field_id_2_type.emplace(field.field_id, &type_descriptor->children[i]);
                    break;
                }
            }
        }

        // start to check struct type
        for (const auto& child_parquet_field : parquet_field->children) {
            auto it = field_id_2_iceberg_schema.find(child_parquet_field.field_id);
            if (it == field_id_2_iceberg_schema.end()) {
                continue;
            }

            auto it_td = field_id_2_type.find(child_parquet_field.field_id);
            if (it_td == field_id_2_type.end()) {
                continue;
            }

            // is compelx type, recursive check it's children
            if (_is_valid_type(&child_parquet_field, it->second, it_td->second)) {
                has_valid_child = true;
                break;
            }
        }
    }

    return has_valid_child;
}

void IcebergMetaHelper::prepare_read_columns(const std::vector<HdfsScannerContext::ColumnInfo>& materialized_columns,
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             std::unordered_set<std::string>& existed_column_names) const {
    for (auto& materialized_column : materialized_columns) {
        const std::string& formatted_name = Utils::format_name(materialized_column.name(), _case_sensitive);
        auto iceberg_it = _field_name_2_iceberg_field.find(formatted_name);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (iceberg_it == _field_name_2_iceberg_field.end()) {
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        int32_t field_idx = _file_metadata->schema().get_field_idx_by_field_id(field_id);
        if (field_idx < 0) continue;

<<<<<<< HEAD
        auto parquet_type = _file_metadata->schema().get_stored_column_by_field_id(field_id)->physical_type;
=======
        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_id(field_id);
        // check is type is invalid
        if (!_is_valid_type(parquet_field, iceberg_it->second, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed, iceberg_it->second);
        read_cols.emplace_back(column);
<<<<<<< HEAD
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

=======
        existed_column_names.emplace(formatted_name);
    }
}

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
} // namespace starrocks::parquet