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

#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/utils.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks::parquet {

void ParquetMetaHelper::build_column_name_2_pos_in_meta(
        std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta, const tparquet::RowGroup& row_group,
        const std::vector<SlotDescriptor*>& slots) const {
    if (!_logical_name_2_pysical_field.empty()) {
        for (const auto& slot : slots) {
            auto it = _logical_name_2_pysical_field.find(Utils::format_name(slot->col_name(), _case_sensitive));
            if (it == _logical_name_2_pysical_field.end()) {
                continue;
            }
            auto& schema = _file_metadata->schema();
            const ParquetField* field = nullptr;
            if (it->second->__isset.field_id) {
                field = schema.get_stored_column_by_field_id(it->second->field_id);
            } else if (it->second->__isset.physical_name) {
                field = schema.get_stored_column_by_column_name(it->second->physical_name);
            }
            // After the column is added, there is no new column when querying the previously
            // imported parquet file. It is skipped here, and this column will be set to NULL
            // in the FileReader::_read_min_max_chunk.
            if (field == nullptr) continue;
            // Put SlotDescriptor's origin column name here!
            column_name_2_pos_in_meta.emplace(slot->col_name(), field->physical_column_index);
        }
        return;
    }

    for (const auto& slot : slots) {
        const std::string format_slot_name = Utils::format_name(slot->col_name(), _case_sensitive);
        for (size_t idx = 0; idx < row_group.columns.size(); idx++) {
            const auto& column = row_group.columns[idx];
            // TODO Not support for non-scalar types now.
            const std::string format_column_name =
                    Utils::format_name(column.meta_data.path_in_schema[0], _case_sensitive);
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
                                             std::unordered_set<std::string>& existed_column_names) const {
    if (!_logical_name_2_pysical_field.empty()) {
        for (auto& materialized_column : materialized_columns) {
            auto physical_field_it = _logical_name_2_pysical_field.find(
                    Utils::format_name(materialized_column.name(), _case_sensitive));
            if (physical_field_it == _logical_name_2_pysical_field.end()) {
                continue;
            }
            const TPhysicalSchemaField* physical_schema_field = physical_field_it->second;

            int32_t field_idx = -1;
            if (physical_schema_field->__isset.field_id) {
                int64_t field_id = physical_schema_field->field_id;
                field_idx = _file_metadata->schema().get_field_idx_by_field_id(field_id);
                if (field_idx < 0) continue;
            } else if (physical_schema_field->__isset.physical_name) {
                auto& physical_name = physical_schema_field->physical_name;
                field_idx = _file_metadata->schema().get_field_idx_by_column_name(physical_name);
                if (field_idx < 0) continue;
            }

            if (field_idx < 0) continue;

            const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_idx(field_idx);

            // check is type is invalid
            if (!_is_valid_type(parquet_field, physical_schema_field, &materialized_column.slot_desc->type())) {
                continue;
            }

            auto parquet_type = parquet_field->physical_type;
            GroupReaderParam::Column column =
                    _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                  materialized_column.decode_needed, nullptr, physical_schema_field);
            read_cols.emplace_back(column);
            existed_column_names.emplace(Utils::format_name(materialized_column.name(), _case_sensitive));
        }
        return;
    }

    for (auto& materialized_column : materialized_columns) {
        int32_t field_idx = _file_metadata->schema().get_field_idx_by_column_name(materialized_column.name());
        if (field_idx < 0) continue;

        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_idx(field_idx);
        // check is type is invalid
        if (!_is_valid_type(parquet_field, nullptr, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;
        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed);
        read_cols.emplace_back(column);
        existed_column_names.emplace(Utils::format_name(materialized_column.name(), _case_sensitive));
    }
}

bool ParquetMetaHelper::_is_valid_type(const ParquetField* parquet_field,
                                       const TPhysicalSchemaField* physical_schema_field,
                                       const TypeDescriptor* type_descriptor) const {
    if (type_descriptor->is_unknown_type()) {
        return false;
    }
    // only check for complex type now
    // if complex type has none valid subfield, we will treat this struct type as invalid type.
    if (!parquet_field->type.is_complex_type()) {
        return true;
    }

    if (parquet_field->type.type != type_descriptor->type) {
        // complex type mismatched
        return false;
    }

    bool has_valid_child = false;

    if (parquet_field->type.is_array_type() || parquet_field->type.is_map_type()) {
        for (size_t idx = 0; idx < parquet_field->children.size(); idx++) {
            if (_is_valid_type(&parquet_field->children[idx],
                               physical_schema_field == nullptr ? nullptr : &physical_schema_field->children[idx],
                               &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (parquet_field->type.is_struct_type()) {
        if (physical_schema_field != nullptr) {
            if (physical_schema_field->__isset.field_id) {
                // check parquet field by field id
                std::unordered_map<int64_t, const TPhysicalSchemaField*> field_id_2_physical_field;
                std::unordered_map<int64_t, const TypeDescriptor*> field_id_2_type;

                for (const auto& field : physical_schema_field->children) {
                    field_id_2_physical_field.emplace(field.field_id, &field);
                    for (size_t i = 0; i < type_descriptor->field_names.size(); i++) {
                        if (type_descriptor->field_names[i] == field.logical_name) {
                            field_id_2_type.emplace(field.field_id, &type_descriptor->children[i]);
                            break;
                        }
                    }
                }

                // start to check struct type
                for (const auto& child_parquet_field : parquet_field->children) {
                    auto physical_field_it = field_id_2_physical_field.find(child_parquet_field.field_id);
                    if (physical_field_it == field_id_2_physical_field.end()) {
                        continue;
                    }

                    auto type_it = field_id_2_type.find(child_parquet_field.field_id);
                    if (type_it == field_id_2_type.end()) {
                        continue;
                    }

                    // is complex type, recursive check it's children
                    if (_is_valid_type(&child_parquet_field, physical_field_it->second, type_it->second)) {
                        has_valid_child = true;
                        break;
                    }
                }
            } else if (physical_schema_field->__isset.physical_name) {
                // check parquet field by physical name
                std::unordered_map<std::string, const TPhysicalSchemaField*> physical_name_2_field;
                std::unordered_map<std::string, const TypeDescriptor*> physical_name_2_type;
                for (const auto& field : physical_schema_field->children) {
                    physical_name_2_field.emplace(field.physical_name, &field);
                    for (size_t i = 0; i < type_descriptor->field_names.size(); i++) {
                        if (type_descriptor->field_names[i] == field.logical_name) {
                            physical_name_2_type.emplace(field.physical_name, &type_descriptor->children[i]);
                            break;
                        }
                    }
                }

                // start to check struct type
                for (const auto& child_parquet_field : parquet_field->children) {
                    auto physical_field_it = physical_name_2_field.find(child_parquet_field.name);
                    if (physical_field_it == physical_name_2_field.end()) {
                        continue;
                    }

                    auto type_it = physical_name_2_type.find(child_parquet_field.name);
                    if (type_it == physical_name_2_type.end()) {
                        continue;
                    }

                    // is complex type, recursive check it's children
                    if (_is_valid_type(&child_parquet_field, physical_field_it->second, type_it->second)) {
                        has_valid_child = true;
                        break;
                    }
                }
            }
        } else {
            // physical_schema_field is nullptr, use logical name directly
            std::unordered_map<std::string, const TypeDescriptor*> field_name_2_type;
            for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
                field_name_2_type.emplace(Utils::format_name(type_descriptor->field_names[idx], _case_sensitive),
                                          &type_descriptor->children[idx]);
            }

            // start to check struct type
            for (const auto& child_parquet_field : parquet_field->children) {
                auto it = field_name_2_type.find(Utils::format_name(child_parquet_field.name, _case_sensitive));
                if (it == field_name_2_type.end()) {
                    continue;
                }

                if (_is_valid_type(&child_parquet_field, nullptr, it->second)) {
                    has_valid_child = true;
                    break;
                }
            }
        }
    }

    return has_valid_child;
}

const ParquetField* ParquetMetaHelper::get_parquet_field(const std::string& col_name) const {
    if (!_logical_name_2_pysical_field.empty()) {
        auto it = _logical_name_2_pysical_field.find(Utils::format_name(col_name, _case_sensitive));
        if (it == _logical_name_2_pysical_field.end()) {
            return nullptr;
        }

        const TPhysicalSchemaField* physicalSchemaField = it->second;
        if (physicalSchemaField->__isset.field_id) {
            return _file_metadata->schema().get_stored_column_by_field_id(physicalSchemaField->field_id);
        } else if (physicalSchemaField->__isset.physical_name) {
            return _file_metadata->schema().get_stored_column_by_column_name(physicalSchemaField->physical_name);
        }
    }
    return _file_metadata->schema().get_stored_column_by_column_name(col_name);
}

void ParquetMetaHelper::_init_field_mapping() {
    if (_t_physical_schema == nullptr) {
        return;
    }
    for (const auto& filed : _t_physical_schema->fields) {
        _logical_name_2_pysical_field.emplace(Utils::format_name(filed.logical_name, _case_sensitive), &filed);
    }
}

void IcebergMetaHelper::_init_field_mapping() {
    for (const auto& each : _t_iceberg_schema->fields) {
        _field_name_2_iceberg_field.emplace(Utils::format_name(each.name, _case_sensitive), &each);
    }
}

bool IcebergMetaHelper::_is_valid_type(const ParquetField* parquet_field, const TIcebergSchemaField* field_schema,
                                       const TypeDescriptor* type_descriptor) const {
    // only check for complex type now
    // if complex type has none valid subfield, we will treat this struct type as invalid type.
    if (!parquet_field->type.is_complex_type()) {
        return true;
    }

    if (parquet_field->type.type != type_descriptor->type) {
        // complex type mismatched
        return false;
    }

    bool has_valid_child = false;

    if (parquet_field->type.is_array_type() || parquet_field->type.is_map_type()) {
        for (size_t idx = 0; idx < parquet_field->children.size(); idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &field_schema->children[idx],
                               &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (parquet_field->type.is_struct_type()) {
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

void IcebergMetaHelper::build_column_name_2_pos_in_meta(
        std::unordered_map<std::string, size_t>& column_name_2_pos_in_meta, const tparquet::RowGroup& row_group,
        const std::vector<SlotDescriptor*>& slots) const {
    for (const auto& slot : slots) {
        auto it = _field_name_2_iceberg_field.find(Utils::format_name(slot->col_name(), _case_sensitive));
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
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             std::unordered_set<std::string>& existed_column_names) const {
    for (auto& materialized_column : materialized_columns) {
        const std::string& formatted_name = Utils::format_name(materialized_column.name(), _case_sensitive);
        auto iceberg_it = _field_name_2_iceberg_field.find(formatted_name);
        if (iceberg_it == _field_name_2_iceberg_field.end()) {
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        int32_t field_idx = _file_metadata->schema().get_field_idx_by_field_id(field_id);
        if (field_idx < 0) continue;

        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_id(field_id);
        // check is type is invalid
        if (!_is_valid_type(parquet_field, iceberg_it->second, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;

        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed, iceberg_it->second);
        read_cols.emplace_back(column);
        existed_column_names.emplace(formatted_name);
    }
}

const ParquetField* IcebergMetaHelper::get_parquet_field(const std::string& col_name) const {
    auto it = _field_name_2_iceberg_field.find(Utils::format_name(col_name, _case_sensitive));
    if (it == _field_name_2_iceberg_field.end()) {
        return nullptr;
    }
    int32_t field_id = it->second->field_id;
    return _file_metadata->schema().get_stored_column_by_field_id(field_id);
}

} // namespace starrocks::parquet