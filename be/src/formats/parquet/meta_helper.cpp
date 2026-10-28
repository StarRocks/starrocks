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

#include <optional>

#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/utils.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks::parquet {

namespace {

struct ExtendedVariantVirtualBinding {
    const ColumnAccessPath* access_path = nullptr;
    std::string leaf_path;
};

// Returns the parquet column name to use when looking up a column by name (no-field-id
// path).  Prefers col_physical_name() so that renamed columns are found correctly; falls
// back to the logical name when no physical name is recorded.
std::string_view parquet_lookup_name(const FormatColumnInfo& column) {
    if (!column.col_physical_name().empty()) {
        return column.col_physical_name();
    }
    return column.name();
}

int32_t find_field_idx_for_materialized_column(const FileMetaData* file_metadata,
                                               const FormatColumnInfo& materialized_column) {
    const SlotDescriptor* slot_desc = materialized_column.slot_desc;
    if (slot_desc->col_unique_id() != -1) {
        return file_metadata->schema().get_field_idx_by_field_id(materialized_column.col_unique_id());
    }
    return file_metadata->schema().get_field_idx_by_column_name(std::string(parquet_lookup_name(materialized_column)));
}

std::optional<ExtendedVariantVirtualBinding> find_extended_variant_virtual_binding(
        const std::vector<ColumnAccessPathPtr>* column_access_paths, std::string_view slot_name) {
    if (column_access_paths == nullptr) {
        return std::nullopt;
    }
    for (const auto& access_path : *column_access_paths) {
        if (access_path == nullptr || !access_path->is_extended()) {
            continue;
        }
        if (access_path->linear_path() != slot_name || access_path->children().empty()) {
            continue;
        }
        ExtendedVariantVirtualBinding binding;
        binding.access_path = access_path.get();
        binding.leaf_path = access_path->children()[0]->linear_path();
        return binding;
    }
    return std::nullopt;
}

} // namespace

// A map whose group key is read with a table key of another shape: keep the column, so that the reader factory
// reports the unsupported key instead of the column being dropped (read as NULL), e.g. for a key-only projection
// whose value is pruned.
static bool reads_incompatible_map_key(const ParquetField* parquet_field, const TypeDescriptor* type_descriptor) {
    if (parquet_field->type != ColumnType::MAP || parquet_field->children.empty() ||
        type_descriptor->children.empty()) {
        return false;
    }
    const ParquetField& key = parquet_field->children[0];
    const TypeDescriptor& key_type = type_descriptor->children[0];
    return key.is_complex_type() && !key_type.is_unknown_type() && !key.has_same_complex_type(key_type);
}

void ParquetMetaHelper::prepare_read_columns(const std::vector<FormatColumnInfo>& materialized_columns,
                                             const std::vector<ColumnAccessPathPtr>* column_access_paths,
                                             std::vector<GroupReaderParam::Column>& read_cols,
                                             std::unordered_set<std::string>& existed_column_names) const {
    for (auto& materialized_column : materialized_columns) {
        auto extended_variant_binding =
                find_extended_variant_virtual_binding(column_access_paths, materialized_column.name());

        int32_t field_idx = find_field_idx_for_materialized_column(_file_metadata, materialized_column);
        if (field_idx < 0) continue;

        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_idx(field_idx);
        // check is type is invalid
        if (!extended_variant_binding.has_value() &&
            !_is_valid_type(parquet_field, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;
        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed);
        if (extended_variant_binding.has_value()) {
            column.is_extended_variant_virtual = true;
            column.source_variant_column_name = std::string(extended_variant_binding->access_path->path());
            column.variant_virtual_leaf_path = std::move(extended_variant_binding->leaf_path);
        }
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

    if (reads_incompatible_map_key(parquet_field, type_descriptor)) {
        return true;
    }
    if (parquet_field->type == ColumnType::ARRAY || parquet_field->type == ColumnType::MAP) {
        for (size_t idx = 0; idx < parquet_field->children.size(); idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (type_descriptor->type == LogicalType::TYPE_VARIANT) {
        // A VARIANT group, or a struct group the reader factory rejects with an error.
        has_valid_child = true;
    } else if (parquet_field->has_struct_layout()) {
        // Same matching as the reader (ColumnReaderFactory::create), so a valid column always gets a reader.
        StructSubfieldMapping mapping = ColumnReaderFactory::resolve_struct_subfields(*parquet_field, *type_descriptor,
                                                                                      _case_sensitive, nullptr, false);
        for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
            const ParquetField* child_parquet_field = mapping.parquet_children[idx];
            if (child_parquet_field != nullptr &&
                _is_valid_type(child_parquet_field, &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    }

    return has_valid_child;
}

void LakeMetaHelper::_init_field_mapping() {
    for (const auto& each : _lake_schema->fields) {
        _field_name_2_lake_field.emplace(Utils::format_name(each.name, _case_sensitive), &each);
    }
}

bool LakeMetaHelper::_is_valid_type(const ParquetField* parquet_field, const TIcebergSchemaField* field_schema,
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
        // ARRAY always has one child (element) and MAP always has two (key, value). The downstream
        // reader (ColumnReaderFactory::create) reads those children by fixed index.
        const size_t required_children = parquet_field->type == ColumnType::MAP ? 2 : 1;
        if (parquet_field->children.size() < required_children || field_schema->children.size() < required_children ||
            type_descriptor->children.size() < required_children) {
            return false;
        }
        if (reads_incompatible_map_key(parquet_field, type_descriptor)) {
            return true;
        }
        for (size_t idx = 0; idx < required_children; idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &field_schema->children[idx],
                               &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    } else if (type_descriptor->type == LogicalType::TYPE_VARIANT) {
        // A VARIANT group, or a struct group the reader factory rejects with an error.
        return true;
    } else if (parquet_field->has_struct_layout()) {
        // Same matching as the reader (ColumnReaderFactory::create), so a valid column always gets a reader.
        // LakeMetaHelper is only used when the parquet file has field ids (see _build_meta_helper).
        StructSubfieldMapping mapping = ColumnReaderFactory::resolve_struct_subfields(
                *parquet_field, *type_descriptor, _case_sensitive, field_schema, /*parquet_has_field_id=*/true);
        for (size_t idx = 0; idx < type_descriptor->children.size(); idx++) {
            const ParquetField* child_parquet_field = mapping.parquet_children[idx];
            if (child_parquet_field != nullptr &&
                _is_valid_type(child_parquet_field, mapping.lake_children[idx], &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
        }
    }

    return has_valid_child;
}

void LakeMetaHelper::prepare_read_columns(const std::vector<FormatColumnInfo>& materialized_columns,
                                          const std::vector<ColumnAccessPathPtr>* column_access_paths,
                                          std::vector<GroupReaderParam::Column>& read_cols,
                                          std::unordered_set<std::string>& existed_column_names) const {
    // LakeMetaHelper is only used when the parquet file has field ids (see _build_meta_helper).
    for (auto& materialized_column : materialized_columns) {
        auto extended_variant_binding =
                find_extended_variant_virtual_binding(column_access_paths, materialized_column.name());
        std::string formatted_name;
        if (extended_variant_binding.has_value()) {
            formatted_name =
                    Utils::format_name(std::string(extended_variant_binding->access_path->path()), _case_sensitive);
        } else {
            formatted_name = Utils::format_name(materialized_column.name(), _case_sensitive);
        }
        auto lake_it = _field_name_2_lake_field.find(formatted_name);
        if (lake_it == _field_name_2_lake_field.end()) {
            continue;
        }

        int32_t field_id = lake_it->second->field_id;

        const int32_t field_idx = _file_metadata->schema().get_field_idx_by_field_id(field_id);
        if (field_idx < 0) continue;

        const ParquetField* parquet_field = _file_metadata->schema().get_stored_column_by_field_id(field_id);
        // check is type is invalid
        if (!extended_variant_binding.has_value() &&
            !_is_valid_type(parquet_field, lake_it->second, &materialized_column.slot_desc->type())) {
            continue;
        }

        auto parquet_type = parquet_field->physical_type;

        GroupReaderParam::Column column = _build_column(field_idx, parquet_type, materialized_column.slot_desc,
                                                        materialized_column.decode_needed, lake_it->second);
        if (extended_variant_binding.has_value()) {
            column.is_extended_variant_virtual = true;
            column.source_variant_column_name = std::string(extended_variant_binding->access_path->path());
            column.variant_virtual_leaf_path = std::move(extended_variant_binding->leaf_path);
        }
        read_cols.emplace_back(column);
        existed_column_names.emplace(Utils::format_name(materialized_column.name(), _case_sensitive));
    }
}

} // namespace starrocks::parquet
