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

Status validate_geo_field(const ParquetField& field, const TIcebergSchemaField* lake_field) {
    const auto& element = field.schema_element;
    const auto& logical = element.logicalType;
    const bool geography = element.__isset.logicalType && logical.__isset.GEOGRAPHY;
    const bool geometry = element.__isset.logicalType && logical.__isset.GEOMETRY;
    const auto* geo = lake_field != nullptr && lake_field->__isset.geo_metadata ? &lake_field->geo_metadata : nullptr;
    if (!geography && !geometry && geo == nullptr) return Status::OK();

    // Geo values are scalar BYTE_ARRAY leaves; containers are handled by the
    // existing selected-child reader traversal, not by a second schema walk.
    if (field.is_complex_type() || !element.__isset.type || element.type != tparquet::Type::BYTE_ARRAY ||
        element.__isset.converted_type || (geography && geometry)) {
        return Status::InvalidArgument("Invalid Parquet geo annotation: " + field.name);
    }
    // Source geo metadata is supplied by FE; only validate the file-side contract here.
    if (geo != nullptr && !geography && !geometry && element.__isset.logicalType) {
        return Status::InvalidArgument("Iceberg geo field has a non-geo Parquet annotation: " + field.name);
    }
    // Unannotated WKB may inherit semantics from the Iceberg schema.
    if (!geography && !geometry) return Status::OK();

    const auto kind = geography ? TIcebergGeoKind::GEOGRAPHY : TIcebergGeoKind::GEOMETRY;
    const std::string_view crs =
            geography ? (logical.GEOGRAPHY.__isset.crs ? std::string_view(logical.GEOGRAPHY.crs) : "OGC:CRS84")
                      : (logical.GEOMETRY.__isset.crs ? std::string_view(logical.GEOMETRY.crs) : "OGC:CRS84");
    if (crs.empty()) return Status::InvalidArgument("Empty CRS in Parquet geo annotation: " + field.name);
    std::string_view edge = "PLANAR";
    if (geography) {
        const auto algorithm = logical.GEOGRAPHY.__isset.algorithm ? logical.GEOGRAPHY.algorithm
                                                                   : tparquet::EdgeInterpolationAlgorithm::SPHERICAL;
        const auto it = tparquet::_EdgeInterpolationAlgorithm_VALUES_TO_NAMES.find(algorithm);
        if (it == tparquet::_EdgeInterpolationAlgorithm_VALUES_TO_NAMES.end()) {
            return Status::NotSupported("Unknown Parquet geo edge algorithm: " + field.name);
        }
        edge = it->second;
    }
    // Missing geo metadata (including older senders) is not proof of an ordinary
    // binary type. Compare declared semantics without changing the reader's type policy.
    if (geo != nullptr && (geo->kind != kind || geo->crs != crs || geo->edge_algorithm != edge)) {
        return Status::InvalidArgument("Iceberg/Parquet geo schema mismatch: " + field.name);
    }
    return Status::OK();
}

Status ParquetMetaHelper::prepare_read_columns(const std::vector<FormatColumnInfo>& materialized_columns,
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
    return Status::OK();
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
        if (type_descriptor->type == LogicalType::TYPE_VARIANT) {
            // variant type currently can be mapped to struct type in parquet
            has_valid_child = true;
        } else if (!type_descriptor->field_ids.empty()) {
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
}

void LakeMetaHelper::_init_field_mapping() {
    for (const auto& each : _lake_schema->fields) {
        _field_name_2_lake_field.emplace(Utils::format_name(each.name, _case_sensitive), &each);
    }
}

StatusOr<bool> LakeMetaHelper::_is_valid_type(const ParquetField* parquet_field,
                                              const TIcebergSchemaField* field_schema,
                                              const TypeDescriptor* type_descriptor) const {
    // Check source/file agreement in the existing selected-field traversal, before
    // an incompatible field can be treated as missing. FE owns source validation.
    if (field_schema->__isset.geo_metadata) {
        RETURN_IF_ERROR(validate_geo_field(*parquet_field, field_schema));
    }
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
        for (size_t idx = 0; idx < required_children; idx++) {
            // MAP readers do not materialize an UNKNOWN (pruned) key or value.
            if (parquet_field->type == ColumnType::MAP && type_descriptor->children[idx].is_unknown_type()) continue;
            ASSIGN_OR_RETURN(bool valid, _is_valid_type(&parquet_field->children[idx], &field_schema->children[idx],
                                                        &type_descriptor->children[idx]));
            has_valid_child |= valid;
        }
    } else if (parquet_field->type == ColumnType::STRUCT) {
        if (type_descriptor->type == LogicalType::TYPE_VARIANT) {
            return true;
        }

        // LakeMetaHelper is only used when the parquet file has field ids (see _build_meta_helper).
        std::unordered_map<int32_t, const TIcebergSchemaField*> field_id_2_lake_schema;
        std::unordered_map<int32_t, const TypeDescriptor*> field_id_2_type;
        for (const auto& field : field_schema->children) {
            field_id_2_lake_schema.emplace(field.field_id, &field);
            for (size_t i = 0; i < type_descriptor->field_names.size(); i++) {
                if (type_descriptor->field_names[i] == field.name) {
                    field_id_2_type.emplace(field.field_id, &type_descriptor->children[i]);
                    break;
                }
            }
        }

        for (const auto& child_parquet_field : parquet_field->children) {
            auto it = field_id_2_lake_schema.find(child_parquet_field.field_id);
            if (it == field_id_2_lake_schema.end()) {
                continue;
            }

            auto it_td = field_id_2_type.find(child_parquet_field.field_id);
            if (it_td == field_id_2_type.end()) {
                continue;
            }

            ASSIGN_OR_RETURN(bool valid, _is_valid_type(&child_parquet_field, it->second, it_td->second));
            has_valid_child |= valid;
        }
    }

    return has_valid_child;
}

Status LakeMetaHelper::prepare_read_columns(const std::vector<FormatColumnInfo>& materialized_columns,
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
        if (!extended_variant_binding.has_value()) {
            ASSIGN_OR_RETURN(bool valid,
                             _is_valid_type(parquet_field, lake_it->second, &materialized_column.slot_desc->type()));
            if (!valid) continue;
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
    return Status::OK();
}

} // namespace starrocks::parquet
