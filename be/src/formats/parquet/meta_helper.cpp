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

namespace {

std::string_view geo_kind_name(TIcebergGeoKind::type kind) {
    switch (kind) {
    case TIcebergGeoKind::GEOGRAPHY:
        return "GEOGRAPHY";
    case TIcebergGeoKind::GEOMETRY:
        return "GEOMETRY";
    default:
        return "UNKNOWN";
    }
}

const TIcebergSchemaField* match_lake_field(const ParquetField& field, const std::vector<TIcebergSchemaField>& fields,
                                            bool case_sensitive) {
    for (const auto& candidate : fields) {
        if (field.schema_element.__isset.field_id) {
            if (candidate.field_id == field.field_id) return &candidate;
        } else if (Utils::format_name(candidate.name, case_sensitive) ==
                   Utils::format_name(field.name, case_sensitive)) {
            return &candidate;
        }
    }
    return nullptr;
}

} // namespace

bool iceberg_contains_geo(const TIcebergSchemaField& field) {
    if (field.__isset.geo_metadata ||
        (field.__isset.iceberg_type && (field.iceberg_type == "GEOGRAPHY" || field.iceberg_type == "GEOMETRY"))) {
        return true;
    }
    for (const auto& child : field.children) {
        if (iceberg_contains_geo(child)) return true;
    }
    return false;
}

Status validate_geo_field(const ParquetField& field, const TIcebergSchemaField* lake_field, bool case_sensitive) {
    const auto& logical = field.schema_element.logicalType;
    // A legacy repeated primitive becomes an ARRAY wrapper plus a leaf. Its
    // physical annotation describes the leaf, not the collection's logical kind.
    const bool geography =
            !field.is_complex_type() && field.schema_element.__isset.logicalType && logical.__isset.GEOGRAPHY;
    const bool geometry =
            !field.is_complex_type() && field.schema_element.__isset.logicalType && logical.__isset.GEOMETRY;
    const bool lake_geo = lake_field != nullptr && lake_field->__isset.geo_metadata;
    if (lake_field != nullptr && lake_field->__isset.iceberg_type) {
        const auto& kind = lake_field->iceberg_type;
        if (((kind == "GEOGRAPHY" || kind == "GEOMETRY") && !lake_geo) ||
            (lake_geo && kind != geo_kind_name(lake_field->geo_metadata.kind))) {
            return Status::InvalidArgument("Inconsistent Iceberg geo schema metadata: " + field.name);
        }
    }
    if (lake_geo && (field.is_complex_type() || field.physical_type != tparquet::Type::BYTE_ARRAY)) {
        return Status::InvalidArgument("Iceberg geo field requires Parquet BYTE_ARRAY: " + field.name);
    }
    if (lake_geo) {
        const auto& geo = lake_field->geo_metadata;
        if (!geo.__isset.kind || !geo.__isset.crs || !geo.__isset.edge_algorithm || geo.crs.empty() ||
            (geo.kind != TIcebergGeoKind::GEOGRAPHY && geo.kind != TIcebergGeoKind::GEOMETRY) ||
            (geo.kind == TIcebergGeoKind::GEOMETRY && geo.edge_algorithm != "PLANAR")) {
            return Status::InvalidArgument("Invalid Iceberg geo metadata: " + field.name);
        }
        if (geo.kind == TIcebergGeoKind::GEOGRAPHY && geo.edge_algorithm != "SPHERICAL" &&
            geo.edge_algorithm != "VINCENTY" && geo.edge_algorithm != "THOMAS" && geo.edge_algorithm != "ANDOYER" &&
            geo.edge_algorithm != "KARNEY") {
            return Status::NotSupported("Unknown Iceberg geo edge algorithm: " + field.name);
        }
        if ((!geography && !geometry && field.schema_element.__isset.logicalType) ||
            field.schema_element.__isset.converted_type) {
            return Status::InvalidArgument("Iceberg geo field has a non-geo Parquet annotation: " + field.name);
        }
    }
    if (geography || geometry) {
        const std::string kind = geography ? "GEOGRAPHY" : "GEOMETRY";
        const std::string crs = geography ? (logical.GEOGRAPHY.__isset.crs ? logical.GEOGRAPHY.crs : "OGC:CRS84")
                                          : (logical.GEOMETRY.__isset.crs ? logical.GEOMETRY.crs : "OGC:CRS84");
        std::string edge = "PLANAR";
        if (geography) {
            const auto algorithm = logical.GEOGRAPHY.__isset.algorithm
                                           ? logical.GEOGRAPHY.algorithm
                                           : tparquet::EdgeInterpolationAlgorithm::SPHERICAL;
            switch (algorithm) {
            case tparquet::EdgeInterpolationAlgorithm::SPHERICAL:
                edge = "SPHERICAL";
                break;
            case tparquet::EdgeInterpolationAlgorithm::VINCENTY:
                edge = "VINCENTY";
                break;
            case tparquet::EdgeInterpolationAlgorithm::THOMAS:
                edge = "THOMAS";
                break;
            case tparquet::EdgeInterpolationAlgorithm::ANDOYER:
                edge = "ANDOYER";
                break;
            case tparquet::EdgeInterpolationAlgorithm::KARNEY:
                edge = "KARNEY";
                break;
            default:
                return Status::NotSupported("Unknown Parquet geo edge algorithm: " + field.name);
            }
        }
        if ((lake_field != nullptr && lake_field->__isset.iceberg_type && lake_field->iceberg_type != kind) ||
            (lake_geo && (geo_kind_name(lake_field->geo_metadata.kind) != kind || lake_field->geo_metadata.crs != crs ||
                          lake_field->geo_metadata.edge_algorithm != edge))) {
            return Status::InvalidArgument("Iceberg/Parquet geo schema mismatch: " + field.name);
        }
    }
    for (const auto& child : field.children) {
        const auto* lake_child =
                lake_field == nullptr ? nullptr : match_lake_field(child, lake_field->children, case_sensitive);
        RETURN_IF_ERROR(validate_geo_field(child, lake_child, case_sensitive));
    }
    return Status::OK();
}

Status validate_geo_scan(const SchemaDescriptor& schema, const TIcebergSchema* lake_schema,
                         const std::vector<FormatColumnInfo>& columns, bool case_sensitive) {
    // Ordinary scans do not need cross-schema matching. In particular, avoid
    // repeated name/ID lookups across wide Iceberg schemas without any geo fields.
    bool has_geo = false;
    for (const auto& field : schema.get_parquet_fields()) {
        has_geo |= field.contains_geo();
    }
    if (lake_schema != nullptr) {
        for (const auto& field : lake_schema->fields) {
            has_geo |= iceberg_contains_geo(field);
        }
    }
    if (!has_geo) return Status::OK();

    for (const auto& field : schema.get_parquet_fields()) {
        const auto* lake_field =
                lake_schema == nullptr ? nullptr : match_lake_field(field, lake_schema->fields, case_sensitive);
        RETURN_IF_ERROR(validate_geo_field(field, lake_field, case_sensitive));
    }
    for (const auto& column : columns) {
        const TIcebergSchemaField* lake_field = nullptr;
        if (lake_schema != nullptr) {
            for (const auto& candidate : lake_schema->fields) {
                if (Utils::format_name(candidate.name, case_sensitive) ==
                    Utils::format_name(column.name(), case_sensitive)) {
                    lake_field = &candidate;
                    break;
                }
            }
        }
        const ParquetField* field = nullptr;
        if (lake_field != nullptr && schema.exist_filed_id()) {
            field = schema.get_stored_column_by_field_id(lake_field->field_id);
        } else if (column.col_unique_id() != -1) {
            field = schema.get_stored_column_by_field_id(column.col_unique_id());
        } else {
            field = schema.get_stored_column_by_column_name(
                    Utils::format_name(parquet_lookup_name(column), case_sensitive));
        }
        if ((lake_field != nullptr && iceberg_contains_geo(*lake_field)) ||
            (field != nullptr && field->contains_geo())) {
            return Status::NotSupported("Native geospatial type support is disabled; cannot project column " +
                                        std::string(column.name()));
        }
    }
    return Status::OK();
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
        for (size_t idx = 0; idx < required_children; idx++) {
            if (_is_valid_type(&parquet_field->children[idx], &field_schema->children[idx],
                               &type_descriptor->children[idx])) {
                has_valid_child = true;
                break;
            }
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

            if (_is_valid_type(&child_parquet_field, it->second, it_td->second)) {
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
