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

#include "formats/parquet/column_reader_factory.h"

#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
<<<<<<< HEAD
=======
#include "formats/parquet/utils.h"
#include "formats/scan_context.h"
>>>>>>> 209630d ([BugFix] Fix Iceberg read follow-ups: unknown transforms in SHOW CREATE, TIME/TIMESTAMPTZ defaults, missing struct subfields, filters on default columns (#81311))
#include "formats/utils.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::parquet {

DEFINE_FAIL_POINT(parquet_reader_returns_global_dict_not_match_status);

<<<<<<< HEAD
=======
namespace {

Status validate_native_iceberg_geography(const ParquetField& field, const TypeDescriptor& col_type,
                                         const TIcebergSchemaField& lake_field) {
    if (!lake_field.__isset.geo_metadata || !col_type.geo_type) {
        return Status::NotSupported("Native Iceberg GEOGRAPHY requires GEO metadata: " + field.name);
    }
    const auto& source = lake_field.geo_metadata;
    const auto& planned = *col_type.geo_type;
    constexpr std::string_view edge_prefix = "GEO_EDGE_ALGORITHM_";
    const std::string edge = GeoEdgeAlgorithmPB_Name(planned.edge_algorithm);
    if (source.kind != TIcebergGeoKind::GEOGRAPHY || planned.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY ||
        source.crs != planned.crs || edge.compare(0, edge_prefix.size(), edge_prefix) != 0 ||
        source.edge_algorithm != edge.substr(edge_prefix.size())) {
        return Status::InvalidArgument("Iceberg/plan geo schema mismatch: " + field.name);
    }
    if (field.physical_type != tparquet::Type::BYTE_ARRAY) {
        return Status::InvalidArgument("Native Iceberg GEOGRAPHY requires Parquet BYTE_ARRAY: " + field.name);
    }
    // Older writers may omit the annotation. When present, it must agree with Iceberg;
    // WKB payload bytes cannot establish logical kind, CRS or edge semantics.
    const auto& schema = field.schema_element;
    if (schema.__isset.logicalType) {
        const auto& logical = schema.logicalType;
        if (!logical.__isset.GEOGRAPHY || logical.__isset.GEOMETRY) {
            return Status::InvalidArgument("Iceberg/Parquet geo logical type mismatch: " + field.name);
        }
        const auto& geography = logical.GEOGRAPHY;
        const std::string_view crs = geography.__isset.crs ? geography.crs : std::string_view("OGC:CRS84");
        const auto algorithm =
                geography.__isset.algorithm ? geography.algorithm : tparquet::EdgeInterpolationAlgorithm::SPHERICAL;
        const auto algorithm_name = tparquet::_EdgeInterpolationAlgorithm_VALUES_TO_NAMES.find(algorithm);
        if (crs != source.crs || algorithm_name == tparquet::_EdgeInterpolationAlgorithm_VALUES_TO_NAMES.end() ||
            algorithm_name->second != source.edge_algorithm) {
            return Status::InvalidArgument("Iceberg/Parquet geo schema mismatch: " + field.name);
        }
    }
    return Status::OK();
}

struct VariantNodeFields {
    const ParquetField* metadata = nullptr;
    const ParquetField* value = nullptr;
    const ParquetField* typed_value = nullptr;
};

Status collect_variant_shredded_fields(const ColumnReaderOptions& opts, const ParquetField* typed_group,
                                       VariantPath* current_path, std::vector<ShreddedFieldNode>* output);

static VariantNodeFields find_variant_node_fields(const ParquetField* struct_field) {
    VariantNodeFields fields;
    if (struct_field == nullptr) {
        return fields;
    }
    for (const auto& child : struct_field->children) {
        if (child.name == "metadata") {
            fields.metadata = &child;
        } else if (child.name == "value") {
            fields.value = &child;
        } else if (child.name == "typed_value") {
            fields.typed_value = &child;
        }
    }
    return fields;
}

struct ScalarArrayLayout {
    bool enabled = false;
    const ParquetField* value_field = nullptr;
    const ParquetField* typed_value_field = nullptr;
    TypeDescriptor element_type;
};

static ScalarArrayLayout detect_scalar_array_layout(const ParquetField* typed_field) {
    ScalarArrayLayout layout;
    if (typed_field == nullptr || typed_field->children.empty()) {
        return layout;
    }
    const ParquetField* element_field = &typed_field->children[0];
    if (element_field->type != ColumnType::STRUCT) {
        return layout;
    }

    VariantNodeFields elem_fields = find_variant_node_fields(element_field);
    const ParquetField* elem_value_field = elem_fields.value;
    const ParquetField* elem_typed_value_field = elem_fields.typed_value;
    // Scalar-array layout: struct {value, typed_value(scalar), ...}
    if (elem_value_field != nullptr && elem_typed_value_field != nullptr &&
        elem_typed_value_field->type != ColumnType::STRUCT && elem_typed_value_field->type != ColumnType::ARRAY) {
        layout.enabled = true;
        layout.value_field = elem_value_field;
        layout.typed_value_field = elem_typed_value_field;
        layout.element_type = variant_typed_desc_from_parquet_field(elem_typed_value_field);
    }
    return layout;
}

static Status build_array_typed_value_reader(const ColumnReaderOptions& opts, const ParquetField* typed_field,
                                             const std::string& full_path, const TypeDescriptor& file_type,
                                             const ScalarArrayLayout& array_layout, ShreddedFieldNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("node should not be null");
    }
    if (array_layout.enabled) {
        TypeDescriptor array_read_type = TypeDescriptor::create_array_type(array_layout.element_type);
        node->typed_value_read_type = std::make_unique<TypeDescriptor>(array_read_type);
        // Use the heap-owned element type to keep ScalarColumnReader's captured type pointer stable.
        const TypeDescriptor& heap_element_type = node->typed_value_read_type->children[0];
        auto element_reader_or = ColumnReaderFactory::create(opts, array_layout.typed_value_field, heap_element_type);
        if (!element_reader_or.ok()) {
            return Status::InternalError(strings::Substitute(
                    "build variant typed scalar-array element reader failed, path=$0, type=$1, err=$2", full_path,
                    array_layout.element_type.debug_string(), element_reader_or.status().to_string()));
        }
        node->typed_value_reader =
                std::make_unique<ListColumnReader>(typed_field, std::move(element_reader_or).value());
        DCHECK(array_layout.value_field != nullptr);
        if (array_layout.value_field->physical_column_index < 0 ||
            static_cast<size_t>(array_layout.value_field->physical_column_index) >=
                    opts.row_group_meta->columns.size()) {
            return Status::InvalidArgument(strings::Substitute(
                    "variant array element.value has out-of-range physical_column_index $0 (num_chunks=$1)",
                    array_layout.value_field->physical_column_index, opts.row_group_meta->columns.size()));
        }
        node->scalar_array_layout = true;
        auto element_value_reader = std::make_unique<ScalarColumnReader>(
                array_layout.value_field,
                &(opts.row_group_meta->columns[array_layout.value_field->physical_column_index]), &TYPE_VARBINARY_DESC,
                opts);
        node->array_element_value_reader =
                std::make_unique<ListColumnReader>(typed_field, std::move(element_value_reader));
        return Status::OK();
    }

    node->scalar_array_layout = false;
    node->array_element_value_reader.reset();
    TypeDescriptor array_read_type = file_type;
    node->typed_value_read_type = std::make_unique<TypeDescriptor>(array_read_type);
    auto typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node->typed_value_read_type);
    if (!typed_reader_or.ok()) {
        return Status::InternalError(
                strings::Substitute("build variant typed array reader failed, path=$0, type=$1, err=$2", full_path,
                                    array_read_type.debug_string(), typed_reader_or.status().to_string()));
    }
    node->typed_value_reader = std::move(typed_reader_or).value();
    return Status::OK();
}

static Status collect_array_element_shredded_children(const ColumnReaderOptions& opts, const ParquetField* typed_field,
                                                      const ScalarArrayLayout& array_layout, ShreddedFieldNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("node should not be null");
    }
    if (array_layout.enabled || typed_field == nullptr || typed_field->children.empty()) {
        return Status::OK();
    }
    const ParquetField* element_field = &typed_field->children[0];
    if (element_field->type != ColumnType::STRUCT) {
        return Status::OK();
    }
    VariantNodeFields element_fields = find_variant_node_fields(element_field);
    const ParquetField* element_typed_value = element_fields.typed_value;
    if (element_typed_value != nullptr && element_typed_value->type == ColumnType::STRUCT) {
        VariantPath element_path;
        RETURN_IF_ERROR(collect_variant_shredded_fields(opts, element_typed_value, &element_path, &node->children));
    }
    return Status::OK();
}

// For array<object> with shredded child fields, the typed object reader is not a complete
// source of truth for each element: `element.value` holds the base variant payload, and
// shredded children only describe overlays on top of that base object. In this case we
// rewrite the array reader to read `element.value` so later reconstruction can merge
// base element values with child overlays correctly.
static void rewrite_object_array_reader_to_element_value_reader(const ColumnReaderOptions& opts,
                                                                const tparquet::ColumnChunk* column_chunks,
                                                                const ParquetField* typed_field,
                                                                const ScalarArrayLayout& array_layout,
                                                                ShreddedFieldNode* node) {
    if (node == nullptr) {
        return;
    }
    if (array_layout.enabled || node->children.empty() || node->typed_value_reader == nullptr ||
        typed_field == nullptr || typed_field->children.empty()) {
        return;
    }
    const ParquetField* elem_struct = &typed_field->children[0];
    const ParquetField* elem_value_field = nullptr;
    for (const auto& ch : elem_struct->children) {
        if (ch.name == "value" && ch.type == ColumnType::SCALAR) {
            elem_value_field = &ch;
            break;
        }
    }
    if (elem_value_field == nullptr) {
        return;
    }
    auto elem_reader = std::make_unique<ScalarColumnReader>(
            elem_value_field, &(column_chunks[elem_value_field->physical_column_index]), &TYPE_VARBINARY_DESC, opts);
    node->typed_value_read_type =
            std::make_unique<TypeDescriptor>(TypeDescriptor::create_array_type(TYPE_VARBINARY_DESC));
    node->typed_value_reader = std::make_unique<ListColumnReader>(typed_field, std::move(elem_reader));
}

// ARRAY typed_value needs a small pipeline instead of a single reader build:
// 1. Build the default typed reader first. This handles both regular typed arrays and
//    scalar-array layout (`element.{value, typed_value}`) where element.typed_value is
//    the primary typed source.
// 2. Collect shredded children from element.typed_value when the element itself is an
//    object-like struct. We need this before any rewrite because the presence of child
//    overlays determines whether the array reader can stay as-is.
// 3. If the array is an object array with shredded children, rewrite the reader to
//    ARRAY<VARBINARY> over element.value. In that case the typed object reader would be
//    incomplete by itself; reconstruction must merge base element.value with child overlays.
static Status build_array_readers_for_variant_node(const ColumnReaderOptions& opts,
                                                   const tparquet::ColumnChunk* column_chunks,
                                                   const ParquetField* typed_field, const std::string& full_path,
                                                   ShreddedFieldNode* node) {
    if (typed_field == nullptr || node == nullptr) {
        return Status::InvalidArgument("typed_field/node should not be null");
    }

    TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_field);
    ScalarArrayLayout array_layout = detect_scalar_array_layout(typed_field);
    RETURN_IF_ERROR(build_array_typed_value_reader(opts, typed_field, full_path, file_type, array_layout, node));
    RETURN_IF_ERROR(collect_array_element_shredded_children(opts, typed_field, array_layout, node));
    rewrite_object_array_reader_to_element_value_reader(opts, column_chunks, typed_field, array_layout, node);
    return Status::OK();
}

// SCALAR typed_value is simpler than ARRAY, but still worth centralizing:
// build the typed reader and fail immediately if reader creation fails.
static Status build_scalar_reader_for_variant_node(const ColumnReaderOptions& opts, const ParquetField* typed_field,
                                                   const std::string& full_path, ShreddedFieldNode* node) {
    if (typed_field == nullptr || node == nullptr) {
        return Status::InvalidArgument("typed_field/node should not be null");
    }

    TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_field);
    node->kind = ShreddedFieldNode::Kind::SCALAR;

    // Keep the read type on heap before creating reader. ScalarColumnReader stores
    // a raw pointer to TypeDescriptor, so a stack-local cannot be referenced.
    node->typed_value_read_type = std::make_unique<TypeDescriptor>(file_type);
    auto typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node->typed_value_read_type);
    if (!typed_reader_or.ok()) {
        return Status::InternalError(strings::Substitute("build variant typed reader failed, path=$0, type=$1, err=$2",
                                                         full_path, file_type.debug_string(),
                                                         typed_reader_or.status().to_string()));
    }
    node->typed_value_reader = std::move(typed_reader_or).value();
    return Status::OK();
}

Status collect_variant_shredded_fields(const ColumnReaderOptions& opts, const ParquetField* typed_group,
                                       VariantPath* current_path, std::vector<ShreddedFieldNode>* output) {
    if (typed_group == nullptr || current_path == nullptr || output == nullptr) {
        return Status::InvalidArgument("typed_group/current_path/output should not be null");
    }
    if (typed_group->type != ColumnType::STRUCT) {
        return Status::InvalidArgument("typed_value group must be struct");
    }

    const tparquet::ColumnChunk* column_chunks = opts.row_group_meta->columns.data();
    const size_t num_column_chunks = opts.row_group_meta->columns.size();
    for (const auto& field_node : typed_group->children) {
        if (field_node.type != ColumnType::STRUCT) {
            continue;
        }
        current_path->segments.emplace_back(VariantSegment::make_object(field_node.name));
        struct PathPopGuard {
            explicit PathPopGuard(VariantPath* p) : path(p) {}
            ~PathPopGuard() { path->segments.pop_back(); }
            VariantPath* path;
        } path_pop_guard(current_path);

        auto encoded_path = current_path->to_shredded_path();
        if (!encoded_path.has_value()) {
            return Status::InvalidArgument(
                    strings::Substitute("failed to encode shredded path at key=$0", field_node.name));
        }
        VariantNodeFields node_fields = find_variant_node_fields(&field_node);
        const ParquetField* value_field = node_fields.value;
        const ParquetField* typed_value_field = node_fields.typed_value;

        // Per the shredded variant spec, "value" is always the binary fallback
        // and "typed_value" is always the typed storage. Roles are identified by
        // name, not by physical type.
        const ParquetField* fallback_field = value_field;
        const ParquetField* typed_field = typed_value_field;
        ShreddedFieldNode node;
        node.name = field_node.name;
        node.full_path = std::move(*encoded_path);
        auto parsed_path = VariantPathParser::parse_shredded_path(std::string_view(node.full_path));
        if (!parsed_path.ok()) {
            return Status::InvalidArgument(strings::Substitute("failed to parse shredded path at key=$0, path=$1",
                                                               field_node.name, node.full_path));
        }
        node.parsed_full_path = std::move(parsed_path).value();
        if (fallback_field != nullptr) {
            if (fallback_field->physical_column_index < 0 ||
                static_cast<size_t>(fallback_field->physical_column_index) >= num_column_chunks) {
                return Status::InvalidArgument(strings::Substitute(
                        "variant shredded field '$0' has out-of-range physical_column_index $1 (num_chunks=$2)",
                        node.full_path, fallback_field->physical_column_index, num_column_chunks));
            }
            node.value_reader = std::make_unique<ScalarColumnReader>(
                    fallback_field, &(column_chunks[fallback_field->physical_column_index]), &TYPE_VARBINARY_DESC,
                    opts);
        }
        if (typed_field == nullptr) {
            output->emplace_back(std::move(node));
            continue;
        }

        if (typed_field->type == ColumnType::SCALAR) {
            RETURN_IF_ERROR(build_scalar_reader_for_variant_node(opts, typed_field, node.full_path, &node));
        } else if (typed_field->type == ColumnType::STRUCT) {
            RETURN_IF_ERROR(collect_variant_shredded_fields(opts, typed_field, current_path, &node.children));
        } else if (typed_field->type == ColumnType::ARRAY) {
            node.kind = ShreddedFieldNode::Kind::ARRAY;
            RETURN_IF_ERROR(
                    build_array_readers_for_variant_node(opts, column_chunks, typed_field, node.full_path, &node));
        }
        output->emplace_back(std::move(node));
    }
    return Status::OK();
}

bool any_reader_not_null(const std::map<std::string, std::unique_ptr<ColumnReader>>& readers) {
    for (const auto& pair : readers) {
        if (pair.second != nullptr) return true;
    }
    return false;
}

} // anonymous namespace

static bool is_readable_level_source(const ParquetField& field) {
    return field.type == ColumnType::SCALAR && variant_typed_desc_from_parquet_field(&field).type != TYPE_VARIANT;
}

// The first readable scalar leaf under `field` reached through structs only, depth first. A leaf under a list or
// map has one value per element, not per row, so it cannot give the row count.
static const ParquetField* find_readable_struct_leaf(const ParquetField& field) {
    if (is_readable_level_source(field)) {
        return &field;
    }
    if (field.type != ColumnType::STRUCT) {
        return nullptr;
    }
    for (const auto& child : field.children) {
        if (const ParquetField* leaf = find_readable_struct_leaf(child); leaf != nullptr) {
            return leaf;
        }
    }
    return nullptr;
}

const ParquetField* ColumnReaderFactory::find_struct_level_source(const ParquetField& field) {
    // Its values are read and dropped, so prefer a fixed-width one over a byte array.
    const ParquetField* byte_array = nullptr;
    for (const auto& child : field.children) {
        if (!is_readable_level_source(child)) {
            continue;
        }
        if (child.physical_type != tparquet::Type::BYTE_ARRAY &&
            child.physical_type != tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return &child;
        }
        if (byte_array == nullptr) {
            byte_array = &child;
        }
    }
    if (byte_array != nullptr) {
        return byte_array;
    }
    // No direct scalar subfield: a leaf under a struct subfield also has one value per row and carries this
    // struct's def levels. A struct whose stored subfields are only lists or maps keeps reading NULL.
    for (const auto& child : field.children) {
        if (child.type != ColumnType::STRUCT) {
            continue;
        }
        if (const ParquetField* leaf = find_readable_struct_leaf(child); leaf != nullptr) {
            return leaf;
        }
    }
    return nullptr;
}

Status VariantShreddedReadHints::add_path(std::string path) {
    ASSIGN_OR_RETURN(auto parsed_path, VariantPathParser::parse_shredded_path(std::string_view(path)));
    shredded_paths.emplace_back(std::move(path));
    parsed_shredded_paths.emplace_back(std::move(parsed_path));
    return Status::OK();
}

void VariantShreddedReadHints::clear() {
    shredded_paths.clear();
    parsed_shredded_paths.clear();
}

>>>>>>> 209630d ([BugFix] Fix Iceberg read follow-ups: unknown transforms in SHOW CREATE, TIME/TIMESTAMPTZ defaults, missing struct subfields, filters on default columns (#81311))
StatusOr<ColumnReaderPtr> ColumnReaderFactory::create(const ColumnReaderOptions& opts, const ParquetField* field,
                                                      const TypeDescriptor& col_type) {
    // We will only set a complex type in ParquetField
    if ((field->is_complex_type() || col_type.is_complex_type()) && !field->has_same_complex_type(col_type)) {
        return Status::InternalError(
                strings::Substitute("ParquetField '$0' file's type $1 is different from table's type $2", field->name,
                                    column_type_to_string(field->type), logical_type_to_string(col_type.type)));
    }
    if (field->type == ColumnType::ARRAY) {
        ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                         ColumnReaderFactory::create(opts, &field->children[0], col_type.children[0]));
        if (child_reader != nullptr) {
            return std::make_unique<ListColumnReader>(field, std::move(child_reader));
        } else {
            return nullptr;
        }
    } else if (field->type == ColumnType::MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        if (!col_type.children[0].is_unknown_type()) {
            ASSIGN_OR_RETURN(key_reader,
                             ColumnReaderFactory::create(opts, &(field->children[0]), col_type.children[0]));
        }
        if (!col_type.children[1].is_unknown_type()) {
            ASSIGN_OR_RETURN(value_reader,
                             ColumnReaderFactory::create(opts, &field->children[1], col_type.children[1]));
        }

        if (key_reader != nullptr || value_reader != nullptr) {
            return std::make_unique<MapColumnReader>(field, std::move(key_reader), std::move(value_reader));
        } else {
            return nullptr;
        }
    } else if (field->type == ColumnType::STRUCT) {
        if (col_type.type == LogicalType::TYPE_VARIANT) {
            return create_variant_column_reader(opts, field);
        }

        std::vector<int32_t> subfield_pos(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, subfield_pos);

        std::map<std::string, ColumnReaderPtr> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed; we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }
            ASSIGN_OR_RETURN(
                    ColumnReaderPtr child_reader,
                    ColumnReaderFactory::create(opts, &field->children[subfield_pos[i]], col_type.children[i]));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        // maybe struct subfield ColumnReader is null
        if (_has_valid_subfield_column_reader(children_readers)) {
            return std::make_unique<StructColumnReader>(field, std::move(children_readers));
        } else {
            return nullptr;
        }
    } else {
        return std::make_unique<ScalarColumnReader>(field, &opts.row_group_meta->columns[field->physical_column_index],
                                                    &col_type, opts);
    }
}

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create(const ColumnReaderOptions& opts, const ParquetField* field,
                                                      const TypeDescriptor& col_type,
                                                      const TIcebergSchemaField* lake_schema_field) {
    // We will only set a complex type in ParquetField
    if ((field->is_complex_type() || col_type.is_complex_type()) && !field->has_same_complex_type(col_type)) {
        return Status::InternalError(
                strings::Substitute("ParquetField '$0' file's type $1 is different from table's type $2", field->name,
                                    column_type_to_string(field->type), logical_type_to_string(col_type.type)));
    }
    DCHECK(lake_schema_field != nullptr);
    if (field->type == ColumnType::ARRAY) {
        const TIcebergSchemaField* element_schema = &lake_schema_field->children[0];
        ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                         ColumnReaderFactory::create(opts, &field->children[0], col_type.children[0], element_schema));
        if (child_reader != nullptr) {
            return std::make_unique<ListColumnReader>(field, std::move(child_reader));
        } else {
            return nullptr;
        }
    } else if (field->type == ColumnType::MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        const TIcebergSchemaField* key_lake_schema = &lake_schema_field->children[0];
        const TIcebergSchemaField* value_lake_schema = &lake_schema_field->children[1];

        if (!col_type.children[0].is_unknown_type()) {
            ASSIGN_OR_RETURN(key_reader, ColumnReaderFactory::create(opts, &(field->children[0]), col_type.children[0],
                                                                     key_lake_schema));
        }
        if (!col_type.children[1].is_unknown_type()) {
            ASSIGN_OR_RETURN(value_reader, ColumnReaderFactory::create(opts, &(field->children[1]),
                                                                       col_type.children[1], value_lake_schema));
        }

        if (key_reader != nullptr || value_reader != nullptr) {
            return std::make_unique<MapColumnReader>(field, std::move(key_reader), std::move(value_reader));
        } else {
            return nullptr;
        }
    } else if (field->type == ColumnType::STRUCT) {
        if (col_type.type == LogicalType::TYPE_VARIANT) {
            return create_variant_column_reader(opts, field);
        }

        std::vector<int32_t> subfield_pos(col_type.children.size());
        std::vector<const TIcebergSchemaField*> lake_schema_subfield(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, lake_schema_field, subfield_pos,
                                          lake_schema_subfield);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed; we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
<<<<<<< HEAD
=======
                const TIcebergSchemaField* lake_child = mapping.lake_children[i];
                if (lake_child != nullptr && lake_child->__isset.initial_default &&
                    !col_type.children[i].is_complex_type()) {
                    // A type the default parser does not support keeps reading NULL, as before
                    // defaults were filled; any other failure still fails the scan.
                    auto default_column = FormatScanContext::create_initial_default_column(col_type.children[i], true,
                                                                                           lake_child->initial_default);
                    if (default_column.ok()) {
                        default_columns.emplace(col_type.field_names[i], std::move(default_column).value());
                    } else if (!default_column.status().is_not_supported()) {
                        return default_column.status();
                    }
                }
>>>>>>> 209630d ([BugFix] Fix Iceberg read follow-ups: unknown transforms in SHOW CREATE, TIME/TIMESTAMPTZ defaults, missing struct subfields, filters on default columns (#81311))
                continue;
            }

            ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                             ColumnReaderFactory::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                         lake_schema_subfield[i]));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        // maybe struct subfield ColumnReader is null
<<<<<<< HEAD
        if (_has_valid_subfield_column_reader(children_readers)) {
            return std::make_unique<StructColumnReader>(field, std::move(children_readers));
        } else {
            return nullptr;
=======
        if (any_reader_not_null(children_readers)) {
            return std::make_unique<StructColumnReader>(field, std::move(children_readers), std::move(default_columns));
>>>>>>> 209630d ([BugFix] Fix Iceberg read follow-ups: unknown transforms in SHOW CREATE, TIME/TIMESTAMPTZ defaults, missing struct subfields, filters on default columns (#81311))
        }
        const ParquetField* level_source_field =
                lake_schema_field != nullptr ? find_struct_level_source(*field) : nullptr;
        if (level_source_field != nullptr) {
            // Iceberg subfields added after the file was written: keep the struct's nulls and fill the defaults.
            // Readers keep a pointer to their type, so it lives in the struct reader.
            auto level_source_type =
                    std::make_unique<TypeDescriptor>(variant_typed_desc_from_parquet_field(level_source_field));
            ASSIGN_OR_RETURN(ColumnReaderPtr level_source, create(opts, level_source_field, *level_source_type));
            return std::make_unique<StructColumnReader>(field, std::move(children_readers), std::move(default_columns),
                                                        std::move(level_source), std::move(level_source_type));
        }
        return nullptr;
    } else {
        return std::make_unique<ScalarColumnReader>(field, &opts.row_group_meta->columns[field->physical_column_index],
                                                    &col_type, opts);
    }
}

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                            const ParquetField* variant_field) {
    DCHECK(opts.row_group_meta != nullptr);
    DCHECK(variant_field->type == ColumnType::STRUCT);
    DCHECK(variant_field->children.size() >= 2);

    int metadata_index = -1;
    int value_index = -1;
    for (size_t i = 0; i < variant_field->children.size(); ++i) {
        const auto& child = variant_field->children[i];
        if (child.name == "metadata") {
            metadata_index = i;
        } else if (child.name == "value") {
            value_index = i;
        }
    }
    if (metadata_index == -1 || value_index == -1) {
        return Status::InvalidArgument("Variant type must have 'metadata' and 'value' fields");
    }

    const tparquet::ColumnChunk* column_chunks = opts.row_group_meta->columns.data();
    const ParquetField* metadata_field = &variant_field->children[metadata_index];
    const ParquetField* value_field = &variant_field->children[value_index];
    auto _metadata_reader = std::make_unique<ScalarColumnReader>(
            metadata_field, &(column_chunks[metadata_field->physical_column_index]), &TYPE_VARBINARY_DESC, opts);
    auto _value_reader = std::make_unique<ScalarColumnReader>(
            value_field, &(column_chunks[value_field->physical_column_index]), &TYPE_VARBINARY_DESC, opts);
    return std::make_unique<VariantColumnReader>(variant_field, std::move(_metadata_reader), std::move(_value_reader));
}

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create(ColumnReaderPtr ori_reader, const GlobalDictMap* dict,
                                                      SlotId slot_id, int64_t num_rows) {
    FAIL_POINT_TRIGGER_EXECUTE(parquet_reader_returns_global_dict_not_match_status, {
        return Status::GlobalDictNotMatch(
                fmt::format("SlotId: {}, Not dict encoded and not low rows on global dict column. ", slot_id));
    });

    if (ori_reader->get_column_parquet_field()->type == ColumnType::ARRAY) {
        ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                         ColumnReaderFactory::create(
                                 std::move((down_cast<ListColumnReader*>(ori_reader.get()))->get_element_reader()),
                                 dict, slot_id, num_rows));
        return std::make_unique<ListColumnReader>(ori_reader->get_column_parquet_field(), std::move(child_reader));
    } else {
        RawColumnReader* raw_reader = dynamic_cast<RawColumnReader*>(ori_reader.get());
        if (raw_reader == nullptr) {
            return Status::InternalError("Error on reader transform for low cardinality reader");
        }
        if (raw_reader->column_all_pages_dict_encoded()) {
            return std::make_unique<LowCardColumnReader>(*raw_reader, dict, slot_id);
        } else if (num_rows <= dict->size()) {
            return std::make_unique<LowRowsColumnReader>(*raw_reader, dict, slot_id);
        } else {
            return Status::GlobalDictNotMatch(
                    fmt::format("SlotId: {}, Not dict encoded and not low rows on global dict column. ", slot_id));
        }
    }
}

void ColumnReaderFactory::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                            bool case_sensitive, std::vector<int32_t>& pos) {
    DCHECK(field.type == ColumnType::STRUCT);
    // A subfield is matched by its field id when it has one, otherwise by its physical name, otherwise by its
    // name. field_ids / field_physical_names are either empty or hold one entry per child, with -1 / "" for a
    // subfield that has none.
    std::unordered_map<int32_t, size_t> field_id_2_pos;
    if (!col_type.field_ids.empty()) {
        for (size_t i = 0; i < field.children.size(); i++) {
            field_id_2_pos.emplace(field.children[i].field_id, i);
        }
    }
    std::unordered_map<std::string, size_t> field_name_2_pos;
    for (size_t i = 0; i < field.children.size(); i++) {
        const std::string& format_field_name = Utils::format_name(field.children[i].name, case_sensitive);
        field_name_2_pos.emplace(format_field_name, i);
    }

    for (size_t i = 0; i < col_type.children.size(); i++) {
        if (!col_type.field_ids.empty() && col_type.field_ids[i] != -1) {
            auto it = field_id_2_pos.find(col_type.field_ids[i]);
            pos[i] = it == field_id_2_pos.end() ? -1 : static_cast<int32_t>(it->second);
            continue;
        }

        const std::string& subfield_name =
                !col_type.field_physical_names.empty() && !col_type.field_physical_names[i].empty()
                        ? col_type.field_physical_names[i]
                        : col_type.field_names[i];
        auto it = field_name_2_pos.find(Utils::format_name(subfield_name, case_sensitive));
        pos[i] = it == field_name_2_pos.end() ? -1 : static_cast<int32_t>(it->second);
    }
}

void ColumnReaderFactory::get_subfield_pos_with_pruned_type(
        const ParquetField& field, const TypeDescriptor& col_type, bool case_sensitive,
        const TIcebergSchemaField* lake_schema_field, std::vector<int32_t>& pos,
        std::vector<const TIcebergSchemaField*>& lake_schema_subfield) {
    // For Struct type with schema change, we need to consider a subfield not existed situation.
    // When Iceberg adds a new struct subfield, the original parquet file does not contain the newly added subfield.
    std::unordered_map<std::string, const TIcebergSchemaField*> subfield_name_2_field_schema{};
    for (const auto& each : lake_schema_field->children) {
        std::string format_subfield_name = case_sensitive ? each.name : boost::algorithm::to_lower_copy(each.name);
        subfield_name_2_field_schema.emplace(format_subfield_name, &each);
    }

    std::unordered_map<int32_t, size_t> field_id_2_pos{};
    for (size_t i = 0; i < field.children.size(); i++) {
        field_id_2_pos.emplace(field.children[i].field_id, i);
    }
    for (size_t i = 0; i < col_type.children.size(); i++) {
        const auto& format_subfield_name =
                case_sensitive ? col_type.field_names[i] : boost::algorithm::to_lower_copy(col_type.field_names[i]);

        auto iceberg_it = subfield_name_2_field_schema.find(format_subfield_name);
        if (iceberg_it == subfield_name_2_field_schema.end()) {
            // This situation should not be happened, means table's struct subfield not existed in iceberg schema
            // Below code is defensive
            DCHECK(false) << "Struct subfield name: " + format_subfield_name + " not found in iceberg schema.";
            pos[i] = -1;
            lake_schema_subfield[i] = nullptr;
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        auto parquet_field_it = field_id_2_pos.find(field_id);
        if (parquet_field_it == field_id_2_pos.end()) {
            // Means newly added struct subfield not existed in an original parquet file, we put nullptr
            // column reader in children_reader, we will append the default value for this subfield later.
            pos[i] = -1;
            lake_schema_subfield[i] = nullptr;
            continue;
        }

        pos[i] = parquet_field_it->second;
        lake_schema_subfield[i] = iceberg_it->second;
    }
}

bool ColumnReaderFactory::_has_valid_subfield_column_reader(
        const std::map<std::string, std::unique_ptr<ColumnReader>>& children_readers) {
    for (const auto& pair : children_readers) {
        if (pair.second != nullptr) {
            return true;
        }
    }
    return false;
}

} // namespace starrocks::parquet
