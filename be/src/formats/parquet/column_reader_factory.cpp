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

#include "base/failpoint/fail_point.h"
#include "column/variant_path_parser.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/utils.h"
#include "formats/utils.h"

namespace starrocks::parquet {

DEFINE_FAIL_POINT(parquet_reader_returns_global_dict_not_match_status);

namespace {

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
        if (any_reader_not_null(children_readers)) {
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
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, lake_schema_field,
                                          opts.file_meta_data->schema().exist_filed_id(), subfield_pos,
                                          lake_schema_subfield);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed; we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }

            ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                             ColumnReaderFactory::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                         lake_schema_subfield[i]));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        // maybe struct subfield ColumnReader is null
        if (any_reader_not_null(children_readers)) {
            return std::make_unique<StructColumnReader>(field, std::move(children_readers));
        } else {
            return nullptr;
        }
    } else {
        return std::make_unique<ScalarColumnReader>(field, &opts.row_group_meta->columns[field->physical_column_index],
                                                    &col_type, opts);
    }
}

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                            const ParquetField* variant_field,
                                                                            const VariantShreddedReadHints& hints) {
    DCHECK(opts.row_group_meta != nullptr);
    DCHECK(variant_field->type == ColumnType::STRUCT);
    DCHECK(variant_field->children.size() >= 2);

    VariantNodeFields top_fields = find_variant_node_fields(variant_field);
    if (top_fields.metadata == nullptr || top_fields.value == nullptr) {
        return Status::InvalidArgument("Variant type must have 'metadata' and 'value' fields");
    }

    const tparquet::ColumnChunk* column_chunks = opts.row_group_meta->columns.data();
    const ParquetField* metadata_field = top_fields.metadata;
    const ParquetField* value_field = top_fields.value;
    auto _metadata_reader = std::make_unique<ScalarColumnReader>(
            metadata_field, &(column_chunks[metadata_field->physical_column_index]), &TYPE_VARBINARY_DESC, opts);
    auto _value_reader = std::make_unique<ScalarColumnReader>(
            value_field, &(column_chunks[value_field->physical_column_index]), &TYPE_VARBINARY_DESC, opts);
    std::vector<ShreddedFieldNode> shredded_fields;
    ColumnReaderPtr root_typed_value_reader = nullptr;
    std::unique_ptr<TypeDescriptor> root_typed_value_type = nullptr;
    if (top_fields.typed_value != nullptr) {
        const ParquetField* typed_value_field = top_fields.typed_value;
        if (typed_value_field->type == ColumnType::STRUCT) {
            VariantPath root_path;
            RETURN_IF_ERROR(collect_variant_shredded_fields(opts, typed_value_field, &root_path, &shredded_fields));
        } else {
            TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_value_field);
            root_typed_value_type = std::make_unique<TypeDescriptor>(file_type);
            auto root_reader_or = ColumnReaderFactory::create(opts, typed_value_field, *root_typed_value_type);
            if (!root_reader_or.ok()) {
                return Status::InternalError(
                        strings::Substitute("build root variant typed reader failed, type=$0, err=$1",
                                            file_type.debug_string(), root_reader_or.status().to_string()));
            }
            root_typed_value_reader = std::move(root_reader_or).value();
        }
    }
    return std::make_unique<VariantColumnReader>(variant_field, std::move(_metadata_reader), std::move(_value_reader),
                                                 std::move(shredded_fields), std::move(hints.parsed_shredded_paths),
                                                 std::move(root_typed_value_reader), std::move(root_typed_value_type));
}

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create(ColumnReaderPtr raw_reader, const GlobalDictMap* dict,
                                                      SlotId slot_id, int64_t num_rows) {
    FAIL_POINT_TRIGGER_EXECUTE(parquet_reader_returns_global_dict_not_match_status, {
        return Status::GlobalDictNotMatch(
                fmt::format("SlotId: {}, Not dict encoded and not low rows on global dict column. ", slot_id));
    });

    if (raw_reader->get_column_parquet_field()->type == ColumnType::ARRAY) {
        ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                         ColumnReaderFactory::create(
                                 std::move((down_cast<ListColumnReader*>(raw_reader.get()))->get_element_reader()),
                                 dict, slot_id, num_rows));
        return std::make_unique<ListColumnReader>(raw_reader->get_column_parquet_field(), std::move(child_reader));
    } else {
        RawColumnReader* scalar_reader = dynamic_cast<RawColumnReader*>(raw_reader.get());
        if (scalar_reader == nullptr) {
            return Status::InternalError("Error on reader transform for low cardinality reader");
        }
        if (scalar_reader->column_all_pages_dict_encoded()) {
            return std::make_unique<LowCardColumnReader>(*scalar_reader, dict, slot_id);
        } else if (num_rows <= dict->size()) {
            return std::make_unique<LowRowsColumnReader>(*scalar_reader, dict, slot_id);
        } else {
            return Status::GlobalDictNotMatch(
                    fmt::format("SlotId: {}, Not dict encoded and not low rows on global dict column. ", slot_id));
        }
    }
}

void ColumnReaderFactory::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                            bool case_sensitive, std::vector<int32_t>& pos) {
    DCHECK(field.type == ColumnType::STRUCT);
    if (!col_type.field_ids.empty()) {
        std::unordered_map<int32_t, size_t> field_id_2_pos;
        for (size_t i = 0; i < field.children.size(); i++) {
            field_id_2_pos.emplace(field.children[i].field_id, i);
        }

        for (size_t i = 0; i < col_type.children.size(); i++) {
            auto it = field_id_2_pos.find(col_type.field_ids[i]);
            if (it == field_id_2_pos.end()) {
                pos[i] = -1;
                continue;
            }
            pos[i] = it->second;
        }
    } else {
        std::unordered_map<std::string, size_t> field_name_2_pos;
        for (size_t i = 0; i < field.children.size(); i++) {
            const std::string& format_field_name = Utils::format_name(field.children[i].name, case_sensitive);
            field_name_2_pos.emplace(format_field_name, i);
        }

        if (!col_type.field_physical_names.empty()) {
            for (size_t i = 0; i < col_type.children.size(); i++) {
                const std::string& formatted_physical_name =
                        Utils::format_name(col_type.field_physical_names[i], case_sensitive);

                auto it = field_name_2_pos.find(formatted_physical_name);
                if (it == field_name_2_pos.end()) {
                    pos[i] = -1;
                    continue;
                }
                pos[i] = it->second;
            }
        } else {
            for (size_t i = 0; i < col_type.children.size(); i++) {
                const std::string formatted_subfield_name = Utils::format_name(col_type.field_names[i], case_sensitive);

                auto it = field_name_2_pos.find(formatted_subfield_name);
                if (it == field_name_2_pos.end()) {
                    pos[i] = -1;
                    continue;
                }
                pos[i] = it->second;
            }
        }
    }
}

void ColumnReaderFactory::get_subfield_pos_with_pruned_type(
        const ParquetField& field, const TypeDescriptor& col_type, bool case_sensitive,
        const TIcebergSchemaField* lake_schema_field, bool parquet_has_field_id, std::vector<int32_t>& pos,
        std::vector<const TIcebergSchemaField*>& lake_schema_subfield) {
    // For Struct type with schema change, we need to consider a subfield not existed situation.
    // When Iceberg adds a new struct subfield, the original parquet file does not contain the newly added subfield.
    std::unordered_map<std::string, const TIcebergSchemaField*> subfield_name_2_field_schema{};
    for (const auto& each : lake_schema_field->children) {
        std::string format_subfield_name = case_sensitive ? each.name : boost::algorithm::to_lower_copy(each.name);
        subfield_name_2_field_schema.emplace(format_subfield_name, &each);
    }

    std::unordered_map<int32_t, size_t> field_id_2_pos{};
    std::unordered_map<std::string, size_t> field_name_2_pos{};
    for (size_t i = 0; i < field.children.size(); i++) {
        if (parquet_has_field_id) {
            field_id_2_pos.emplace(field.children[i].field_id, i);
        } else {
            field_name_2_pos.emplace(Utils::format_name(field.children[i].name, case_sensitive), i);
        }
    }
    for (size_t i = 0; i < col_type.children.size(); i++) {
        const auto schema_subfield_name =
                case_sensitive ? col_type.field_names[i] : boost::algorithm::to_lower_copy(col_type.field_names[i]);
        const auto parquet_subfield_name =
                !col_type.field_physical_names.empty()
                        ? Utils::format_name(col_type.field_physical_names[i], case_sensitive)
                        : schema_subfield_name;

        auto iceberg_it = subfield_name_2_field_schema.find(schema_subfield_name);
        if (iceberg_it == subfield_name_2_field_schema.end()) {
            // This situation should not be happened, means table's struct subfield not existed in iceberg schema
            // Below code is defensive
            DCHECK(false) << "Struct subfield name: " + schema_subfield_name + " not found in iceberg schema.";
            pos[i] = -1;
            lake_schema_subfield[i] = nullptr;
            continue;
        }

        if (parquet_has_field_id) {
            int32_t field_id = iceberg_it->second->field_id;
            auto parquet_field_it = field_id_2_pos.find(field_id);
            if (parquet_field_it == field_id_2_pos.end()) {
                pos[i] = -1;
                lake_schema_subfield[i] = nullptr;
                continue;
            }
            pos[i] = parquet_field_it->second;
            lake_schema_subfield[i] = iceberg_it->second;
        } else {
            auto parquet_field_it = field_name_2_pos.find(parquet_subfield_name);
            if (parquet_field_it == field_name_2_pos.end()) {
                pos[i] = -1;
                lake_schema_subfield[i] = nullptr;
                continue;
            }
            pos[i] = parquet_field_it->second;
            lake_schema_subfield[i] = iceberg_it->second;
        }
    }
}

} // namespace starrocks::parquet
