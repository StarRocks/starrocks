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

#include <unordered_set>

#include "base/failpoint/fail_point.h"
#include "column/variant_path_parser.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/utils.h"

namespace starrocks::parquet {

DEFINE_FAIL_POINT(parquet_reader_returns_global_dict_not_match_status);

namespace {

const TypeDescriptor& variant_type_desc() {
    static const TypeDescriptor k_variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    return k_variant_type;
}

TypeDescriptor variant_decimal_desc_from_schema(const ParquetField* field) {
    const int precision = field->precision;
    const int scale = field->scale;
    if (precision <= 0 || scale < 0 || scale > precision) {
        return variant_type_desc();
    }
    TypeDescriptor desc = TypeDescriptor::promote_decimal_type(precision, scale);
    if (!desc.is_decimal_type()) {
        return variant_type_desc();
    }
    return desc;
}

TypeDescriptor variant_integer_desc_from_bitwidth(int bit_width, bool is_signed) {
    if (is_signed) {
        switch (bit_width) {
        case 8:
            return TYPE_TINYINT_DESC;
        case 16:
            return TYPE_SMALLINT_DESC;
        case 32:
            return TYPE_INT_DESC;
        case 64:
            return TYPE_BIGINT_DESC;
        default:
            return variant_type_desc();
        }
    }
    // StarRocks has no native UINT types, widen to a safe signed type where possible.
    switch (bit_width) {
    case 8:
        return TYPE_SMALLINT_DESC;
    case 16:
        return TYPE_INT_DESC;
    case 32:
        return TYPE_BIGINT_DESC;
    case 64:
    default:
        // UINT64 cannot be losslessly represented by BIGINT.
        return variant_type_desc();
    }
}

TypeDescriptor variant_scalar_typed_desc_from_parquet_field(const ParquetField* field) {
    DCHECK(field != nullptr);
    const auto& schema = field->schema_element;

    if (schema.__isset.logicalType) {
        const auto& logical_type = schema.logicalType;
        if (logical_type.__isset.DECIMAL) {
            return variant_decimal_desc_from_schema(field);
        }
        if (logical_type.__isset.DATE) {
            return TYPE_DATE_DESC;
        }
        if (logical_type.__isset.TIME) {
            return TYPE_TIME_DESC;
        }
        if (logical_type.__isset.TIMESTAMP) {
            return TYPE_DATETIME_DESC;
        }
        if (logical_type.__isset.INTEGER) {
            return variant_integer_desc_from_bitwidth(logical_type.INTEGER.bitWidth, logical_type.INTEGER.isSigned);
        }
        if (logical_type.__isset.STRING || logical_type.__isset.ENUM || logical_type.__isset.JSON) {
            return TYPE_VARCHAR_DESC;
        }
        if (logical_type.__isset.BSON || logical_type.__isset.UUID) {
            return TYPE_VARBINARY_DESC;
        }
    }

    if (schema.__isset.converted_type) {
        switch (schema.converted_type) {
        case tparquet::ConvertedType::UTF8:
        case tparquet::ConvertedType::ENUM:
        case tparquet::ConvertedType::JSON:
            return TYPE_VARCHAR_DESC;
        case tparquet::ConvertedType::BSON:
        case tparquet::ConvertedType::INTERVAL:
            return TYPE_VARBINARY_DESC;
        case tparquet::ConvertedType::DECIMAL:
            return variant_decimal_desc_from_schema(field);
        case tparquet::ConvertedType::DATE:
            return TYPE_DATE_DESC;
        case tparquet::ConvertedType::TIME_MILLIS:
        case tparquet::ConvertedType::TIME_MICROS:
            return TYPE_TIME_DESC;
        case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        case tparquet::ConvertedType::TIMESTAMP_MICROS:
            return TYPE_DATETIME_DESC;
        case tparquet::ConvertedType::INT_8:
            return TYPE_TINYINT_DESC;
        case tparquet::ConvertedType::INT_16:
            return TYPE_SMALLINT_DESC;
        case tparquet::ConvertedType::INT_32:
            return TYPE_INT_DESC;
        case tparquet::ConvertedType::INT_64:
            return TYPE_BIGINT_DESC;
        case tparquet::ConvertedType::UINT_8:
            return TYPE_SMALLINT_DESC;
        case tparquet::ConvertedType::UINT_16:
            return TYPE_INT_DESC;
        case tparquet::ConvertedType::UINT_32:
            return TYPE_BIGINT_DESC;
        case tparquet::ConvertedType::UINT_64:
            return variant_type_desc();
        default:
            break;
        }
    }

    switch (field->physical_type) {
    case tparquet::Type::BOOLEAN:
        return TYPE_BOOLEAN_DESC;
    case tparquet::Type::INT32:
        return TYPE_INT_DESC;
    case tparquet::Type::INT64:
        return TYPE_BIGINT_DESC;
    case tparquet::Type::FLOAT:
        return TYPE_FLOAT_DESC;
    case tparquet::Type::DOUBLE:
        return TYPE_DOUBLE_DESC;
    case tparquet::Type::BYTE_ARRAY:
        return TYPE_VARBINARY_DESC;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return TYPE_VARBINARY_DESC;
    default:
        return variant_type_desc();
    }
}

bool is_variant_preferred_type_compatible(const TypeDescriptor& preferred, const TypeDescriptor& file_type) {
    if (preferred == file_type) {
        return true;
    }
    if (preferred.type == LogicalType::TYPE_VARIANT) {
        return true;
    }
    if (preferred.is_decimal_type() && file_type.is_decimal_type()) {
        return true;
    }
    if (preferred.is_integer_type() && file_type.is_integer_type()) {
        return true;
    }
    if (preferred.is_date_type() && file_type.is_date_type()) {
        return true;
    }
    if (preferred.is_string_type() && file_type.is_string_type()) {
        return true;
    }
    if (preferred.type == LogicalType::TYPE_JSON && file_type.type == LogicalType::TYPE_VARCHAR) {
        return true;
    }
    return false;
}

struct NormalizedVariantShreddedReadHints {
    std::vector<std::string> shredded_paths;
    std::unordered_map<std::string, TypeDescriptor> preferred_types_by_path;
    bool strict_preferred_type = false;
};

NormalizedVariantShreddedReadHints normalize_variant_shredded_read_hints(const VariantShreddedReadHints& hints) {
    NormalizedVariantShreddedReadHints out;
    out.strict_preferred_type = hints.strict_preferred_type;

    std::unordered_set<std::string> seen_paths;
    seen_paths.reserve(hints.path_type_hints.size());
    out.shredded_paths.reserve(hints.path_type_hints.size());
    out.preferred_types_by_path.reserve(hints.path_type_hints.size());
    for (const auto& hint : hints.path_type_hints) {
        if (hint.path.empty()) {
            continue;
        }
        if (seen_paths.emplace(hint.path).second) {
            out.shredded_paths.emplace_back(hint.path);
        }
        if (!hint.preferred_type.is_unknown_type()) {
            out.preferred_types_by_path[hint.path] = hint.preferred_type;
        }
    }
    return out;
}

TypeDescriptor variant_typed_desc_from_parquet_field(const ParquetField* field) {
    DCHECK(field != nullptr);
    const TypeDescriptor& k_variant_type = variant_type_desc();
    switch (field->type) {
    case ColumnType::ARRAY:
        if (field->children.empty()) {
            return TypeDescriptor::create_array_type(k_variant_type);
        }
        return TypeDescriptor::create_array_type(variant_typed_desc_from_parquet_field(&field->children[0]));
    case ColumnType::MAP:
        if (field->children.size() < 2) {
            return k_variant_type;
        }
        return TypeDescriptor::create_map_type(variant_typed_desc_from_parquet_field(&field->children[0]),
                                               variant_typed_desc_from_parquet_field(&field->children[1]));
    case ColumnType::STRUCT: {
        std::vector<std::string> field_names;
        std::vector<TypeDescriptor> children;
        field_names.reserve(field->children.size());
        children.reserve(field->children.size());
        for (const auto& child : field->children) {
            field_names.emplace_back(child.name);
            children.emplace_back(variant_typed_desc_from_parquet_field(&child));
        }
        return TypeDescriptor::create_struct_type(std::move(field_names), std::move(children));
    }
    case ColumnType::SCALAR:
        return variant_scalar_typed_desc_from_parquet_field(field);
    }
    return k_variant_type;
}

Status collect_variant_shredded_fields(const ColumnReaderOptions& opts, const ParquetField* typed_group,
                                       VariantPath* current_path, const NormalizedVariantShreddedReadHints& hints,
                                       std::vector<ShreddedFieldNode>* output) {
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
        const ParquetField* value_field = nullptr;
        const ParquetField* typed_value_field = nullptr;
        for (const auto& child : field_node.children) {
            if (child.name == "value") {
                value_field = &child;
            } else if (child.name == "typed_value") {
                typed_value_field = &child;
            }
        }

        // Per the shredded variant spec, "value" is always the binary fallback
        // and "typed_value" is always the typed storage. Roles are identified by
        // name, not by physical type.
        const ParquetField* fallback_field = value_field;
        const ParquetField* typed_field = typed_value_field;
        ShreddedFieldNode node;
        node.name = field_node.name;
        node.full_path = std::move(*encoded_path);
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
            node.typed_kind = ShreddedTypedKind::SCALAR;
            TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_field);
            if (file_type.type == LogicalType::TYPE_UNKNOWN) {
                return Status::InternalError(
                        strings::Substitute("variant typed reader got unknown type, path=$0, parquet_physical=$1",
                                            node.full_path, ::tparquet::to_string(typed_field->physical_type)));
            }
            TypeDescriptor selected_type = file_type;
            auto preferred_it = hints.preferred_types_by_path.find(node.full_path);
            const bool has_preferred = preferred_it != hints.preferred_types_by_path.end();
            if (has_preferred) {
                if (!is_variant_preferred_type_compatible(preferred_it->second, file_type)) {
                    if (hints.strict_preferred_type) {
                        return Status::InvalidArgument(strings::Substitute(
                                "incompatible preferred variant type, path=$0, preferred=$1, file=$2", node.full_path,
                                preferred_it->second.debug_string(), file_type.debug_string()));
                    }
                } else {
                    selected_type = preferred_it->second;
                }
            }

            // Keep the read type on heap before creating reader. ScalarColumnReader stores
            // a raw pointer to TypeDescriptor, so selected_type local cannot be referenced.
            node.typed_value_read_type = std::make_unique<TypeDescriptor>(selected_type);
            auto typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node.typed_value_read_type);
            if (!typed_reader_or.ok() && has_preferred && !hints.strict_preferred_type && selected_type != file_type) {
                selected_type = file_type;
                *node.typed_value_read_type = selected_type;
                typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node.typed_value_read_type);
            }
            if (!typed_reader_or.ok()) {
                if (node.value_reader != nullptr) {
                    // Typed projection is best-effort for shredded scalar paths.
                    // If typed reader creation fails, keep fallback binary reader and degrade to fallback-only.
                    node.typed_value_reader.reset();
                    node.typed_value_read_type.reset();
                } else {
                    return Status::InternalError(strings::Substitute(
                            "build variant typed reader failed, path=$0, type=$1, err=$2", node.full_path,
                            selected_type.debug_string(), typed_reader_or.status().to_string()));
                }
            } else {
                node.typed_value_reader = std::move(typed_reader_or).value();
            }
        } else if (typed_field->type == ColumnType::STRUCT) {
            RETURN_IF_ERROR(collect_variant_shredded_fields(opts, typed_field, current_path, hints, &node.children));
        } else if (typed_field->type == ColumnType::ARRAY) {
            node.typed_kind = ShreddedTypedKind::ARRAY;
            TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_field);
            TypeDescriptor selected_type = file_type;
            auto preferred_it = hints.preferred_types_by_path.find(node.full_path);
            const bool has_preferred = preferred_it != hints.preferred_types_by_path.end();
            if (has_preferred) {
                if (!is_variant_preferred_type_compatible(preferred_it->second, file_type)) {
                    if (hints.strict_preferred_type) {
                        return Status::InvalidArgument(strings::Substitute(
                                "incompatible preferred variant array type, path=$0, preferred=$1, file=$2",
                                node.full_path, preferred_it->second.debug_string(), file_type.debug_string()));
                    }
                } else {
                    selected_type = preferred_it->second;
                }
            }
            // Detect scalar-array shredding wrapper:
            // list.element is encoded as struct {value, typed_value(scalar)}.
            // Keep list reconstruction on the ARRAY node, but unwrap element reader to typed_value.
            bool use_scalar_array_wrapper = false;
            const ParquetField* scalar_array_typed_value_field = nullptr;
            TypeDescriptor scalar_array_element_type;
            if (!typed_field->children.empty()) {
                const ParquetField* element_field = &typed_field->children[0];
                if (element_field->type == ColumnType::STRUCT) {
                    const ParquetField* elem_value_field = nullptr;
                    const ParquetField* elem_typed_value_field = nullptr;
                    for (const auto& child : element_field->children) {
                        if (child.name == "value") {
                            elem_value_field = &child;
                        } else if (child.name == "typed_value") {
                            elem_typed_value_field = &child;
                        }
                    }
                    // If this is a scalar array shredding format (struct {value, typed_scalar, ...}),
                    // unwrap to typed_value for element reader. The struct may have extra fields.
                    if (elem_value_field != nullptr && elem_typed_value_field != nullptr &&
                        elem_typed_value_field->type != ColumnType::STRUCT &&
                        elem_typed_value_field->type != ColumnType::ARRAY) {
                        use_scalar_array_wrapper = true;
                        scalar_array_typed_value_field = elem_typed_value_field;
                        scalar_array_element_type = variant_typed_desc_from_parquet_field(elem_typed_value_field);
                    }
                }
            }

            // Keep the read type on heap before creating reader. ScalarColumnReader stores
            // a raw pointer to TypeDescriptor, so selected_type local cannot be referenced.
            if (use_scalar_array_wrapper) {
                TypeDescriptor array_read_type = TypeDescriptor::create_array_type(scalar_array_element_type);
                node.typed_value_read_type = std::make_unique<TypeDescriptor>(array_read_type);
                // Use the element type stored inside the heap-allocated array type (children[0]),
                // not the local scalar_array_element_type. ScalarColumnReader captures a raw pointer
                // to the TypeDescriptor; using the local would produce a dangling pointer after
                // collect_variant_shredded_fields returns (or after any recursive call overwrites
                // the stack frame). node.typed_value_read_type->children[0] lives on the heap and
                // is stable for the lifetime of the ShreddedFieldNode.
                const TypeDescriptor& heap_element_type = node.typed_value_read_type->children[0];
                auto element_reader_or =
                        ColumnReaderFactory::create(opts, scalar_array_typed_value_field, heap_element_type);
                if (!element_reader_or.ok()) {
                    return Status::InternalError(strings::Substitute(
                            "build variant typed scalar-array element reader failed, path=$0, type=$1, err=$2",
                            node.full_path, scalar_array_element_type.debug_string(),
                            element_reader_or.status().to_string()));
                }
                node.typed_value_reader =
                        std::make_unique<ListColumnReader>(typed_field, std::move(element_reader_or).value());
            } else {
                TypeDescriptor array_read_type = selected_type;
                node.typed_value_read_type = std::make_unique<TypeDescriptor>(array_read_type);
                auto typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node.typed_value_read_type);
                if (!typed_reader_or.ok() && has_preferred && !hints.strict_preferred_type &&
                    array_read_type != file_type) {
                    array_read_type = file_type;
                    *node.typed_value_read_type = array_read_type;
                    typed_reader_or = ColumnReaderFactory::create(opts, typed_field, *node.typed_value_read_type);
                }
                if (!typed_reader_or.ok()) {
                    return Status::InternalError(strings::Substitute(
                            "build variant typed array reader failed, path=$0, type=$1, err=$2", node.full_path,
                            array_read_type.debug_string(), typed_reader_or.status().to_string()));
                }
                node.typed_value_reader = std::move(typed_reader_or).value();
            }

            // Walk the typed_value of the array element (list.element.typed_value) to get per-element shredded fields.
            // Only do this for object arrays where element_typed_value is a STRUCT with nested fields.
            // Array element overlays are built against each element root, so paths should be relative.
            if (use_scalar_array_wrapper) {
                // Wrapper array uses scalar typed_value elements, so there are no nested shredded sub-paths.
            } else if (!typed_field->children.empty()) {
                const ParquetField* element_field = &typed_field->children[0];
                const ParquetField* element_typed_value = nullptr;
                if (element_field->type == ColumnType::STRUCT) {
                    for (const auto& child : element_field->children) {
                        if (child.name == "typed_value") {
                            element_typed_value = &child;
                            break;
                        }
                    }
                }
                // Only collect children for object arrays (element_typed_value is STRUCT)
                if (element_typed_value != nullptr && element_typed_value->type == ColumnType::STRUCT) {
                    VariantPath element_path;
                    RETURN_IF_ERROR(collect_variant_shredded_fields(opts, element_typed_value, &element_path, hints,
                                                                    &node.children));
                }
            }

            // For object arrays (non-wrapper ARRAY with shredded children), the full typed_value_reader
            // reads all nested element physical columns, which overlaps with node.children.
            // Replace it with a lightweight reader that only reads the element's remain-binary to get
            // array structure (offsets/counts). _rebuild_array_overlay Path 1 only uses the offsets.
            if (!use_scalar_array_wrapper && !node.children.empty() && node.typed_value_reader != nullptr &&
                !typed_field->children.empty()) {
                const ParquetField* elem_struct = &typed_field->children[0];
                const ParquetField* elem_value_field = nullptr;
                for (const auto& ch : elem_struct->children) {
                    if (ch.name == "value" && ch.type == ColumnType::SCALAR) {
                        elem_value_field = &ch;
                        break;
                    }
                }
                if (elem_value_field != nullptr) {
                    auto elem_reader = std::make_unique<ScalarColumnReader>(
                            elem_value_field, &(column_chunks[elem_value_field->physical_column_index]),
                            &TYPE_VARBINARY_DESC, opts);
                    node.typed_value_read_type =
                            std::make_unique<TypeDescriptor>(TypeDescriptor::create_array_type(TYPE_VARBINARY_DESC));
                    node.typed_value_reader = std::make_unique<ListColumnReader>(typed_field, std::move(elem_reader));
                }
            }
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
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, lake_schema_field, subfield_pos,
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

    NormalizedVariantShreddedReadHints normalized_hints = normalize_variant_shredded_read_hints(hints);

    int metadata_index = -1;
    int value_index = -1;
    int typed_value_index = -1;
    for (size_t i = 0; i < variant_field->children.size(); ++i) {
        const auto& child = variant_field->children[i];
        if (child.name == "metadata") {
            metadata_index = i;
        } else if (child.name == "value") {
            value_index = i;
        } else if (child.name == "typed_value") {
            typed_value_index = i;
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
    std::vector<ShreddedFieldNode> shredded_fields;
    ColumnReaderPtr root_typed_value_reader = nullptr;
    std::unique_ptr<TypeDescriptor> root_typed_value_type = nullptr;
    if (typed_value_index != -1) {
        const ParquetField* typed_value_field = &variant_field->children[typed_value_index];
        if (typed_value_field->type == ColumnType::STRUCT) {
            VariantPath root_path;
            RETURN_IF_ERROR(collect_variant_shredded_fields(opts, typed_value_field, &root_path, normalized_hints,
                                                            &shredded_fields));
        } else {
            TypeDescriptor file_type = variant_typed_desc_from_parquet_field(typed_value_field);
            if (!file_type.is_unknown_type()) {
                root_typed_value_type = std::make_unique<TypeDescriptor>(file_type);
                auto root_reader_or = ColumnReaderFactory::create(opts, typed_value_field, *root_typed_value_type);
                if (!root_reader_or.ok()) {
                    if (value_field != nullptr) {
                        // Root typed projection is best-effort when fallback binary exists.
                        // Keep fallback value path and skip root typed reader on failures.
                        root_typed_value_type.reset();
                    } else {
                        return Status::InternalError(
                                strings::Substitute("build root variant typed reader failed, type=$0, err=$1",
                                                    file_type.debug_string(), root_reader_or.status().to_string()));
                    }
                } else {
                    root_typed_value_reader = std::move(root_reader_or).value();
                }
            }
        }
    }
    return std::make_unique<VariantColumnReader>(variant_field, std::move(_metadata_reader), std::move(_value_reader),
                                                 std::move(shredded_fields), std::move(normalized_hints.shredded_paths),
                                                 std::move(root_typed_value_reader), std::move(root_typed_value_type));
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

} // namespace starrocks::parquet
