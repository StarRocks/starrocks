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
#include "formats/utils.h"
#include "runtime/types.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::parquet {

DEFINE_FAIL_POINT(parquet_reader_returns_global_dict_not_match_status);

static TypeDescriptor _decimal_type_from_field(const ParquetField& field) {
    const int precision = field.precision > 0 ? field.precision : TypeDescriptor::MAX_DECIMAL8_PRECISION;
    const int scale = field.scale >= 0 ? field.scale : 0;
    if (precision <= TypeDescriptor::MAX_DECIMAL4_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, precision, scale);
    }
    if (precision <= TypeDescriptor::MAX_DECIMAL8_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, precision, scale);
    }
    return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, precision, scale);
}

static LogicalType _logical_type_from_integer(const tparquet::IntType& int_type) {
    const int bit_width = int_type.bitWidth;
    if (!int_type.isSigned) {
        if (bit_width <= 8) {
            return TYPE_SMALLINT;
        }
        if (bit_width <= 16) {
            return TYPE_INT;
        }
        return TYPE_BIGINT;
    }
    if (bit_width <= 8) {
        return TYPE_TINYINT;
    }
    if (bit_width <= 16) {
        return TYPE_SMALLINT;
    }
    if (bit_width <= 32) {
        return TYPE_INT;
    }
    return TYPE_BIGINT;
}

static TypeDescriptor _primitive_type_from_field(const ParquetField& field) {
    const auto& schema_element = field.schema_element;

    if (schema_element.__isset.logicalType) {
        const auto& logical_type = schema_element.logicalType;
        if (logical_type.__isset.STRING) {
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
        if (logical_type.__isset.JSON) {
            return TypeDescriptor::create_json_type();
        }
        if (logical_type.__isset.BSON) {
            return TypeDescriptor::create_json_type();
        }
        if (logical_type.__isset.DATE) {
            return TypeDescriptor(TYPE_DATE);
        }
        if (logical_type.__isset.TIME) {
            return TypeDescriptor(TYPE_TIME);
        }
        if (logical_type.__isset.TIMESTAMP) {
            return TypeDescriptor(TYPE_DATETIME);
        }
        if (logical_type.__isset.INTEGER) {
            return TypeDescriptor(_logical_type_from_integer(logical_type.INTEGER));
        }
        if (logical_type.__isset.DECIMAL) {
            return _decimal_type_from_field(field);
        }
        if (logical_type.__isset.UUID) {
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        }
    } else if (schema_element.__isset.converted_type) {
        switch (schema_element.converted_type) {
        case tparquet::ConvertedType::UTF8:
            return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        case tparquet::ConvertedType::JSON:
            return TypeDescriptor::create_json_type();
        case tparquet::ConvertedType::DATE:
            return TypeDescriptor(TYPE_DATE);
        case tparquet::ConvertedType::TIME_MILLIS:
        case tparquet::ConvertedType::TIME_MICROS:
            return TypeDescriptor(TYPE_TIME);
        case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        case tparquet::ConvertedType::TIMESTAMP_MICROS:
            return TypeDescriptor(TYPE_DATETIME);
        case tparquet::ConvertedType::DECIMAL:
            return _decimal_type_from_field(field);
        default:
            break;
        }
    }

    switch (field.physical_type) {
    case tparquet::Type::BOOLEAN:
        return TypeDescriptor(TYPE_BOOLEAN);
    case tparquet::Type::INT32:
        return TypeDescriptor(TYPE_INT);
    case tparquet::Type::INT64:
        return TypeDescriptor(TYPE_BIGINT);
    case tparquet::Type::INT96:
        return TypeDescriptor(TYPE_DATETIME);
    case tparquet::Type::FLOAT:
        return TypeDescriptor(TYPE_FLOAT);
    case tparquet::Type::DOUBLE:
        return TypeDescriptor(TYPE_DOUBLE);
    case tparquet::Type::BYTE_ARRAY:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    default:
        return TypeDescriptor(TYPE_UNKNOWN);
    }
}

static TypeDescriptor _type_desc_from_parquet_field(const ParquetField& field) {
    if (field.type == ColumnType::STRUCT) {
        std::vector<std::string> field_names;
        std::vector<TypeDescriptor> children;
        field_names.reserve(field.children.size());
        children.reserve(field.children.size());
        for (const auto& child : field.children) {
            field_names.emplace_back(child.name);
            children.emplace_back(_type_desc_from_parquet_field(child));
        }
        return TypeDescriptor::create_struct_type(field_names, children);
    }
    if (field.type == ColumnType::ARRAY) {
        if (field.children.empty()) {
            return TypeDescriptor::create_array_type(TypeDescriptor(TYPE_UNKNOWN));
        }
        return TypeDescriptor::create_array_type(_type_desc_from_parquet_field(field.children[0]));
    }
    if (field.type == ColumnType::MAP) {
        if (field.children.size() < 2) {
            return TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNKNOWN), TypeDescriptor(TYPE_UNKNOWN));
        }
        return TypeDescriptor::create_map_type(_type_desc_from_parquet_field(field.children[0]),
                                               _type_desc_from_parquet_field(field.children[1]));
    }
    return _primitive_type_from_field(field);
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
                continue;
            }

            ASSIGN_OR_RETURN(ColumnReaderPtr child_reader,
                             ColumnReaderFactory::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                         lake_schema_subfield[i]));
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

StatusOr<ColumnReaderPtr> ColumnReaderFactory::create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                            const ParquetField* variant_field) {
    DCHECK(opts.row_group_meta != nullptr);
    DCHECK(variant_field->type == ColumnType::STRUCT);
    DCHECK(variant_field->children.size() >= 2);

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

    ColumnReaderPtr typed_value_reader = nullptr;
    TypeDescriptor typed_value_type(TYPE_UNKNOWN);
    if (typed_value_index != -1) {
        const ParquetField* typed_value_field = &variant_field->children[typed_value_index];
        typed_value_type = _type_desc_from_parquet_field(*typed_value_field);
        if (!typed_value_type.is_unknown_type()) {
            ASSIGN_OR_RETURN(typed_value_reader,
                             ColumnReaderFactory::create(opts, typed_value_field, typed_value_type));
        }
    }

    return std::make_unique<VariantColumnReader>(variant_field, std::move(_metadata_reader), std::move(_value_reader),
                                                 std::move(typed_value_reader), std::move(typed_value_type));
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
