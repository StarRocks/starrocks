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

<<<<<<< HEAD
=======
#include "base/failpoint/fail_point.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/variant_path_parser.h"
>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/utils.h"
<<<<<<< HEAD
#include "util/failpoint/fail_point.h"
=======
#include "runtime/mem_pool.h"
#include "runtime/type_info_allocator_adapter.h"
#include "types/type_info.h"
>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))

namespace starrocks::parquet {

// A one-row nullable column holding an Iceberg initial-default, parsed with the subfield's type.
static StatusOr<ColumnPtr> create_default_column(const TypeDescriptor& type, const std::string& default_value) {
    auto type_info = get_type_info(type);
    if (type_info == nullptr) {
        return Status::NotSupported(fmt::format("unsupported initial-default type {}", type.debug_string()));
    }
    MemPool mem_pool;
    TypeInfoAllocator type_info_allocator = make_type_info_allocator(&mem_pool);
    Datum datum;
    RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, default_value, &type_info_allocator));
    auto column = ColumnHelper::create_column(type, true);
    column->append_datum(datum);
    return column;
}

DEFINE_FAIL_POINT(parquet_reader_returns_global_dict_not_match_status);

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

<<<<<<< HEAD
        std::vector<int32_t> subfield_pos(col_type.children.size());
        std::vector<const TIcebergSchemaField*> lake_schema_subfield(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, lake_schema_field, subfield_pos,
                                          lake_schema_subfield);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
=======
        std::map<std::string, ColumnReaderPtr> children_readers;
        std::map<std::string, ColumnPtr> default_columns;
>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed; we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                const TIcebergSchemaField* lake_child = mapping.lake_children[i];
                if (lake_child != nullptr && lake_child->__isset.initial_default &&
                    !col_type.children[i].is_complex_type()) {
                    // A type the default parser does not support (e.g. TIME) keeps reading NULL, as before
                    // defaults were filled; any other failure still fails the scan.
                    auto default_column = create_default_column(col_type.children[i], lake_child->initial_default);
                    if (default_column.ok()) {
                        default_columns.emplace(col_type.field_names[i], std::move(default_column).value());
                    } else if (!default_column.status().is_not_supported()) {
                        return default_column.status();
                    }
                }
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
=======
        if (any_reader_not_null(children_readers)) {
            return std::make_unique<StructColumnReader>(field, std::move(children_readers), std::move(default_columns));
>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))
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

<<<<<<< HEAD
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
=======
StructSubfieldMapping ColumnReaderFactory::resolve_struct_subfields(const ParquetField& field,
                                                                    const TypeDescriptor& col_type, bool case_sensitive,
                                                                    const TIcebergSchemaField* lake_schema_field,
                                                                    bool parquet_has_field_id) {
    DCHECK(field.has_struct_layout());
    const size_t num_children = col_type.children.size();
    StructSubfieldMapping mapping;
    mapping.parquet_children.assign(num_children, nullptr);
    mapping.lake_children.assign(num_children, nullptr);

    std::unordered_map<int32_t, const ParquetField*> parquet_by_id;
    std::unordered_map<std::string, const ParquetField*> parquet_by_name;
    for (const auto& child : field.children) {
        parquet_by_id.emplace(child.field_id, &child);
        parquet_by_name.emplace(Utils::format_name(child.name, case_sensitive), &child);
    }
    auto find_by_id = [&](int32_t id) -> const ParquetField* {
        auto it = parquet_by_id.find(id);
        return it != parquet_by_id.end() ? it->second : nullptr;
    };
    auto find_by_name = [&](const std::string& name) -> const ParquetField* {
        auto it = parquet_by_name.find(Utils::format_name(name, case_sensitive));
        return it != parquet_by_name.end() ? it->second : nullptr;
    };
    // The name the file stores the i-th type child under: its physical name if there is one, else its field name.
    // The vectors may be shorter than the children (or padded with ""), so a missing entry means "no physical name".
    auto parquet_name = [&](size_t i) -> const std::string& {
        if (i < col_type.field_physical_names.size() && !col_type.field_physical_names[i].empty()) {
            return col_type.field_physical_names[i];
        }
        return col_type.field_names[i];
    };

    if (lake_schema_field != nullptr) {
        // For struct type with schema change, a subfield may not exist in the file: when Iceberg adds a new struct
        // subfield, the original parquet file does not contain it.
        std::unordered_map<std::string, const TIcebergSchemaField*> lake_by_name;
        for (const auto& each : lake_schema_field->children) {
            lake_by_name.emplace(Utils::format_name(each.name, case_sensitive), &each);
        }
        for (size_t i = 0; i < num_children; i++) {
            auto lake_it = lake_by_name.find(Utils::format_name(col_type.field_names[i], case_sensitive));
            if (lake_it == lake_by_name.end()) {
                // The table's struct subfield is not in the Iceberg schema; should not happen, defensive.
                DCHECK(false) << "Struct subfield name: " << col_type.field_names[i] << " not found in iceberg schema.";
                continue;
            }
            const ParquetField* parquet_child =
                    parquet_has_field_id ? find_by_id(lake_it->second->field_id) : find_by_name(parquet_name(i));
            // Keep the lake child even when the file lacks the subfield: its initial-default fills the subfield.
            mapping.parquet_children[i] = parquet_child;
            mapping.lake_children[i] = lake_it->second;
        }
        return mapping;
    }

    for (size_t i = 0; i < num_children; i++) {
        // An id of -1 (or a vector shorter than the children) means the type has no id for this child.
        if (i < col_type.field_ids.size() && col_type.field_ids[i] != -1) {
            mapping.parquet_children[i] = find_by_id(col_type.field_ids[i]);
        } else {
            mapping.parquet_children[i] = find_by_name(parquet_name(i));
>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))
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
