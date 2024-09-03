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

#include "formats/parquet/column_reader.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <map>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr_context.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/parquet_types.h"
#include "simd/batch_run_counter.h"
#include "storage/column_or_predicate.h"
#include "storage/column_predicate.h"

namespace starrocks::parquet {

Status ColumnDictFilterContext::rewrite_conjunct_ctxs_to_predicate(StoredColumnReader* reader,
                                                                   bool* is_group_filtered) {
    // create dict value chunk for evaluation.
    ColumnPtr dict_value_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    RETURN_IF_ERROR(reader->get_dict_values(dict_value_column.get()));
    // append a null value to check if null is ok or not.
    dict_value_column->append_default();

    ColumnPtr result_column = dict_value_column;
    for (int32_t i = sub_field_path.size() - 1; i >= 0; i--) {
        if (!result_column->is_nullable()) {
            result_column =
                    NullableColumn::create(std::move(result_column), NullColumn::create(result_column->size(), 0));
        }
        Columns columns;
        columns.emplace_back(result_column);
        std::vector<std::string> field_names;
        field_names.emplace_back(sub_field_path[i]);
        result_column = StructColumn::create(std::move(columns), std::move(field_names));
    }

    ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
    dict_value_chunk->append_column(result_column, slot_id);
    Filter filter(dict_value_column->size(), 1);
    int dict_values_after_filter = 0;
    ASSIGN_OR_RETURN(dict_values_after_filter,
                     ExecNode::eval_conjuncts_into_filter(conjunct_ctxs, dict_value_chunk.get(), &filter));

    // dict column is empty after conjunct eval, file group can be skipped
    if (dict_values_after_filter == 0) {
        *is_group_filtered = true;
        return Status::OK();
    }

    // ---------
    // get dict codes according to dict values pos.
    std::vector<int32_t> dict_codes;
    BatchRunCounter<32> batch_run(filter.data(), 0, filter.size() - 1);
    BatchCount batch = batch_run.next_batch();
    int index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            for (int32_t i = 0; i < batch.length; i++) {
                dict_codes.emplace_back(index + i);
            }
        } else if (batch.NoneSet()) {
            // do nothing
        } else {
            for (int32_t i = 0; i < batch.length; i++) {
                if (filter[index + i]) {
                    dict_codes.emplace_back(index + i);
                }
            }
        }
        index += batch.length;
        batch = batch_run.next_batch();
    }

    bool null_is_ok = filter[filter.size() - 1] == 1;

    // eq predicate is faster than in predicate
    // TODO: improve not eq and not in
    if (dict_codes.size() == 0) {
        predicate = nullptr;
    } else if (dict_codes.size() == 1) {
        predicate = obj_pool.add(
                new_column_eq_predicate(get_type_info(kDictCodeFieldType), slot_id, std::to_string(dict_codes[0])));
    } else {
        predicate = obj_pool.add(new_dictionary_code_in_predicate(get_type_info(kDictCodeFieldType), slot_id,
                                                                  dict_codes, dict_value_column->size()));
    }

    // deal with if NULL works or not.
    if (null_is_ok) {
        ColumnPredicate* result = nullptr;
        ColumnPredicate* is_null_pred =
                obj_pool.add(new_column_null_predicate(get_type_info(kDictCodeFieldType), slot_id, true));

        if (predicate != nullptr) {
            ColumnOrPredicate* or_pred =
                    obj_pool.add(new ColumnOrPredicate(get_type_info(kDictCodeFieldType), slot_id));
            or_pred->add_child(predicate);
            or_pred->add_child(is_null_pred);
            result = or_pred;
        } else {
            result = is_null_pred;
        }
        predicate = result;
    }

    return Status::OK();
}

void ColumnReader::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                     bool case_sensitive, std::vector<int32_t>& pos) {
    DCHECK(field.type.type == LogicalType::TYPE_STRUCT);

    // build tmp mapping for ParquetField
    std::unordered_map<std::string, size_t> field_name_2_pos;
    for (size_t i = 0; i < field.children.size(); i++) {
        const std::string format_field_name =
                case_sensitive ? field.children[i].name : boost::algorithm::to_lower_copy(field.children[i].name);
        field_name_2_pos.emplace(format_field_name, i);
    }

    for (size_t i = 0; i < col_type.children.size(); i++) {
        const std::string formatted_subfield_name =
                case_sensitive ? col_type.field_names[i] : boost::algorithm::to_lower_copy(col_type.field_names[i]);

        auto it = field_name_2_pos.find(formatted_subfield_name);
        if (it == field_name_2_pos.end()) {
            pos[i] = -1;
            continue;
        }
        pos[i] = it->second;
    }
}

void ColumnReader::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                     bool case_sensitive,
                                                     const TIcebergSchemaField* iceberg_schema_field,
                                                     std::vector<int32_t>& pos,
                                                     std::vector<const TIcebergSchemaField*>& iceberg_schema_subfield) {
    // For Struct type with schema change, we need consider subfield not existed suitition.
    // When Iceberg add a new struct subfield, the original parquet file do not contains newly added subfield,
    std::unordered_map<std::string, const TIcebergSchemaField*> subfield_name_2_field_schema{};
    for (const auto& each : iceberg_schema_field->children) {
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
            // This suitition should not be happened, means table's struct subfield not existed in iceberg schema
            // Below code is defensive
            DCHECK(false) << "Struct subfield name: " + format_subfield_name + " not found in iceberg schema.";
            pos[i] = -1;
            iceberg_schema_subfield[i] = nullptr;
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        auto parquet_field_it = field_id_2_pos.find(field_id);
        if (parquet_field_it == field_id_2_pos.end()) {
            // Means newly added struct subfield not existed in original parquet file, we put nullptr
            // column reader in children_reader, we will append default value for this subfield later.
            pos[i] = -1;
            iceberg_schema_subfield[i] = nullptr;
            continue;
        }

        pos[i] = parquet_field_it->second;
        iceberg_schema_subfield[i] = iceberg_it->second;
    }
}

bool ColumnReader::_has_valid_subfield_column_reader(
        const std::map<std::string, std::unique_ptr<ColumnReader>>& children_readers) {
    for (const auto& pair : children_readers) {
        if (pair.second != nullptr) {
            return true;
        }
    }
    return false;
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            std::unique_ptr<ColumnReader>* output) {
    // We will only set a complex type in ParquetField
    if ((field->type.is_complex_type() || col_type.is_complex_type()) && (field->type.type != col_type.type)) {
        return Status::InternalError(
                strings::Substitute("ParquetField '$0' file's type $1 is different from table's type $2", field->name,
                                    logical_type_to_string(field->type.type), logical_type_to_string(col_type.type)));
    }
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[0], col_type.children[0], &child_reader));
        if (child_reader != nullptr) {
            std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
            RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0]), col_type.children[0], &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[1]), col_type.children[1], &value_reader));
        }

        if (key_reader != nullptr || value_reader != nullptr) {
            std::unique_ptr<MapColumnReader> reader(new MapColumnReader());
            RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, subfield_pos);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }
            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(
                    ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i], &child_reader));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        // maybe struct subfield ColumnReader is null
        if (_has_valid_subfield_column_reader(children_readers)) {
            std::unique_ptr<StructColumnReader> reader(new StructColumnReader());
            RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            const TIcebergSchemaField* iceberg_schema_field, std::unique_ptr<ColumnReader>* output) {
    // We will only set a complex type in ParquetField
    if ((field->type.is_complex_type() || col_type.is_complex_type()) && (field->type.type != col_type.type)) {
        return Status::InternalError(strings::Substitute("ParquetField's type $0 is different from table's type $1",
                                                         field->type.type, col_type.type));
    }
    DCHECK(iceberg_schema_field != nullptr);
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        const TIcebergSchemaField* element_schema = &iceberg_schema_field->children[0];
        RETURN_IF_ERROR(
                ColumnReader::create(opts, &field->children[0], col_type.children[0], element_schema, &child_reader));
        if (child_reader != nullptr) {
            std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
            RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        const TIcebergSchemaField* key_iceberg_schema = &iceberg_schema_field->children[0];
        const TIcebergSchemaField* value_iceberg_schema = &iceberg_schema_field->children[1];

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0]), col_type.children[0], key_iceberg_schema,
                                                 &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[1]), col_type.children[1],
                                                 value_iceberg_schema, &value_reader));
        }

        if (key_reader != nullptr || value_reader != nullptr) {
            std::unique_ptr<MapColumnReader> reader(new MapColumnReader());
            RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        std::vector<const TIcebergSchemaField*> iceberg_schema_subfield(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, iceberg_schema_field, subfield_pos,
                                          iceberg_schema_subfield);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }

            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                 iceberg_schema_subfield[i], &child_reader));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        // maybe struct subfield ColumnReader is null
        if (_has_valid_subfield_column_reader(children_readers)) {
            std::unique_ptr<StructColumnReader> reader(new StructColumnReader());
            RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
            *output = std::move(reader);
        } else {
            *output = nullptr;
        }
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
