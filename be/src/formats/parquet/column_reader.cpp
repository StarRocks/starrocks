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

#include <boost/algorithm/string.hpp>
#include <cstddef>

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/stored_column_reader.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::parquet {

template <typename TOffset, typename TIsNull>
static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, TOffset* offsets, TIsNull* is_nulls, size_t* num_offsets,
                              bool* has_null) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when def_level is less than immediate_repeated_ancestor_def_level, it means that level
        // will affect its ancestor.
        // when rep_level is greater than max_rep_level, this means that level affects its
        // descendants.
        // So we can skip this levels
        if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
            rep_levels[i] > level_info.max_rep_level) {
            continue;
        }
        if (rep_levels[i] == level_info.max_rep_level) {
            offsets[offset_pos]++;
            continue;
        }

        // Start for a new row
        offset_pos++;
        offsets[offset_pos] = offsets[offset_pos - 1];
        if (def_levels[i] >= level_info.max_def_level) {
            offsets[offset_pos]++;
        }

        // when def_level equals with max_def_level, this is a non null element or a required element
        // when def_level equals with (max_def_level - 1), this indicates an empty array
        // when def_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
            *has_null = true;
        }
    }
    *num_offsets = offset_pos;
}

class ScalarColumnReader : public ColumnReader {
public:
    explicit ScalarColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~ScalarColumnReader() override = default;

    Status init(const ParquetField* field, const TypeDescriptor& col_type,
                const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, col_type, _opts.timezone, &converter));
        return StoredColumnReader::create(_opts, field, chunk_metadata, &_reader);
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        DCHECK(_field->is_nullable ? dst->is_nullable() : true);
        if (!converter->need_convert) {
            return _reader->read_records(num_records, content_type, dst);
        } else {
            auto column = converter->create_src_column();

            Status status = _reader->read_records(num_records, content_type, column.get());
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            {
                SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                RETURN_IF_ERROR(converter->convert(column, dst));
            }

            return Status::OK();
        }
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    Status get_dict_values(Column* column) override { return _reader->get_dict_values(column); }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                           Column* column) override {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    void set_need_parse_levels(bool need_parse_levels) override { _reader->set_need_parse_levels(need_parse_levels); }

private:
    const ColumnReaderOptions& _opts;

    std::unique_ptr<StoredColumnReader> _reader;
    const ParquetField* _field = nullptr;
};

class ListColumnReader : public ColumnReader {
public:
    explicit ListColumnReader(const ColumnReaderOptions& opts) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        ArrayColumn* array_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_array());
            array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_array());
            DCHECK(!_field->is_nullable);
            array_column = down_cast<ArrayColumn*>(dst);
        }
        auto* child_column = array_column->elements_column().get();
        auto st = _element_reader->prepare_batch(num_records, content_type, child_column);

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

        auto& offsets = array_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return st;
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        _element_reader->set_need_parse_levels(need_parse_levels);
    }

private:
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

class MapColumnReader : public ColumnReader {
public:
    explicit MapColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~MapColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> key_reader,
                std::unique_ptr<ColumnReader> value_reader) {
        _field = field;
        _key_reader = std::move(key_reader);
        _value_reader = std::move(value_reader);

        // Check must has one valid column reader
        if (_key_reader == nullptr && _value_reader == nullptr) {
            return Status::InternalError("No avaliable parquet subfield column reader in MapColumn");
        }

        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        MapColumn* map_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_map());
            map_column = down_cast<MapColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_map());
            DCHECK(!_field->is_nullable);
            map_column = down_cast<MapColumn*>(dst);
        }
        auto* key_column = map_column->keys_column().get();
        auto* value_column = map_column->values_column().get();
        Status st;

        // TODO(SmithCruise) Ugly code, it's a temporary solution,
        //  to reset late materialization's rows_to_skip before read each subfield column
        size_t origin_next_row = _opts.context->next_row;
        size_t origin_rows_to_skip = _opts.context->rows_to_skip;

        if (_key_reader != nullptr) {
            st = _key_reader->prepare_batch(num_records, content_type, key_column);
            if (!st.ok() && !st.is_end_of_file()) {
                return st;
            }
        }

        if (_value_reader != nullptr) {
            // do reset
            _opts.context->next_row = origin_next_row;
            _opts.context->rows_to_skip = origin_rows_to_skip;

            st = _value_reader->prepare_batch(num_records, content_type, value_column);
            if (!st.ok() && !st.is_end_of_file()) {
                return st;
            }
        }

        // if neither key_reader not value_reader is nullptr , check the value_column size is the same with key_column
        DCHECK((_key_reader == nullptr) || (_value_reader == nullptr) || (value_column->size() == key_column->size()));

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;

        if (_key_reader != nullptr) {
            _key_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }

        auto& offsets = map_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;

        // ParquetFiled Map -> Map<Struct<key,value>>
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        // fill with default
        if (_key_reader == nullptr) {
            key_column->append_default(offsets.back());
        }
        if (_value_reader == nullptr) {
            value_column->append_default(offsets.back());
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return st;
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // check _value_reader
        if (_key_reader != nullptr) {
            _key_reader->get_levels(def_levels, rep_levels, num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(def_levels, rep_levels, num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        if (_key_reader != nullptr) {
            _key_reader->set_need_parse_levels(need_parse_levels);
        }

        if (_value_reader != nullptr) {
            _value_reader->set_need_parse_levels(need_parse_levels);
        }
    }

private:
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _key_reader;
    std::unique_ptr<ColumnReader> _value_reader;
    const ColumnReaderOptions& _opts;
};

class StructColumnReader : public ColumnReader {
public:
    explicit StructColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~StructColumnReader() override = default;

    Status init(const ParquetField* field, std::vector<std::unique_ptr<ColumnReader>>&& child_readers) {
        _field = field;
        _child_readers = std::move(child_readers);

        if (_child_readers.empty()) {
            return Status::InternalError("No avaliable parquet subfield column reader in StructColumn");
        }

        for (const auto& child_reader : _child_readers) {
            if (child_reader != nullptr) {
                _def_rep_level_child_reader = &child_reader;
                return Status::OK();
            }
        }

        return Status::InternalError("No existed parquet subfield column reader in StructColumn");
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        StructColumn* struct_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_struct());
            struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column = down_cast<StructColumn*>(dst);
        }

        Columns fields_column = struct_column->fields_column();

        DCHECK_EQ(fields_column.size(), _child_readers.size());

        // TODO(SmithCruise) Ugly code, it's a temporary solution,
        //  to reset late materialization's rows_to_skip before read each subfield column
        size_t origin_next_row = _opts.context->next_row;
        size_t origin_rows_to_skip = _opts.context->rows_to_skip;

        // Fill data for non-nullptr subfield column reader
        for (size_t i = 0; i < fields_column.size(); i++) {
            Column* child_column = fields_column[i].get();
            if (_child_readers[i] != nullptr) {
                _opts.context->next_row = origin_next_row;
                _opts.context->rows_to_skip = origin_rows_to_skip;
                RETURN_IF_ERROR(_child_readers[i]->prepare_batch(num_records, content_type, child_column));
            }
        }

        // Append default value for not selected subfield
        for (size_t i = 0; i < fields_column.size(); i++) {
            Column* child_column = fields_column[i].get();
            if (_child_readers[i] == nullptr) {
                child_column->append_default(*num_records);
            }
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            size_t row_nums = fields_column[0]->size();
            NullColumn null_column(row_nums, 0);
            auto& is_nulls = null_column.get_data();
            bool has_null = false;
            _handle_null_rows(is_nulls.data(), &has_null, row_nums);

            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }
        return Status::OK();
    }

    Status finish_batch() override { return Status::OK(); }

    // get_levels functions only called by complex type
    // If parent is a struct type, only def_levels has value.
    // If parent is list or map type, def_levels & rep_levels both have value.
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        for (const auto& reader : _child_readers) {
            // Considering not existed subfield, we will not create its ColumnReader
            // So we should pick up the first existed subfield column reader
            if (reader != nullptr) {
                reader->get_levels(def_levels, rep_levels, num_levels);
                return;
            }
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        for (const auto& reader : _child_readers) {
            if (reader != nullptr) {
                reader->set_need_parse_levels(need_parse_levels);
            }
        }
    }

private:
    void _handle_null_rows(uint8_t* is_nulls, bool* has_null, size_t num_rows) {
        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        (*_def_rep_level_child_reader)->get_levels(&def_levels, &rep_levels, &num_levels);

        if (def_levels == nullptr) {
            // If subfields are required, def_levels is nullptr
            *has_null = false;
            return;
        }

        LevelInfo level_info = _field->level_info;

        if (rep_levels != nullptr) {
            // It's a RepeatedStoredColumnReader
            size_t rows = 0;
            for (size_t i = 0; i < num_levels; i++) {
                if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
                    rep_levels[i] > level_info.max_rep_level) {
                    continue;
                }

                // Start for a new row
                if (def_levels[i] >= level_info.max_def_level) {
                    is_nulls[rows] = 0;
                } else {
                    is_nulls[rows] = 1;
                    *has_null = true;
                }
                rows++;
            }
            DCHECK_EQ(num_rows, rows);
        } else {
            // For OptionalStoredColumnReader, num_levels is equal to num_rows
            DCHECK(num_rows == num_levels);
            for (size_t i = 0; i < num_levels; i++) {
                if (def_levels[i] >= level_info.max_def_level) {
                    is_nulls[i] = 0;
                } else {
                    is_nulls[i] = 1;
                    *has_null = true;
                }
            }
        }
    }

    // _field is generated by parquet format, so it's child order may different from _child_readers.
    const ParquetField* _field = nullptr;
    // _children_readers order is the same as TypeDescriptor children order.
    std::vector<std::unique_ptr<ColumnReader>> _child_readers;
    // First non-nullptr child ColumnReader, used to get def & rep levels
    const std::unique_ptr<ColumnReader>* _def_rep_level_child_reader = nullptr;
    const ColumnReaderOptions& _opts;
};

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
            LOG(WARNING) << "Struct subfield name: " + formatted_subfield_name + " not found.";
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
            LOG(INFO) << "Struct subfield name: " + format_subfield_name + " not found in ParquetField.";
            pos[i] = -1;
            iceberg_schema_subfield[i] = nullptr;
            continue;
        }

        pos[i] = parquet_field_it->second;
        iceberg_schema_subfield[i] = iceberg_it->second;
    }
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            std::unique_ptr<ColumnReader>* output) {
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[0], col_type.children[0], &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;
        // ParquetFiled Map -> Map<Struct<key,value>>
        DCHECK(field->children[0].type.is_struct_type());
        DCHECK(field->children[0].children.size() == 2);

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(
                    ColumnReader::create(opts, &(field->children[0].children[0]), col_type.children[0], &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(
                    ColumnReader::create(opts, &(field->children[0].children[1]), col_type.children[1], &value_reader));
        }

        std::unique_ptr<MapColumnReader> reader(new MapColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, subfield_pos);

        std::vector<std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace_back nullptr
                children_readers.emplace_back(nullptr);
                continue;
            }
            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(
                    ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i], &child_reader));
            children_readers.emplace_back(std::move(child_reader));
        }

        std::unique_ptr<StructColumnReader> reader(new StructColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
        *output = std::move(reader);
        return Status::OK();
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            const TIcebergSchemaField* iceberg_schema_field, std::unique_ptr<ColumnReader>* output) {
    DCHECK(iceberg_schema_field != nullptr);
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        const TIcebergSchemaField* element_schema = &iceberg_schema_field->children[0];
        RETURN_IF_ERROR(
                ColumnReader::create(opts, &field->children[0], col_type.children[0], element_schema, &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;
        // ParquetFiled Map -> Map<Struct<key,value>>
        DCHECK(field->children[0].type.is_struct_type());
        DCHECK(field->children[0].children.size() == 2);

        const TIcebergSchemaField* key_iceberg_schema = &iceberg_schema_field->children[0];
        const TIcebergSchemaField* value_iceberg_schema = &iceberg_schema_field->children[1];

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0].children[0]), col_type.children[0],
                                                 key_iceberg_schema, &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0].children[1]), col_type.children[1],
                                                 value_iceberg_schema, &value_reader));
        }

        std::unique_ptr<MapColumnReader> reader(new MapColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        std::vector<const TIcebergSchemaField*> iceberg_schema_subfield(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, iceberg_schema_field, subfield_pos,
                                          iceberg_schema_subfield);

        std::vector<std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace_back nullptr
                children_readers.emplace_back(nullptr);
                continue;
            }

            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                 iceberg_schema_subfield[i], &child_reader));
            children_readers.emplace_back(std::move(child_reader));
        }

        std::unique_ptr<StructColumnReader> reader(new StructColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
        *output = std::move(reader);
        return Status::OK();
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
