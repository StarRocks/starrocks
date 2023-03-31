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

#include "parquet_builder.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"
#include "gutil/casts.h"
#include "column/array_column.h"
#include "column/struct_column.h"
#include "column/map_column.h"

namespace starrocks {

ParquetOutputStream::ParquetOutputStream(std::unique_ptr<WritableFile> writable_file)
        : _writable_file(std::move(writable_file)) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    arrow::Status st = ParquetOutputStream::Close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed, err msg: " << st.ToString();
    }
}

arrow::Status ParquetOutputStream::Write(const std::shared_ptr<arrow::Buffer>& data) {
    arrow::Status st = Write(data->data(), data->size());
    if (!st.ok()) {
        LOG(WARNING) << "Failed to write data to output stream, err msg: " << st.message();
    }
    return st;
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::IOError("The output stream is closed but there are still inputs");
    }

    const char* ch = reinterpret_cast<const char*>(data);

    Slice slice(ch, nbytes);
    Status st = _writable_file->append(slice);
    if (!st.ok()) {
        return arrow::Status::IOError(st.to_string());
    }

    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    return _writable_file->size();
}

arrow::Status ParquetOutputStream::Close() {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _writable_file->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed, err msg: " << st;
        return arrow::Status::IOError(st.to_string());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

ParquetBuilder::ParquetBuilder(std::unique_ptr<WritableFile> writable_file,
                               const std::vector<ExprContext*>& output_expr_ctxs, const ParquetBuilderOptions& options,
                               const std::vector<std::string>& file_column_names)
        : _writable_file(std::move(writable_file)),
          _output_expr_ctxs(output_expr_ctxs),
          _row_group_max_size(options.row_group_max_size) {
    _init(options, file_column_names);
}

Status ParquetBuilder::_init(const ParquetBuilderOptions& options, const std::vector<std::string>& file_column_names) {
    _init_properties(options);
    Status st = _init_schema(file_column_names);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to init parquet schema: " << st;
    }

    _output_stream = std::make_shared<ParquetOutputStream>(std::move(_writable_file));
    _file_writer = ::parquet::ParquetFileWriter::Open(_output_stream, _schema, _properties);
    _buffered_values_estimate.resize(_file_writer->num_columns());
    std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
    return Status::OK();
}

void ParquetBuilder::_init_properties(const ParquetBuilderOptions& options) {
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    options.use_dict ? builder.enable_dictionary() : builder.disable_dictionary();
    ParquetBuildHelper::build_compression_type(builder, options.compression_type);
    _properties = builder.build();
}

Status ParquetBuilder::_init_schema(const std::vector<std::string>& file_column_names) {
    ::parquet::schema::NodeVector fields;
    for (int i = 0; i < _output_expr_ctxs.size(); i++) {
        auto column_expr = _output_expr_ctxs[i]->root();
        auto nodePtr = _init_schema_node(file_column_names[i], column_expr->type(),
                                         ::parquet::Repetition::OPTIONAL);
        if (nodePtr == nullptr) {
            return Status::InvalidArgument(fmt::format("Field {} of type {} (or its subtypes) is not supported",
                                                       file_column_names[i], type_to_string_v2(column_expr->type().type)));
        }
        fields.push_back(nodePtr);
    }

    _schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, fields));
    return Status::OK();
}

// Repetition of subtype in nested type is set by default now, due to type descriptor has no nullable field.
::parquet::schema::NodePtr ParquetBuilder::_init_schema_node(const std::string& name, const TypeDescriptor& type_desc, ::parquet::Repetition::type rep_type) {
    switch (type_desc.type) {
        case TYPE_STRUCT: {
            DCHECK(type_desc.children.size() == type_desc.field_names.size());
            int col_idx = ++_col_idx;
            ::parquet::schema::NodeVector fields;
            for (size_t i = 0; i < type_desc.children.size(); i++) {
                auto child = _init_schema_node(type_desc.field_names[i], type_desc.children[i], ::parquet::Repetition::OPTIONAL); // use optional as default
                fields.push_back(child);
            }
            return ::parquet::schema::GroupNode::Make(name, rep_type, fields, ::parquet::ConvertedType::NONE, col_idx);
        }
        case TYPE_ARRAY: {
            DCHECK(type_desc.children.size() == 1);
            int col_idx = ++_col_idx;
            auto element = _init_schema_node("element", type_desc.children[0], ::parquet::Repetition::OPTIONAL); // use optional as default
            auto list = ::parquet::schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {element});
            return ::parquet::schema::GroupNode::Make(name, rep_type, {list}, ::parquet::LogicalType::List(), col_idx);
        }
        case TYPE_MAP: {
            DCHECK(type_desc.children.size() == 2);
            int col_idx = ++_col_idx;
            auto key = _init_schema_node("key", type_desc.children[0], ::parquet::Repetition::REQUIRED);
            auto value = _init_schema_node("value", type_desc.children[1], ::parquet::Repetition::OPTIONAL);
            auto key_value = ::parquet::schema::GroupNode::Make("key_value", parquet::Repetition::REPEATED, {key, value});
            return ::parquet::schema::GroupNode::Make(name, rep_type, {key_value}, ::parquet::LogicalType::Map(), col_idx);
        }
        case TYPE_TINYINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(8, true), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_UNSIGNED_TINYINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(8, false), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_SMALLINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(16, true), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_UNSIGNED_SMALLINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(16, false), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_INT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(32, true), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_UNSIGNED_INT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(32, false), ::parquet::Type::INT32, -1, ++_col_idx);
        }
        case TYPE_BIGINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(64, true), ::parquet::Type::INT64, -1, ++_col_idx);
        }
        case TYPE_UNSIGNED_BIGINT: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Int(64, false), ::parquet::Type::INT64, -1, ++_col_idx);
        }
        case TYPE_VARCHAR: { // TODO: use len
            return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::String(), ::parquet::Type::BYTE_ARRAY, -1, ++_col_idx);
        }
        case TYPE_DATETIME: {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::unit::MILLIS), ::parquet::Type::INT64, -1, ++_col_idx);
        }
        case TYPE_DECIMAL:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128: {
            DCHECK(type_desc.is_decimal_type());
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale), parquet::Type::BYTE_ARRAY, -1, ++_col_idx);
        }
        default: {
            return {};
        }
    }
}

void ParquetBuildHelper::build_file_data_type(parquet::Type::type& parquet_data_type,
                                              const LogicalType& column_data_type) {
    switch (column_data_type) {
    case TYPE_BOOLEAN: {
        parquet_data_type = parquet::Type::BOOLEAN;
        break;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        parquet_data_type = parquet::Type::INT32;
        break;
    }
    case TYPE_BIGINT:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        parquet_data_type = parquet::Type::INT64;
        break;
    }
    case TYPE_LARGEINT: {
        parquet_data_type = parquet::Type::INT96;
        break;
    }
    case TYPE_FLOAT: {
        parquet_data_type = parquet::Type::FLOAT;
        break;
    }
    case TYPE_DOUBLE: {
        parquet_data_type = parquet::Type::DOUBLE;
        break;
    }
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_DECIMAL:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DECIMALV2: {
        parquet_data_type = parquet::Type::BYTE_ARRAY;
        break;
    }
    default:
        parquet_data_type = parquet::Type::UNDEFINED;
    }
}

void ParquetBuildHelper::build_parquet_repetition_type(parquet::Repetition::type& parquet_repetition_type,
                                                       const bool is_nullable) {
    parquet_repetition_type = is_nullable ? parquet::Repetition::OPTIONAL : parquet::Repetition::REQUIRED;
}

void ParquetBuildHelper::build_compression_type(parquet::WriterProperties::Builder& builder,
                                                const TCompressionType::type& compression_type) {
    switch (compression_type) {
    case TCompressionType::SNAPPY: {
        builder.compression(parquet::Compression::SNAPPY);
        break;
    }
    case TCompressionType::GZIP: {
        builder.compression(parquet::Compression::GZIP);
        break;
    }
    case TCompressionType::BROTLI: {
        builder.compression(parquet::Compression::BROTLI);
        break;
    }
    case TCompressionType::ZSTD: {
        builder.compression(parquet::Compression::ZSTD);
        break;
    }
    case TCompressionType::LZ4: {
        builder.compression(parquet::Compression::LZ4);
        break;
    }
    case TCompressionType::LZO: {
        builder.compression(parquet::Compression::LZO);
        break;
    }
    case TCompressionType::BZIP2: {
        builder.compression(parquet::Compression::BZ2);
        break;
    }
    default:
        builder.compression(parquet::Compression::UNCOMPRESSED);
    }
}

void ParquetBuilder::_generate_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _file_writer->AppendBufferedRowGroup();
    }
}

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                                         \
    ParquetBuilder::_generate_rg_writer();                                                                        \
    parquet::WRITER* col_writer = static_cast<parquet::WRITER*>(_rg_writer->column(_col_idx));                           \
    col_writer->WriteBatch(                                                                                       \
            num_rows, def_levels.data(), nullptr,                                             \
            reinterpret_cast<const NATIVE_TYPE*>(down_cast<const COLUMN_TYPE*>(data_column)->get_data().data())); \
    _buffered_values_estimate[_col_idx] = col_writer->EstimatedBufferedValueBytes(); \
    _col_idx++; \


Status ParquetBuilder::add_chunk(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    _col_idx = 0; // reset index on writing

    // TODO(letian-jiang): wrap into column chunk context
    auto chunk_size = chunk->num_rows();
    std::vector<int16_t> def_level(chunk_size, 0);
    std::vector<int16_t> rep_level(chunk_size, 0);
    std::vector<bool> is_null(chunk_size, false);
    std::map<int, int> mapping;
    // TODO(letian-jiang): add shortcut for flat schema
    for (size_t i = 0; i < chunk_size; i++) {
        mapping[i] = i;
    }

    for (size_t i = 0; i < chunk->num_columns(); i++) {
        auto col = chunk->get_column_by_index(i);
        auto type_desc = _output_expr_ctxs[i]->root()->type();
        auto ret = _add_column_chunk(type_desc, col, def_level, rep_level, 0, is_null, mapping);
        if (!ret.ok()) {
            return ret;
        }
    }

    _check_size();
    return Status::OK();
}

// TODO(letian-jiang): traverse type_desc and schema simultaneously
Status ParquetBuilder::_add_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                       const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                       int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    auto prev_level_size = def_level.size();
    DCHECK(rep_level.size() == prev_level_size);
    DCHECK(is_null.size() == prev_level_size);

    switch (type_desc.type) {
        case TYPE_STRUCT: {
            return _add_struct_column_chunk(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        }
        case TYPE_ARRAY: {
            return _add_array_column_chunk(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        }
        case TYPE_MAP: {
            return _add_map_column_chunk(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        }
        case TYPE_TINYINT:
            return _add_int_column_chunk<TYPE_TINYINT, parquet::Type::INT32>(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        case TYPE_SMALLINT:
            return _add_int_column_chunk<TYPE_SMALLINT, parquet::Type::INT32>(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        case TYPE_INT:
            return _add_int_column_chunk<TYPE_INT, parquet::Type::INT32>(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        case TYPE_BIGINT:
            return _add_int_column_chunk<TYPE_BIGINT, parquet::Type::INT64>(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        case TYPE_VARCHAR: {
            return _add_varchar_column_chunk(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        }
        case TYPE_DATETIME:
            return _add_datetime_column_chunk(type_desc, col, def_level, rep_level, max_rep_level, is_null, mapping);
        default: {
            return Status::InvalidArgument(fmt::format("Type {} is not supported", type_to_string_v2(type_desc.type)));
        }
    }
}

Status ParquetBuilder::_add_struct_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                              const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                              int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
    const auto nulls = null_column->get_data();
    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto struct_column = down_cast<StructColumn*>(data_column);

    std::vector<int16_t> cur_def_level;
    std::vector<int16_t> cur_rep_level;
    std::vector<bool> cur_is_null;
    std::map<int, int> cur_mapping;

    int j = 0; // pointer to next not-null value, increment upon non-empty array
    for (size_t i = 0; i < def_level.size(); i++) { // pointer to cur_def_level, cur_rep_level, cur_is_null
        if (is_null[i]) { // Null from parent column
            cur_def_level.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        int idx = mapping.at(i); // idx is pointer to column, nulls, and offsets
        if (nulls[idx]) { // Null in this column
            cur_def_level.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            cur_mapping[cur_def_level.size() - 1] = j++; // try
            continue;
        }
        // non-empty struct
        cur_def_level.push_back(def_level[i] + 1); // assume fields are optional
        cur_rep_level.push_back(rep_level[i]);
        cur_is_null.push_back(false);
        cur_mapping[cur_def_level.size() - 1] = j++;
    }

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        auto sub_col = struct_column->field_column(type_desc.field_names[i]);
        auto ret = _add_column_chunk(type_desc.children[i], sub_col, cur_def_level, cur_rep_level, max_rep_level,
                          cur_is_null, cur_mapping);
        if (!ret.ok()) {
            return ret;
        }
    }
    return Status::OK();
}


Status ParquetBuilder::_add_array_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                              const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                              int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
    const auto nulls = null_column->get_data();
    const auto array_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto elements = array_column->elements_column();
    const auto offsets = array_column->offsets_column()->get_data();

    std::vector<int16_t> cur_def_level;
    std::vector<int16_t> cur_rep_level;
    std::vector<bool> cur_is_null;
    std::map<int, int> cur_mapping;

    max_rep_level++;

    int j = 0; // pointer to next not-null value, increment upon non-empty array
    for (size_t i = 0; i < def_level.size(); i++) { // pointer to cur_def_level, cur_rep_level, cur_is_null
        if (is_null[i]) { // Null from parent column
            cur_def_level.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        int idx = mapping.at(i); // idx is pointer to column, nulls, and offsets
        if (nulls[idx]) { // Null in this column
            cur_def_level.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        if (offsets[idx + 1] == offsets[idx]) { // []
            cur_def_level.push_back(def_level[i] + 1);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        // non-empty array (e.g. [Null, ...])
        cur_def_level.push_back(def_level[i] + 2);
        cur_rep_level.push_back(rep_level[i]);
        cur_is_null.push_back(false);
        cur_mapping[cur_def_level.size() - 1] = j++;
        for (auto k = 1; k < offsets[idx + 1] - offsets[idx]; k++) {
            cur_def_level.push_back(def_level[idx] + 2);
            cur_rep_level.push_back(max_rep_level);
            cur_is_null.push_back(false);
            cur_mapping[cur_def_level.size() - 1] = j++;
        }
    }

    return _add_column_chunk(type_desc.children[0], elements, cur_def_level, cur_rep_level, max_rep_level, cur_is_null,
                      cur_mapping);
}

Status ParquetBuilder::_add_map_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                              const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                              int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
    const auto nulls = null_column->get_data();
    const auto map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto keys = map_column->keys_column();
    const auto values = map_column->values_column();
    const auto offsets = map_column->offsets_column()->get_data();

    std::vector<int16_t> cur_def_level_key;
    std::vector<int16_t> cur_def_level_value;
    std::vector<int16_t> cur_rep_level;
    std::vector<bool> cur_is_null;
    std::map<int, int> cur_mapping;

    max_rep_level++;

    int j = 0; // pointer to next not-null value, increment upon non-empty array
    for (size_t i = 0; i < def_level.size(); i++) { // pointer to cur_def_level, cur_rep_level, cur_is_null
        if (is_null[i]) { // Null from parent column
            cur_def_level_key.push_back(def_level[i]);
            cur_def_level_value.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        int idx = mapping.at(i); // idx is pointer to column, nulls, and offsets
        if (nulls[idx]) { // Null in this column
            cur_def_level_key.push_back(def_level[i]);
            cur_def_level_value.push_back(def_level[i]);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        if (offsets[idx + 1] == offsets[idx]) { // []
            cur_def_level_key.push_back(def_level[i]);
            cur_def_level_value.push_back(def_level[i] + 1);
            cur_is_null.push_back(true);
            cur_rep_level.push_back(rep_level[i]);
            continue;
        }
        // non-empty array (e.g. [Null, ...])
        cur_def_level_key.push_back(def_level[i] + 1);
        cur_def_level_value.push_back(def_level[i] + 2);
        cur_rep_level.push_back(rep_level[i]);
        cur_is_null.push_back(false);
        cur_mapping[cur_def_level_key.size() - 1] = j++;
        for (auto k = 1; k < offsets[idx + 1] - offsets[idx]; k++) {
            cur_def_level_key.push_back(def_level[i] + 1);
            cur_def_level_value.push_back(def_level[i] + 2);
            cur_rep_level.push_back(max_rep_level);
            cur_is_null.push_back(false);
            cur_mapping[cur_def_level_key.size() - 1] = j++;
        }
    }

    auto ret = _add_column_chunk(type_desc.children[0], keys, cur_def_level_key, cur_rep_level, max_rep_level, cur_is_null,
                      cur_mapping);
    if (!ret.ok()) {
        return ret;
    }
    return _add_column_chunk(type_desc.children[1], values, cur_def_level_value, cur_rep_level, max_rep_level,
                      cur_is_null, cur_mapping);
}

Status ParquetBuilder::_add_varchar_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                              const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                              int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_col = down_cast<const RunTimeColumnType<TYPE_VARCHAR>*>(data_column);
    auto vo = raw_col->get_offset();
    auto vb = raw_col->get_bytes();

    _generate_rg_writer();
    auto col_writer = static_cast<parquet::ByteArrayWriter*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    auto write = [&](int16_t def_level, int16_t rep_level, unsigned char* ptr, auto len) {
        parquet::ByteArray value;
        value.ptr = reinterpret_cast<const uint8_t*>(ptr);
        value.len = len;
        col_writer->WriteBatch(1, &def_level, &rep_level, &value);
    };

    for (size_t i = 0; i < def_level.size(); i++) {
        if (is_null[i]) {
            write(def_level[i], rep_level[i], nullptr, 0);
            continue;
        }
        auto idx = mapping.at(i);
        if (nulls != nullptr && nulls[idx]) {
            write(def_level[i], rep_level[i], nullptr, 0);
            continue;
        }
        write(def_level[i] + 1 /* increment if optional */, rep_level[i], vb.data() + vo[idx], vo[idx + 1] - vo[idx]);
    }

    _buffered_values_estimate[_col_idx] = col_writer->EstimatedBufferedValueBytes();
    _col_idx++;
    return Status::OK();
}

Status ParquetBuilder::_add_datetime_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                                 const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                                 int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_col = down_cast<const RunTimeColumnType<TYPE_DATETIME>*>(data_column)->get_data().data();

    _generate_rg_writer();
    auto col_writer = static_cast<parquet::Int64Writer*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    auto write = [&](int16_t def_level, int16_t rep_level, int64_t value) {
        col_writer->WriteBatch(1, &def_level, &rep_level, &value);
    };

    for (size_t i = 0; i < def_level.size(); i++) {
        if (is_null[i]) {
            write(def_level[i], rep_level[i], -1);
            continue;
        }
        auto idx = mapping.at(i);
        if (nulls != nullptr && nulls[idx]) {
            write(def_level[i], rep_level[i], -1);
            continue;
        }
        write(def_level[i] + 1 /* increment if optional */, rep_level[i], raw_col[idx].to_unix_second() * 1000); // seconds -> milliseconds
    }

    _buffered_values_estimate[_col_idx] = col_writer->EstimatedBufferedValueBytes();
    _col_idx++;
    return Status::OK();
}

template <LogicalType lt, ::parquet::Type::type pt>
Status ParquetBuilder::_add_int_column_chunk(const TypeDescriptor& type_desc, const ColumnPtr col,
                                                 const std::vector<int16_t>& def_level, const std::vector<int16_t>& rep_level,
                                                 int16_t max_rep_level, const std::vector<bool>& is_null, const std::map<int, int>& mapping) {
    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto raw_col = down_cast<RunTimeColumnType<lt>*>(data_column)->get_data().data();

    _generate_rg_writer();
    auto col_writer = static_cast<parquet::TypedColumnWriter<parquet::PhysicalType<pt>>*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    auto write = [&](int16_t def_level, int16_t rep_level, typename parquet::type_traits<pt>::value_type value) {
        col_writer->WriteBatch(1, &def_level, &rep_level, &value);
    };

    for (size_t i = 0; i < def_level.size(); i++) {
        if (is_null[i]) {
            write(def_level[i], rep_level[i], -1);
            continue;
        }
        auto idx = mapping.at(i);
        if (nulls != nullptr && nulls[idx]) {
            write(def_level[i], rep_level[i], -1);
            continue;
        }
        write(def_level[i] + 1 /* increment if optional */, rep_level[i], raw_col[idx]);
    }

    _buffered_values_estimate[_col_idx] = col_writer->EstimatedBufferedValueBytes();
    _col_idx++;
    return Status::OK();
}

// The current row group written bytes = total_bytes_written + total_compressed_bytes + estimated_bytes.
// total_bytes_written: total bytes written by the page writer
// total_compressed_types: total bytes still compressed but not written
// estimated_bytes: estimated size of all column chunk uncompressed values that are not written to a page yet. it
// mainly includes value buffer size and repetition buffer size and definition buffer value for each column.
size_t ParquetBuilder::_get_rg_written_bytes() {
    if (_rg_writer == nullptr) {
        return 0;
    }
    auto estimated_bytes = std::accumulate(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
    return _rg_writer->total_bytes_written() + _rg_writer->total_compressed_bytes() + estimated_bytes;
}

// TODO(stephen): we should use the average of each row bytes to calculate the remaining writable size.
void ParquetBuilder::_check_size() {
    if (ParquetBuilder::_get_rg_written_bytes() > _row_group_max_size) {
        _flush_row_group();
    }
}

void ParquetBuilder::_flush_row_group() {
    _rg_writer->Close();
    _total_row_group_writen_bytes = _output_stream->Tell().MoveValueUnsafe();
    _rg_writer = nullptr;
    std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
}

std::size_t ParquetBuilder::file_size() {
    DCHECK(_output_stream != nullptr);
    if (_rg_writer == nullptr) {
        return 0;
    }

    return _total_row_group_writen_bytes + _get_rg_written_bytes();
}

Status ParquetBuilder::finish() {
    if (_closed) {
        return Status::OK();
    }

    if (_rg_writer != nullptr) {
        _flush_row_group();
    }

    _file_writer->Close();
    auto st = _output_stream->Close();
    if (st != ::arrow::Status::OK()) {
        return Status::InternalError("Close file failed!");
    }

    _closed = true;
    return Status::OK();
}

} // namespace starrocks
