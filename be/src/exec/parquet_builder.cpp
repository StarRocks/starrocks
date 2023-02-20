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
    _buffered_values_estimate.reserve(_schema->field_count());
    _file_writer = ::parquet::ParquetFileWriter::Open(_output_stream, _schema, _properties);
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
        ::parquet::Repetition::type parquet_repetition_type;
        ::parquet::Type::type parquet_data_type;
        auto column_expr = _output_expr_ctxs[i]->root();
        ParquetBuildHelper::build_file_data_type(parquet_data_type, column_expr->type().type);
        ParquetBuildHelper::build_parquet_repetition_type(parquet_repetition_type, column_expr->is_nullable());
        ::parquet::schema::NodePtr nodePtr = ::parquet::schema::PrimitiveNode::Make(
                file_column_names[i], parquet_repetition_type, parquet_data_type);
        fields.push_back(nodePtr);
    }

    _schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));
    return Status::OK();
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
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_DECIMAL:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
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
    parquet::WRITER* col_writer = static_cast<parquet::WRITER*>(_rg_writer->column(i));                           \
    col_writer->WriteBatch(                                                                                       \
            num_rows, nullable ? def_level.data() : nullptr, nullptr,                                             \
            reinterpret_cast<const NATIVE_TYPE*>(down_cast<const COLUMN_TYPE*>(data_column)->get_data().data())); \
    _buffered_values_estimate[i] = col_writer->EstimatedBufferedValueBytes();

Status ParquetBuilder::add_chunk(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < chunk->num_columns(); i++) {
        const auto& col = chunk->get_column_by_index(i);
        bool nullable = col->is_nullable();
        auto null_column = nullable && down_cast<NullableColumn*>(col.get())->has_null()
                                   ? down_cast<NullableColumn*>(col.get())->null_column()
                                   : nullptr;
        const auto data_column = ColumnHelper::get_data_column(col.get());

        std::vector<int16_t> def_level(num_rows, 1);
        if (null_column != nullptr) {
            auto nulls = null_column->get_data();
            for (size_t j = 0; j < num_rows; j++) {
                def_level[j] = nulls[j] == 0;
            }
        }

        const auto type = _output_expr_ctxs[i]->root()->type().type;
        switch (type) {
        case TYPE_BOOLEAN: {
            DISPATCH_PARQUET_NUMERIC_WRITER(BoolWriter, BooleanColumn, bool)
            break;
        }
        case TYPE_INT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int32Writer, Int32Column, int32_t)
            break;
        }
        case TYPE_BIGINT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int64Writer, Int64Column, int64_t)
            break;
        }
        case TYPE_FLOAT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(FloatWriter, FloatColumn, float)
            break;
        }
        case TYPE_DOUBLE: {
            DISPATCH_PARQUET_NUMERIC_WRITER(DoubleWriter, DoubleColumn, double)
            break;
        }
        default: {
            return Status::InvalidArgument("Unsupported type");
        }
        }
    }

    _check_size();
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