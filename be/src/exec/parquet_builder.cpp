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

#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/column_ref.h"

#include "column/chunk.h"
#include "column/column_helper.h"
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
    Write(data->data(), data->size());
    return arrow::Status::OK();
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::OK();
    }

    const char* ch = reinterpret_cast<const char*>(data);

    Slice slice(ch, nbytes);
    Status st = _writable_file->append(slice);
    if (!st.ok()) {
        return arrow::Status::IOError(st.to_string());
    }

    _cur_pos += nbytes;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    return _cur_pos;
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

std::size_t ParquetOutputStream::get_written_len() const {
    return _cur_pos;
}

ParquetBuilder::ParquetBuilder(std::unique_ptr<WritableFile> writable_file,
                               const std::vector<ExprContext*>& output_expr_ctxs,
                               const ParquetBuilderOptions& options)
        : _writable_file(std::move(writable_file)), _output_expr_ctxs(output_expr_ctxs),
           _row_group_max_size(options.row_group_max_size), _is_async(false) {
    init(options);
}

Status ParquetBuilder::init(const ParquetBuilderOptions& options) {
    _init_properties(options);
    Status st = _init_schema(options.parquet_schema);
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
    options.use_dictionary ? builder.enable_dictionary() : builder.disable_dictionary();
    ParquetBuildHelper::build_compression_type(builder, options.compression_type);
    _properties = builder.build();
}

Status ParquetBuilder::_init_schema(const TParquetSchema& schema) {
    ::parquet::schema::NodeVector fields;
    for (const auto& column : schema.columns) {
        ::parquet::Repetition::type parquet_repetition_type;
        ::parquet::Type::type parquet_data_type;
        ParquetBuildHelper::build_file_data_type(parquet_data_type, column.type);
        ParquetBuildHelper::build_repetition_type(parquet_repetition_type, column.repetition_type);
        ::parquet::schema::NodePtr nodePtr = ::parquet::schema::PrimitiveNode::Make(column.name,
                                                                                    parquet_repetition_type,
                                                                                    parquet_data_type);
        fields.push_back(nodePtr);
    }

    _schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));
    return Status::OK();
}

void ParquetBuildHelper::build_file_data_type(parquet::Type::type& parquet_data_type,
                                              const TParquetColumnType::type& column_data_type) {
    switch (column_data_type) {
        case TParquetColumnType::BOOLEAN: {
            parquet_data_type = parquet::Type::BOOLEAN;
            break;
        }
        case TParquetColumnType::INT32: {
            parquet_data_type = parquet::Type::INT32;
            break;
        }
        case TParquetColumnType::INT64: {
            parquet_data_type = parquet::Type::INT64;
            break;
        }
        case TParquetColumnType::INT96: {
            parquet_data_type = parquet::Type::INT96;
            break;
        }
        case TParquetColumnType::FLOAT: {
            parquet_data_type = parquet::Type::FLOAT;
            break;
        }
        case TParquetColumnType::DOUBLE: {
            parquet_data_type = parquet::Type::DOUBLE;
            break;
        }
        case TParquetColumnType::BYTE_ARRAY: {
            parquet_data_type = parquet::Type::BYTE_ARRAY;
            break;
        }

        default:
            parquet_data_type = parquet::Type::UNDEFINED;
    }
}

void ParquetBuildHelper::build_repetition_type(
        parquet::Repetition::type& parquet_repetition_type,
        const TParquetRepetitionType::type& column_repetition_type) {
    switch (column_repetition_type) {
        case TParquetRepetitionType::REQUIRED: {
            parquet_repetition_type = parquet::Repetition::REQUIRED;
            break;
        }
        case TParquetRepetitionType::REPEATED: {
            parquet_repetition_type = parquet::Repetition::REPEATED;
            break;
        }
        case TParquetRepetitionType::OPTIONAL: {
            parquet_repetition_type = parquet::Repetition::OPTIONAL;
            break;
        }
        default:
            parquet_repetition_type = parquet::Repetition::UNDEFINED;
    }
}

void ParquetBuildHelper::build_compression_type(
        parquet::WriterProperties::Builder& builder,
        const TParquetCompressionType::type& compression_type) {
    switch (compression_type) {
        case TParquetCompressionType::SNAPPY: {
            builder.compression(parquet::Compression::SNAPPY);
            break;
        }
        case TParquetCompressionType::GZIP: {
            builder.compression(parquet::Compression::GZIP);
            break;
        }
        case TParquetCompressionType::BROTLI: {
            builder.compression(parquet::Compression::BROTLI);
            break;
        }
        case TParquetCompressionType::ZSTD: {
            builder.compression(parquet::Compression::ZSTD);
            break;
        }
        case TParquetCompressionType::LZ4: {
            builder.compression(parquet::Compression::LZ4);
            break;
        }
        case TParquetCompressionType::LZO: {
            builder.compression(parquet::Compression::LZO);
            break;
        }
        case TParquetCompressionType::BZ2: {
            builder.compression(parquet::Compression::BZ2);
            break;
        }
        case TParquetCompressionType::UNCOMPRESSED: {
            builder.compression(parquet::Compression::UNCOMPRESSED);
            break;
        }
        default:
            builder.compression(parquet::Compression::UNCOMPRESSED);
    }
}

void ParquetBuilder::_generate_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _file_writer->AppendBufferedRowGroup();
        _cur_written_rows = 0;
    }
}

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                                     \
    ParquetBuilder::_generate_rg_writer();                                                                    \
    parquet::WRITER* col_writer = static_cast<parquet::WRITER*>(_rg_writer->column(i));                       \
    col_writer->WriteBatch(num_rows, nullable ? def_level.data() : nullptr, nullptr,                          \
        reinterpret_cast<const NATIVE_TYPE*>(down_cast<const COLUMN_TYPE*>(data_column)->get_data().data())); \
    _buffered_values_estimate[i] = col_writer->EstimatedBufferedValueBytes();                                 \

Status ParquetBuilder::add_chunk(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < chunk->num_columns(); i++) {
        auto& col = chunk->get_column_by_index(i);
        bool nullable = col->is_nullable();
        auto null_column = nullable && down_cast<NullableColumn*>(col.get())->has_null()
                           ? down_cast<NullableColumn*>(col.get())->null_column()
                           : nullptr;
        const auto data_column = ColumnHelper::get_data_column(col.get());

        std::vector<int16_t> def_level(num_rows);
        std::fill(def_level.begin(), def_level.end(), 1);
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

    _cur_written_rows += num_rows;
    _check_size();
    return Status::OK();
}

size_t ParquetBuilder::_get_rg_written_bytes() {
    if (_rg_writer == nullptr) {
        return 0;
    }
    int64_t estimated_bytes = 0;

    for (long i : _buffered_values_estimate) {
        estimated_bytes += i;
    }

    return _rg_writer->total_bytes_written() + _rg_writer->total_compressed_bytes() + estimated_bytes;
}

void ParquetBuilder::_check_size() {
    if (ParquetBuilder::_get_rg_written_bytes() > _row_group_max_size) {
        if (_is_async) {
            bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([&]() {
                _rg_writer_close();
            });
            if (!ret) {
                _rg_writer_closing.store(false);
            }
        } else {
            _rg_writer_close();
        }
    }
}

void ParquetBuilder::_rg_writer_close() {
    _rg_writer_closing.store(true);
    _rg_writer->Close();
    _total_row_group_writen_bytes = _output_stream->get_written_len();
    _rg_writer = nullptr;
    _rg_writer_closing.store(false);
    std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
}

std::size_t ParquetBuilder::file_size() {
    DCHECK(_output_stream != nullptr);
    if (_rg_writer_closing || _rg_writer == nullptr) {
        return 0;
    }

    return _total_row_group_writen_bytes + _get_rg_written_bytes();
}

Status ParquetBuilder::finish() {
    if (_closed) {
        return Status::OK();
    }

    if (_rg_writer != nullptr) {
        _rg_writer_close();
    }

    _file_writer->Close();
    auto st = _output_stream->Close();
    if (st != ::arrow::Status::OK()) {
        return Status::InternalError("Close file failed!");
    }

    _closed.store(true);
    return Status::OK();
}

}