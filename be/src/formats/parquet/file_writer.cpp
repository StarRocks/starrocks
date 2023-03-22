
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

#include "formats/parquet/file_writer.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks::parquet {

ParquetOutputStream::ParquetOutputStream(std::unique_ptr<starrocks::WritableFile> wfile) : _wfile(std::move(wfile)) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    arrow::Status st = ParquetOutputStream::Close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
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

    if (_header_state == INITED) {
        _header_state = CACHED;
    } else {
        if (_header_state == CACHED) {
            _header_state = WRITEN;
            Status st = _wfile->append(Slice("PAR1"));
            if (!st.ok()) {
                return arrow::Status::IOError(st.to_string());
            }
        }
        Status st = _wfile->append(Slice(ch, nbytes));
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string());
        }
    }

    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    if (_header_state == CACHED) {
        return 4;
    } else {
        return _wfile->size();
    }
}

arrow::Status ParquetOutputStream::Close() {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _wfile->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
        return arrow::Status::IOError(st.to_string());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

void ParquetBuildHelper::build_file_data_type(::parquet::Type::type& parquet_data_type,
                                              const LogicalType& column_data_type) {
    switch (column_data_type) {
    case TYPE_BOOLEAN: {
        parquet_data_type = ::parquet::Type::BOOLEAN;
        break;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        parquet_data_type = ::parquet::Type::INT32;
        break;
    }
    case TYPE_BIGINT:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        parquet_data_type = ::parquet::Type::INT64;
        break;
    }
    case TYPE_LARGEINT: {
        parquet_data_type = ::parquet::Type::INT96;
        break;
    }
    case TYPE_FLOAT: {
        parquet_data_type = ::parquet::Type::FLOAT;
        break;
    }
    case TYPE_DOUBLE: {
        parquet_data_type = ::parquet::Type::DOUBLE;
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_DECIMAL:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMALV2: {
        parquet_data_type = ::parquet::Type::BYTE_ARRAY;
        break;
    }
    default:
        parquet_data_type = ::parquet::Type::UNDEFINED;
    }
}

void ParquetBuildHelper::build_parquet_repetition_type(::parquet::Repetition::type& parquet_repetition_type,
                                                       const bool is_nullable) {
    parquet_repetition_type = is_nullable ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED;
}

void ParquetBuildHelper::build_compression_type(::parquet::WriterProperties::Builder& builder,
                                                const TCompressionType::type& compression_type) {
    switch (compression_type) {
    case TCompressionType::SNAPPY: {
        builder.compression(::parquet::Compression::SNAPPY);
        break;
    }
    case TCompressionType::GZIP: {
        builder.compression(::parquet::Compression::GZIP);
        break;
    }
    case TCompressionType::BROTLI: {
        builder.compression(::parquet::Compression::BROTLI);
        break;
    }
    case TCompressionType::ZSTD: {
        builder.compression(::parquet::Compression::ZSTD);
        break;
    }
    case TCompressionType::LZ4: {
        builder.compression(::parquet::Compression::LZ4);
        break;
    }
    case TCompressionType::LZO: {
        builder.compression(::parquet::Compression::LZO);
        break;
    }
    case TCompressionType::BZIP2: {
        builder.compression(::parquet::Compression::BZ2);
        break;
    }
    default:
        builder.compression(::parquet::Compression::UNCOMPRESSED);
    }
}

FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs)
        : _properties(std::move(properties)), _schema(std::move(schema)), _output_expr_ctxs(output_expr_ctxs) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
    _buffered_values_estimate.reserve(_schema->field_count());
}

Status FileWriterBase::init() {
    _writer = ::parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

::parquet::RowGroupWriter* FileWriterBase::_get_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _writer->AppendBufferedRowGroup();
    }
    return _rg_writer;
}

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                                         \
    ::parquet::RowGroupWriter* rg_writer = _get_rg_writer();                                                      \
    ::parquet::WRITER* col_writer = static_cast<::parquet::WRITER*>(rg_writer->column(i));                        \
    col_writer->WriteBatch(                                                                                       \
            num_rows, nullable ? def_level.data() : nullptr, nullptr,                                             \
            reinterpret_cast<const NATIVE_TYPE*>(down_cast<const COLUMN_TYPE*>(data_column)->get_data().data())); \
    _buffered_values_estimate[i] = col_writer->EstimatedBufferedValueBytes();

#define DISPATCH_PARQUET_STRING_WRITER()                                                                     \
    ::parquet::RowGroupWriter* rg_writer = _get_rg_writer();                                                 \
    ::parquet::ByteArrayWriter* col_writer = static_cast<::parquet::ByteArrayWriter*>(rg_writer->column(i)); \
    if (nullable) {                                                                                          \
        LOG(WARNING) << "nullable string writer";                                                            \
        ::parquet::ByteArray value;                                                                          \
        const BinaryColumn* binary_col = down_cast<const BinaryColumn*>(data_column);                        \
        for (size_t i = 0; i < num_rows; i++) {                                                              \
            value.ptr = reinterpret_cast<const uint8_t*>(binary_col->get_slice(i).get_data());               \
            value.len = binary_col->get_slice(i).get_size();                                                 \
            col_writer->WriteBatch(1, &def_level[i], nullptr, &value);                                       \
        }                                                                                                    \
    } else {                                                                                                 \
        ::parquet::ByteArray value;                                                                          \
        const BinaryColumn* binary_col = down_cast<const BinaryColumn*>(data_column);                        \
        for (size_t i = 0; i < num_rows; i++) {                                                              \
            value.ptr = reinterpret_cast<const uint8_t*>(binary_col->get_slice(i).get_data());               \
            value.len = binary_col->get_slice(i).get_size();                                                 \
            col_writer->WriteBatch(1, nullptr, nullptr, &value);                                             \
        }                                                                                                    \
    }                                                                                                        \
    _buffered_values_estimate[i] = col_writer->EstimatedBufferedValueBytes();

Status FileWriterBase::write(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    Columns result_columns;
    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk));
        //column = _output_expr_ctxs[i]->root()->type().type == TYPE_TIME
        //         ? vectorized::ColumnHelper::convert_time_column_from_double_to_str(column)
        //         : column;
        result_columns.emplace_back(std::move(column));
    }

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < result_columns.size(); i++) {
        // auto &col = chunk->get_column_by_index(i);
        auto& col = result_columns[i];
        bool nullable = col->is_nullable();
        auto null_column = nullable && down_cast<starrocks::NullableColumn*>(col.get())->has_null()
                                   ? down_cast<starrocks::NullableColumn*>(col.get())->null_column()
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
            DISPATCH_PARQUET_NUMERIC_WRITER(BoolWriter, starrocks::BooleanColumn, bool)
            break;
        }
        case TYPE_INT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int32Writer, starrocks::Int32Column, int32_t)
            break;
        }
        case TYPE_BIGINT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(Int64Writer, starrocks::Int64Column, int64_t)
            break;
        }
        case TYPE_FLOAT: {
            DISPATCH_PARQUET_NUMERIC_WRITER(FloatWriter, starrocks::FloatColumn, float)
            break;
        }
        case TYPE_DOUBLE: {
            DISPATCH_PARQUET_NUMERIC_WRITER(DoubleWriter, starrocks::DoubleColumn, double)
            break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            DISPATCH_PARQUET_STRING_WRITER()
            break;
        }
        default: {
            //TODO: support other types
            return Status::InvalidArgument("Unsupported type");
        }
        }
    }

    if (_get_current_rg_written_bytes() > _max_row_group_size) {
        _flush_row_group();
    }

    return Status::OK();
}

// The current row group written bytes = total_bytes_written + total_compressed_bytes + estimated_bytes.
// total_bytes_written: total bytes written by the page writer
// total_compressed_types: total bytes still compressed but not written
// estimated_bytes: estimated size of all column chunk uncompressed values that are not written to a page yet. it
// mainly includes value buffer size and repetition buffer size and definition buffer value for each column.
std::size_t FileWriterBase::_get_current_rg_written_bytes() const {
    if (_rg_writer == nullptr) {
        return 0;
    }

    auto estimated_bytes = std::accumulate(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);

    return _rg_writer->total_bytes_written() + _rg_writer->total_compressed_bytes() + estimated_bytes;
}

std::size_t FileWriterBase::file_size() const {
    DCHECK(_outstream != nullptr);
    return _outstream->Tell().MoveValueUnsafe() + _get_current_rg_written_bytes();
}

Status FileWriterBase::split_offsets(std::vector<int64_t>& splitOffsets) const {
    if (_file_metadata == nullptr) {
        LOG(WARNING) << "file metadata null";
        return Status::InternalError("Get split offsets while the file metadata is null");
    }
    for (int i = 0; i < _file_metadata->num_row_groups(); i++) {
        auto first_column_meta = _file_metadata->RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset
                                       ? dict_page_offset
                                       : first_data_page_offset;
        splitOffsets.emplace_back(split_offset);
    }
    return Status::OK();
}

void SyncFileWriter::_flush_row_group() {
    _rg_writer->Close();
    _rg_writer = nullptr;
    std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
}

Status SyncFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }

    _writer->Close();
    _rg_writer = nullptr;
    auto st = _outstream->Close();
    if (st != ::arrow::Status::OK()) {
        return Status::InternalError("Close file failed!");
    }

    _closed = true;
    return Status::OK();
}

AsyncFileWriter::AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_name,
                                 std::string& file_dir, std::shared_ptr<::parquet::WriterProperties> properties,
                                 std::shared_ptr<::parquet::schema::GroupNode> schema,
                                 const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                                 RuntimeProfile* parent_profile)
        : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs),
          _file_name(file_name),
          _file_dir(file_dir),
          _executor_pool(executor_pool),
          _parent_profile(parent_profile) {
    _io_timer = ADD_TIMER(_parent_profile, "FileWriterIoTimer");
}

void AsyncFileWriter::_flush_row_group() {
    {
        auto lock = std::unique_lock(_m);
        _rg_writer_closing = true;
    }
    bool ret = _executor_pool->try_offer([&]() {
        SCOPED_TIMER(_io_timer);
        _rg_writer->Close();
        _rg_writer = nullptr;
        std::fill(_buffered_values_estimate.begin(), _buffered_values_estimate.end(), 0);
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
            lock.unlock();
            _cv.notify_one();
        }
    });
    if (!ret) {
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
            lock.unlock();
            _cv.notify_one();
        }
    }
}

Status AsyncFileWriter::close(RuntimeState* state,
                              std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> cb) {
    bool ret = _executor_pool->try_offer([&, state, cb]() {
        SCOPED_TIMER(_io_timer);
        {
            auto lock = std::unique_lock(_m);
            _cv.wait(lock, [&] { return !_rg_writer_closing; });
        }
        _writer->Close();
        _rg_writer = nullptr;
        _file_metadata = _writer->metadata();
        auto st = _outstream->Close();
        if (cb != nullptr) {
            cb(this, state);
        }
        _closed.store(true);
    });
    if (ret) {
        return Status::OK();
    } else {
        return Status::InternalError("Submit close file error");
    }
}

} // namespace starrocks::parquet